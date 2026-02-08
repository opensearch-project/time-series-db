/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.hash.MurmurHash3;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.opensearch.tsdb.core.model.LabelConstants.LABEL_DELIMITER;
import static org.opensearch.tsdb.core.model.LabelConstants.EMPTY_STRING;
import static org.opensearch.tsdb.core.model.LabelConstants.SPACE_SEPARATOR;

/**
 * IndexedByteLabels implements Labels using a byte array encoding with an embedded offset header
 * that enables O(log n) binary search on label lookups with zero initialization cost.
 *
 * <h2>Encoding Format</h2>
 * The byte array is split into a HEADER section and a DATA section:
 * <pre>
 * ┌─────────────────────────── HEADER ───────────────────────────┐
 * │ [label_count: 2 bytes, little-endian, unsigned]              │
 * │ [offset_0:    2 bytes, little-endian, unsigned]              │
 * │ [offset_1:    2 bytes, little-endian, unsigned]              │
 * │ ...                                                          │
 * │ [offset_n-1:  2 bytes, little-endian, unsigned]              │
 * ├─────────────────────────── DATA ─────────────────────────────┤
 * │ [name1_bytes][DELIMITER][value1_bytes]                       │
 * │ [name2_bytes][DELIMITER][value2_bytes]                       │
 * │ ...                                                          │
 * └──────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h2>Header Protocol</h2>
 * <ul>
 *   <li><strong>label_count</strong> (bytes 0-1): Number of label key-value pairs, as a 2-byte
 *       unsigned little-endian integer. Max 65535 labels.</li>
 *   <li><strong>offset_i</strong> (bytes 2+2i to 3+2i): Byte offset of the i-th label entry,
 *       measured from the start of the DATA section. Each offset is a 2-byte unsigned
 *       little-endian integer. Max 65535-byte data section.</li>
 *   <li><strong>Header size</strong>: {@code 2 + (labelCount * 2)} bytes.</li>
 *   <li><strong>DATA section start</strong>: at byte position {@code headerSize}.</li>
 * </ul>
 *
 * <h2>DATA Section Entry Format</h2>
 * Each label entry uses a delimiter ({@link LabelConstants#LABEL_DELIMITER}, ':') to separate
 * name from value, eliminating the need for length-prefix bytes:
 * <pre>
 * [name_bytes][':'  (1 byte delimiter)][value_bytes]
 * </pre>
 * This saves 1 byte per label compared to the length-prefix format (which uses 2 bytes:
 * one for name_len, one for value_len). The name/value boundary is found by scanning for the
 * first delimiter byte. The entry's total length is known from the next offset (or end of data).
 *
 * <h2>Reading a label during binary search</h2>
 * <pre>{@code
 * int labelCount = (data[0] & 0xFF) | ((data[1] & 0xFF) << 8);
 * int headerSize = 2 + (labelCount * 2);
 * int relativeOffset = (data[2 + i * 2] & 0xFF) | ((data[3 + i * 2] & 0xFF) << 8);
 * int entryStart = headerSize + relativeOffset;
 * int entryEnd = (i < labelCount - 1) ? headerSize + nextOffset : data.length;
 * // scan from entryStart to find DELIMITER — name is before, value is after
 * }</pre>
 *
 * <h2>Empty Labels</h2>
 * Empty labels are encoded as a 2-byte header {@code [0x00, 0x00]} with no data section.
 *
 * <h2>Compatibility</h2>
 * <ul>
 *   <li>{@code stableHash()} and {@code equals()} convert to ByteLabels-compatible format
 *       internally so that labels with the same content produce identical hashes regardless
 *       of storage format.</li>
 *   <li>On-disk format is <strong>not compatible</strong> with ByteLabels — requires
 *       {@code BINARY_INDEXED_BYTESLABEL} index setting.</li>
 * </ul>
 */
public class IndexedByteLabels implements Labels {
    private final byte[] data;

    /** Cached label count, read from header bytes 0-1. Computed once in constructor. */
    private final int labelCount;

    /** Cached header size = 2 + (labelCount * 2). Computed once in constructor. */
    private final int headerSize;

    private long hash = Long.MIN_VALUE;

    /** Delimiter byte used to separate name from value within each label entry. */
    private static final byte DELIMITER_BYTE = (byte) LABEL_DELIMITER;

    /**
     * Base memory overhead for an IndexedByteLabels instance in bytes.
     * <p>Memory breakdown:
     * <ul>
     *   <li>Object header: 16 bytes (mark word + class pointer)</li>
     *   <li>Field: byte[] data reference: 8 bytes</li>
     *   <li>Field: int labelCount: 4 bytes</li>
     *   <li>Field: int headerSize: 4 bytes</li>
     *   <li>Field: long hash: 8 bytes</li>
     *   <li>byte[] array object header: 16 bytes (array metadata + length)</li>
     * </ul>
     * Total: 56 bytes + data.length
     * </p>
     */
    private static final long ESTIMATED_MEMORY_OVERHEAD = 56;

    private static final IndexedByteLabels EMPTY = new IndexedByteLabels(new byte[] { 0, 0 });

    /** ThreadLocal cache for TreeMap instances to reduce object allocation during label creation. */
    private static final ThreadLocal<TreeMap<String, String>> TREE_MAP_CACHE = ThreadLocal.withInitial(TreeMap::new);

    private IndexedByteLabels(byte[] data) {
        this.data = data;
        // Eagerly cache header fields — O(1) and data is final, so these never change.
        this.labelCount = (data[0] & 0xFF) | ((data[1] & 0xFF) << 8);
        this.headerSize = 2 + (labelCount * 2);
    }

    // ========== Header access helpers ==========

    /**
     * Returns the absolute position in the data array where label i's entry starts.
     * Uses cached headerSize — just one array read + add.
     */
    private int labelAbsolutePos(int i) {
        int relativeOffset = (data[2 + i * 2] & 0xFF) | ((data[3 + i * 2] & 0xFF) << 8);
        return headerSize + relativeOffset;
    }

    /**
     * Returns the end position (exclusive) of label i's entry.
     * For the last label, this is data.length. Otherwise, it's the start of label i+1.
     */
    private int labelEntryEnd(int i) {
        return (i < labelCount - 1) ? labelAbsolutePos(i + 1) : data.length;
    }

    /**
     * Finds the position of the delimiter byte within a label entry.
     * The delimiter separates the name from the value.
     *
     * @param entryStart start of the label entry (inclusive)
     * @param entryEnd end of the label entry (exclusive)
     * @return position of the delimiter byte
     */
    private int findDelimiter(int entryStart, int entryEnd) {
        for (int p = entryStart; p < entryEnd; p++) {
            if (data[p] == DELIMITER_BYTE) {
                return p;
            }
        }
        // Should never happen for well-formed data — every entry has a delimiter
        return entryEnd;
    }

    /**
     * Compares the name portion of a label entry with a target byte array,
     * scanning byte-by-byte until the delimiter or a mismatch is found.
     * This fuses the delimiter search with the comparison into a single pass,
     * avoiding a separate findDelimiter() call during binary search.
     *
     * @param entryStart start of the label entry (name begins here)
     * @param entryEnd end of the label entry (exclusive)
     * @param target the name bytes to compare against
     * @return negative if name < target, zero if equal, positive if name > target
     */
    private int compareNameInEntry(int entryStart, int entryEnd, byte[] target) {
        int pos = entryStart;
        int targetIdx = 0;

        while (pos < entryEnd && data[pos] != DELIMITER_BYTE && targetIdx < target.length) {
            int diff = (data[pos] & 0xFF) - (target[targetIdx] & 0xFF);
            if (diff != 0) return diff;
            pos++;
            targetIdx++;
        }

        // Check how both sides ended
        boolean nameEnded = (pos >= entryEnd || data[pos] == DELIMITER_BYTE);
        boolean targetEnded = (targetIdx >= target.length);

        if (nameEnded && targetEnded) return 0;  // exact match
        if (nameEnded) return -1;                 // name is shorter → name < target
        return 1;                                 // target is shorter → name > target
    }

    // ========== Factory methods ==========

    /**
     * Creates an IndexedByteLabels instance from an array of alternating name-value strings.
     *
     * @param labels an array where even indices are names and odd indices are values
     *               (e.g., "name1", "value1", "name2", "value2")
     * @return a new IndexedByteLabels instance with the given labels
     * @throws IllegalArgumentException if the array length is not even (unpaired labels)
     */
    public static IndexedByteLabels fromStrings(String... labels) {
        if (labels.length % 2 != 0) {
            throw new IllegalArgumentException(
                "Labels must be in pairs (key-value). Received " + labels.length + " labels: " + Arrays.toString(labels)
            );
        }

        TreeMap<String, String> sorted = TREE_MAP_CACHE.get();
        sorted.clear();
        for (int i = 0; i < labels.length; i += 2) {
            sorted.put(labels[i], labels[i + 1]);
        }

        return encodeLabels(sorted);
    }

    /**
     * Creates an IndexedByteLabels instance from a pre-sorted array of alternating name-value strings.
     *
     * @param labels a sorted array where even indices are names and odd indices are values
     * @return a new IndexedByteLabels instance with the given labels
     * @throws IllegalArgumentException if the array length is not even
     */
    public static IndexedByteLabels fromSortedStrings(String... labels) {
        if (labels.length % 2 != 0) {
            throw new IllegalArgumentException(
                "Labels must be in pairs (key-value). Received " + labels.length + " labels: " + Arrays.toString(labels)
            );
        }

        if (labels.length == 0) {
            return EMPTY;
        }

        TreeMap<String, String> sorted = new TreeMap<>();
        for (int i = 0; i < labels.length; i += 2) {
            String value = labels[i + 1];
            sorted.put(labels[i], value == null ? EMPTY_STRING : value);
        }

        return encodeLabels(sorted);
    }

    /**
     * Creates an IndexedByteLabels instance from a sorted array of key:value String pairs.
     *
     * @param keyValuePairs pairs to encode, delimited by {@link LabelConstants#LABEL_DELIMITER}
     * @return a new IndexedByteLabels instance with the given labels
     */
    public static IndexedByteLabels fromSortedKeyValuePairs(List<String> keyValuePairs) {
        if (keyValuePairs == null || keyValuePairs.isEmpty()) {
            return EMPTY;
        }

        String[] splitPairs = new String[keyValuePairs.size() * 2];
        for (int i = 0; i < keyValuePairs.size(); i++) {
            String pair = keyValuePairs.get(i);
            int delimiterIndex = pair.indexOf(LabelConstants.LABEL_DELIMITER);
            if (delimiterIndex <= 0) {
                throw new IllegalArgumentException("Invalid key value pair: " + pair);
            }
            splitPairs[i * 2] = pair.substring(0, delimiterIndex);
            splitPairs[i * 2 + 1] = pair.substring(delimiterIndex + 1);
        }

        return fromSortedStrings(splitPairs);
    }

    /**
     * Creates an IndexedByteLabels instance from a map of label names to values.
     *
     * @param labelMap a map containing label names as keys and label values as values
     * @return a new IndexedByteLabels instance with the given labels, sorted by name
     */
    public static IndexedByteLabels fromMap(Map<String, String> labelMap) {
        if (labelMap == null || labelMap.isEmpty()) {
            return EMPTY;
        }

        TreeMap<String, String> sorted = TREE_MAP_CACHE.get();
        sorted.clear();
        for (Map.Entry<String, String> entry : labelMap.entrySet()) {
            String value = entry.getValue();
            sorted.put(entry.getKey(), value == null ? EMPTY_STRING : value);
        }

        return encodeLabels(sorted);
    }

    /**
     * Returns the raw internal byte array (header + data) for serialization to BinaryDocValues.
     *
     * @return internal byte array in IndexedByteLabels format
     */
    public byte[] getRawBytes() {
        return data;
    }

    /**
     * Converts the delimiter-based label data to ByteLabels-compatible format
     * (length-prefix encoding, no header). This is NOT the raw DATA section bytes —
     * it re-encodes each {@code name:value} entry as {@code [name_len][name][value_len][value]}.
     * Used for cross-type equals/hash comparisons with ByteLabels.
     *
     * @return byte array in ByteLabels length-prefix format
     */
    public byte[] getDataBytes() {
        return toByteLabelFormat();
    }

    /**
     * Creates an IndexedByteLabels instance directly from a byte array.
     * The byte array must be in the IndexedByteLabels format (header + delimiter-based data).
     * This is O(1) — just stores the reference, no parsing.
     *
     * @param data byte array in IndexedByteLabels format
     * @return new IndexedByteLabels instance
     * @throws IllegalArgumentException if data is null
     */
    public static IndexedByteLabels fromRawBytes(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        return new IndexedByteLabels(data);
    }

    /**
     * Returns a singleton empty IndexedByteLabels instance.
     *
     * @return empty IndexedByteLabels instance
     */
    public static IndexedByteLabels emptyLabels() {
        return EMPTY;
    }

    // ========== Labels interface methods ==========

    @Override
    public boolean isEmpty() {
        return labelCount == 0;
    }

    @Override
    public String get(String name) {
        if (name == null || name.isEmpty()) return EMPTY_STRING;
        if (labelCount == 0) return EMPTY_STRING;

        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);

        // Binary search using embedded offsets — O(log n), zero init cost.
        // compareNameInEntry fuses delimiter scan with comparison in a single byte-by-byte pass.
        int low = 0, high = labelCount - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int entryStart = labelAbsolutePos(mid);
            int entryEnd = labelEntryEnd(mid);

            int cmp = compareNameInEntry(entryStart, entryEnd, nameBytes);

            if (cmp == 0) {
                // Match found — delimiter is at entryStart + nameBytes.length
                int valueStart = entryStart + nameBytes.length + 1;
                int valueLen = entryEnd - valueStart;
                return new String(data, valueStart, valueLen, StandardCharsets.UTF_8);
            } else if (cmp < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return EMPTY_STRING;
    }

    @Override
    public boolean has(String name) {
        if (name == null || name.isEmpty()) return false;
        if (labelCount == 0) return false;

        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);

        // Binary search using embedded offsets — O(log n), zero init cost.
        // compareNameInEntry fuses delimiter scan with comparison in a single byte-by-byte pass.
        int low = 0, high = labelCount - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int entryStart = labelAbsolutePos(mid);
            int entryEnd = labelEntryEnd(mid);

            int cmp = compareNameInEntry(entryStart, entryEnd, nameBytes);

            if (cmp == 0) {
                return true;
            } else if (cmp < 0) {
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return false;
    }

    @Override
    public long stableHash() {
        if (hash != Long.MIN_VALUE) return hash;

        // Convert to ByteLabels-compatible format for cross-type hash consistency.
        // This conversion is O(n) but cached — only computed once per instance.
        byte[] byteLabelFormat = toByteLabelFormat();
        hash = MurmurHash3.hash128(byteLabelFormat, 0, byteLabelFormat.length, 0, new MurmurHash3.Hash128()).h1;
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof IndexedByteLabels other) {
            // Both are IndexedByteLabels — compare raw data (header + data)
            return Arrays.equals(this.data, other.data);
        }
        if (o instanceof ByteLabels other) {
            // Compare our content with ByteLabels by converting to ByteLabels format
            return Arrays.equals(toByteLabelFormat(), other.getRawBytes());
        }
        return false;
    }

    @Override
    public int hashCode() {
        // Must be consistent with ByteLabels.hashCode() for cross-type equality
        return Long.hashCode(stableHash());
    }

    @Override
    public String toString() {
        return toKeyValueString();
    }

    @Override
    public String toKeyValueString() {

        if (labelCount == 0) return EMPTY_STRING;

        StringBuilder result = new StringBuilder();
        for (int i = 0; i < labelCount; i++) {
            int entryStart = labelAbsolutePos(i);
            int entryEnd = labelEntryEnd(i);
            int delimPos = findDelimiter(entryStart, entryEnd);

            if (i > 0) {
                result.append(SPACE_SEPARATOR);
            }
            // The entry is already stored as name:value with the delimiter
            result.append(new String(data, entryStart, entryEnd - entryStart, StandardCharsets.UTF_8));
        }

        return result.toString();
    }

    @Override
    public BytesRef[] toKeyValueBytesRefs() {

        if (labelCount == 0) return new BytesRef[0];

        BytesRef[] result = new BytesRef[labelCount];
        for (int i = 0; i < labelCount; i++) {
            int entryStart = labelAbsolutePos(i);
            int entryEnd = labelEntryEnd(i);
            // The entry bytes are already in "name:value" format (with delimiter)
            result[i] = new BytesRef(data, entryStart, entryEnd - entryStart);
        }

        return result;
    }

    @Override
    public Map<String, String> toMapView() {

        Map<String, String> result = new LinkedHashMap<>();

        for (int i = 0; i < labelCount; i++) {
            int entryStart = labelAbsolutePos(i);
            int entryEnd = labelEntryEnd(i);
            int delimPos = findDelimiter(entryStart, entryEnd);

            String name = new String(data, entryStart, delimPos - entryStart, StandardCharsets.UTF_8);
            String value = new String(data, delimPos + 1, entryEnd - delimPos - 1, StandardCharsets.UTF_8);
            result.put(name, value);
        }

        return result;
    }

    @Override
    public Set<String> toIndexSet() {

        Set<String> result = new TreeSet<>();

        for (int i = 0; i < labelCount; i++) {
            int entryStart = labelAbsolutePos(i);
            int entryEnd = labelEntryEnd(i);
            // The entry is already "name:value" with the delimiter
            result.add(new String(data, entryStart, entryEnd - entryStart, StandardCharsets.UTF_8));
        }

        return result;
    }

    @Override
    public Labels withLabel(String name, String value) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Label name cannot be null or empty");
        }
        if (value == null) {
            value = EMPTY_STRING;
        }

        Map<String, String> map = toMapView();
        map.put(name, value);
        return fromMap(map);
    }

    @Override
    public Labels withLabels(Map<String, String> newLabels) {
        if (newLabels == null || newLabels.isEmpty()) {
            return this;
        }

        Map<String, String> map = toMapView();
        for (Map.Entry<String, String> entry : newLabels.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("Label name cannot be null or empty");
            }
            String value = entry.getValue();
            map.put(key, value == null ? EMPTY_STRING : value);
        }
        return fromMap(map);
    }

    @Override
    public Labels deepCopy() {
        return fromRawBytes(Arrays.copyOf(data, data.length));
    }

    @Override
    public long estimateBytes() {
        return ESTIMATED_MEMORY_OVERHEAD + data.length;
    }

    @Override
    public List<String> extractSortedNames() {

        List<String> names = new ArrayList<>(labelCount);

        for (int i = 0; i < labelCount; i++) {
            int entryStart = labelAbsolutePos(i);
            int entryEnd = labelEntryEnd(i);
            int delimPos = findDelimiter(entryStart, entryEnd);
            names.add(new String(data, entryStart, delimPos - entryStart, StandardCharsets.UTF_8));
        }

        return names;
    }

    @Override
    public List<String> findCommonNamesWithSortedList(List<String> sortedNames) {
        if (sortedNames == null || sortedNames.isEmpty() || isEmpty()) {
            return new ArrayList<>();
        }

        List<String> commonNames = new ArrayList<>();
        int labelIdx = 0;
        int sortedNamesIdx = 0;

        // Two-pointer merge-like algorithm since both are sorted
        while (labelIdx < labelCount && sortedNamesIdx < sortedNames.size()) {
            int entryStart = labelAbsolutePos(labelIdx);
            int entryEnd = labelEntryEnd(labelIdx);
            int delimPos = findDelimiter(entryStart, entryEnd);
            int nameLen = delimPos - entryStart;

            String currentName = sortedNames.get(sortedNamesIdx);
            byte[] currentNameBytes = currentName.getBytes(StandardCharsets.UTF_8);

            int cmp = compareBytes(data, entryStart, nameLen, currentNameBytes);

            if (cmp == 0) {
                commonNames.add(currentName);
                labelIdx++;
                sortedNamesIdx++;
            } else if (cmp < 0) {
                labelIdx++;
            } else {
                sortedNamesIdx++;
            }
        }

        return commonNames;
    }

    // ========== Encoding ==========

    /**
     * Encodes sorted labels into the IndexedByteLabels format with embedded offset header
     * and delimiter-separated name:value entries.
     *
     * <p>Encoding process:
     * <ol>
     *   <li>Encode each label as {@code name_bytes + DELIMITER + value_bytes} into a DATA stream</li>
     *   <li>Track the byte offset of each label entry within the DATA stream</li>
     *   <li>Prepend a HEADER containing label_count and the offsets</li>
     * </ol>
     */
    private static IndexedByteLabels encodeLabels(TreeMap<String, String> labels) {
        if (labels.isEmpty()) {
            return EMPTY;
        }

        // Step 1: Encode all labels into the DATA section using delimiter format
        ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
        List<Integer> offsets = new ArrayList<>(labels.size());

        for (Map.Entry<String, String> entry : labels.entrySet()) {
            offsets.add(dataStream.size()); // offset relative to DATA start
            String value = entry.getValue();
            if (value == null) {
                value = EMPTY_STRING;
            }
            byte[] nameBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            dataStream.writeBytes(nameBytes);
            dataStream.write(DELIMITER_BYTE);
            dataStream.writeBytes(valueBytes);
        }

        byte[] dataBytes = dataStream.toByteArray();
        int labelCount = offsets.size();
        int headerSize = 2 + (labelCount * 2);

        // Step 2: Build final byte array = HEADER + DATA
        byte[] result = new byte[headerSize + dataBytes.length];

        // Write label_count (2 bytes, little-endian)
        result[0] = (byte) (labelCount & 0xFF);
        result[1] = (byte) ((labelCount >>> 8) & 0xFF);

        // Write offsets (2 bytes each, little-endian)
        for (int i = 0; i < labelCount; i++) {
            int offset = offsets.get(i);
            result[2 + i * 2] = (byte) (offset & 0xFF);
            result[3 + i * 2] = (byte) ((offset >>> 8) & 0xFF);
        }

        // Copy DATA section after header
        System.arraycopy(dataBytes, 0, result, headerSize, dataBytes.length);

        return new IndexedByteLabels(result);
    }

    // ========== Cross-type compatibility ==========

    /**
     * Converts the label data to ByteLabels-compatible format (length-prefix encoding).
     * Used for cross-type stableHash() and equals() comparisons.
     * This is O(n) but only called for hash computation (cached) and equals().
     */
    private byte[] toByteLabelFormat() {

        if (labelCount == 0) return new byte[0];

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            for (int i = 0; i < labelCount; i++) {
                int entryStart = labelAbsolutePos(i);
                int entryEnd = labelEntryEnd(i);
                int delimPos = findDelimiter(entryStart, entryEnd);

                int nameLen = delimPos - entryStart;
                int valueLen = entryEnd - delimPos - 1;

                // Write in ByteLabels format: [name_len][name_bytes][value_len][value_bytes]
                appendLength(baos, nameLen);
                baos.write(data, entryStart, nameLen);
                appendLength(baos, valueLen);
                baos.write(data, delimPos + 1, valueLen);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to convert to ByteLabel format", e);
        }
        return baos.toByteArray();
    }

    /**
     * Writes a length value in ByteLabels variable-length format.
     */
    private static void appendLength(ByteArrayOutputStream baos, int length) throws IOException {
        if (length < 255) {
            baos.write(length);
        } else {
            baos.write(255);
            baos.write(length & 0xFF);
            baos.write((length >>> 8) & 0xFF);
            baos.write((length >>> 16) & 0xFF);
        }
    }

    // ========== Byte comparison ==========

    private static int compareBytes(byte[] data, int pos, int len, byte[] target) {
        int minLen = Math.min(len, target.length);
        for (int i = 0; i < minLen; i++) {
            int diff = (data[pos + i] & 0xFF) - (target[i] & 0xFF);
            if (diff != 0) return diff;
        }
        return len - target.length;
    }
}
