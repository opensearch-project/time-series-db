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
 * <pre>
 * ┌─────────────────────────── HEADER ────────────────────────────┐
 * │ [label_count:  2 bytes, little-endian, unsigned]              │
 * │ [packed 12-bit offsets: ceil(n * 1.5) bytes]                  │
 * │ [name_len_0: 1 byte] [name_len_1: 1 byte] ... [name_len_n-1] │
 * ├─────────────────────────── DATA ──────────────────────────────┤
 * │ [name1_bytes][value1_bytes]                                   │
 * │ [name2_bytes][value2_bytes]                                   │
 * │ ...                                                           │
 * └───────────────────────────────────────────────────────────────┘
 * </pre>
 *
 * <h2>Header Protocol</h2>
 * <ul>
 *   <li><strong>label_count</strong> (bytes 0-1): 2-byte unsigned little-endian integer.</li>
 *   <li><strong>Packed 12-bit offsets</strong>: Each offset is the byte position of a label entry
 *       relative to the DATA section start. Two 12-bit offsets pack into 3 bytes:
 *       <pre>
 *       byte[0] = offset_even[7:0]
 *       byte[1] = offset_even[11:8] | offset_odd[3:0] &lt;&lt; 4
 *       byte[2] = offset_odd[11:4]
 *       </pre>
 *       Max offset: 4095. Total bytes: {@code ceil(labelCount * 1.5)}.</li>
 *   <li><strong>Name lengths</strong>: 1 byte per label storing the UTF-8 byte length of the
 *       label name (max 255). This eliminates the need for a delimiter or length-prefix in the
 *       DATA section — the name/value boundary is known from the header.</li>
 *   <li><strong>Header size</strong>: {@code 2 + ceil(labelCount * 1.5) + labelCount}.</li>
 * </ul>
 *
 * <h2>DATA Section</h2>
 * Label entries contain raw name and value bytes concatenated with <strong>zero overhead</strong>:
 * <pre>
 * [name_bytes][value_bytes]
 * </pre>
 * The name length is read from the header. The value length is computed as
 * {@code entryEnd - entryStart - nameLen}. No delimiter, no length-prefix bytes.
 *
 * <h2>Binary search lookup</h2>
 * <pre>{@code
 * int nameLen = data[nameLenOffset + i] & 0xFF;  // 1 byte read from header
 * int entryStart = labelAbsolutePos(i);           // 12-bit offset read
 * int cmp = compareBytes(data, entryStart, nameLen, target);  // tight loop
 * }</pre>
 *
 * <h2>Empty Labels</h2>
 * Encoded as a 2-byte header {@code [0x00, 0x00]} with no data section.
 *
 * <h2>Compatibility</h2>
 * <ul>
 *   <li>{@code stableHash()} and {@code equals()} convert to ByteLabels-compatible format
 *       internally for cross-type consistency.</li>
 *   <li>On-disk format is <strong>not compatible</strong> with ByteLabels — requires
 *       {@code BINARY_INDEXED_BYTESLABEL} index setting.</li>
 * </ul>
 */
public class IndexedByteLabels implements Labels {
    private final byte[] data;

    /** Cached label count, read from header bytes 0-1. */
    private final int labelCount;

    /** Cached header size = 2 + ceil(labelCount * 1.5) + labelCount. */
    private final int headerSize;

    /** Byte offset within data[] where the name_len array starts. */
    private final int nameLenOffset;

    private long hash = Long.MIN_VALUE;

    /**
     * Base memory overhead for an IndexedByteLabels instance in bytes.
     * <p>Memory breakdown:
     * <ul>
     *   <li>Object header: 16 bytes</li>
     *   <li>Field: byte[] data reference: 8 bytes</li>
     *   <li>Field: int labelCount: 4 bytes</li>
     *   <li>Field: int headerSize: 4 bytes</li>
     *   <li>Field: int nameLenOffset: 4 bytes</li>
     *   <li>Field: long hash: 8 bytes</li>
     *   <li>Padding: 4 bytes</li>
     *   <li>byte[] array object header: 16 bytes</li>
     * </ul>
     * Total: 64 bytes + data.length
     * </p>
     */
    private static final long ESTIMATED_MEMORY_OVERHEAD = 64;

    private static final IndexedByteLabels EMPTY = new IndexedByteLabels(new byte[] { 0, 0 });

    private static final ThreadLocal<TreeMap<String, String>> TREE_MAP_CACHE = ThreadLocal.withInitial(TreeMap::new);

    /** Maximum offset value representable in 12 bits. */
    private static final int MAX_12BIT_OFFSET = 4095;

    private IndexedByteLabels(byte[] data) {
        this.data = data;
        this.labelCount = (data[0] & 0xFF) | ((data[1] & 0xFF) << 8);
        int offsetBytes = (labelCount * 3 + 1) / 2;
        this.nameLenOffset = 2 + offsetBytes;
        this.headerSize = nameLenOffset + labelCount;
    }

    // ========== Header access helpers ==========

    /**
     * Reads the 12-bit packed offset for label i and returns its absolute position in data[].
     */
    private int labelAbsolutePos(int i) {
        int pairByteStart = 2 + (i / 2) * 3;
        int relativeOffset;
        if ((i & 1) == 0) {
            relativeOffset = (data[pairByteStart] & 0xFF) | ((data[pairByteStart + 1] & 0x0F) << 8);
        } else {
            relativeOffset = ((data[pairByteStart + 1] >> 4) & 0x0F) | ((data[pairByteStart + 2] & 0xFF) << 4);
        }
        return headerSize + relativeOffset;
    }

    /**
     * Returns the end position (exclusive) of label i's entry.
     */
    private int labelEntryEnd(int i) {
        return (i < labelCount - 1) ? labelAbsolutePos(i + 1) : data.length;
    }

    /**
     * Reads the name length of label i from the header (1 byte, unsigned).
     */
    private int nameLength(int i) {
        return data[nameLenOffset + i] & 0xFF;
    }

    // ========== Factory methods ==========

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

    public byte[] getRawBytes() {
        return data;
    }

    /**
     * Converts to ByteLabels-compatible format (length-prefix encoding, no header).
     * Used for cross-type equals/hash comparisons with ByteLabels.
     */
    public byte[] getDataBytes() {
        return toByteLabelFormat();
    }

    public static IndexedByteLabels fromRawBytes(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        return new IndexedByteLabels(data);
    }

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

        // Binary search — name length from header, direct compareBytes (no delimiter scan)
        int low = 0, high = labelCount - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int entryStart = labelAbsolutePos(mid);
            int nameLen = nameLength(mid);

            int cmp = compareBytes(data, entryStart, nameLen, nameBytes);

            if (cmp == 0) {
                int valueStart = entryStart + nameLen;
                int entryEnd = labelEntryEnd(mid);
                return new String(data, valueStart, entryEnd - valueStart, StandardCharsets.UTF_8);
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

        // Binary search — name length from header, direct compareBytes (no delimiter scan)
        int low = 0, high = labelCount - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            int entryStart = labelAbsolutePos(mid);
            int nameLen = nameLength(mid);

            int cmp = compareBytes(data, entryStart, nameLen, nameBytes);

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

        byte[] byteLabelFormat = toByteLabelFormat();
        hash = MurmurHash3.hash128(byteLabelFormat, 0, byteLabelFormat.length, 0, new MurmurHash3.Hash128()).h1;
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof IndexedByteLabels other) {
            return Arrays.equals(this.data, other.data);
        }
        if (o instanceof ByteLabels other) {
            return Arrays.equals(toByteLabelFormat(), other.getRawBytes());
        }
        return false;
    }

    @Override
    public int hashCode() {
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
            int nameLen = nameLength(i);

            if (i > 0) result.append(SPACE_SEPARATOR);
            result.append(new String(data, entryStart, nameLen, StandardCharsets.UTF_8));
            result.append(LABEL_DELIMITER);
            result.append(new String(data, entryStart + nameLen, entryEnd - entryStart - nameLen, StandardCharsets.UTF_8));
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
            int nameLen = nameLength(i);
            int valueLen = entryEnd - entryStart - nameLen;

            // Build "name:value" BytesRef
            byte[] kv = new byte[nameLen + 1 + valueLen];
            System.arraycopy(data, entryStart, kv, 0, nameLen);
            kv[nameLen] = (byte) LABEL_DELIMITER;
            System.arraycopy(data, entryStart + nameLen, kv, nameLen + 1, valueLen);
            result[i] = new BytesRef(kv);
        }

        return result;
    }

    @Override
    public Map<String, String> toMapView() {
        Map<String, String> result = new LinkedHashMap<>();

        for (int i = 0; i < labelCount; i++) {
            int entryStart = labelAbsolutePos(i);
            int entryEnd = labelEntryEnd(i);
            int nameLen = nameLength(i);

            String name = new String(data, entryStart, nameLen, StandardCharsets.UTF_8);
            String value = new String(data, entryStart + nameLen, entryEnd - entryStart - nameLen, StandardCharsets.UTF_8);
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
            int nameLen = nameLength(i);

            String name = new String(data, entryStart, nameLen, StandardCharsets.UTF_8);
            String value = new String(data, entryStart + nameLen, entryEnd - entryStart - nameLen, StandardCharsets.UTF_8);
            result.add(name + LABEL_DELIMITER + value);
        }

        return result;
    }

    @Override
    public Labels withLabel(String name, String value) {
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Label name cannot be null or empty");
        }
        if (value == null) value = EMPTY_STRING;

        Map<String, String> map = toMapView();
        map.put(name, value);
        return fromMap(map);
    }

    @Override
    public Labels withLabels(Map<String, String> newLabels) {
        if (newLabels == null || newLabels.isEmpty()) return this;

        Map<String, String> map = toMapView();
        for (Map.Entry<String, String> entry : newLabels.entrySet()) {
            String key = entry.getKey();
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("Label name cannot be null or empty");
            }
            map.put(key, entry.getValue() == null ? EMPTY_STRING : entry.getValue());
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
            int nameLen = nameLength(i);
            names.add(new String(data, entryStart, nameLen, StandardCharsets.UTF_8));
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

        while (labelIdx < labelCount && sortedNamesIdx < sortedNames.size()) {
            int entryStart = labelAbsolutePos(labelIdx);
            int nameLen = nameLength(labelIdx);

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
     * Encodes sorted labels into the IndexedByteLabels format:
     * HEADER = [label_count:2][packed 12-bit offsets][name_len per label]
     * DATA   = [name_bytes][value_bytes] per label (zero overhead)
     */
    private static IndexedByteLabels encodeLabels(TreeMap<String, String> labels) {
        if (labels.isEmpty()) {
            return EMPTY;
        }

        // Step 1: Encode DATA section and collect offsets + name lengths
        ByteArrayOutputStream dataStream = new ByteArrayOutputStream();
        List<Integer> offsets = new ArrayList<>(labels.size());
        List<Integer> nameLens = new ArrayList<>(labels.size());

        for (Map.Entry<String, String> entry : labels.entrySet()) {
            offsets.add(dataStream.size());
            String value = entry.getValue();
            if (value == null) value = EMPTY_STRING;
            byte[] nameBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            nameLens.add(nameBytes.length);
            dataStream.writeBytes(nameBytes);
            dataStream.writeBytes(valueBytes);
        }

        byte[] dataBytes = dataStream.toByteArray();
        int n = offsets.size();
        int offsetBytes = (n * 3 + 1) / 2;
        int headerSize = 2 + offsetBytes + n;

        // Validate offsets fit in 12 bits
        for (int offset : offsets) {
            if (offset > MAX_12BIT_OFFSET) {
                throw new IllegalArgumentException(
                    "Label data section too large: offset " + offset + " exceeds 12-bit max (" + MAX_12BIT_OFFSET + ")"
                );
            }
        }

        // Step 2: Build HEADER + DATA
        byte[] result = new byte[headerSize + dataBytes.length];

        // label_count (2 bytes, little-endian)
        result[0] = (byte) (n & 0xFF);
        result[1] = (byte) ((n >>> 8) & 0xFF);

        // Packed 12-bit offsets
        for (int i = 0; i < n; i += 2) {
            int pairByteStart = 2 + (i / 2) * 3;
            int offset0 = offsets.get(i);

            if (i + 1 < n) {
                int offset1 = offsets.get(i + 1);
                result[pairByteStart] = (byte) (offset0 & 0xFF);
                result[pairByteStart + 1] = (byte) (((offset0 >>> 8) & 0x0F) | ((offset1 & 0x0F) << 4));
                result[pairByteStart + 2] = (byte) ((offset1 >>> 4) & 0xFF);
            } else {
                result[pairByteStart] = (byte) (offset0 & 0xFF);
                result[pairByteStart + 1] = (byte) ((offset0 >>> 8) & 0x0F);
            }
        }

        // Name lengths (1 byte each)
        int nameLenStart = 2 + offsetBytes;
        for (int i = 0; i < n; i++) {
            result[nameLenStart + i] = (byte) (nameLens.get(i) & 0xFF);
        }

        // DATA section
        System.arraycopy(dataBytes, 0, result, headerSize, dataBytes.length);

        return new IndexedByteLabels(result);
    }

    // ========== Cross-type compatibility ==========

    /**
     * Converts to ByteLabels-compatible length-prefix format.
     * O(n) but cached via stableHash().
     */
    private byte[] toByteLabelFormat() {
        if (labelCount == 0) return new byte[0];

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            for (int i = 0; i < labelCount; i++) {
                int entryStart = labelAbsolutePos(i);
                int entryEnd = labelEntryEnd(i);
                int nameLen = nameLength(i);
                int valueLen = entryEnd - entryStart - nameLen;

                appendLength(baos, nameLen);
                baos.write(data, entryStart, nameLen);
                appendLength(baos, valueLen);
                baos.write(data, entryStart + nameLen, valueLen);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to convert to ByteLabel format", e);
        }
        return baos.toByteArray();
    }

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
