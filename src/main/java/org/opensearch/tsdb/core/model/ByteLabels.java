/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
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
 * ByteLabels implements Labels using a space-efficient flat byte array encoding.
 *
 * <h2>Encoding Format</h2>
 * Labels are stored as a sequence of name-value pairs in a flat byte array:
 * <pre>
 * [name1_len][name1_bytes][value1_len][value1_bytes][name2_len][name2_bytes]...
 * </pre>
 *
 * <h2>Length Encoding</h2>
 * String lengths use variable-length encoding:
 * <ul>
 * <li><strong>Short strings (0-254 bytes):</strong> 1 byte containing the length directly</li>
 * <li><strong>Long strings (255+ bytes):</strong> 4 bytes total - first byte is 255 (marker),
 *     followed by 3 bytes containing the actual length in little-endian format (max 16MB)</li>
 * </ul>
 *
 * TODO: support configurable label name/value length limits
 */
public class ByteLabels implements Labels {
    private final byte[] data;

    private long hash = Long.MIN_VALUE;

    /** Shallow size of a ByteLabels instance (object header + fields). */
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ByteLabels.class);

    private static final ByteLabels EMPTY = new ByteLabels(new byte[0]);

    /** ThreadLocal cache for TreeMap instances to reduce object allocation during label creation. */
    private static final ThreadLocal<TreeMap<String, String>> TREE_MAP_CACHE = ThreadLocal.withInitial(TreeMap::new);

    private ByteLabels(byte[] data) {
        this.data = data;
    }

    /**
     * Creates a ByteLabels instance from an array of alternating name-value strings.
     *
     * @param labels an array where even indices are names and odd indices are values
     *               (e.g., "name1", "value1", "name2", "value2")
     * @return a new ByteLabels instance with the given labels
     * @throws IllegalArgumentException if the array length is not even (unpaired labels)
     */
    public static ByteLabels fromStrings(String... labels) {
        if (labels.length % 2 != 0) {
            throw new IllegalArgumentException(
                "Labels must be in pairs (key-value). Received " + labels.length + " labels: " + Arrays.toString(labels)
            );
        }

        TreeMap<String, String> sorted = TREE_MAP_CACHE.get();
        sorted.clear(); // Reuse existing TreeMap
        for (int i = 0; i < labels.length; i += 2) {
            sorted.put(labels[i], labels[i + 1]);
        }

        return encodeLabels(sorted);
    }

    /**
     * Creates a ByteLabels instance from a sorted array of alternating name-value strings.
     * @param labels an array where even indices are names and odd indices are values,
     *               sorted by name (e.g., "name1", "value1", "name2", "value2")
     * @return a new ByteLabels instance with the given labels
     * @throws IllegalArgumentException if the array length is not even (unpaired labels)
     */
    public static ByteLabels fromSortedStrings(String... labels) {
        if (labels == null) {
            return EMPTY;
        }

        if (labels.length % 2 != 0) {
            throw new IllegalArgumentException("Labels must be in pairs");
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            for (int i = 0; i < labels.length; i += 2) {
                appendEncodedString(baos, labels[i]);
                appendEncodedString(baos, labels[i + 1]);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode labels", e);
        }
        return new ByteLabels(baos.toByteArray());
    }

    /**
     * Creates a ByteLabels instance from a sorted array of key:value String pairs
     * @param keyValuePairs pairs to encode, delimited by {@link LabelConstants#LABEL_DELIMITER}
     * @return a new ByteLabels instance with the given labels
     */
    public static ByteLabels fromSortedKeyValuePairs(List<String> keyValuePairs) {
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
     * Creates a ByteLabels instance from a map of label names to values.
     *
     * @param labelMap a map containing label names as keys and label values as values
     * @return a new ByteLabels instance with the given labels, sorted by name
     */
    public static ByteLabels fromMap(Map<String, String> labelMap) {
        TreeMap<String, String> sorted = TREE_MAP_CACHE.get();
        sorted.clear(); // Reuse existing TreeMap
        sorted.putAll(labelMap);
        return encodeLabels(sorted);
    }

    /**
     * Returns a shared empty ByteLabels instance.
     *
     * @return an empty ByteLabels instance
     */
    public static ByteLabels emptyLabels() {
        return EMPTY;
    }

    /**
     * Returns the internal byte array representation of labels.
     * The returned array is the internal representation and should not be modified.
     * This is intended for efficient serialization to BinaryDocValues.
     *
     * @return internal byte array in ByteLabels format
     */
    public byte[] getRawBytes() {
        return data;
    }

    /**
     * Creates a ByteLabels instance directly from a byte array.
     * The byte array must be in the ByteLabels internal format.
     * This is intended for efficient deserialization from BinaryDocValues.
     *
     * @param data byte array in ByteLabels format
     * @return new ByteLabels instance
     * @throws IllegalArgumentException if data is null
     */
    public static ByteLabels fromRawBytes(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        return new ByteLabels(data);
    }

    private static ByteLabels encodeLabels(TreeMap<String, String> labels) {
        if (labels.isEmpty()) {
            return EMPTY;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            for (Map.Entry<String, String> entry : labels.entrySet()) {
                appendEncodedString(baos, entry.getKey());
                appendEncodedString(baos, entry.getValue());
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode labels", e);
        }
        return new ByteLabels(baos.toByteArray());
    }

    /**
     * Encodes a string using variable-length encoding and appends it to the output stream.
     *
     * <p>Encoding format:
     * <ul>
     * <li>For strings 0-254 bytes: [length_byte][string_bytes]</li>
     * <li>For strings 255+ bytes: [255][len_byte1][len_byte2][len_byte3][string_bytes]</li>
     * </ul>
     *
     * @param baos the output stream to append to
     * @param str the string to encode
     * @throws IOException if writing to the stream fails
     * @throws IllegalArgumentException if the string exceeds 16MB (0xFFFFFF bytes)
     */
    private static void appendEncodedString(ByteArrayOutputStream baos, String str) throws IOException {
        byte[] strBytes = str.getBytes(StandardCharsets.UTF_8);
        int length = strBytes.length;

        if (length < 255) {
            baos.write(length); // Write length directly in 1 byte
        } else if (length <= 0xFFFFFF) {
            baos.write(255); // Write 255 as marker byte, next 3 bytes contain actual length
            baos.write(length & 0xFF);
            baos.write((length >> 8) & 0xFF);
            baos.write((length >> 16) & 0xFF);
        } else {
            throw new IllegalArgumentException("String too long: " + length);
        }
        baos.write(strBytes);
    }

    /**
     * Decodes a string from the byte array at the given position.
     *
     * @param data the byte array containing encoded strings
     * @param pos the position to start decoding from
     * @return a DecodedString containing the decoded string and the next position
     * @throws IllegalArgumentException if the data is malformed or truncated
     */
    private static DecodedString decodeString(byte[] data, int pos) {
        if (pos >= data.length) {
            throw new IllegalArgumentException("Index out of bounds");
        }

        int length;
        int nextPos = pos + 1;

        int firstByte = data[pos] & 0xFF;
        if (firstByte == 255) {
            // Extended length encoding (4 bytes total)
            if (pos + 4 > data.length) {
                throw new IllegalArgumentException("Incomplete length encoding");
            }
            length = (data[pos + 1] & 0xFF) | ((data[pos + 2] & 0xFF) << 8) | ((data[pos + 3] & 0xFF) << 16);
            nextPos = pos + 4;
        } else {
            // Short length encoding (1 byte)
            length = firstByte;
        }

        if (nextPos + length > data.length) {
            throw new IllegalArgumentException("String extends beyond data bounds");
        }

        String decoded = new String(data, nextPos, length, StandardCharsets.UTF_8);
        return new DecodedString(decoded, nextPos + length);
    }

    /**
     * Parses string length information without decoding the actual string content.
     * This is more efficient when only position information is needed.
     *
     * @param data the byte array containing encoded data
     * @param pos the position to parse from
     * @return a StringPosition containing the length, data start position, and next position
     */
    private static StringPosition parseStringPos(byte[] data, int pos) {
        int length, nextPos = pos + 1;
        int firstByte = data[pos] & 0xFF;
        if (firstByte == 255) {
            length = (data[pos + 1] & 0xFF) | ((data[pos + 2] & 0xFF) << 8) | ((data[pos + 3] & 0xFF) << 16);
            nextPos = pos + 4;
        } else {
            length = firstByte;
        }

        return new StringPosition(length, nextPos, nextPos + length);
    }

    /**
     * Decodes byte references for a name-value pair without creating intermediate String objects.
     * This is more efficient for operations that need to work directly with the underlying bytes.
     *
     * @param data the byte array containing encoded strings
     * @param pos the position to start decoding from
     * @return a DecodedByteRefs containing BytesRef objects for name and value, and the next position
     */
    private static DecodedByteRefs decodeByteRefs(byte[] data, int pos) {
        StringPosition namePos = parseStringPos(data, pos);
        StringPosition valuePos = parseStringPos(data, namePos.nextPos());

        BytesRef nameRef = new BytesRef(data, namePos.dataStart(), namePos.length());
        BytesRef valueRef = new BytesRef(data, valuePos.dataStart(), valuePos.length());

        return new DecodedByteRefs(nameRef, valueRef, valuePos.nextPos());
    }

    /**
     * Compares a portion of the data array with a target byte array lexicographically.
     * This avoids string creation during label lookups for better performance.
     *
     * @param data the source byte array
     * @param pos the starting position in the source array
     * @param len the length of data to compare
     * @param target the target byte array to compare against
     * @return negative if data < target, zero if equal, positive if data > target
     */
    private static int compareBytes(byte[] data, int pos, int len, byte[] target) {
        int minLen = Math.min(len, target.length);
        for (int i = 0; i < minLen; i++) {
            int diff = (data[pos + i] & 0xFF) - (target[i] & 0xFF);
            if (diff != 0) return diff;
        }
        return len - target.length;
    }

    /**
     * Record representing a decoded string and the position immediately following it.
     *
     * @param value the decoded string value
     * @param nextPos the position in the byte array after this string's data
     */
    private record DecodedString(String value, int nextPos) {
    }

    /**
     * Record representing decoded byte references for a name-value pair and the next position.
     *
     * @param nameRef BytesRef pointing to the name bytes
     * @param valueRef BytesRef pointing to the value bytes
     * @param nextPos the position in the byte array after both strings' data
     */
    private record DecodedByteRefs(BytesRef nameRef, BytesRef valueRef, int nextPos) {
    }

    /**
     * Record representing the position information for a string in the byte array.
     *
     * @param length the decoded length of the string in bytes
     * @param dataStart the position where the string data begins
     * @param nextPos the position after the string data ends
     */
    private record StringPosition(int length, int dataStart, int nextPos) {
    }

    @Override
    public String toKeyValueString() {
        if (data.length == 0) return EMPTY_STRING;

        StringBuilder sb = new StringBuilder();
        int pos = 0;
        boolean first = true;

        while (pos < data.length) {
            if (!first) sb.append(SPACE_SEPARATOR);
            first = false;

            DecodedString name = decodeString(data, pos);
            pos = name.nextPos;
            DecodedString value = decodeString(data, pos);
            pos = value.nextPos;

            sb.append(name.value).append(LabelConstants.LABEL_DELIMITER).append(value.value);
        }

        return sb.toString();
    }

    @Override
    public BytesRef[] toKeyValueBytesRefs() {
        if (data.length == 0) return new BytesRef[0];

        // two pass approach was validated to be faster than using a List, but should be challenged with other datasets
        int count = 0;
        int pos = 0;
        while (pos < data.length) {
            pos = parseStringPos(data, parseStringPos(data, pos).nextPos()).nextPos();
            count++;
        }

        BytesRef[] result = new BytesRef[count];
        pos = 0;
        int index = 0;

        while (pos < data.length) {
            DecodedByteRefs refs = decodeByteRefs(data, pos);
            pos = refs.nextPos();

            int totalLength = refs.nameRef().length + 1 + refs.valueRef().length;
            byte[] combined = new byte[totalLength];
            System.arraycopy(refs.nameRef().bytes, refs.nameRef().offset, combined, 0, refs.nameRef().length);
            combined[refs.nameRef().length] = (byte) LABEL_DELIMITER;
            System.arraycopy(refs.valueRef().bytes, refs.valueRef().offset, combined, refs.nameRef().length + 1, refs.valueRef().length);

            result[index++] = new BytesRef(combined);
        }

        return result;
    }

    @Override
    public Map<String, String> toMapView() {
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        int pos = 0;

        while (pos < data.length) {
            DecodedString name = decodeString(data, pos);
            pos = name.nextPos;
            DecodedString value = decodeString(data, pos);
            pos = value.nextPos;

            result.put(name.value, value.value);
        }

        return result;
    }

    @Override
    public boolean isEmpty() {
        return data.length == 0;
    }

    @Override
    public String get(String name) {
        if (name == null || name.isEmpty()) return EMPTY_STRING;

        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        int pos = 0;

        while (pos < data.length) {
            StringPosition namePos = parseStringPos(data, pos);
            StringPosition valuePos = parseStringPos(data, namePos.nextPos());
            pos = valuePos.nextPos();

            int cmp = compareBytes(data, namePos.dataStart(), namePos.length(), nameBytes);
            if (cmp == 0) {
                return new String(data, valuePos.dataStart(), valuePos.length(), StandardCharsets.UTF_8);
            } else if (cmp > 0) {
                break;
            }
        }

        return EMPTY_STRING;
    }

    @Override
    public boolean has(String name) {
        if (name == null || name.isEmpty()) return false;

        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        int pos = 0;

        while (pos < data.length) {
            StringPosition namePos = parseStringPos(data, pos);
            StringPosition valuePos = parseStringPos(data, namePos.nextPos());
            pos = valuePos.nextPos();

            int cmp = compareBytes(data, namePos.dataStart(), namePos.length(), nameBytes);
            if (cmp == 0) {
                return true;
            } else if (cmp > 0) {
                break;
            }
        }

        return false;
    }

    @Override
    public long stableHash() {
        if (hash != Long.MIN_VALUE) return hash;

        // TODO: use a faster hash function, the current one is 2-3x slower then the one in prometheus
        hash = MurmurHash3.hash128(data, 0, data.length, 0, new MurmurHash3.Hash128()).h1;
        return hash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ByteLabels)) return false;
        ByteLabels other = (ByteLabels) o;
        return Arrays.equals(this.data, other.data);
    }

    @Override
    public int hashCode() {
        long stableHash = stableHash();
        return Long.hashCode(stableHash);
    }

    @Override
    public Set<String> toIndexSet() {
        TreeSet<String> result = new TreeSet<>();
        int pos = 0;

        while (pos < data.length) {
            DecodedString name = decodeString(data, pos);
            pos = name.nextPos;
            DecodedString value = decodeString(data, pos);
            pos = value.nextPos;

            result.add(name.value + LabelConstants.LABEL_DELIMITER + value.value);
        }

        return result;
    }

    @Override
    public Labels withLabel(String name, String value) {
        // TODO: Consider lazy wrapper approach for performance - instead of immediately re-encoding,
        // wrap existing labels with new labels and handle reads lazily. This could be faster for
        // cases where labels are added but not immediately read. Benchmark to compare:
        // 1. Current: eager merge + re-encode on every withLabel call
        // 2. Alternative: lazy wrapper that defers encoding until labels are accessed
        // Trade-offs: lazy approach adds complexity and may use more memory for wrapper objects

        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Label name cannot be null or empty");
        }
        if (value == null) {
            value = EMPTY_STRING;
        }

        byte[] nameBytes = name.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);

        // Find the insertion/update position
        LabelPosition position = findLabelPosition(nameBytes);

        if (position.exists()) {
            // Update existing label with same value - no change needed
            if (position.valueLength() == valueBytes.length
                && compareBytes(data, position.valueStart(), position.valueLength(), valueBytes) == 0) {
                return this;
            }
            return updateExistingLabel(position, nameBytes, valueBytes);
        } else {
            return insertNewLabel(position.insertPos(), nameBytes, valueBytes);
        }
    }

    @Override
    public Labels withLabels(Map<String, String> newLabels) {
        if (newLabels == null || newLabels.isEmpty()) {
            return this;
        }

        // Validate all label names upfront
        for (String name : newLabels.keySet()) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("Label name cannot be null or empty");
            }
        }

        // Merge existing labels with new labels
        TreeMap<String, String> merged = TREE_MAP_CACHE.get();
        merged.clear();

        // Add existing labels
        int pos = 0;
        while (pos < data.length) {
            DecodedString name = decodeString(data, pos);
            pos = name.nextPos;
            DecodedString value = decodeString(data, pos);
            pos = value.nextPos;
            merged.put(name.value, value.value);
        }

        // Add/update with new labels
        for (Map.Entry<String, String> entry : newLabels.entrySet()) {
            String value = entry.getValue();
            merged.put(entry.getKey(), value == null ? EMPTY_STRING : value);
        }

        // Encode merged labels in one pass
        return encodeLabels(merged);
    }

    @Override
    public Labels deepCopy() {
        return fromRawBytes(Arrays.copyOf(data, data.length));
    }

    /**
     * Finds the position where a label should be inserted or updated.
     * Returns information about whether the label exists and where to insert/update.
     */
    private LabelPosition findLabelPosition(byte[] nameBytes) {
        int pos = 0;
        int labelIndex = 0;

        while (pos < data.length) {
            StringPosition namePos = parseStringPos(data, pos);
            StringPosition valuePos = parseStringPos(data, namePos.nextPos());

            int cmp = compareBytes(data, namePos.dataStart(), namePos.length(), nameBytes);
            if (cmp == 0) {
                // Found exact match
                return new LabelPosition(
                    true,
                    pos,
                    labelIndex,
                    namePos.dataStart(),
                    namePos.length(),
                    valuePos.dataStart(),
                    valuePos.length(),
                    valuePos.nextPos()
                );
            } else if (cmp > 0) {
                // Found position where we should insert (labels are sorted)
                return new LabelPosition(false, pos, labelIndex, 0, 0, 0, 0, 0);
            }

            pos = valuePos.nextPos();
            labelIndex++;
        }

        // Insert at the end
        return new LabelPosition(false, pos, labelIndex, 0, 0, 0, 0, 0);
    }

    /**
     * Updates an existing label with a new value.
     */
    private ByteLabels updateExistingLabel(LabelPosition position, byte[] nameBytes, byte[] valueBytes) {
        // Calculate encoded lengths
        int newNameEncodedSize = getEncodedStringSize(nameBytes.length);
        int newValueEncodedSize = getEncodedStringSize(valueBytes.length);
        int newLabelSize = newNameEncodedSize + nameBytes.length + newValueEncodedSize + valueBytes.length;

        int oldNameEncodedSize = getEncodedStringSize(position.nameLength());
        int oldValueEncodedSize = getEncodedStringSize(position.valueLength());
        int oldLabelSize = oldNameEncodedSize + position.nameLength() + oldValueEncodedSize + position.valueLength();

        int sizeDelta = newLabelSize - oldLabelSize;
        byte[] newData = new byte[data.length + sizeDelta];

        // Copy data before the updated label
        System.arraycopy(data, 0, newData, 0, position.startPos());

        // Write the updated label
        int writePos = position.startPos();
        writePos = writeEncodedString(newData, writePos, nameBytes);
        writePos = writeEncodedString(newData, writePos, valueBytes);

        // Copy data after the updated label
        System.arraycopy(data, position.endPos(), newData, writePos, data.length - position.endPos());

        return new ByteLabels(newData);
    }

    /**
     * Inserts a new label at the specified position.
     */
    private ByteLabels insertNewLabel(int insertPos, byte[] nameBytes, byte[] valueBytes) {
        // Calculate encoded size for the new label
        int nameEncodedSize = getEncodedStringSize(nameBytes.length);
        int valueEncodedSize = getEncodedStringSize(valueBytes.length);
        int newLabelSize = nameEncodedSize + nameBytes.length + valueEncodedSize + valueBytes.length;

        byte[] newData = new byte[data.length + newLabelSize];

        // Copy data before insertion point
        System.arraycopy(data, 0, newData, 0, insertPos);

        // Write the new label
        int writePos = insertPos;
        writePos = writeEncodedString(newData, writePos, nameBytes);
        writePos = writeEncodedString(newData, writePos, valueBytes);

        // Copy data after insertion point
        System.arraycopy(data, insertPos, newData, writePos, data.length - insertPos);

        return new ByteLabels(newData);
    }

    /**
     * Calculates the encoded size needed for a string of the given length.
     */
    private static int getEncodedStringSize(int stringLength) {
        return stringLength < 255 ? 1 : 4;
    }

    /**
     * Writes an encoded string to the byte array at the specified position.
     * Returns the position after the written data.
     */
    private static int writeEncodedString(byte[] dest, int pos, byte[] stringBytes) {
        int length = stringBytes.length;

        if (length < 255) {
            dest[pos++] = (byte) length;
        } else {
            dest[pos++] = (byte) 255;
            dest[pos++] = (byte) (length & 0xFF);
            dest[pos++] = (byte) ((length >> 8) & 0xFF);
            dest[pos++] = (byte) ((length >> 16) & 0xFF);
        }

        System.arraycopy(stringBytes, 0, dest, pos, length);
        return pos + length;
    }

    /**
     * Record representing the position information for a label in the byte array.
     */
    private record LabelPosition(boolean exists, int startPos, int labelIndex, int nameStart, int nameLength, int valueStart,
        int valueLength, int endPos) {
        int insertPos() {
            return startPos;
        }
    }

    @Override
    public String toString() {
        return toKeyValueString();
    }

    /**
     * Extract sorted names (label names) from this ByteLabels instance.
     * Since ByteLabels are stored in sorted order, this efficiently scans the byte array.
     * Overrides the default implementation to avoid map conversion.
     *
     * @return a list of sorted label names
     */
    @Override
    public List<String> extractSortedNames() {
        List<String> names = new ArrayList<>();
        int pos = 0;

        while (pos < data.length) {
            StringPosition namePos = parseStringPos(data, pos);
            StringPosition valuePos = parseStringPos(data, namePos.nextPos());
            pos = valuePos.nextPos();

            String name = new String(data, namePos.dataStart(), namePos.length(), StandardCharsets.UTF_8);
            names.add(name);
        }

        return names;
    }

    /**
     * Find common names between this ByteLabels instance and a sorted list of label names.
     * Leverages the sorted nature of both ByteLabels and the input list for efficient comparison.
     * Overrides the default implementation to avoid map conversion and use efficient byte array scanning.
     *
     * @param sortedNames a sorted list of label names to compare with
     * @return a list of common names, sorted by name
     */
    @Override
    public List<String> findCommonNamesWithSortedList(List<String> sortedNames) {
        if (sortedNames.isEmpty() || this.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> commonNames = new ArrayList<>();
        int sortedNamesIndex = 0;
        int pos = 0;

        // Two-pointer merge-like algorithm since both are sorted
        while (pos < data.length && sortedNamesIndex < sortedNames.size()) {
            StringPosition namePos = parseStringPos(data, pos);
            StringPosition valuePos = parseStringPos(data, namePos.nextPos());
            int nextPos = valuePos.nextPos();

            String currentName = sortedNames.get(sortedNamesIndex);
            byte[] currentNameBytes = currentName.getBytes(StandardCharsets.UTF_8);

            // Compare names byte-by-byte
            int cmp = compareBytes(data, namePos.dataStart(), namePos.length(), currentNameBytes);

            if (cmp == 0) {
                // Names match - add to common names
                commonNames.add(currentName);
                pos = nextPos;
                sortedNamesIndex++;
            } else if (cmp < 0) {
                // ByteLabels name is smaller - advance ByteLabels position
                pos = nextPos;
            } else {
                // Sorted list name is smaller - advance sorted list index
                sortedNamesIndex++;
            }
        }

        return commonNames;
    }

    /**
     * Return the memory usage of this ByteLabels instance in bytes.
     * Includes the shallow size of the object plus the byte array contents.
     *
     * @return memory usage in bytes
     */
    @Override
    public long ramBytesUsed() {
        // SHALLOW_SIZE includes the data reference, so add the array's full size
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(data);
    }

    /**
     * Get the shallow size for testing.
     * @return bytes for ByteLabels object overhead (excluding data)
     */
    static long getShallowSize() {
        return SHALLOW_SIZE;
    }

    /**
     * Get the internal data array for testing.
     * Package-private for test validation.
     *
     * @return the internal data byte array
     */
    byte[] getDataForTesting() {
        return data;
    }

}
