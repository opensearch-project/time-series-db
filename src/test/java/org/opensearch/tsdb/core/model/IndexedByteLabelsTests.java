/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Tests for IndexedByteLabels with embedded offset header format.
 *
 * Verifies:
 * - Embedded header protocol (label_count + offsets)
 * - O(log n) binary search via get()/has() on raw bytes
 * - Round-trip: fromStrings() → getRawBytes() → fromRawBytes() → get()
 * - Cross-type stableHash()/equals() compatibility with ByteLabels
 * - All Labels interface contracts
 */
public class IndexedByteLabelsTests extends OpenSearchTestCase {

    // ================= BASIC FUNCTIONALITY =================

    public void testBasicFunctionality() {
        IndexedByteLabels labels = IndexedByteLabels.fromStrings("k1", "v1", "k2", "v2");

        assertEquals("v1", labels.get("k1"));
        assertEquals("v2", labels.get("k2"));
        assertEquals("", labels.get("nonexistent"));
        assertEquals("", labels.get(null));
        assertEquals("", labels.get(""));
        assertTrue(labels.has("k1"));
        assertFalse(labels.has("nonexistent"));
        assertFalse(labels.has(null));
        assertFalse(labels.has(""));
        assertFalse(labels.isEmpty());

        String kvString = labels.toKeyValueString();
        assertTrue("Should contain k1:v1", kvString.contains("k1:v1"));
        assertTrue("Should contain k2:v2", kvString.contains("k2:v2"));
        assertEquals("toString should match toKeyValueString", kvString, labels.toString());
    }

    public void testInvalidInput() {
        expectThrows(IllegalArgumentException.class, () -> IndexedByteLabels.fromStrings("k1", "v1", "k2"));
    }

    public void testEmptyLabels() {
        IndexedByteLabels empty = IndexedByteLabels.emptyLabels();
        assertTrue("Empty labels should be empty", empty.isEmpty());
        assertEquals("", empty.get("any"));
        assertFalse(empty.has("any"));
        assertEquals("", empty.toKeyValueString());
        assertEquals(0, empty.toKeyValueBytesRefs().length);
        assertTrue(empty.toMapView().isEmpty());
        assertTrue(empty.toIndexSet().isEmpty());
    }

    public void testLabelSorting() {
        IndexedByteLabels labels1 = IndexedByteLabels.fromStrings("z", "1", "a", "2", "m", "3");
        IndexedByteLabels labels2 = IndexedByteLabels.fromStrings("a", "2", "m", "3", "z", "1");

        assertEquals("Different input orders should produce equal labels", labels1, labels2);
        assertEquals("Hash codes should match", labels1.hashCode(), labels2.hashCode());
        assertEquals("Stable hashes should match", labels1.stableHash(), labels2.stableHash());
    }

    // ================= EMBEDDED HEADER FORMAT =================

    public void testHeaderFormat() {
        IndexedByteLabels labels = IndexedByteLabels.fromStrings("a", "1", "b", "2", "c", "3");
        byte[] raw = labels.getRawBytes();

        // First 2 bytes should be label count (3) in little-endian
        int labelCount = (raw[0] & 0xFF) | ((raw[1] & 0xFF) << 8);
        assertEquals("Header should contain label count", 3, labelCount);

        // Header = 2 (count) + ceil(3*1.5)=5 (offsets) + 3 (name_lens) = 10 bytes
        int offsetBytes = (labelCount * 3 + 1) / 2;
        int headerSize = 2 + offsetBytes + labelCount;
        assertEquals("Header size should be 10", 10, headerSize);

        // First offset (even index) should be 0 — read 12-bit packed value
        int offset0 = (raw[2] & 0xFF) | ((raw[3] & 0x0F) << 8);
        assertEquals("First offset should be 0", 0, offset0);

        // Name lengths should be stored after offsets
        int nameLenStart = 2 + offsetBytes;
        assertEquals("name_len[0] for 'a' should be 1", 1, raw[nameLenStart] & 0xFF);
        assertEquals("name_len[1] for 'b' should be 1", 1, raw[nameLenStart + 1] & 0xFF);
        assertEquals("name_len[2] for 'c' should be 1", 1, raw[nameLenStart + 2] & 0xFF);
    }

    public void testHeaderOverheadSize() {
        // Header = 2 (count) + ceil(n*1.5) (offsets) + n (name_lens)
        // DATA saves 2 bytes per label (no length prefixes) vs ByteLabels
        // Net overhead = (2 + ceil(n*1.5) + n) - 2n = 2 + ceil(n*1.5) - n
        // With name_len in header instead of delimiter: saves 1 byte/label in DATA,
        // adds 1 byte/label in header → same total as delimiter format
        for (int n : new int[] { 1, 5, 10, 20, 30 }) {
            String[] labelArray = new String[n * 2];
            for (int i = 0; i < n; i++) {
                labelArray[i * 2] = String.format(Locale.ROOT, "key_%03d", i);
                labelArray[i * 2 + 1] = String.format(Locale.ROOT, "val_%03d", i);
            }

            IndexedByteLabels indexed = IndexedByteLabels.fromStrings(labelArray);
            ByteLabels byteLabels = ByteLabels.fromStrings(labelArray);

            int offsetBytes = (n * 3 + 1) / 2;
            // header = 2 + offsetBytes + n, saves 2n bytes (no len prefixes), net = 2 + offsetBytes - n
            int expectedNetOverhead = 2 + offsetBytes - n;
            int actualOverhead = indexed.getRawBytes().length - byteLabels.getRawBytes().length;
            assertEquals("Net overhead for " + n + " labels", expectedNetOverhead, actualOverhead);
        }
    }

    // ================= RAW BYTES ROUND-TRIP =================

    public void testRawBytesRoundTrip() {
        IndexedByteLabels original = IndexedByteLabels.fromStrings("app", "web", "env", "prod", "version", "1.0");

        // Simulate the hot path: fromRawBytes() → get()
        byte[] serialized = original.getRawBytes();
        IndexedByteLabels deserialized = IndexedByteLabels.fromRawBytes(serialized);

        assertEquals("web", deserialized.get("app"));
        assertEquals("prod", deserialized.get("env"));
        assertEquals("1.0", deserialized.get("version"));
        assertEquals("", deserialized.get("nonexistent"));
        assertEquals(original, deserialized);
    }

    public void testFromRawBytesIsZeroCost() {
        // fromRawBytes() should just store the reference — no parsing
        IndexedByteLabels labels = IndexedByteLabels.fromStrings("a", "1");
        byte[] raw = labels.getRawBytes();
        IndexedByteLabels fromRaw = IndexedByteLabels.fromRawBytes(raw);

        // Verify it works immediately (binary search on embedded offsets, no lazy init)
        assertEquals("1", fromRaw.get("a"));
        assertFalse(fromRaw.has("b"));
    }

    // ================= BINARY SEARCH CORRECTNESS =================

    public void testBinarySearchLargeSet() {
        String[] labelArray = new String[60]; // 30 labels
        for (int i = 0; i < 30; i++) {
            labelArray[i * 2] = String.format(Locale.ROOT, "label_%03d", i);
            labelArray[i * 2 + 1] = String.format(Locale.ROOT, "value_%03d", i);
        }

        IndexedByteLabels labels = IndexedByteLabels.fromStrings(labelArray);

        assertEquals("value_000", labels.get("label_000")); // First
        assertEquals("value_015", labels.get("label_015")); // Middle
        assertEquals("value_029", labels.get("label_029")); // Last
        assertEquals("", labels.get("nonexistent"));

        assertTrue(labels.has("label_000"));
        assertTrue(labels.has("label_015"));
        assertTrue(labels.has("label_029"));
        assertFalse(labels.has("nonexistent"));
    }

    public void testBinarySearchEdgeCases() {
        IndexedByteLabels singleLabel = IndexedByteLabels.fromStrings("single", "value");
        assertEquals("value", singleLabel.get("single"));
        assertFalse(singleLabel.has("notfound"));

        IndexedByteLabels twoLabels = IndexedByteLabels.fromStrings("a", "1", "z", "26");
        assertEquals("1", twoLabels.get("a"));
        assertEquals("26", twoLabels.get("z"));
        assertEquals("", twoLabels.get("m")); // Between a and z
        assertEquals("", twoLabels.get("@")); // Before a
    }

    // ================= CROSS-TYPE COMPATIBILITY WITH BYTELABELS =================

    public void testStableHashMatchesByteLabels() {
        String[] testData = { "app", "frontend", "env", "production", "version", "2.1.0" };

        ByteLabels byteLabels = ByteLabels.fromStrings(testData);
        IndexedByteLabels indexedLabels = IndexedByteLabels.fromStrings(testData);

        assertEquals("stableHash should match across types", byteLabels.stableHash(), indexedLabels.stableHash());
        assertEquals("hashCode should match across types", byteLabels.hashCode(), indexedLabels.hashCode());
    }

    public void testCrossTypeEquals() {
        String[] testData = { "app", "frontend", "env", "production" };

        ByteLabels byteLabels = ByteLabels.fromStrings(testData);
        IndexedByteLabels indexedLabels = IndexedByteLabels.fromStrings(testData);

        assertEquals("IndexedByteLabels should equal ByteLabels", indexedLabels, byteLabels);
        assertEquals("ByteLabels should equal IndexedByteLabels", byteLabels, indexedLabels);
    }

    public void testDataBytesMatchByteLabels() {
        String[] testData = { "k1", "v1", "k2", "v2" };

        ByteLabels byteLabels = ByteLabels.fromStrings(testData);
        IndexedByteLabels indexedLabels = IndexedByteLabels.fromStrings(testData);

        // getDataBytes() converts delimiter-based format to ByteLabels length-prefix format
        assertArrayEquals(
            "getDataBytes() should produce ByteLabels-compatible bytes",
            byteLabels.getRawBytes(),
            indexedLabels.getDataBytes()
        );
    }

    public void testRawBytesAreDifferentEncoding() {
        // Verify that the raw byte formats are genuinely different —
        // IndexedByteLabels uses header + delimiter, ByteLabels uses length-prefix.
        // Cross-type equals still works because getDataBytes() converts formats.
        String[] testData = { "k1", "v1", "k2", "v2" };

        ByteLabels byteLabels = ByteLabels.fromStrings(testData);
        IndexedByteLabels indexedLabels = IndexedByteLabels.fromStrings(testData);

        byte[] byteRaw = byteLabels.getRawBytes();
        byte[] indexedRaw = indexedLabels.getRawBytes();

        // Raw bytes must differ (different encodings)
        assertFalse("Raw bytes should differ due to different encodings", Arrays.equals(byteRaw, indexedRaw));

        // But equals() still works (converts internally)
        assertEquals("Cross-type equals must work despite different encodings", byteLabels, indexedLabels);
        assertEquals("Cross-type equals must be symmetric", indexedLabels, byteLabels);

        // And hashes match
        assertEquals("stableHash must match across types", byteLabels.stableHash(), indexedLabels.stableHash());
    }

    public void testCrossTypeEqualsWithManyLabels() {
        // Test cross-type equals with a larger label set to ensure
        // the delimiter→length-prefix conversion is correct at scale
        String[] testData = new String[40]; // 20 labels
        for (int i = 0; i < 20; i++) {
            testData[i * 2] = "key" + i;
            testData[i * 2 + 1] = "value" + i;
        }

        ByteLabels byteLabels = ByteLabels.fromStrings(testData);
        IndexedByteLabels indexedLabels = IndexedByteLabels.fromStrings(testData);

        assertEquals(byteLabels, indexedLabels);
        assertEquals(indexedLabels, byteLabels);
        assertEquals(byteLabels.stableHash(), indexedLabels.stableHash());
    }

    public void testCrossTypeEqualsNotEqualWhenDifferent() {
        ByteLabels byteLabels = ByteLabels.fromStrings("a", "1", "b", "2");
        IndexedByteLabels indexedLabels = IndexedByteLabels.fromStrings("a", "1", "b", "DIFFERENT");

        assertNotEquals(byteLabels, indexedLabels);
        assertNotEquals(indexedLabels, byteLabels);
    }

    public void testInterfaceMethodCompatibility() {
        String[] testData = { "app", "frontend", "env", "production", "version", "2.1.0" };

        ByteLabels byteLabels = ByteLabels.fromStrings(testData);
        IndexedByteLabels indexedLabels = IndexedByteLabels.fromStrings(testData);

        assertEquals("toKeyValueString should match", byteLabels.toKeyValueString(), indexedLabels.toKeyValueString());
        assertEquals("toMapView should match", byteLabels.toMapView(), indexedLabels.toMapView());
        assertEquals("toIndexSet should match", byteLabels.toIndexSet(), indexedLabels.toIndexSet());

        BytesRef[] byteRefs = byteLabels.toKeyValueBytesRefs();
        BytesRef[] indexedRefs = indexedLabels.toKeyValueBytesRefs();
        assertEquals(byteRefs.length, indexedRefs.length);
        for (int i = 0; i < byteRefs.length; i++) {
            assertEquals(byteRefs[i].utf8ToString(), indexedRefs[i].utf8ToString());
        }
    }

    // ================= MODIFICATION METHODS =================

    public void testWithLabel() {
        IndexedByteLabels original = IndexedByteLabels.fromStrings("a", "1", "c", "3");

        Labels updated = original.withLabel("b", "2");
        assertEquals("1", updated.get("a"));
        assertEquals("2", updated.get("b"));
        assertEquals("3", updated.get("c"));

        updated = original.withLabel("a", "updated");
        assertEquals("updated", updated.get("a"));

        updated = original.withLabel("nulltest", null);
        assertEquals("", updated.get("nulltest"));
    }

    public void testWithLabels() {
        IndexedByteLabels original = IndexedByteLabels.fromStrings("a", "1", "c", "3", "e", "5");

        Map<String, String> updates = Map.of("b", "2", "d", "4", "c", "updated");
        Labels updated = original.withLabels(updates);

        assertEquals("1", updated.get("a"));
        assertEquals("2", updated.get("b"));
        assertEquals("updated", updated.get("c"));
        assertEquals("4", updated.get("d"));
        assertEquals("5", updated.get("e"));
    }

    // ================= OTHER INTERFACE METHODS =================

    public void testFromMap() {
        Map<String, String> labelMap = Map.of("key1", "value1", "key2", "value2");
        IndexedByteLabels labels = IndexedByteLabels.fromMap(labelMap);

        assertEquals("value1", labels.get("key1"));
        assertEquals("value2", labels.get("key2"));
    }

    public void testFromSortedKeyValuePairs() {
        List<String> pairs = List.of("app:frontend", "env:prod", "version:1.2.3");
        IndexedByteLabels labels = IndexedByteLabels.fromSortedKeyValuePairs(pairs);

        assertEquals("frontend", labels.get("app"));
        assertEquals("prod", labels.get("env"));
        assertEquals("1.2.3", labels.get("version"));
    }

    public void testDeepCopy() {
        IndexedByteLabels original = IndexedByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels copy = original.deepCopy();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    public void testExtractSortedNames() {
        IndexedByteLabels labels = IndexedByteLabels.fromStrings("z", "1", "a", "2", "m", "3");
        assertEquals(List.of("a", "m", "z"), labels.extractSortedNames());
    }

    public void testFindCommonNamesWithSortedList() {
        IndexedByteLabels labels = IndexedByteLabels.fromStrings("a", "1", "c", "3", "e", "5");
        List<String> common = labels.findCommonNamesWithSortedList(List.of("a", "b", "c", "d", "f"));
        assertEquals(List.of("a", "c"), common);
    }

    public void testToKeyValueBytesRefs() {
        IndexedByteLabels labels = IndexedByteLabels.fromStrings("app", "web", "env", "prod");
        BytesRef[] refs = labels.toKeyValueBytesRefs();
        assertEquals(2, refs.length);
        assertEquals("app:web", refs[0].utf8ToString());
        assertEquals("env:prod", refs[1].utf8ToString());
    }

    public void testLongStringEncoding() {
        String longValue = "x".repeat(300);
        IndexedByteLabels labels = IndexedByteLabels.fromStrings("longkey", longValue);
        assertEquals(longValue, labels.get("longkey"));
        assertTrue(labels.has("longkey"));
    }

    public void testSpecialCharacters() {
        IndexedByteLabels labels = IndexedByteLabels.fromStrings(
            "unicode",
            "测试",
            "symbols",
            "!@#$%^&*()",
            "delimiter",
            "key:value:with:colons"
        );

        assertEquals("测试", labels.get("unicode"));
        assertEquals("!@#$%^&*()", labels.get("symbols"));
        assertEquals("key:value:with:colons", labels.get("delimiter"));
    }

    public void testEqualsAndHashCode() {
        IndexedByteLabels labels1 = IndexedByteLabels.fromStrings("a", "1", "b", "2");
        IndexedByteLabels labels2 = IndexedByteLabels.fromStrings("a", "1", "b", "2");
        IndexedByteLabels labels3 = IndexedByteLabels.fromStrings("a", "1", "b", "different");

        assertEquals(labels1, labels2);
        assertNotEquals(labels1, labels3);
        assertEquals(labels1.hashCode(), labels2.hashCode());
    }

    public void testMemoryOverheadEstimation() {
        IndexedByteLabels empty = IndexedByteLabels.emptyLabels();
        long emptyEstimate = empty.estimateBytes();
        assertTrue("Empty labels estimate should be reasonable", emptyEstimate >= 48);

        IndexedByteLabels large = IndexedByteLabels.fromStrings("key1", "value1", "key2", "value2", "key3", "value3");
        assertTrue("Large labels estimate should be greater than empty", large.estimateBytes() > emptyEstimate);
    }

    public void testWithLabelInvalidInput() {
        IndexedByteLabels labels = IndexedByteLabels.fromStrings("a", "1");
        expectThrows(IllegalArgumentException.class, () -> labels.withLabel(null, "value"));
        expectThrows(IllegalArgumentException.class, () -> labels.withLabel("", "value"));
    }

    public void testWithLabelsInvalidName() {
        IndexedByteLabels labels = IndexedByteLabels.fromStrings("a", "1");
        expectThrows(IllegalArgumentException.class, () -> labels.withLabels(Map.of("", "value")));
    }

    public void testWithLabelsEmpty() {
        IndexedByteLabels labels = IndexedByteLabels.fromStrings("a", "1");
        assertSame("Empty map should return same instance", labels, labels.withLabels(Map.of()));
        assertSame("Null map should return same instance", labels, labels.withLabels(null));
    }
}
