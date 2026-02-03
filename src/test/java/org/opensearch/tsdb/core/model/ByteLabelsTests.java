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

import java.util.ArrayList;
import java.util.Locale;
import java.util.List;
import java.util.Map;

public class ByteLabelsTests extends OpenSearchTestCase {

    public void testBasicFunctionality() {
        ByteLabels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");

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

        // Test toKeyValueString
        String kvString = labels.toKeyValueString();
        assertTrue("Should contain k1:v1", kvString.contains("k1:v1"));
        assertTrue("Should contain k2:v2", kvString.contains("k2:v2"));

        // Test toString
        assertEquals("toString should match toKeyValueString", kvString, labels.toString());
    }

    public void testInvalidInput() {
        expectThrows(IllegalArgumentException.class, () -> ByteLabels.fromStrings("k1", "v1", "k2"));
    }

    public void testWithLabels() {
        // Start with some existing labels
        ByteLabels original = ByteLabels.fromStrings("a", "1", "c", "3", "e", "5");

        // Add multiple labels at once
        Map<String, String> newLabels = Map.of("b", "2", "d", "4", "f", "6");
        Labels result = original.withLabels(newLabels);

        // Verify all labels are present (original + new)
        assertEquals("1", result.get("a"));
        assertEquals("2", result.get("b"));
        assertEquals("3", result.get("c"));
        assertEquals("4", result.get("d"));
        assertEquals("5", result.get("e"));
        assertEquals("6", result.get("f"));

        // Verify sorted order
        String kvString = result.toKeyValueString();
        assertTrue("Should maintain sorted order", kvString.indexOf("a:1") < kvString.indexOf("b:2"));
        assertTrue("Should maintain sorted order", kvString.indexOf("b:2") < kvString.indexOf("c:3"));
    }

    public void testWithLabelsUpdate() {
        // Start with existing labels
        ByteLabels original = ByteLabels.fromStrings("a", "1", "b", "2", "c", "3");

        // Update some and add new ones
        Map<String, String> updates = Map.of("b", "updated", "d", "new");
        Labels result = original.withLabels(updates);

        // Verify updates and additions
        assertEquals("1", result.get("a"));
        assertEquals("updated", result.get("b"));
        assertEquals("3", result.get("c"));
        assertEquals("new", result.get("d"));
    }

    public void testWithLabelsEmpty() {
        ByteLabels original = ByteLabels.fromStrings("a", "1", "b", "2");

        // Empty map should return same instance
        Labels result = original.withLabels(Map.of());
        assertSame("Empty map should return same instance", original, result);

        // Null map should also return same instance
        result = original.withLabels(null);
        assertSame("Null map should return same instance", original, result);
    }

    public void testWithLabelsNullValue() {
        ByteLabels original = ByteLabels.fromStrings("a", "1");

        // Null value should be converted to empty string
        Map<String, String> updates = new java.util.HashMap<>();
        updates.put("b", null);
        Labels result = original.withLabels(updates);

        assertEquals("", result.get("b"));
    }

    public void testWithLabelsInvalidName() {
        ByteLabels original = ByteLabels.fromStrings("a", "1");

        // Null name should throw
        Map<String, String> invalidNull = new java.util.HashMap<>();
        invalidNull.put(null, "value");
        expectThrows(IllegalArgumentException.class, () -> original.withLabels(invalidNull));

        // Empty name should throw
        Map<String, String> invalidEmpty = Map.of("", "value");
        expectThrows(IllegalArgumentException.class, () -> original.withLabels(invalidEmpty));
    }

    public void testWithLabelsVsChainedWithLabel() {
        // Verify that withLabels produces same result as chained withLabel calls
        ByteLabels original = ByteLabels.fromStrings("a", "1");

        // Using withLabels
        Map<String, String> newLabels = Map.of("b", "2", "c", "3", "d", "4");
        Labels batchResult = original.withLabels(newLabels);

        // Using chained withLabel
        Labels chainedResult = original.withLabel("b", "2").withLabel("c", "3").withLabel("d", "4");

        // Both should produce identical results
        assertEquals(batchResult.toKeyValueString(), chainedResult.toKeyValueString());
        assertEquals(batchResult.get("a"), chainedResult.get("a"));
        assertEquals(batchResult.get("b"), chainedResult.get("b"));
        assertEquals(batchResult.get("c"), chainedResult.get("c"));
        assertEquals(batchResult.get("d"), chainedResult.get("d"));
    }

    public void testWithLabelsOnEmpty() {
        ByteLabels empty = ByteLabels.emptyLabels();

        // Add labels to empty
        Map<String, String> newLabels = Map.of("a", "1", "b", "2");
        Labels result = empty.withLabels(newLabels);

        assertEquals("1", result.get("a"));
        assertEquals("2", result.get("b"));
        assertFalse(result.isEmpty());
    }

    public void testEmptyLabels() {
        ByteLabels empty = ByteLabels.emptyLabels();
        assertTrue(empty.isEmpty());
        assertEquals("", empty.toKeyValueString());
        assertEquals("", empty.toString());
        assertEquals("", empty.get("anything"));
        assertFalse(empty.has("anything"));

        ByteLabels emptyFromMap = ByteLabels.fromMap(Map.of());
        assertTrue(emptyFromMap.isEmpty());
        assertEquals(empty, emptyFromMap);
    }

    public void testLabelSorting() {
        ByteLabels labels1 = ByteLabels.fromStrings("zebra", "z", "apple", "a");
        ByteLabels labels2 = ByteLabels.fromStrings("apple", "a", "zebra", "z");

        assertEquals(labels1.toMapView(), labels2.toMapView());
        assertEquals(labels1.stableHash(), labels2.stableHash());
        assertEquals(labels1, labels2);
    }

    public void testStableHash() {
        ByteLabels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        ByteLabels labels2 = ByteLabels.fromStrings("k2", "v2", "k1", "v1");

        assertEquals(labels1.stableHash(), labels2.stableHash());
        assertEquals(labels1.hashCode(), labels2.hashCode());
    }

    public void testLongStringEncoding() {
        // Create a string longer than 254 bytes to test extended encoding
        String longKey = "very_long_key_" + "x".repeat(250);
        String longValue = "very_long_value_" + "y".repeat(250);

        ByteLabels labels = ByteLabels.fromStrings(longKey, longValue, "short", "val");

        assertEquals(longValue, labels.get(longKey));
        assertEquals("val", labels.get("short"));
        assertTrue(labels.has(longKey));

        // Verify it works with fromMap too
        ByteLabels labels2 = ByteLabels.fromMap(Map.of(longKey, longValue, "short", "val"));
        assertEquals(labels, labels2);
    }

    public void testEqualsAndHashCode() {
        ByteLabels labels1 = ByteLabels.fromStrings("a", "1", "b", "2");
        ByteLabels labels2 = ByteLabels.fromStrings("b", "2", "a", "1"); // Different order
        ByteLabels labels3 = ByteLabels.fromStrings("a", "1", "b", "3"); // Different value

        // Test equals
        assertEquals("Same labels should be equal", labels1, labels2);
        assertNotEquals("Different labels should not be equal", labels1, labels3);

        // Test hashCode consistency
        assertEquals("Equal objects should have same hashCode", labels1.hashCode(), labels2.hashCode());
    }

    public void testToKeyValueBytesRefs() {
        ByteLabels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        BytesRef[] result = labels.toKeyValueBytesRefs();

        assertEquals("Should have 2 label pairs", 2, result.length);

        // Verify the content of each BytesRef
        assertEquals("k1" + LabelConstants.LABEL_DELIMITER + "v1", result[0].utf8ToString());
        assertEquals("k2" + LabelConstants.LABEL_DELIMITER + "v2", result[1].utf8ToString());

        // Verify they are in sorted order (k1 before k2)
        assertEquals("First label should be k1 v1", "k1" + LabelConstants.LABEL_DELIMITER + "v1", result[0].utf8ToString());
        assertEquals("Second label should be k2 v2", "k2" + LabelConstants.LABEL_DELIMITER + "v2", result[1].utf8ToString());
    }

    public void testToKeyValueBytesRefsEmpty() {
        ByteLabels empty = ByteLabels.emptyLabels();
        BytesRef[] result = empty.toKeyValueBytesRefs();

        assertEquals("Empty labels should return empty array", 0, result.length);
    }

    public void testToKeyValueBytesRefsWithLongStrings() {
        // >250 is chosen to test decoding with ByteLabels' var length encoding
        String longKey = "very_long_key_" + "x".repeat(250);
        String longValue = "very_long_value_" + "y".repeat(250);

        ByteLabels labels = ByteLabels.fromStrings(longKey, longValue, "short", "val");
        BytesRef[] result = labels.toKeyValueBytesRefs();

        assertEquals("Should have 2 label pairs", 2, result.length);

        // Find which result corresponds to which label (they're sorted)
        BytesRef shortResult = null, longResult = null;
        for (BytesRef ref : result) {
            if (ref.utf8ToString().startsWith("short")) {
                shortResult = ref;
            } else if (ref.utf8ToString().startsWith("very_long_key_")) {
                longResult = ref;
            }
        }

        assertNotNull("Should find short label", shortResult);
        assertNotNull("Should find long label", longResult);
        assertEquals("short" + LabelConstants.LABEL_DELIMITER + "val", shortResult.utf8ToString());
        assertEquals(longKey + LabelConstants.LABEL_DELIMITER + longValue, longResult.utf8ToString());
    }

    public void testToKeyValueBytesRefsConsistencyWithToKeyValueString() {
        ByteLabels labels = ByteLabels.fromStrings("a", "1", "b", "2", "c", "3");

        BytesRef[] bytesRefs = labels.toKeyValueBytesRefs();
        String keyValueString = labels.toKeyValueString();

        // Convert BytesRef array to space-separated string
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < bytesRefs.length; i++) {
            if (i > 0) sb.append(" ");
            sb.append(bytesRefs[i].utf8ToString());
        }

        assertEquals("BytesRef array should match toKeyValueString", keyValueString, sb.toString());
    }

    public void testFromSortedKeyValuePairs() {
        List<String> keyValuePairs = List.of(
            "a" + LabelConstants.LABEL_DELIMITER + "1",
            "b" + LabelConstants.LABEL_DELIMITER + "2",
            "c" + LabelConstants.LABEL_DELIMITER + "3"
        );
        ByteLabels labels = ByteLabels.fromSortedKeyValuePairs(keyValuePairs);

        assertEquals("1", labels.get("a"));
        assertEquals("2", labels.get("b"));
        assertEquals("3", labels.get("c"));
        assertEquals("", labels.get("nonexistent"));
        assertTrue(labels.has("a"));
        assertTrue(labels.has("b"));
        assertTrue(labels.has("c"));
        assertFalse(labels.has("nonexistent"));
        assertFalse(labels.isEmpty());

        String kvString = labels.toKeyValueString();
        assertEquals(
            "a" + LabelConstants.LABEL_DELIMITER + "1 b" + LabelConstants.LABEL_DELIMITER + "2 c" + LabelConstants.LABEL_DELIMITER + "3",
            kvString
        );
    }

    public void testFromSortedKeyValuePairsEmpty() {
        ByteLabels empty1 = ByteLabels.fromSortedKeyValuePairs(null);
        ByteLabels empty2 = ByteLabels.fromSortedKeyValuePairs(List.of());

        assertTrue(empty1.isEmpty());
        assertTrue(empty2.isEmpty());
        assertEquals(ByteLabels.emptyLabels(), empty1);
        assertEquals(ByteLabels.emptyLabels(), empty2);
    }

    public void testFromSortedKeyValuePairsInvalidInput() {
        // Missing delimiter
        expectThrows(IllegalArgumentException.class, () -> ByteLabels.fromSortedKeyValuePairs(List.of("key_without_delim")));

        // Delimiter at start
        expectThrows(
            IllegalArgumentException.class,
            () -> ByteLabels.fromSortedKeyValuePairs(List.of(LabelConstants.LABEL_DELIMITER + "value"))
        );

        // Only delimiter
        expectThrows(
            IllegalArgumentException.class,
            () -> ByteLabels.fromSortedKeyValuePairs(List.of(String.valueOf(LabelConstants.LABEL_DELIMITER)))
        );
    }

    public void testFromSortedKeyValuePairsEmptyValue() {
        Labels labels = ByteLabels.fromSortedKeyValuePairs(List.of("key" + LabelConstants.LABEL_DELIMITER));

        assertTrue(labels.has("key"));
        assertEquals(labels.get("key"), "");
        assertEquals("key" + LabelConstants.LABEL_DELIMITER, labels.toKeyValueString());
    }

    public void testFromSortedKeyValuePairsConsistencyWithFromStrings() {
        List<String> keyValuePairs = List.of(
            "a" + LabelConstants.LABEL_DELIMITER + "1",
            "b" + LabelConstants.LABEL_DELIMITER + "2",
            "z" + LabelConstants.LABEL_DELIMITER + "9"
        );
        ByteLabels labelsFromKV = ByteLabels.fromSortedKeyValuePairs(keyValuePairs);
        ByteLabels labelsFromStrings = ByteLabels.fromStrings("a", "1", "b", "2", "z", "9");

        assertEquals(labelsFromKV, labelsFromStrings);
        assertEquals(labelsFromKV.stableHash(), labelsFromStrings.stableHash());
        assertEquals(labelsFromKV.toKeyValueString(), labelsFromStrings.toKeyValueString());
    }

    public void testFromSortedKeyValuePairsLongValues() {
        // >250 is chosen to test decoding with ByteLabels' var length encoding
        String longKey = "very_long_key_" + "x".repeat(250);
        String longValue = "very_long_value_" + "y".repeat(250);
        List<String> keyValuePairs = List.of(
            "a" + LabelConstants.LABEL_DELIMITER + "1",
            longKey + LabelConstants.LABEL_DELIMITER + longValue
        );

        ByteLabels labels = ByteLabels.fromSortedKeyValuePairs(keyValuePairs);

        assertEquals("1", labels.get("a"));
        assertEquals(longValue, labels.get(longKey));
        assertTrue(labels.has("a"));
        assertTrue(labels.has(longKey));
    }

    public void testFromSortedStringsInvalidInput() {
        ByteLabels labels = ByteLabels.fromSortedStrings((String[]) null);
        assertEquals(ByteLabels.emptyLabels(), labels);
        assertTrue(labels.isEmpty());

        expectThrows(IllegalArgumentException.class, () -> ByteLabels.fromSortedStrings("k1", "v1", "k2"));
    }

    // Tests for withLabel API

    public void testWithLabelInsertNew() {
        // Test inserting new labels at different positions
        ByteLabels base = ByteLabels.fromStrings("b", "2", "d", "4");

        // Insert at beginning
        Labels result1 = base.withLabel("a", "1");
        assertEquals("1", result1.get("a"));
        assertEquals("2", result1.get("b"));
        assertEquals("4", result1.get("d"));
        assertEquals("a:1 b:2 d:4", result1.toKeyValueString());

        // Insert in middle
        Labels result2 = base.withLabel("c", "3");
        assertEquals("2", result2.get("b"));
        assertEquals("3", result2.get("c"));
        assertEquals("4", result2.get("d"));
        assertEquals("b:2 c:3 d:4", result2.toKeyValueString());

        // Insert at end
        Labels result3 = base.withLabel("e", "5");
        assertEquals("2", result3.get("b"));
        assertEquals("4", result3.get("d"));
        assertEquals("5", result3.get("e"));
        assertEquals("b:2 d:4 e:5", result3.toKeyValueString());
    }

    public void testWithLabelUpdateExisting() {
        ByteLabels base = ByteLabels.fromStrings("a", "1", "b", "2", "c", "3");

        // Update first label
        Labels result1 = base.withLabel("a", "new_value_1");
        assertEquals("new_value_1", result1.get("a"));
        assertEquals("2", result1.get("b"));
        assertEquals("3", result1.get("c"));

        // Update middle label
        Labels result2 = base.withLabel("b", "new_value_2");
        assertEquals("1", result2.get("a"));
        assertEquals("new_value_2", result2.get("b"));
        assertEquals("3", result2.get("c"));

        // Update last label
        Labels result3 = base.withLabel("c", "new_value_3");
        assertEquals("1", result3.get("a"));
        assertEquals("2", result3.get("b"));
        assertEquals("new_value_3", result3.get("c"));
    }

    public void testWithLabelUpdateSameValue() {
        ByteLabels base = ByteLabels.fromStrings("a", "1", "b", "2");

        // Update with same value should return the same instance
        Labels result = base.withLabel("a", "1");
        assertSame("Should return same instance when value unchanged", base, result);

        // Update with different value should return new instance
        Labels result2 = base.withLabel("a", "different");
        assertNotSame("Should return new instance when value changed", base, result2);
        assertEquals("different", result2.get("a"));
    }

    public void testWithLabelEmptyLabels() {
        ByteLabels empty = ByteLabels.emptyLabels();

        Labels result = empty.withLabel("first", "value");
        assertEquals("value", result.get("first"));
        assertEquals("first:value", result.toKeyValueString());
        assertFalse(result.isEmpty());
    }

    public void testWithLabelNullValue() {
        ByteLabels base = ByteLabels.fromStrings("a", "1");

        // Null value should be treated as empty string
        Labels result = base.withLabel("b", null);
        assertEquals("", result.get("b"));
        assertTrue(result.has("b"));
    }

    public void testWithLabelInvalidInput() {
        ByteLabels base = ByteLabels.fromStrings("a", "1");

        // Null name should throw exception
        expectThrows(IllegalArgumentException.class, () -> base.withLabel(null, "value"));

        // Empty name should throw exception
        expectThrows(IllegalArgumentException.class, () -> base.withLabel("", "value"));
    }

    public void testWithLabelMaintainsSortOrder() {
        ByteLabels base = ByteLabels.fromStrings("b", "2", "d", "4", "f", "6");

        // Add multiple labels in unsorted order
        Labels result = base.withLabel("z", "26")  // Add at end
            .withLabel("a", "1")   // Add at beginning
            .withLabel("c", "3")   // Add in middle
            .withLabel("e", "5");  // Add in middle

        // Verify all labels are present and sorted
        assertEquals("1", result.get("a"));
        assertEquals("2", result.get("b"));
        assertEquals("3", result.get("c"));
        assertEquals("4", result.get("d"));
        assertEquals("5", result.get("e"));
        assertEquals("6", result.get("f"));
        assertEquals("26", result.get("z"));

        // Verify string representation maintains sort order
        assertEquals("a:1 b:2 c:3 d:4 e:5 f:6 z:26", result.toKeyValueString());
    }

    public void testWithLabelChaining() {
        ByteLabels base = ByteLabels.fromStrings("a", "1");

        // Test method chaining
        Labels result = base.withLabel("b", "2")
            .withLabel("c", "3")
            .withLabel("a", "updated")  // Update existing
            .withLabel("d", "4");

        assertEquals("updated", result.get("a"));
        assertEquals("2", result.get("b"));
        assertEquals("3", result.get("c"));
        assertEquals("4", result.get("d"));
        assertEquals("a:updated b:2 c:3 d:4", result.toKeyValueString());
    }

    public void testWithLabelHashAndEquals() {
        ByteLabels base1 = ByteLabels.fromStrings("a", "1", "b", "2");
        ByteLabels base2 = ByteLabels.fromStrings("a", "1");

        // Add same label to both
        Labels result1 = base1.withLabel("c", "3");
        Labels result2 = base2.withLabel("b", "2").withLabel("c", "3");

        // Results should be equal
        assertEquals(result1, result2);
        assertEquals(result1.hashCode(), result2.hashCode());
        assertEquals(result1.stableHash(), result2.stableHash());
    }

    public void testWithLabelPercentileExample() {
        // Test the specific use case mentioned in the requirements
        ByteLabels base = ByteLabels.fromStrings("service", "api", "region", "us-east");

        // Add percentile label
        Labels withP50 = base.withLabel("percentile", "p50");
        assertEquals("p50", withP50.get("percentile"));
        assertEquals("api", withP50.get("service"));
        assertEquals("us-east", withP50.get("region"));

        // Update percentile label
        Labels withP95 = withP50.withLabel("percentile", "p95");
        assertEquals("p95", withP95.get("percentile"));
        assertEquals("api", withP95.get("service"));
        assertEquals("us-east", withP95.get("region"));

        // Verify sorted order includes percentile in correct position
        assertTrue(withP95.toKeyValueString().contains("percentile:p95"));
    }

    public void testGetCommonTagKeys() {
        ByteLabels labels1 = ByteLabels.fromStrings("a", "1", "b", "2", "c", "3");
        ByteLabels labels2 = ByteLabels.fromStrings("b", "2", "c", "3", "d", "4");

        List<String> commonKeys = Labels.findCommonLabelNames(List.of(labels1, labels2));
        assertEquals(2, commonKeys.size());
        assertEquals("b", commonKeys.get(0));
        assertEquals("c", commonKeys.get(1));
    }

    public void testGetCommonTagKeysAllCommon() {
        ByteLabels labels1 = ByteLabels.fromStrings("a", "1", "b", "2", "c", "3");
        ByteLabels labels2 = ByteLabels.fromStrings("a", "10", "b", "20", "c", "30");

        List<String> commonKeys = Labels.findCommonLabelNames(List.of(labels1, labels2));
        assertEquals(3, commonKeys.size());
        assertEquals("a", commonKeys.get(0));
        assertEquals("b", commonKeys.get(1));
        assertEquals("c", commonKeys.get(2));
    }

    public void testGetCommonTagKeysEmptyLabels() {
        ByteLabels labels1 = ByteLabels.emptyLabels();
        ByteLabels labels2 = ByteLabels.fromStrings("a", "1", "b", "2");

        List<String> commonKeys = Labels.findCommonLabelNames(List.of(labels1, labels2));
        assertTrue(commonKeys.isEmpty());

        commonKeys = Labels.findCommonLabelNames(List.of(labels2, labels1));
        assertTrue(commonKeys.isEmpty());
    }

    public void testGetCommonTagKeysEarlyTermination() {
        // Test case where commonKeys becomes empty during iteration (early termination)
        ByteLabels labels1 = ByteLabels.fromStrings("a", "1", "b", "2", "c", "3");
        ByteLabels labels2 = ByteLabels.fromStrings("d", "4", "e", "5", "f", "6");
        ByteLabels labels3 = ByteLabels.fromStrings("g", "7", "h", "8");

        // labels1 and labels2 have no common keys, so result should be empty
        // This tests the early termination when commonKeys becomes empty
        List<String> commonKeys = Labels.findCommonLabelNames(List.of(labels1, labels2, labels3));
        assertTrue(commonKeys.isEmpty());
    }

    public void testGetCommonTagKeysWithNull() {
        ByteLabels labels1 = ByteLabels.fromStrings("a", "1", "b", "2");
        ByteLabels labels2 = ByteLabels.fromStrings("a", "10", "b", "20");

        // Null in the list should return empty
        List<Labels> listWithNull = new ArrayList<>();
        listWithNull.add(labels1);
        listWithNull.add(null);
        listWithNull.add(labels2);
        List<String> commonKeys = Labels.findCommonLabelNames(listWithNull);
        assertTrue(commonKeys.isEmpty());
    }

    public void testGetCommonTagKeysNullList() {
        List<String> commonKeys = Labels.findCommonLabelNames(null);
        assertTrue(commonKeys.isEmpty());
    }

    public void testGetCommonTagKeysEmptyList() {
        List<String> commonKeys = Labels.findCommonLabelNames(List.of());
        assertTrue(commonKeys.isEmpty());
    }

    public void testFindCommonKeysWithSortedListEmptySortedKeys() {
        // Empty sortedKeys should return empty list
        ByteLabels labels = ByteLabels.fromStrings("a", "1", "b", "2", "c", "3");
        List<String> emptySortedKeys = List.of();

        List<String> result = labels.findCommonNamesWithSortedList(emptySortedKeys);
        assertTrue("Empty sortedKeys should return empty list", result.isEmpty());
    }

    public void testFindCommonKeysWithSortedListEmptyByteLabels() {
        // Empty ByteLabels should return empty list
        ByteLabels emptyLabels = ByteLabels.emptyLabels();
        List<String> sortedKeys = List.of("a", "b", "c");

        List<String> result = emptyLabels.findCommonNamesWithSortedList(sortedKeys);
        assertTrue("Empty ByteLabels should return empty list", result.isEmpty());
    }

    public void testFindCommonKeysWithSortedListByteLabelsKeySmaller() {
        // ByteLabels key is smaller (cmp < 0) - should advance ByteLabels position
        // ByteLabels has keys: a, c, e
        // Sorted list has keys: b, c, d
        // When comparing a < b, should advance ByteLabels position
        ByteLabels labels = ByteLabels.fromStrings("a", "1", "c", "3", "e", "5");
        List<String> sortedKeys = List.of("b", "c", "d");

        List<String> result = labels.findCommonNamesWithSortedList(sortedKeys);
        assertEquals(1, result.size());
        assertEquals("c", result.get(0));
    }

    public void testFindCommonKeysWithSortedListSortedKeySmaller() {
        // Sorted list key is smaller (cmp > 0) - should advance sorted list index
        // ByteLabels has keys: c, e, g
        // Sorted list has keys: a, b, c, d
        // When comparing c > a, should advance sorted list index
        ByteLabels labels = ByteLabels.fromStrings("c", "3", "e", "5", "g", "7");
        List<String> sortedKeys = List.of("a", "b", "c", "d");

        List<String> result = labels.findCommonNamesWithSortedList(sortedKeys);
        assertEquals(1, result.size());
        assertEquals("c", result.get(0));
    }

    public void testFindCommonKeysWithSortedListAlternatingComparison() {
        // Test both comparison branches in sequence
        // ByteLabels: a, c, e, g
        // Sorted list: b, c, f, g
        // Sequence: a < b (advance ByteLabels), c == c (match), e < f (advance ByteLabels), g == g (match)
        ByteLabels labels = ByteLabels.fromStrings("a", "1", "c", "3", "e", "5", "g", "7");
        List<String> sortedKeys = List.of("b", "c", "f", "g");

        List<String> result = labels.findCommonNamesWithSortedList(sortedKeys);
        assertEquals(2, result.size());
        assertEquals("c", result.get(0));
        assertEquals("g", result.get(1));
    }

    public void testFindCommonKeysWithSortedListNoMatches() {
        // Test case where ByteLabels keys are all smaller than sorted list keys
        // ByteLabels: a, b, c
        // Sorted list: d, e, f
        // All comparisons will be cmp < 0, advancing ByteLabels position
        ByteLabels labels = ByteLabels.fromStrings("a", "1", "b", "2", "c", "3");
        List<String> sortedKeys = List.of("d", "e", "f");

        List<String> result = labels.findCommonNamesWithSortedList(sortedKeys);
        assertTrue("No matches should return empty list", result.isEmpty());
    }

    public void testFindCommonKeysWithSortedListSortedKeysAllSmaller() {
        // Test case where sorted list keys are all smaller than ByteLabels keys
        // ByteLabels: d, e, f
        // Sorted list: a, b, c
        // All comparisons will be cmp > 0, advancing sorted list index
        ByteLabels labels = ByteLabels.fromStrings("d", "4", "e", "5", "f", "6");
        List<String> sortedKeys = List.of("a", "b", "c");

        List<String> result = labels.findCommonNamesWithSortedList(sortedKeys);
        assertTrue("No matches should return empty list", result.isEmpty());
    }

    /**
     * Validate that ESTIMATED_MEMORY_OVERHEAD constant matches actual JVM object layout.
     * This test uses JOL (Java Object Layout) to calculate the actual memory overhead
     * and compares it with the hardcoded constant.
     *
     * <p>If this test fails, it means:
     * <ul>
     *   <li>Fields were added/removed from ByteLabels without updating ESTIMATED_MEMORY_OVERHEAD</li>
     *   <li>The JVM's object layout has changed (e.g., different JVM vendor/version)</li>
     * </ul>
     *
     * <p><strong>To fix:</strong> Update ByteLabels.ESTIMATED_MEMORY_OVERHEAD to the value shown
     * in the test failure message.
     */
    public void testEstimatedMemoryOverheadIsAccurate() {
        // Use JOL (Java Object Layout) to get actual memory layout
        org.openjdk.jol.info.ClassLayout byteLabelsLayout;
        org.openjdk.jol.info.ClassLayout byteArrayLayout;

        try {
            // Create an empty ByteLabels instance
            ByteLabels empty = ByteLabels.fromStrings();

            // Get the layout of the ByteLabels object
            byteLabelsLayout = org.openjdk.jol.info.ClassLayout.parseInstance(empty);

            // Get the layout of the internal byte array
            byte[] dataArray = empty.getDataForTesting();
            byteArrayLayout = org.openjdk.jol.info.ClassLayout.parseInstance(dataArray);

            // Calculate actual overhead:
            // - ByteLabels object size (includes object header + fields)
            // - byte[] array header (does NOT include array data)
            long actualOverhead = byteLabelsLayout.instanceSize() + byteArrayLayout.instanceSize();

            long constantOverhead = ByteLabels.getEstimatedMemoryOverhead();

            // Allow small variance for JVM-specific differences (alignment, compressed oops, etc.)
            // Typically the difference should be 0, but allow up to 8 bytes for padding variations
            long allowedDelta = 8;
            long difference = Math.abs(actualOverhead - constantOverhead);

            if (difference > allowedDelta) {
                fail(
                    String.format(
                        Locale.ROOT,
                        "ESTIMATED_MEMORY_OVERHEAD constant (%d bytes) does not match actual JVM layout (%d bytes)!\n"
                            + "\n"
                            + "ByteLabels object layout:\n%s\n"
                            + "byte[] array layout:\n%s\n"
                            + "\n"
                            + "ACTION REQUIRED: Update ByteLabels.ESTIMATED_MEMORY_OVERHEAD to %d\n"
                            + "\n"
                            + "This usually happens when:\n"
                            + "  1. Fields were added/removed from ByteLabels\n"
                            + "  2. JVM version or vendor changed\n"
                            + "  3. JVM flags changed (e.g., -XX:+UseCompressedOops)",
                        constantOverhead,
                        actualOverhead,
                        byteLabelsLayout.toPrintable(),
                        byteArrayLayout.toPrintable(),
                        actualOverhead
                    )
                );
            }

            // Test passes - constant is accurate!
            // Log the breakdown for documentation
            logger.info(
                "ByteLabels memory overhead validation passed:\n"
                    + "  ESTIMATED_MEMORY_OVERHEAD constant: {} bytes\n"
                    + "  Actual JVM layout: {} bytes\n"
                    + "  ByteLabels object: {} bytes\n"
                    + "  byte[] array header: {} bytes",
                constantOverhead,
                actualOverhead,
                byteLabelsLayout.instanceSize(),
                byteArrayLayout.instanceSize()
            );

        } catch (Exception e) {
            fail("Failed to validate memory overhead using JOL: " + e.getMessage());
        }
    }
}
