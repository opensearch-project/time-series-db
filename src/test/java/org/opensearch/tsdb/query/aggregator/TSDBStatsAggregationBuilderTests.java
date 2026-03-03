/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Unit tests for TSDBStatsAggregationBuilder.
 *
 * <p>Test coverage includes:
 * <ul>
 *   <li>Constructor validation and parameter validation</li>
 *   <li>Getter methods</li>
 *   <li>Serialization and deserialization (wire format)</li>
 *   <li>XContent parsing and generation (JSON format)</li>
 *   <li>Equality and hash code</li>
 *   <li>Shallow copy functionality</li>
 * </ul>
 */
public class TSDBStatsAggregationBuilderTests extends OpenSearchTestCase {

    private static final String TEST_NAME = "test-tsdb-stats";
    private static final long MIN_TIMESTAMP = 1000L;
    private static final long MAX_TIMESTAMP = 2000L;

    // ========== Constructor Tests ==========

    public void testConstructorWithValidParameters() {
        // Arrange & Act
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Assert
        assertEquals(TEST_NAME, builder.getName());
        assertEquals(MIN_TIMESTAMP, builder.getMinTimestamp());
        assertEquals(MAX_TIMESTAMP, builder.getMaxTimestamp());
        assertTrue(builder.isIncludeValueStats());
    }

    public void testConstructorWithValueStatsDisabled() {
        // Arrange & Act
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, false);

        // Assert
        assertFalse(builder.isIncludeValueStats());
    }

    public void testConstructorRejectsInvalidTimeRange() {
        // Arrange
        long minTimestamp = 2000L;
        long maxTimestamp = 1000L;

        // Act & Assert
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TSDBStatsAggregationBuilder(TEST_NAME, minTimestamp, maxTimestamp, true)
        );
        assertTrue(exception.getMessage().contains("maxTimestamp must be greater than minTimestamp"));
        assertTrue(exception.getMessage().contains("minTimestamp=2000"));
        assertTrue(exception.getMessage().contains("maxTimestamp=1000"));
    }

    public void testConstructorRejectsEqualTimestamps() {
        // Arrange
        long timestamp = 1000L;

        // Act & Assert
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new TSDBStatsAggregationBuilder(TEST_NAME, timestamp, timestamp, true)
        );
        assertTrue(exception.getMessage().contains("maxTimestamp must be greater than minTimestamp"));
    }

    public void testConstructorWithLargeTimeRange() {
        // Arrange & Act
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, Long.MIN_VALUE, Long.MAX_VALUE, true);

        // Assert
        assertEquals(Long.MIN_VALUE, builder.getMinTimestamp());
        assertEquals(Long.MAX_VALUE, builder.getMaxTimestamp());
    }

    // ========== Getter Tests ==========

    public void testGetters() {
        // Arrange
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act & Assert
        assertEquals(MIN_TIMESTAMP, builder.getMinTimestamp());
        assertEquals(MAX_TIMESTAMP, builder.getMaxTimestamp());
        assertTrue(builder.isIncludeValueStats());
    }

    public void testGetType() {
        // Arrange
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act & Assert
        assertEquals("tsdb_stats_agg", builder.getType());
    }

    public void testBucketCardinality() {
        // Arrange
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act & Assert
        assertEquals(AggregationBuilder.BucketCardinality.NONE, builder.bucketCardinality());
    }

    // ========== Serialization Tests ==========

    public void testFullSerialization() throws IOException {
        // Arrange
        TSDBStatsAggregationBuilder original = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        TSDBStatsAggregationBuilder deserialized = new TSDBStatsAggregationBuilder(in);

        // Assert
        assertEquals(original, deserialized);
        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getMinTimestamp(), deserialized.getMinTimestamp());
        assertEquals(original.getMaxTimestamp(), deserialized.getMaxTimestamp());
        assertEquals(original.isIncludeValueStats(), deserialized.isIncludeValueStats());
    }

    public void testSerializationWithValueStatsDisabled() throws IOException {
        // Arrange
        TSDBStatsAggregationBuilder original = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, false);

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        TSDBStatsAggregationBuilder deserialized = new TSDBStatsAggregationBuilder(in);

        // Assert
        assertEquals(original, deserialized);
        assertFalse(deserialized.isIncludeValueStats());
    }

    public void testSerializationWithEdgeCaseTimestamps() throws IOException {
        // Arrange
        TSDBStatsAggregationBuilder original = new TSDBStatsAggregationBuilder(TEST_NAME, Long.MIN_VALUE, Long.MAX_VALUE, true);

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        TSDBStatsAggregationBuilder deserialized = new TSDBStatsAggregationBuilder(in);

        // Assert
        assertEquals(original, deserialized);
        assertEquals(Long.MIN_VALUE, deserialized.getMinTimestamp());
        assertEquals(Long.MAX_VALUE, deserialized.getMaxTimestamp());
    }

    // ========== XContent Tests ==========

    public void testXContentGeneration() throws IOException {
        // Arrange
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        builder.internalXContent(xContentBuilder, null);

        // Assert
        String json = xContentBuilder.toString();
        assertNotNull(json);
        assertTrue(json.contains("\"min_timestamp\":" + MIN_TIMESTAMP));
        assertTrue(json.contains("\"max_timestamp\":" + MAX_TIMESTAMP));
        assertTrue(json.contains("\"include_value_stats\":true"));
    }

    public void testXContentGenerationWithValueStatsDisabled() throws IOException {
        // Arrange
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, false);

        // Act
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        builder.internalXContent(xContentBuilder, null);

        // Assert
        String json = xContentBuilder.toString();
        assertTrue(json.contains("\"include_value_stats\":false"));
    }

    public void testXContentParsing() throws IOException {
        // Arrange
        String json = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"max_timestamp\":%d,\"include_value_stats\":true}",
            MIN_TIMESTAMP,
            MAX_TIMESTAMP
        );

        // Act
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken(); // Move to START_OBJECT
            TSDBStatsAggregationBuilder parsed = TSDBStatsAggregationBuilder.parse(TEST_NAME, parser);

            // Assert
            assertEquals(TEST_NAME, parsed.getName());
            assertEquals(MIN_TIMESTAMP, parsed.getMinTimestamp());
            assertEquals(MAX_TIMESTAMP, parsed.getMaxTimestamp());
            assertTrue(parsed.isIncludeValueStats());
        }
    }

    public void testXContentParsingWithValueStatsDisabled() throws IOException {
        // Arrange
        String json = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"max_timestamp\":%d,\"include_value_stats\":false}",
            MIN_TIMESTAMP,
            MAX_TIMESTAMP
        );

        // Act
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            TSDBStatsAggregationBuilder parsed = TSDBStatsAggregationBuilder.parse(TEST_NAME, parser);

            // Assert
            assertFalse(parsed.isIncludeValueStats());
        }
    }

    public void testXContentParsingMissingMinTimestamp() throws IOException {
        // Arrange
        String json = "{\"max_timestamp\":2000,\"include_value_stats\":true}";

        // Act & Assert
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TSDBStatsAggregationBuilder.parse(TEST_NAME, parser)
            );
            assertTrue(exception.getMessage().contains("Required parameter 'min_timestamp' is missing"));
            assertTrue(exception.getMessage().contains(TEST_NAME));
        }
    }

    public void testXContentParsingMissingMaxTimestamp() throws IOException {
        // Arrange
        String json = "{\"min_timestamp\":1000,\"include_value_stats\":true}";

        // Act & Assert
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TSDBStatsAggregationBuilder.parse(TEST_NAME, parser)
            );
            assertTrue(exception.getMessage().contains("Required parameter 'max_timestamp' is missing"));
        }
    }

    public void testXContentParsingMissingIncludeValueStats() throws IOException {
        // Arrange
        String json = "{\"min_timestamp\":1000,\"max_timestamp\":2000}";

        // Act & Assert
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TSDBStatsAggregationBuilder.parse(TEST_NAME, parser)
            );
            assertTrue(exception.getMessage().contains("Required parameter 'include_value_stats' is missing"));
        }
    }

    public void testXContentParsingWithUnknownFields() throws IOException {
        // Arrange - Should ignore unknown fields
        String json = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"max_timestamp\":%d,\"include_value_stats\":true,\"unknown_field\":\"value\"}",
            MIN_TIMESTAMP,
            MAX_TIMESTAMP
        );

        // Act
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            TSDBStatsAggregationBuilder parsed = TSDBStatsAggregationBuilder.parse(TEST_NAME, parser);

            // Assert - Should successfully parse, ignoring unknown field
            assertEquals(TEST_NAME, parsed.getName());
            assertEquals(MIN_TIMESTAMP, parsed.getMinTimestamp());
            assertEquals(MAX_TIMESTAMP, parsed.getMaxTimestamp());
        }
    }

    // ========== Equals and HashCode Tests ==========

    public void testEquals() {
        // Arrange
        TSDBStatsAggregationBuilder builder1 = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        TSDBStatsAggregationBuilder builder2 = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        TSDBStatsAggregationBuilder builder3 = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, false);
        TSDBStatsAggregationBuilder builder4 = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, 3000L, true);

        // Act & Assert
        assertEquals(builder1, builder2);
        assertNotEquals(builder1, builder3);
        assertNotEquals(builder1, builder4);
        assertEquals(builder1, builder1);
        assertNotEquals(builder1, null);
        assertNotEquals(builder1, new Object());
    }

    public void testHashCode() {
        // Arrange
        TSDBStatsAggregationBuilder builder1 = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        TSDBStatsAggregationBuilder builder2 = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act & Assert
        assertEquals(builder1.hashCode(), builder2.hashCode());
    }

    public void testHashCodeConsistency() {
        // Arrange
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act
        int hash1 = builder.hashCode();
        int hash2 = builder.hashCode();

        // Assert
        assertEquals(hash1, hash2);
    }

    // ========== Shallow Copy Tests ==========

    public void testShallowCopy() {
        // Arrange
        TSDBStatsAggregationBuilder original = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        org.opensearch.search.aggregations.AggregatorFactories.Builder subFactoriesBuilder =
            new org.opensearch.search.aggregations.AggregatorFactories.Builder();

        // Act
        TSDBStatsAggregationBuilder copy = (TSDBStatsAggregationBuilder) original.shallowCopy(subFactoriesBuilder, Map.of());

        // Assert
        assertEquals(original.getName(), copy.getName());
        assertEquals(original.getMinTimestamp(), copy.getMinTimestamp());
        assertEquals(original.getMaxTimestamp(), copy.getMaxTimestamp());
        assertEquals(original.isIncludeValueStats(), copy.isIncludeValueStats());
        assertNotSame(original, copy);
    }

    public void testShallowCopyWithMetadata() {
        // Arrange
        TSDBStatsAggregationBuilder original = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);
        Map<String, Object> metadata = Map.of("key1", "value1", "key2", 42);
        org.opensearch.search.aggregations.AggregatorFactories.Builder subFactoriesBuilder =
            new org.opensearch.search.aggregations.AggregatorFactories.Builder();

        // Act
        TSDBStatsAggregationBuilder copy = (TSDBStatsAggregationBuilder) original.shallowCopy(subFactoriesBuilder, metadata);

        // Assert
        assertEquals(original.getName(), copy.getName());
        assertEquals(original.getMinTimestamp(), copy.getMinTimestamp());
        assertEquals(original.getMaxTimestamp(), copy.getMaxTimestamp());
        assertEquals(original.isIncludeValueStats(), copy.isIncludeValueStats());
    }

    // ========== Edge Case Tests ==========

    public void testWithZeroMinTimestamp() {
        // Arrange & Act
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, 0L, 1000L, true);

        // Assert
        assertEquals(0L, builder.getMinTimestamp());
    }

    public void testWithNegativeTimestamps() {
        // Arrange & Act
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, -2000L, -1000L, true);

        // Assert
        assertEquals(-2000L, builder.getMinTimestamp());
        assertEquals(-1000L, builder.getMaxTimestamp());
    }

    public void testWithLargeTimestampRange() {
        // Arrange & Act
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, 0L, Long.MAX_VALUE, true);

        // Assert
        assertEquals(0L, builder.getMinTimestamp());
        assertEquals(Long.MAX_VALUE, builder.getMaxTimestamp());
    }

    // ========== Parse Edge Case Tests ==========

    public void testXContentParsingWithUnknownNumberField() throws IOException {
        // Tests the else branch in VALUE_NUMBER parsing (unknown number field -> skipChildren)
        String json = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"unknown_number\":12345,\"max_timestamp\":%d,\"include_value_stats\":true}",
            MIN_TIMESTAMP,
            MAX_TIMESTAMP
        );

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            TSDBStatsAggregationBuilder parsed = TSDBStatsAggregationBuilder.parse(TEST_NAME, parser);

            assertEquals(MIN_TIMESTAMP, parsed.getMinTimestamp());
            assertEquals(MAX_TIMESTAMP, parsed.getMaxTimestamp());
            assertTrue(parsed.isIncludeValueStats());
        }
    }

    public void testXContentParsingWithUnknownBooleanField() throws IOException {
        // Tests the else branch in VALUE_BOOLEAN parsing (unknown boolean field -> skipChildren)
        String json = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"max_timestamp\":%d,\"unknown_bool\":false,\"include_value_stats\":true}",
            MIN_TIMESTAMP,
            MAX_TIMESTAMP
        );

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            TSDBStatsAggregationBuilder parsed = TSDBStatsAggregationBuilder.parse(TEST_NAME, parser);

            assertEquals(MIN_TIMESTAMP, parsed.getMinTimestamp());
            assertEquals(MAX_TIMESTAMP, parsed.getMaxTimestamp());
            assertTrue(parsed.isIncludeValueStats());
        }
    }

    public void testXContentParsingWithNestedObject() throws IOException {
        // Tests the START_OBJECT/START_ARRAY skip branch in parsing
        String json = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"max_timestamp\":%d,\"include_value_stats\":true,\"nested\":{\"a\":1}}",
            MIN_TIMESTAMP,
            MAX_TIMESTAMP
        );

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            TSDBStatsAggregationBuilder parsed = TSDBStatsAggregationBuilder.parse(TEST_NAME, parser);

            assertEquals(MIN_TIMESTAMP, parsed.getMinTimestamp());
            assertEquals(MAX_TIMESTAMP, parsed.getMaxTimestamp());
        }
    }

    public void testXContentParsingWithNestedArray() throws IOException {
        // Tests the START_ARRAY skip branch in parsing
        String json = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"max_timestamp\":%d,\"include_value_stats\":true,\"arr\":[1,2,3]}",
            MIN_TIMESTAMP,
            MAX_TIMESTAMP
        );

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            TSDBStatsAggregationBuilder parsed = TSDBStatsAggregationBuilder.parse(TEST_NAME, parser);

            assertEquals(MIN_TIMESTAMP, parsed.getMinTimestamp());
            assertEquals(MAX_TIMESTAMP, parsed.getMaxTimestamp());
        }
    }

    // ========== Round Trip Tests ==========

    public void testRoundTripSerializationPreservesEquality() throws IOException {
        // Arrange
        TSDBStatsAggregationBuilder original = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true);

        // Act
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        TSDBStatsAggregationBuilder deserialized = new TSDBStatsAggregationBuilder(in);

        // Assert
        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
    }
}
