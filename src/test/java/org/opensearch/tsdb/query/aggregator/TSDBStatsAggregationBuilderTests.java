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
 * <p>Covers: constructor validation, serialization roundtrip, XContent parsing/generation,
 * equals/hashCode, and shallow copy. Follows the test minimalism rule — minimum tests for ≥85% coverage.</p>
 */
public class TSDBStatsAggregationBuilderTests extends OpenSearchTestCase {

    private static final String TEST_NAME = "test-tsdb-stats";
    private static final long MIN_TIMESTAMP = 1000L;
    private static final long MAX_TIMESTAMP = 2000L;

    // ========== Constructor Tests ==========

    public void testConstructorWithValidParameters() {
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true, true);

        assertEquals(TEST_NAME, builder.getName());
        assertEquals(MIN_TIMESTAMP, builder.getMinTimestamp());
        assertEquals(MAX_TIMESTAMP, builder.getMaxTimestamp());
        assertTrue(builder.isIncludeValueStats());
        assertTrue(builder.isIncludeHeadStats());
        assertEquals("tsdb_stats_agg", builder.getType());
        assertEquals(AggregationBuilder.BucketCardinality.NONE, builder.bucketCardinality());
    }

    public void testConstructorRejectsInvalidTimeRange() {
        // max < min
        IllegalArgumentException ex1 = expectThrows(
            IllegalArgumentException.class,
            () -> new TSDBStatsAggregationBuilder(TEST_NAME, 2000L, 1000L, true, true)
        );
        assertTrue(ex1.getMessage().contains("maxTimestamp must be greater than minTimestamp"));

        // max == min
        IllegalArgumentException ex2 = expectThrows(
            IllegalArgumentException.class,
            () -> new TSDBStatsAggregationBuilder(TEST_NAME, 1000L, 1000L, true, true)
        );
        assertTrue(ex2.getMessage().contains("maxTimestamp must be greater than minTimestamp"));
    }

    // ========== Serialization Tests ==========

    public void testSerializationRoundTrip() throws IOException {
        // includeValueStats=true, includeHeadStats=true
        TSDBStatsAggregationBuilder original = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true, true);
        TSDBStatsAggregationBuilder deserialized = serializeAndDeserialize(original);

        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getMinTimestamp(), deserialized.getMinTimestamp());
        assertEquals(original.getMaxTimestamp(), deserialized.getMaxTimestamp());
        assertEquals(original.isIncludeValueStats(), deserialized.isIncludeValueStats());
        assertEquals(original.isIncludeHeadStats(), deserialized.isIncludeHeadStats());

        // includeValueStats=false, includeHeadStats=false
        TSDBStatsAggregationBuilder original2 = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, false, false);
        TSDBStatsAggregationBuilder deserialized2 = serializeAndDeserialize(original2);
        assertEquals(original2, deserialized2);
        assertFalse(deserialized2.isIncludeValueStats());
        assertFalse(deserialized2.isIncludeHeadStats());
    }

    public void testSerializationWithEdgeCaseTimestamps() throws IOException {
        TSDBStatsAggregationBuilder original = new TSDBStatsAggregationBuilder(TEST_NAME, Long.MIN_VALUE, Long.MAX_VALUE, true, true);
        TSDBStatsAggregationBuilder deserialized = serializeAndDeserialize(original);

        assertEquals(original, deserialized);
        assertEquals(Long.MIN_VALUE, deserialized.getMinTimestamp());
        assertEquals(Long.MAX_VALUE, deserialized.getMaxTimestamp());
    }

    // ========== XContent Tests ==========

    public void testXContentGeneration() throws IOException {
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true, true);

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        builder.internalXContent(xContentBuilder, null);

        String json = xContentBuilder.toString();
        assertTrue(json.contains("\"min_timestamp\":" + MIN_TIMESTAMP));
        assertTrue(json.contains("\"max_timestamp\":" + MAX_TIMESTAMP));
        assertTrue(json.contains("\"include_value_stats\":true"));
        assertTrue(json.contains("\"include_head_stats\":true"));
    }

    public void testXContentParsing() throws IOException {
        // Full round-trip: parse with all fields
        String json = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"max_timestamp\":%d,\"include_value_stats\":true,\"include_head_stats\":true}",
            MIN_TIMESTAMP,
            MAX_TIMESTAMP
        );

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            TSDBStatsAggregationBuilder parsed = TSDBStatsAggregationBuilder.parse(TEST_NAME, parser);

            assertEquals(TEST_NAME, parsed.getName());
            assertEquals(MIN_TIMESTAMP, parsed.getMinTimestamp());
            assertEquals(MAX_TIMESTAMP, parsed.getMaxTimestamp());
            assertTrue(parsed.isIncludeValueStats());
            assertTrue(parsed.isIncludeHeadStats());
        }

        // With include_value_stats=false, include_head_stats=false
        String json2 = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"max_timestamp\":%d,\"include_value_stats\":false,\"include_head_stats\":false}",
            MIN_TIMESTAMP,
            MAX_TIMESTAMP
        );

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json2)) {
            parser.nextToken();
            TSDBStatsAggregationBuilder parsed = TSDBStatsAggregationBuilder.parse(TEST_NAME, parser);
            assertFalse(parsed.isIncludeValueStats());
            assertFalse(parsed.isIncludeHeadStats());
        }
    }

    public void testXContentParsingMissingRequiredFields() throws IOException {
        // Missing min_timestamp
        try (
            XContentParser parser = createParser(
                XContentType.JSON.xContent(),
                "{\"max_timestamp\":2000,\"include_value_stats\":true,\"include_head_stats\":true}"
            )
        ) {
            parser.nextToken();
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> TSDBStatsAggregationBuilder.parse(TEST_NAME, parser)
            );
            assertTrue(ex.getMessage().contains("Required parameter 'min_timestamp' is missing"));
        }

        // Missing max_timestamp
        try (
            XContentParser parser = createParser(
                XContentType.JSON.xContent(),
                "{\"min_timestamp\":1000,\"include_value_stats\":true,\"include_head_stats\":true}"
            )
        ) {
            parser.nextToken();
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> TSDBStatsAggregationBuilder.parse(TEST_NAME, parser)
            );
            assertTrue(ex.getMessage().contains("Required parameter 'max_timestamp' is missing"));
        }

        // Missing include_value_stats
        try (
            XContentParser parser = createParser(
                XContentType.JSON.xContent(),
                "{\"min_timestamp\":1000,\"max_timestamp\":2000,\"include_head_stats\":true}"
            )
        ) {
            parser.nextToken();
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> TSDBStatsAggregationBuilder.parse(TEST_NAME, parser)
            );
            assertTrue(ex.getMessage().contains("Required parameter 'include_value_stats' is missing"));
        }

        // Missing include_head_stats: should default to false (not throw)
        try (
            XContentParser parser = createParser(
                XContentType.JSON.xContent(),
                "{\"min_timestamp\":1000,\"max_timestamp\":2000,\"include_value_stats\":true}"
            )
        ) {
            parser.nextToken();
            TSDBStatsAggregationBuilder parsed = TSDBStatsAggregationBuilder.parse(TEST_NAME, parser);
            assertFalse("include_head_stats should default to false when not provided", parsed.isIncludeHeadStats());
        }
    }

    public void testXContentParsingWithUnknownFields() throws IOException {
        // Unknown string, number, boolean, object, and array fields should all be skipped
        String json = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"unknown_num\":99,\"max_timestamp\":%d,"
                + "\"unknown_bool\":false,\"include_value_stats\":true,\"include_head_stats\":true,"
                + "\"nested\":{\"a\":1},\"arr\":[1,2],\"unknown_str\":\"val\"}",
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

    // ========== Equals and HashCode Tests ==========

    public void testEqualsAndHashCode() {
        TSDBStatsAggregationBuilder builder1 = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true, true);
        TSDBStatsAggregationBuilder builder2 = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true, true);
        TSDBStatsAggregationBuilder differentValueStats = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            MIN_TIMESTAMP,
            MAX_TIMESTAMP,
            false,
            true
        );
        TSDBStatsAggregationBuilder differentHeadStats = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            MIN_TIMESTAMP,
            MAX_TIMESTAMP,
            true,
            false
        );
        TSDBStatsAggregationBuilder differentMax = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, 3000L, true, true);

        // equals
        assertEquals(builder1, builder2);
        assertEquals(builder1, builder1);
        assertNotEquals(builder1, differentValueStats);
        assertNotEquals(builder1, differentHeadStats);
        assertNotEquals(builder1, differentMax);
        assertNotEquals(builder1, null);
        assertNotEquals(builder1, new Object());

        // hashCode
        assertEquals(builder1.hashCode(), builder2.hashCode());
    }

    // ========== Shallow Copy Tests ==========

    public void testShallowCopy() {
        TSDBStatsAggregationBuilder original = new TSDBStatsAggregationBuilder(TEST_NAME, MIN_TIMESTAMP, MAX_TIMESTAMP, true, true);
        org.opensearch.search.aggregations.AggregatorFactories.Builder subFactoriesBuilder =
            new org.opensearch.search.aggregations.AggregatorFactories.Builder();

        TSDBStatsAggregationBuilder copy = (TSDBStatsAggregationBuilder) original.shallowCopy(subFactoriesBuilder, Map.of());

        assertEquals(original.getName(), copy.getName());
        assertEquals(original.getMinTimestamp(), copy.getMinTimestamp());
        assertEquals(original.getMaxTimestamp(), copy.getMaxTimestamp());
        assertEquals(original.isIncludeValueStats(), copy.isIncludeValueStats());
        assertEquals(original.isIncludeHeadStats(), copy.isIncludeHeadStats());
        assertNotSame(original, copy);

        // With metadata
        Map<String, Object> metadata = Map.of("key1", "value1", "key2", 42);
        TSDBStatsAggregationBuilder copyWithMeta = (TSDBStatsAggregationBuilder) original.shallowCopy(subFactoriesBuilder, metadata);
        assertEquals(original.getName(), copyWithMeta.getName());
        assertEquals(original.getMinTimestamp(), copyWithMeta.getMinTimestamp());
    }

    // ========== Helper ==========

    private TSDBStatsAggregationBuilder serializeAndDeserialize(TSDBStatsAggregationBuilder original) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        return new TSDBStatsAggregationBuilder(in);
    }
}
