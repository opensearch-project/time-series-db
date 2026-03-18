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
import org.opensearch.tsdb.query.utils.TSDBStatsConstants;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Unit tests for TSDBStatsAggregationBuilder.
 *
 * <p>Covers: constructor validation, serialization roundtrip, XContent parsing/generation,
 * equals/hashCode, and shallow copy. Follows the test minimalism rule -- minimum tests for >=85% coverage.</p>
 */
public class TSDBStatsAggregationBuilderTests extends OpenSearchTestCase {

    private static final String TEST_NAME = "test-tsdb-stats";
    private static final long MIN_TIMESTAMP = 1000L;
    private static final long MAX_TIMESTAMP = 2000L;

    // ========== Constructor Tests ==========

    public void testConstructorWithValidParameters() {
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            MIN_TIMESTAMP,
            MAX_TIMESTAMP,
            true,
            TSDBStatsConstants.DEDUP_MODE_INDEXED
        );

        assertEquals(TEST_NAME, builder.getName());
        assertEquals(MIN_TIMESTAMP, builder.getMinTimestamp());
        assertEquals(MAX_TIMESTAMP, builder.getMaxTimestamp());
        assertTrue(builder.isIncludeValueStats());
        assertEquals(TSDBStatsConstants.DEDUP_MODE_INDEXED, builder.getDedupMode());
        assertEquals("tsdb_stats_agg", builder.getType());
        assertEquals(AggregationBuilder.BucketCardinality.NONE, builder.bucketCardinality());
    }

    public void testConstructorRejectsInvalidTimeRange() {
        // max < min
        IllegalArgumentException ex1 = expectThrows(
            IllegalArgumentException.class,
            () -> new TSDBStatsAggregationBuilder(TEST_NAME, 2000L, 1000L, true, TSDBStatsConstants.DEDUP_MODE_INDEXED)
        );
        assertTrue(ex1.getMessage().contains("maxTimestamp must be greater than minTimestamp"));

        // max == min
        IllegalArgumentException ex2 = expectThrows(
            IllegalArgumentException.class,
            () -> new TSDBStatsAggregationBuilder(TEST_NAME, 1000L, 1000L, true, TSDBStatsConstants.DEDUP_MODE_INDEXED)
        );
        assertTrue(ex2.getMessage().contains("maxTimestamp must be greater than minTimestamp"));
    }

    // ========== Serialization Tests ==========

    public void testSerializationRoundTrip() throws IOException {
        // includeValueStats=true, indexed mode
        TSDBStatsAggregationBuilder original = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            MIN_TIMESTAMP,
            MAX_TIMESTAMP,
            true,
            TSDBStatsConstants.DEDUP_MODE_INDEXED
        );
        TSDBStatsAggregationBuilder deserialized = serializeAndDeserialize(original);

        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getMinTimestamp(), deserialized.getMinTimestamp());
        assertEquals(original.getMaxTimestamp(), deserialized.getMaxTimestamp());
        assertEquals(original.isIncludeValueStats(), deserialized.isIncludeValueStats());
        assertEquals(original.getDedupMode(), deserialized.getDedupMode());

        // includeValueStats=false, recomputed mode
        TSDBStatsAggregationBuilder original2 = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            MIN_TIMESTAMP,
            MAX_TIMESTAMP,
            false,
            TSDBStatsConstants.DEDUP_MODE_RECOMPUTED
        );
        TSDBStatsAggregationBuilder deserialized2 = serializeAndDeserialize(original2);
        assertEquals(original2, deserialized2);
        assertFalse(deserialized2.isIncludeValueStats());
        assertEquals(TSDBStatsConstants.DEDUP_MODE_RECOMPUTED, deserialized2.getDedupMode());
    }

    public void testSerializationWithEdgeCaseTimestamps() throws IOException {
        TSDBStatsAggregationBuilder original = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            true,
            TSDBStatsConstants.DEDUP_MODE_INDEXED
        );
        TSDBStatsAggregationBuilder deserialized = serializeAndDeserialize(original);

        assertEquals(original, deserialized);
        assertEquals(Long.MIN_VALUE, deserialized.getMinTimestamp());
        assertEquals(Long.MAX_VALUE, deserialized.getMaxTimestamp());
    }

    // ========== XContent Tests ==========

    public void testXContentGeneration() throws IOException {
        TSDBStatsAggregationBuilder builder = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            MIN_TIMESTAMP,
            MAX_TIMESTAMP,
            true,
            TSDBStatsConstants.DEDUP_MODE_INDEXED
        );

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        builder.internalXContent(xContentBuilder, null);

        String json = xContentBuilder.toString();
        assertTrue(json.contains("\"min_timestamp\":" + MIN_TIMESTAMP));
        assertTrue(json.contains("\"max_timestamp\":" + MAX_TIMESTAMP));
        assertTrue(json.contains("\"include_value_stats\":true"));
        assertTrue(json.contains("\"dedup_mode\":\"indexed\""));
    }

    public void testXContentParsing() throws IOException {
        // Full round-trip: parse with all fields including dedup_mode
        String json = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"max_timestamp\":%d,\"include_value_stats\":true,\"dedup_mode\":\"recomputed\"}",
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
            assertEquals(TSDBStatsConstants.DEDUP_MODE_RECOMPUTED, parsed.getDedupMode());
        }

        // Without dedup_mode (should default to "indexed")
        String json2 = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"max_timestamp\":%d,\"include_value_stats\":false}",
            MIN_TIMESTAMP,
            MAX_TIMESTAMP
        );

        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json2)) {
            parser.nextToken();
            TSDBStatsAggregationBuilder parsed = TSDBStatsAggregationBuilder.parse(TEST_NAME, parser);
            assertFalse(parsed.isIncludeValueStats());
            assertEquals(TSDBStatsConstants.DEDUP_MODE_INDEXED, parsed.getDedupMode());
        }
    }

    public void testXContentParsingMissingRequiredFields() throws IOException {
        // Missing min_timestamp
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"max_timestamp\":2000,\"include_value_stats\":true}")) {
            parser.nextToken();
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> TSDBStatsAggregationBuilder.parse(TEST_NAME, parser)
            );
            assertTrue(ex.getMessage().contains("Required parameter 'min_timestamp' is missing"));
        }

        // Missing max_timestamp
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"min_timestamp\":1000,\"include_value_stats\":true}")) {
            parser.nextToken();
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> TSDBStatsAggregationBuilder.parse(TEST_NAME, parser)
            );
            assertTrue(ex.getMessage().contains("Required parameter 'max_timestamp' is missing"));
        }

        // Missing include_value_stats
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"min_timestamp\":1000,\"max_timestamp\":2000}")) {
            parser.nextToken();
            IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> TSDBStatsAggregationBuilder.parse(TEST_NAME, parser)
            );
            assertTrue(ex.getMessage().contains("Required parameter 'include_value_stats' is missing"));
        }
    }

    public void testXContentParsingWithUnknownFields() throws IOException {
        // Unknown string, number, boolean, object, and array fields should all be skipped
        String json = String.format(
            Locale.ROOT,
            "{\"min_timestamp\":%d,\"unknown_num\":99,\"max_timestamp\":%d,"
                + "\"unknown_bool\":false,\"include_value_stats\":true,"
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
        TSDBStatsAggregationBuilder builder1 = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            MIN_TIMESTAMP,
            MAX_TIMESTAMP,
            true,
            TSDBStatsConstants.DEDUP_MODE_INDEXED
        );
        TSDBStatsAggregationBuilder builder2 = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            MIN_TIMESTAMP,
            MAX_TIMESTAMP,
            true,
            TSDBStatsConstants.DEDUP_MODE_INDEXED
        );
        TSDBStatsAggregationBuilder differentValueStats = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            MIN_TIMESTAMP,
            MAX_TIMESTAMP,
            false,
            TSDBStatsConstants.DEDUP_MODE_INDEXED
        );
        TSDBStatsAggregationBuilder differentMax = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            MIN_TIMESTAMP,
            3000L,
            true,
            TSDBStatsConstants.DEDUP_MODE_INDEXED
        );
        TSDBStatsAggregationBuilder differentDedupMode = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            MIN_TIMESTAMP,
            MAX_TIMESTAMP,
            true,
            TSDBStatsConstants.DEDUP_MODE_RECOMPUTED
        );

        // equals
        assertEquals(builder1, builder2);
        assertEquals(builder1, builder1);
        assertNotEquals(builder1, differentValueStats);
        assertNotEquals(builder1, differentMax);
        assertNotEquals(builder1, differentDedupMode);
        assertNotEquals(builder1, null);
        assertNotEquals(builder1, new Object());

        // hashCode
        assertEquals(builder1.hashCode(), builder2.hashCode());
    }

    // ========== Shallow Copy Tests ==========

    public void testShallowCopy() {
        TSDBStatsAggregationBuilder original = new TSDBStatsAggregationBuilder(
            TEST_NAME,
            MIN_TIMESTAMP,
            MAX_TIMESTAMP,
            true,
            TSDBStatsConstants.DEDUP_MODE_RECOMPUTED
        );
        org.opensearch.search.aggregations.AggregatorFactories.Builder subFactoriesBuilder =
            new org.opensearch.search.aggregations.AggregatorFactories.Builder();

        TSDBStatsAggregationBuilder copy = (TSDBStatsAggregationBuilder) original.shallowCopy(subFactoriesBuilder, Map.of());

        assertEquals(original.getName(), copy.getName());
        assertEquals(original.getMinTimestamp(), copy.getMinTimestamp());
        assertEquals(original.getMaxTimestamp(), copy.getMaxTimestamp());
        assertEquals(original.isIncludeValueStats(), copy.isIncludeValueStats());
        assertEquals(original.getDedupMode(), copy.getDedupMode());
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
