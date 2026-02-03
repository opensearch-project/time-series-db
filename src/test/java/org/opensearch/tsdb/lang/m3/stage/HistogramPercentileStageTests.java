/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for HistogramPercentileStage.
 * Tests histogram percentile calculation with duration buckets and multiple groups.
 */
public class HistogramPercentileStageTests extends AbstractWireSerializingTestCase<HistogramPercentileStage> {
    // ========== AbstractWireSerializingTestCase Required Methods ==========

    @Override
    protected Writeable.Reader<HistogramPercentileStage> instanceReader() {
        return HistogramPercentileStage::readFrom;
    }

    @Override
    protected HistogramPercentileStage createTestInstance() {
        String bucketId = randomAlphaOfLength(5);
        String bucketRange = randomAlphaOfLength(5);
        int numPercentiles = randomIntBetween(1, 5);
        List<Float> percentiles = new ArrayList<>();
        for (int i = 0; i < numPercentiles; i++) {
            percentiles.add((float) randomDoubleBetween(0.1, 99.9, false));
        }
        return new HistogramPercentileStage(bucketId, bucketRange, percentiles);
    }

    /**
     * Test basic P99 latency calculation with a single group using timeout buckets.
     * This simulates a histogram where most requests are fast (&lt; 100ms) but some are slower.
     */
    public void testBasicP99LatencyCalculation() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(99.0f));

        // Create time series for each bucket representing request counts
        List<TimeSeries> input = new ArrayList<>();
        long timestamp = 1000L;

        // Simulate latency distribution: most requests < 100ms, few slower ones
        Map<String, Integer> bucketCounts = new HashMap<>();
        bucketCounts.put("0-1ms", 10);      // very fast
        bucketCounts.put("1ms-5ms", 50);    // fast
        bucketCounts.put("5ms-10ms", 100);  // normal
        bucketCounts.put("10ms-25ms", 80);  // normal
        bucketCounts.put("25ms-50ms", 40);  // normal
        bucketCounts.put("50ms-75ms", 15);  // getting slower
        bucketCounts.put("75ms-100ms", 4);  // slow
        bucketCounts.put("100ms-200ms", 1); // very slow - this should be the P99 bucket

        String[] bucketIds = { "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8" };
        String[] bucketRanges = { "0-1ms", "1ms-5ms", "5ms-10ms", "10ms-25ms", "25ms-50ms", "50ms-75ms", "75ms-100ms", "100ms-200ms" };

        for (int i = 0; i < bucketIds.length; i++) {
            String bucketId = bucketIds[i];
            String bucketRange = bucketRanges[i];
            double count = bucketCounts.get(bucketRange);

            // Create labels for this bucket
            ByteLabels labels = ByteLabels.fromStrings("service", "api-server", "bucketId", bucketId, "bucket", bucketRange);

            List<Sample> samples = List.of(new FloatSample(timestamp, count));
            TimeSeries series = new TimeSeries(samples, labels, timestamp, timestamp, 1000L, null);
            input.add(series);
        }

        // Execute
        List<TimeSeries> result = stage.process(input);

        // Verify
        assertEquals(1, result.size()); // One result series for P99
        TimeSeries p99Series = result.get(0);

        // Check labels
        assertTrue(p99Series.getLabels().has("histogramPercentile"));
        assertEquals("p99", p99Series.getLabels().get("histogramPercentile"));
        assertEquals("api-server", p99Series.getLabels().get("service"));

        // Check P99 value - with 300 total requests, P99 is at 297th request
        // Cumulative: 10+50+100+80+40+15+4 = 299 requests up to 100ms bucket
        // So P99 (297th request) falls in the 100ms bucket, upper bound = 100ms
        List<Sample> samples = p99Series.getSamples().toList();
        assertEquals(1, samples.size());
        assertEquals(100.0, ((FloatSample) samples.get(0)).getValue(), 0.001);
    }

    /**
     * Test multiple groups with multiple percentiles.
     * Simulates different services with different latency characteristics.
     */
    public void testMultipleGroupsMultiplePercentiles() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(50.0f, 95.0f, 99.0f));

        List<TimeSeries> input = new ArrayList<>();
        long timestamp = 2000L;

        // Service A - Fast service (most requests &lt; 50ms)
        String[] serviceABuckets = { "0-10ms", "10ms-25ms", "25ms-50ms", "50ms-100ms", "100ms-200ms" };
        int[] serviceACounts = { 100, 80, 15, 4, 1 }; // Total: 200 requests

        for (int i = 0; i < serviceABuckets.length; i++) {
            ByteLabels labels = ByteLabels.fromStrings("service", "fast-api", "bucketId", "b" + (i + 1), "bucket", serviceABuckets[i]);

            List<Sample> samples = List.of(new FloatSample(timestamp, serviceACounts[i]));
            TimeSeries series = new TimeSeries(samples, labels, timestamp, timestamp, 1000L, null);
            input.add(series);
        }

        // Service B - Slower service (more requests in higher buckets)
        String[] serviceBBuckets = { "0-10ms", "10ms-50ms", "50ms-100ms", "100ms-200ms", "200ms-500ms" };
        int[] serviceBCounts = { 20, 30, 40, 35, 25 }; // Total: 150 requests

        for (int i = 0; i < serviceBBuckets.length; i++) {
            ByteLabels labels = ByteLabels.fromStrings("service", "slow-api", "bucketId", "b" + (i + 1), "bucket", serviceBBuckets[i]);

            List<Sample> samples = List.of(new FloatSample(timestamp, serviceBCounts[i]));
            TimeSeries series = new TimeSeries(samples, labels, timestamp, timestamp, 1000L, null);
            input.add(series);
        }

        // Execute
        List<TimeSeries> result = stage.process(input);

        // Verify - should have 6 result series (2 services * 3 percentiles)
        assertEquals(6, result.size());

        // Group results by service and percentile
        Map<String, Map<String, TimeSeries>> resultsByService = new HashMap<>();
        for (TimeSeries series : result) {
            String service = series.getLabels().get("service");
            String percentile = series.getLabels().get("histogramPercentile");
            resultsByService.computeIfAbsent(service, k -> new HashMap<>()).put(percentile, series);
        }

        // Verify fast-api percentiles
        Map<String, TimeSeries> fastApiResults = resultsByService.get("fast-api");
        assertNotNull(fastApiResults);
        assertEquals(3, fastApiResults.size());

        // P50 for fast-api: 100th request out of 200 falls at the end of 0-10ms bucket
        TimeSeries p50Fast = fastApiResults.get("p50");
        assertNotNull(p50Fast);
        assertEquals(10.0, p50Fast.getSamples().getValue(0), 0.001);

        // P95 for fast-api: 190th request falls in 25ms-50ms bucket
        TimeSeries p95Fast = fastApiResults.get("p95");
        assertNotNull(p95Fast);
        assertEquals(50.0, p95Fast.getSamples().getValue(0), 0.001);

        // P99 for fast-api: 198th request falls in 50ms-100ms bucket
        TimeSeries p99Fast = fastApiResults.get("p99");
        assertNotNull(p99Fast);
        assertEquals(100.0, p99Fast.getSamples().getValue(0), 0.001);

        // Verify slow-api percentiles
        Map<String, TimeSeries> slowApiResults = resultsByService.get("slow-api");
        assertNotNull(slowApiResults);
        assertEquals(3, slowApiResults.size());

        // P50 for slow-api: 75th request out of 150 falls in 50ms-100ms bucket (after 20+30=50)
        TimeSeries p50Slow = slowApiResults.get("p50");
        assertNotNull(p50Slow);
        assertEquals(100.0, p50Slow.getSamples().getValue(0), 0.001);

        // P95 for slow-api: 142.5th request falls in 200ms-500ms bucket
        TimeSeries p95Slow = slowApiResults.get("p95");
        assertNotNull(p95Slow);
        assertEquals(500.0, p95Slow.getSamples().getValue(0), 0.001);

        // P99 for slow-api: 148.5th request falls in 200ms-500ms bucket
        TimeSeries p99Slow = slowApiResults.get("p99");
        assertNotNull(p99Slow);
        assertEquals(500.0, p99Slow.getSamples().getValue(0), 0.001);
    }

    /**
     * Test with multiple timestamps.
     */
    public void testMultipleTimestamps() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(95.0f));

        List<TimeSeries> input = new ArrayList<>();
        long[] timestamps = { 1000L, 2000L, 3000L };

        // Create buckets for different timestamps
        String[] bucketRanges = { "0-10ms", "10ms-50ms", "50ms-100ms" };
        String[] bucketIds = { "b1", "b2", "b3" };

        // Different distributions at each timestamp
        int[][] counts = {
            { 80, 15, 5 },  // t=1000: most requests fast
            { 60, 30, 10 }, // t=2000: getting slower
            { 40, 40, 20 }  // t=3000: even slower
        };

        for (int bucketIdx = 0; bucketIdx < bucketRanges.length; bucketIdx++) {
            ByteLabels labels = ByteLabels.fromStrings(
                "service",
                "test-service",
                "bucketId",
                bucketIds[bucketIdx],
                "bucket",
                bucketRanges[bucketIdx]
            );

            List<Sample> samples = new ArrayList<>();
            for (int timeIdx = 0; timeIdx < timestamps.length; timeIdx++) {
                samples.add(new FloatSample(timestamps[timeIdx], counts[timeIdx][bucketIdx]));
            }

            TimeSeries series = new TimeSeries(samples, labels, timestamps[0], timestamps[timestamps.length - 1], 1000L, null);
            input.add(series);
        }

        // Execute
        List<TimeSeries> result = stage.process(input);

        // Verify
        assertEquals(1, result.size());
        TimeSeries p95Series = result.get(0);
        List<Sample> samples = p95Series.getSamples().toList();
        assertEquals(3, samples.size());

        List<Sample> expectedSamples = List.of(
            new FloatSample(1000L, 50.0), // t=1000: 95% of 100 = 95th request, falls in 10ms-50ms bucket (after 80)
            new FloatSample(2000L, 100.0),  // t=2000: 95% of 100 = 95th request, falls in 50ms-100ms bucket (after 60+30=90)
            new FloatSample(3000L, 100.0) // t=3000: 95% of 100 = 95th request, falls in 50ms-100ms bucket (after 40+40=80)
        );

        TestUtils.assertSamplesEqual("testMultipleTimestamps histogramPercentile samples should match", expectedSamples, samples, 0.001);
    }

    /**
     * Test edge case: empty input.
     */
    public void testEmptyInput() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(99.0f));
        List<TimeSeries> result = stage.process(new ArrayList<>());
        assertEquals(0, result.size());
    }

    /**
     * Test edge case: missing required labels.
     */
    public void testMissingRequiredLabels() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(50.0f));

        List<TimeSeries> input = new ArrayList<>();

        // Series missing "bucketId" label
        ByteLabels labels1 = ByteLabels.fromStrings(
            "service",
            "test",
            "bucket",
            "0-10ms" // missing "bucketId"
        );

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries series1 = new TimeSeries(samples, labels1, 1000L, 1000L, 1000L, null);
        input.add(series1);

        // Series missing "bucket" label
        ByteLabels labels2 = ByteLabels.fromStrings(
            "service",
            "test",
            "bucketId",
            "b1" // missing "bucket"
        );

        TimeSeries series2 = new TimeSeries(samples, labels2, 1000L, 1000L, 1000L, null);
        input.add(series2);

        // Execute
        List<TimeSeries> result = stage.process(input);

        // Verify - should have no results since all series are missing required labels
        assertEquals(0, result.size());
    }

    /**
     * Test invalid bucket range format throws exception.
     */
    public void testInvalidBucketRange() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(50.0f));

        List<TimeSeries> input = new ArrayList<>();

        ByteLabels labels = ByteLabels.fromStrings(
            "service",
            "test",
            "bucketId",
            "b1",
            "bucket",
            "invalid-range" // invalid format
        );

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);
        input.add(series);

        // Execute - should throw exception for invalid bucket range
        assertThrows(IllegalArgumentException.class, () -> stage.process(input));
    }

    /**
     * Test constructor validation.
     */
    public void testConstructorValidation() {
        // Test null percentiles
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage("bucketId", "bucket", null));

        // Test empty percentiles
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage("bucketId", "bucket", new ArrayList<>()));

        // Test valid edge percentile values
        HistogramPercentileStage stage0 = new HistogramPercentileStage("bucketId", "bucket", List.of(0.0f));
        assertNotNull(stage0);
        HistogramPercentileStage stage100 = new HistogramPercentileStage("bucketId", "bucket", List.of(100.0f));
        assertNotNull(stage100);

        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage("bucketId", "bucket", List.of(-10.0f)));

        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage("bucketId", "bucket", List.of(150.0f)));

        // Test valid constructor
        HistogramPercentileStage validStage = new HistogramPercentileStage("bucketId", "bucket", List.of(50.0f, 95.0f, 99.0f));
        assertEquals("histogram_percentile", validStage.getName());
    }

    /**
     * Test value-based bucket ranges (not duration-based).
     */
    public void testValueBasedBuckets() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "range", List.of(90.0f));

        List<TimeSeries> input = new ArrayList<>();
        long timestamp = 1000L;

        // Create buckets with value ranges instead of duration
        String[] bucketRanges = { "0-10", "10-50", "50-100", "100-200" };
        int[] counts = { 30, 40, 25, 5 }; // Total: 100

        for (int i = 0; i < bucketRanges.length; i++) {
            ByteLabels labels = ByteLabels.fromStrings("metric", "response_size", "bucketId", "b" + (i + 1), "range", bucketRanges[i]);

            List<Sample> samples = List.of(new FloatSample(timestamp, counts[i]));
            TimeSeries series = new TimeSeries(samples, labels, timestamp, timestamp, 1000L, null);
            input.add(series);
        }

        // Execute
        List<TimeSeries> result = stage.process(input);

        // Verify
        assertEquals(1, result.size());
        TimeSeries p90Series = result.get(0);

        // P90 of 100 = 90th value, falls in 50-100 bucket (after 30+40=70)
        List<Sample> samples = p90Series.getSamples().toList();
        assertEquals(1, samples.size());
        assertEquals(100.0, ((FloatSample) samples.get(0)).getValue(), 0.001);

        // Check labels
        assertEquals("p90", p90Series.getLabels().get("histogramPercentile"));
        assertEquals("response_size", p90Series.getLabels().get("metric"));
    }

    /**
     * Test  duration format parsing.
     */
    public void testHistogramDurationFormats() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(50.0f));

        List<TimeSeries> input = new ArrayList<>();
        long timestamp = 1000L;

        // Test various upstream duration formats: ns, us/µs, ms, s, m, h
        String[] bucketRanges = { "100ns-200ns", "1us-5us", "10ms-50ms", "1s-2s" };
        int[] counts = { 25, 25, 25, 25 }; // Total: 100 requests

        for (int i = 0; i < bucketRanges.length; i++) {
            ByteLabels labels = ByteLabels.fromStrings("service", "test", "bucketId", "b" + (i + 1), "bucket", bucketRanges[i]);

            List<Sample> samples = List.of(new FloatSample(timestamp, counts[i]));
            TimeSeries series = new TimeSeries(samples, labels, timestamp, timestamp, 1000L, null);
            input.add(series);
        }

        // Execute - should not throw and should process correctly
        List<TimeSeries> result = stage.process(input);

        // Verify - should have 1 result series for P50
        assertEquals(1, result.size());
        TimeSeries p50Series = result.get(0);

        // P50 of 100 = 50th value, falls in the 1us-5us bucket (after 25+25=50)
        List<Sample> samples = p50Series.getSamples().toList();
        assertEquals(1, samples.size());
        // Upper bound of 1us-5us bucket is 5us = 0.005ms
        assertEquals(0.005, ((FloatSample) samples.get(0)).getValue(), 0.001);
    }

    /**
     * Test with zero counts in some buckets.
     */
    public void testZeroCounts() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(50.0f));

        List<TimeSeries> input = new ArrayList<>();
        long timestamp = 1000L;

        // Some buckets have zero counts
        String[] bucketRanges = { "0-10ms", "10ms-50ms", "50ms-100ms", "100ms-200ms" };
        int[] counts = { 50, 0, 40, 10 }; // Total: 100, but middle bucket is empty

        for (int i = 0; i < bucketRanges.length; i++) {
            ByteLabels labels = ByteLabels.fromStrings("service", "test", "bucketId", "b" + (i + 1), "bucket", bucketRanges[i]);

            List<Sample> samples = List.of(new FloatSample(timestamp, counts[i]));
            TimeSeries series = new TimeSeries(samples, labels, timestamp, timestamp, 1000L, null);
            input.add(series);
        }

        // Execute
        List<TimeSeries> result = stage.process(input);

        // Verify
        assertEquals(1, result.size());
        TimeSeries p50Series = result.get(0);

        // P50 of 100 = 50th value, exactly at end of first bucket
        // The 50th request falls at the boundary of the "0-10ms" bucket
        List<Sample> samples = p50Series.getSamples().toList();
        assertEquals(1, samples.size());
        assertEquals(10.0, ((FloatSample) samples.get(0)).getValue(), 0.001);
    }

    // ========== fromArgs Tests ==========

    public void testFromArgsValid() {
        Map<String, Object> args = new HashMap<>();
        args.put("bucket_id", "bucketId");
        args.put("bucket_range", "bucket");
        args.put("percentiles", List.of(50.0, 95.0, 99.0));

        HistogramPercentileStage stage = HistogramPercentileStage.fromArgs(args);
        assertNotNull(stage);
        assertEquals("histogram_percentile", stage.getName());
    }

    public void testFromArgsNullArgs() {
        assertThrows(IllegalArgumentException.class, () -> HistogramPercentileStage.fromArgs(null));
    }

    public void testFromArgsMissingBucketId() {
        Map<String, Object> args = new HashMap<>();
        args.put("bucket_range", "bucket");
        args.put("percentiles", List.of(50.0));

        assertThrows(IllegalArgumentException.class, () -> HistogramPercentileStage.fromArgs(args));
    }

    public void testFromArgsMissingBucketRange() {
        Map<String, Object> args = new HashMap<>();
        args.put("bucket_id", "bucketId");
        args.put("percentiles", List.of(50.0));

        assertThrows(IllegalArgumentException.class, () -> HistogramPercentileStage.fromArgs(args));
    }

    public void testFromArgsMissingPercentiles() {
        Map<String, Object> args = new HashMap<>();
        args.put("bucket_id", "bucketId");
        args.put("bucket_range", "bucket");

        assertThrows(IllegalArgumentException.class, () -> HistogramPercentileStage.fromArgs(args));
    }

    public void testFromArgsInvalidPercentileType() {
        Map<String, Object> args = new HashMap<>();
        args.put("bucket_id", "bucketId");
        args.put("bucket_range", "bucket");
        args.put("percentiles", "invalid");

        assertThrows(IllegalArgumentException.class, () -> HistogramPercentileStage.fromArgs(args));
    }

    public void testFromArgsInvalidPercentileValues() {
        Map<String, Object> args = new HashMap<>();
        args.put("bucket_id", "bucketId");
        args.put("bucket_range", "bucket");

        // Create a list that allows null values
        List<Object> percentilesList = new ArrayList<>();
        percentilesList.add(50.0);
        percentilesList.add(null);
        percentilesList.add(95.0);
        args.put("percentiles", percentilesList);

        assertThrows(IllegalArgumentException.class, () -> HistogramPercentileStage.fromArgs(args));
    }

    // ========== toXContent Tests ==========

    public void testToXContent() throws Exception {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(50.0f, 95.0f));
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"bucket_id\":\"bucketId\",\"bucket_range\":\"bucket\",\"percentiles\":[50.0,95.0]}", json);
    }

    // ========== isCoordinatorOnly Tests ==========

    public void testIsCoordinatorOnly() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(95.0f));
        assertTrue(stage.isCoordinatorOnly());
    }

    // ========== BucketInfo Tests ==========

    public void testBucketInfoValueRange() {
        // Test value-based bucket range parsing
        // Using reflection to access private BucketInfo class
        List<TimeSeries> input = new ArrayList<>();
        Map<String, String> labels = new HashMap<>();
        labels.put("service", "test");
        labels.put("bucketId", "b1");
        labels.put("bucket", "10-20"); // value range

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries series = new TimeSeries(samples, ByteLabels.fromMap(labels), 1000L, 1000L, 1000L, null);
        input.add(series);

        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(95.0f));
        List<TimeSeries> result = stage.process(input);

        // Should process without error, indicating BucketInfo parsed correctly
        assertEquals(1, result.size());
        assertEquals(20.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testBucketInfoDurationRange() {
        // Test duration-based bucket range parsing
        List<TimeSeries> input = new ArrayList<>();
        Map<String, String> labels = new HashMap<>();
        labels.put("service", "test");
        labels.put("bucketId", "b1");
        labels.put("bucket", "10ms-20ms"); // duration range

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries series = new TimeSeries(samples, ByteLabels.fromMap(labels), 1000L, 1000L, 1000L, null);
        input.add(series);

        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(95.0f));
        List<TimeSeries> result = stage.process(input);

        // Should process without error, indicating BucketInfo parsed correctly
        assertEquals(1, result.size());
        assertEquals(20.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testBucketInfoInfinityRange() {
        // Test infinity bucket range parsing
        List<TimeSeries> input = new ArrayList<>();
        Map<String, String> labels = new HashMap<>();
        labels.put("service", "test");
        labels.put("bucketId", "b1");
        labels.put("bucket", "infinity"); // infinity range

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries series = new TimeSeries(samples, ByteLabels.fromMap(labels), 1000L, 1000L, 1000L, null);
        input.add(series);

        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(95.0f));
        List<TimeSeries> result = stage.process(input);

        // Should process without error, indicating BucketInfo parsed correctly
        assertEquals(1, result.size());
        assertEquals(Double.POSITIVE_INFINITY, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testBucketInfoInvalidRange() {
        // Test that invalid bucket ranges throw exception
        List<TimeSeries> input = new ArrayList<>();
        Map<String, String> labels = new HashMap<>();
        labels.put("service", "test");
        labels.put("bucketId", "b1");
        labels.put("bucket", "invalid-format"); // invalid format

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries series = new TimeSeries(samples, ByteLabels.fromMap(labels), 1000L, 1000L, 1000L, null);
        input.add(series);

        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(95.0f));

        // Should throw exception for invalid bucket range
        assertThrows(IllegalArgumentException.class, () -> stage.process(input));
    }

    public void testBucketInfoEqualsAndHashCode() {
        // Test BucketInfo equals and hashCode through processing
        List<TimeSeries> input = new ArrayList<>();

        // Add two series with same bucket info to test deduplication logic
        for (int i = 0; i < 2; i++) {
            Map<String, String> labels = new HashMap<>();
            labels.put("service", "test");
            labels.put("instance", String.valueOf(i));
            labels.put("bucketId", "b1");
            labels.put("bucket", "10-20");

            List<Sample> samples = List.of(new FloatSample(1000L, 5.0));
            TimeSeries series = new TimeSeries(samples, ByteLabels.fromMap(labels), 1000L, 1000L, 1000L, null);
            input.add(series);
        }

        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(50.0f));
        List<TimeSeries> result = stage.process(input);

        // Should have 2 result series (one for each instance grouping)
        assertEquals(2, result.size());

        // Both should have same percentile value since they have same bucket structure
        for (TimeSeries series : result) {
            assertEquals(20.0, series.getSamples().getValue(0), 0.001);
        }

        // Test BucketInfo equals and hashCode directly
        HistogramPercentileStage.BucketInfo bucket1 = new HistogramPercentileStage.BucketInfo("b1", "10-20");
        HistogramPercentileStage.BucketInfo bucket2 = new HistogramPercentileStage.BucketInfo("b1", "10-20");
        HistogramPercentileStage.BucketInfo bucket3 = new HistogramPercentileStage.BucketInfo("b2", "10-20");
        HistogramPercentileStage.BucketInfo bucket4 = new HistogramPercentileStage.BucketInfo("b1", "20-30");

        // Test reflexive property
        assertEquals("BucketInfo should equal itself", bucket1, bucket1);

        // Test symmetric property
        assertEquals("Equal BucketInfo objects should be equal", bucket1, bucket2);
        assertEquals("Equal BucketInfo objects should be equal", bucket2, bucket1);

        // Test hash code consistency
        assertEquals("Equal objects must have equal hash codes", bucket1.hashCode(), bucket2.hashCode());

        // Test inequality for different bucketId
        assertNotEquals("Different bucketId should not be equal", bucket1, bucket3);
        assertNotEquals("Different bucketId should not be equal", bucket3, bucket1);

        // Test inequality for different bucketRange
        assertNotEquals("Different bucketRange should not be equal", bucket1, bucket4);
        assertNotEquals("Different bucketRange should not be equal", bucket4, bucket1);

        // Test null comparison
        assertNotEquals("BucketInfo should not equal null", bucket1, null);

        // Test different type comparison
        assertNotEquals("BucketInfo should not equal different type", bucket1, "string");

        // Test with duration ranges
        HistogramPercentileStage.BucketInfo durationBucket1 = new HistogramPercentileStage.BucketInfo("d1", "10ms-20ms");
        HistogramPercentileStage.BucketInfo durationBucket2 = new HistogramPercentileStage.BucketInfo("d1", "10ms-20ms");
        assertEquals("Duration buckets should be equal", durationBucket1, durationBucket2);
        assertEquals("Duration buckets should have same hash code", durationBucket1.hashCode(), durationBucket2.hashCode());

        // Test with infinity buckets
        HistogramPercentileStage.BucketInfo infBucket1 = new HistogramPercentileStage.BucketInfo("inf1", "infinity");
        HistogramPercentileStage.BucketInfo infBucket2 = new HistogramPercentileStage.BucketInfo("inf1", "infinity");
        assertEquals("Infinity buckets should be equal", infBucket1, infBucket2);
        assertEquals("Infinity buckets should have same hash code", infBucket1.hashCode(), infBucket2.hashCode());
    }

    public void testBucketInfoValueRangeBasic() {
        // Test basic value range parsing
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b1", "10-20");
        assertEquals(10.0, bucket.getLowerBound(), 0.001);
        assertEquals(20.0, bucket.getUpperBound(), 0.001);
    }

    public void testBucketInfoValueRangeWithDecimals() {
        // Test value range with decimal numbers
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b2", "1.5-3.7");
        assertEquals(1.5, bucket.getLowerBound(), 0.001);
        assertEquals(3.7, bucket.getUpperBound(), 0.001);
    }

    public void testBucketInfoValueRangeWithNegativeNumbers() {
        // Test value range with negative numbers
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b3", "-10-5");
        assertEquals(-10.0, bucket.getLowerBound(), 0.001);
        assertEquals(5.0, bucket.getUpperBound(), 0.001);
    }

    public void testBucketInfoDurationRangeMilliseconds() {
        // Test duration range in milliseconds
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b4", "10ms-50ms");
        assertEquals(10.0, bucket.getLowerBound(), 0.001);
        assertEquals(50.0, bucket.getUpperBound(), 0.001);
    }

    public void testBucketInfoDurationRangeSeconds() {
        // Test duration range in seconds
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b5", "1s-5s");
        assertEquals(1000.0, bucket.getLowerBound(), 0.001); // 1 second = 1000ms
        assertEquals(5000.0, bucket.getUpperBound(), 0.001); // 5 seconds = 5000ms
    }

    public void testBucketInfoDurationRangeMinutes() {
        // Test duration range in minutes
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b6", "1m-2m");
        assertEquals(60000.0, bucket.getLowerBound(), 0.001); // 1 minute = 60000ms
        assertEquals(120000.0, bucket.getUpperBound(), 0.001); // 2 minutes = 120000ms
    }

    public void testBucketInfoDurationRangeHours() {
        // Test duration range in hours
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b7", "1h-3h");
        assertEquals(3600000.0, bucket.getLowerBound(), 0.001); // 1 hour = 3600000ms
        assertEquals(10800000.0, bucket.getUpperBound(), 0.001); // 3 hours = 10800000ms
    }

    public void testBucketInfoDurationRangeNanoseconds() {
        // Test duration range in nanoseconds
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b8", "1000ns-5000ns");
        assertEquals(0.001, bucket.getLowerBound(), 0.0001); // 1000ns = 0.001ms
        assertEquals(0.005, bucket.getUpperBound(), 0.0001); // 5000ns = 0.005ms
    }

    public void testBucketInfoDurationRangeMicroseconds() {
        // Test duration range in microseconds
        HistogramPercentileStage.BucketInfo bucket1 = new HistogramPercentileStage.BucketInfo("b9", "100us-500us");
        assertEquals(0.1, bucket1.getLowerBound(), 0.0001); // 100µs = 0.1ms
        assertEquals(0.5, bucket1.getUpperBound(), 0.0001); // 500µs = 0.5ms

        // Test with µ symbol
        HistogramPercentileStage.BucketInfo bucket2 = new HistogramPercentileStage.BucketInfo("b10", "100µs-500µs");
        assertEquals(0.1, bucket2.getLowerBound(), 0.0001);
        assertEquals(0.5, bucket2.getUpperBound(), 0.0001);
    }

    public void testBucketInfoInfinityRangeComprehensive() {
        // Test infinity range
        HistogramPercentileStage.BucketInfo bucket1 = new HistogramPercentileStage.BucketInfo("b11", "infinity");
        assertEquals(0.0, bucket1.getLowerBound(), 0.001);
        assertEquals(Double.POSITIVE_INFINITY, bucket1.getUpperBound(), 0.001);

        // Test +Inf notation
        HistogramPercentileStage.BucketInfo bucket2 = new HistogramPercentileStage.BucketInfo("b12", "+Inf");
        assertEquals(0.0, bucket2.getLowerBound(), 0.001);
        assertEquals(Double.POSITIVE_INFINITY, bucket2.getUpperBound(), 0.001);

        // Test value range with infinity upper bound
        HistogramPercentileStage.BucketInfo bucket3 = new HistogramPercentileStage.BucketInfo("b13", "100-infinity");
        assertEquals(100.0, bucket3.getLowerBound(), 0.001);
        assertEquals(100.0, bucket3.getUpperBound(), 0.001); // For infinity buckets, upper bound equals lower bound
    }

    public void testBucketInfoDurationInfinityRange() {
        // Test duration with positive infinity
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b13", "1s-infinity");
        assertEquals(1000.0, bucket.getLowerBound(), 0.001);
        assertEquals(1000.0, bucket.getUpperBound(), 0.001); // For infinity buckets, upper bound equals lower bound

        // Test duration with +Inf notation
        HistogramPercentileStage.BucketInfo bucketPlusInf = new HistogramPercentileStage.BucketInfo("b14", "500ms-+Inf");
        assertEquals(500.0, bucketPlusInf.getLowerBound(), 0.001);
        assertEquals(500.0, bucketPlusInf.getUpperBound(), 0.001); // For infinity buckets, upper bound equals lower bound

        // Test duration with negative infinity (-Inf)
        HistogramPercentileStage.BucketInfo negInfBucket = new HistogramPercentileStage.BucketInfo("b15", "-Inf-100ms");
        // -Inf should be represented as Duration.ofNanos(Long.MIN_VALUE) which converts to a very large negative millisecond value
        double expectedNegInfMs = Long.MIN_VALUE / 1_000_000.0; // Convert nanoseconds to milliseconds
        assertEquals(expectedNegInfMs, negInfBucket.getLowerBound(), 0.001);
        assertEquals(100.0, negInfBucket.getUpperBound(), 0.001);

        // Test zero to positive infinity
        HistogramPercentileStage.BucketInfo zeroToInf = new HistogramPercentileStage.BucketInfo("b16", "0-infinity");
        assertEquals(0.0, zeroToInf.getLowerBound(), 0.001);
        assertEquals(0.0, zeroToInf.getUpperBound(), 0.001); // For infinity buckets, upper bound equals lower bound

        // Test duration with negative infinity (-infinity lowercase)
        HistogramPercentileStage.BucketInfo negInfinityBucket = new HistogramPercentileStage.BucketInfo("b17", "-infinity-2ms");
        double expectedNegInfinityMs = Long.MIN_VALUE / 1_000_000.0; // Convert nanoseconds to milliseconds
        assertEquals(expectedNegInfinityMs, negInfinityBucket.getLowerBound(), 0.001);
        assertEquals(2.0, negInfinityBucket.getUpperBound(), 0.001);
    }

    public void testBucketInfoZeroDuration() {
        // Test zero duration as lower bound
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b14", "0-10ms");
        assertEquals(0.0, bucket.getLowerBound(), 0.001);
        assertEquals(10.0, bucket.getUpperBound(), 0.001);
    }

    public void testBucketInfoToString() {
        // Test toString method
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b1", "10-20");
        String toString = bucket.toString();

        assertTrue("toString should contain bucketId", toString.contains("bucketId='b1'"));
        assertTrue("toString should contain bucketRange", toString.contains("bucketRange='10-20'"));
        assertTrue("toString should contain upper bound", toString.contains("upper=20.0"));
        assertTrue("toString should start with class name", toString.startsWith("BucketInfo{"));
    }

    public void testBucketInfoInvalidRangeFormats() {
        // Test invalid range formats
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b1", "invalid-format"));

        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b2", "10"));

        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b3", ""));

        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b4", null));
    }

    public void testBucketInfoInvalidDurationFormats() {
        // Test invalid duration formats
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b1", "10invalid-20invalid"));

        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b2", "10x-20x"));

        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b3", "abc-def"));
    }

    public void testBucketInfoInvalidValueRanges() {
        // Test invalid value ranges (high <= low)
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b1", "20-10"));

        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b2", "5-5"));

        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b3", "10ms-5ms"));
    }

    public void testBucketInfoMixedPrecisionDurations() {
        // Test various precision combinations
        HistogramPercentileStage.BucketInfo bucket1 = new HistogramPercentileStage.BucketInfo("b1", "1.5s-2.5s");
        assertEquals(1500.0, bucket1.getLowerBound(), 0.001);
        assertEquals(2500.0, bucket1.getUpperBound(), 0.001);

        HistogramPercentileStage.BucketInfo bucket2 = new HistogramPercentileStage.BucketInfo("b2", "0.5ms-1.5ms");
        assertEquals(0.5, bucket2.getLowerBound(), 0.001);
        assertEquals(1.5, bucket2.getUpperBound(), 0.001);
    }

    public void testBucketInfoLargeDurations() {
        // Test large duration values
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b1", "24h-48h");
        assertEquals(86400000.0, bucket.getLowerBound(), 0.001); // 24 hours = 86400000ms
        assertEquals(172800000.0, bucket.getUpperBound(), 0.001); // 48 hours = 172800000ms
    }

    public void testBucketInfoSmallDurations() {
        // Test very small duration values
        HistogramPercentileStage.BucketInfo bucket = new HistogramPercentileStage.BucketInfo("b1", "1ns-10ns");
        assertEquals(0.000001, bucket.getLowerBound(), 0.0000001); // 1ns = 0.000001ms
        assertEquals(0.00001, bucket.getUpperBound(), 0.0000001); // 10ns = 0.00001ms
    }

    public void testBucketInfoComprehensiveInfinityAndEdgeCases() {
        // Test comprehensive coverage of parseDurationRange() with -Inf, +Inf, and edge cases

        // Test -Inf to positive value (most negative duration to normal duration)
        HistogramPercentileStage.BucketInfo negInfToPosValue = new HistogramPercentileStage.BucketInfo("edge1", "-Inf-1s");
        double expectedNegInfMs = Long.MIN_VALUE / 1_000_000.0; // -Inf as nanoseconds converted to milliseconds
        assertEquals(expectedNegInfMs, negInfToPosValue.getLowerBound(), 0.001);
        assertEquals(1000.0, negInfToPosValue.getUpperBound(), 0.001);

        // Test -Inf to zero - this should be a valid value range
        HistogramPercentileStage.BucketInfo negativeInfToZero = new HistogramPercentileStage.BucketInfo("edge2", "-Inf-0");
        assertEquals(Double.NEGATIVE_INFINITY, negativeInfToZero.getLowerBound(), 0.001);
        assertEquals(0, negativeInfToZero.getUpperBound(), 0.001);

        // Test -infinity (lowercase) to zero - this should be a valid value range
        HistogramPercentileStage.BucketInfo negativeInfinityToZero = new HistogramPercentileStage.BucketInfo("edge2b", "-infinity-0");
        assertEquals(Double.NEGATIVE_INFINITY, negativeInfinityToZero.getLowerBound(), 0.001);
        assertEquals(0, negativeInfinityToZero.getUpperBound(), 0.001);

        // Test positive value to +Inf
        HistogramPercentileStage.BucketInfo posValueToPosInf = new HistogramPercentileStage.BucketInfo("edge3", "10ms-+Inf");
        assertEquals(10.0, posValueToPosInf.getLowerBound(), 0.001);
        assertEquals(10.0, posValueToPosInf.getUpperBound(), 0.001); // For +Inf buckets, upper = lower

        // Test zero to +Inf
        HistogramPercentileStage.BucketInfo zeroToPosInf = new HistogramPercentileStage.BucketInfo("edge4", "0-+Inf");
        assertEquals(0.0, zeroToPosInf.getLowerBound(), 0.001);
        assertEquals(0.0, zeroToPosInf.getUpperBound(), 0.001); // For +Inf buckets, upper = lower

        // Test parseDurationRange with fractional values involving infinity
        HistogramPercentileStage.BucketInfo fracToPosInf = new HistogramPercentileStage.BucketInfo("frac1", "2.5s-infinity");
        assertEquals(2500.0, fracToPosInf.getLowerBound(), 0.001);
        assertEquals(2500.0, fracToPosInf.getUpperBound(), 0.001); // For infinity buckets, upper = lower

        // Test that equal infinity buckets are equal
        HistogramPercentileStage.BucketInfo infBucket1 = new HistogramPercentileStage.BucketInfo("inf1", "-Inf-5s");
        HistogramPercentileStage.BucketInfo infBucket2 = new HistogramPercentileStage.BucketInfo("inf1", "-Inf-5s");
        assertEquals("Negative infinity buckets should be equal", infBucket1, infBucket2);
        assertEquals("Negative infinity buckets should have equal hash codes", infBucket1.hashCode(), infBucket2.hashCode());

        // Test that different infinity buckets are not equal
        HistogramPercentileStage.BucketInfo differentInf = new HistogramPercentileStage.BucketInfo("inf1", "-Inf-10s");
        assertNotEquals("Different negative infinity ranges should not be equal", infBucket1, differentInf);

        // Test toString contains correct information for infinity buckets
        String negInfToString = negInfToPosValue.toString();
        assertTrue("toString should contain bucketId", negInfToString.contains("edge1"));
        assertTrue("toString should contain bucketRange", negInfToString.contains("-Inf-1s"));
        assertTrue("toString should contain computed upper bound", negInfToString.contains("upper=1000.0"));
    }

    /**
     * Test HistogramPercentileStage with null input throws exception.
     */
    public void testNullInputThrowsException() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketid", "bucket", List.of(99.0f));
        TestUtils.assertNullInputThrowsException(stage, "histogram_percentile");
    }

    /**
     * Test percentile calculation with -infinity-2ms bucket.
     * Verifies that the upper bound of the -infinity-2ms bucket (2.0) is returned instead of null.
     * Based on scenario: bucket=-infinity-2ms with count=24, and other buckets (2ms-4ms, 4ms-6ms, 6ms-8ms) with count=0.
     */
    public void testPercentileWithNegativeInfinityBucket() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketid", "bucket", List.of(50.0f));

        List<TimeSeries> input = new ArrayList<>();
        long timestamp = 1000L;

        // Create 4 time series matching the scenario from the image:
        // 1. -infinity-2ms bucket with count=24
        ByteLabels labels1 = ByteLabels.fromStrings("uber_region", "phx", "bucketid", "0000", "bucket", "-infinity-2ms");
        List<Sample> samples1 = List.of(new FloatSample(timestamp, 24.0));
        TimeSeries series1 = new TimeSeries(samples1, labels1, timestamp, timestamp, 1000L, null);
        input.add(series1);

        // 2. 2ms-4ms bucket with count=0
        ByteLabels labels2 = ByteLabels.fromStrings("uber_region", "phx", "bucketid", "0001", "bucket", "2ms-4ms");
        List<Sample> samples2 = List.of(new FloatSample(timestamp, 0.0));
        TimeSeries series2 = new TimeSeries(samples2, labels2, timestamp, timestamp, 1000L, null);
        input.add(series2);

        // 3. 4ms-6ms bucket with count=0
        ByteLabels labels3 = ByteLabels.fromStrings("uber_region", "phx", "bucketid", "0002", "bucket", "4ms-6ms");
        List<Sample> samples3 = List.of(new FloatSample(timestamp, 0.0));
        TimeSeries series3 = new TimeSeries(samples3, labels3, timestamp, timestamp, 1000L, null);
        input.add(series3);

        // 4. 6ms-8ms bucket with count=0
        ByteLabels labels4 = ByteLabels.fromStrings("uber_region", "phx", "bucketid", "0003", "bucket", "6ms-8ms");
        List<Sample> samples4 = List.of(new FloatSample(timestamp, 0.0));
        TimeSeries series4 = new TimeSeries(samples4, labels4, timestamp, timestamp, 1000L, null);
        input.add(series4);

        // Execute
        List<TimeSeries> result = stage.process(input);

        // Verify
        assertEquals(1, result.size()); // One result series for P50
        TimeSeries p50Series = result.get(0);

        // Check labels
        assertTrue(p50Series.getLabels().has("histogramPercentile"));
        assertEquals("p50", p50Series.getLabels().get("histogramPercentile"));
        assertEquals("phx", p50Series.getLabels().get("uber_region"));

        // Check P50 value
        // Total count = 24, P50 target = 12 (50% of 24)
        // Cumulative: 24 at -infinity-2ms bucket
        // So P50 falls in the -infinity-2ms bucket, upper bound = 2.0ms
        List<Sample> samples = p50Series.getSamples().toList();
        assertEquals(1, samples.size());
        assertEquals(2.0, ((FloatSample) samples.get(0)).getValue(), 0.001);
    }

    // ========== Complex Duration Format Tests ==========

    /**
     * Test complex multi-unit duration formats like "2m11.072s" and "1m5.536s".
     * This tests the enhanced parseDuration method's ability to handle Go-style duration formats.
     */
    public void testComplexDurationFormats() {
        // Test the specific failing cases mentioned in the issue
        HistogramPercentileStage.BucketInfo bucket1 = new HistogramPercentileStage.BucketInfo("b1", "2m11.072s-infinity");
        assertEquals(131072.0, bucket1.getLowerBound(), 0.001); // 2m + 11.072s = 120000ms + 11072ms = 131072ms

        HistogramPercentileStage.BucketInfo bucket2 = new HistogramPercentileStage.BucketInfo("b2", "32.768s-1m5.536s");
        assertEquals(32768.0, bucket2.getLowerBound(), 0.001); // 32.768s = 32768ms
        assertEquals(65536.0, bucket2.getUpperBound(), 0.001); // 1m + 5.536s = 60000ms + 5536ms = 65536ms

        // Test other multi-unit combinations
        HistogramPercentileStage.BucketInfo bucket3 = new HistogramPercentileStage.BucketInfo("b3", "2m37s-5m10s");
        assertEquals(157000.0, bucket3.getLowerBound(), 0.001); // 2m37s = 120000ms + 37000ms = 157000ms
        assertEquals(310000.0, bucket3.getUpperBound(), 0.001); // 5m10s = 300000ms + 10000ms = 310000ms

        // Test hour + minute combinations
        HistogramPercentileStage.BucketInfo bucket4 = new HistogramPercentileStage.BucketInfo("b4", "1h30m-2h45m");
        assertEquals(5400000.0, bucket4.getLowerBound(), 0.001); // 1h30m = 3600000ms + 1800000ms = 5400000ms
        assertEquals(9900000.0, bucket4.getUpperBound(), 0.001); // 2h45m = 7200000ms + 2700000ms = 9900000ms
    }

    /**
     * Test fractional values in multi-unit durations.
     */
    public void testFractionalMultiUnit() {
        // Test fractional seconds with minutes
        HistogramPercentileStage.BucketInfo bucket1 = new HistogramPercentileStage.BucketInfo("b1", "1m30.5s-2m45.25s");
        assertEquals(90500.0, bucket1.getLowerBound(), 0.001); // 1m30.5s = 60000ms + 30500ms = 90500ms
        assertEquals(165250.0, bucket1.getUpperBound(), 0.001); // 2m45.25s = 120000ms + 45250ms = 165250ms

        // Test fractional minutes
        HistogramPercentileStage.BucketInfo bucket2 = new HistogramPercentileStage.BucketInfo("b2", "1.5m-2.5m");
        assertEquals(90000.0, bucket2.getLowerBound(), 0.001); // 1.5m = 90000ms
        assertEquals(150000.0, bucket2.getUpperBound(), 0.001); // 2.5m = 150000ms

        // Test fractional hours with minutes
        HistogramPercentileStage.BucketInfo bucket3 = new HistogramPercentileStage.BucketInfo("b3", "1.5h30m-2h30m");
        assertEquals(7200000.0, bucket3.getLowerBound(), 0.001); // 1.5h + 30m = 5400000ms + 1800000ms = 7200000ms
        assertEquals(9000000.0, bucket3.getUpperBound(), 0.001); // 2h30m = 7200000ms + 1800000ms = 9000000ms
    }

    /**
     * Test the __ → µ replacement workaround for bad data.
     */
    public void testWorkaroundReplacement() {
        // Test microsecond replacement
        HistogramPercentileStage.BucketInfo bucket1 = new HistogramPercentileStage.BucketInfo("b1", "100__s-200__s");
        assertEquals(0.1, bucket1.getLowerBound(), 0.0001); // 100µs = 0.1ms
        assertEquals(0.2, bucket1.getUpperBound(), 0.0001); // 200µs = 0.2ms

        // Test mixed with other units
        HistogramPercentileStage.BucketInfo bucket2 = new HistogramPercentileStage.BucketInfo("b2", "1ms100__s-5ms");
        assertEquals(1.1, bucket2.getLowerBound(), 0.0001); // 1ms + 100µs = 1.1ms
        assertEquals(5.0, bucket2.getUpperBound(), 0.001); // 5ms
    }

    /**
     * Test error cases for duplicate units and invalid formats.
     */
    public void testDurationErrorCases() {
        // Test duplicate units (Go-compatible behavior)
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b1", "1m2m-3m"));
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b2", "1s5s-10s"));
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b3", "1h2h-3h"));

        // Test invalid units
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b4", "2m5x-10m"));
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b5", "1y-2y")); // 'y' not supported

        // Test invalid characters without units
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b6", "2m5-10m"));
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b7", "2m5s10-3m"));

        // Test malformed input
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b8", "2m..5s-10m"));
        assertThrows(IllegalArgumentException.class, () -> new HistogramPercentileStage.BucketInfo("b9", "2m 5s-10m")); // space not allowed
    }

    /**
     * Test histogram percentile calculation with complex duration bucket ranges.
     * This ensures the enhanced parsing works end-to-end with the percentile calculation.
     */
    public void testHistogramWithComplexDurations() {
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(50.0f));

        List<TimeSeries> input = new ArrayList<>();
        long timestamp = 1000L;

        // Create buckets with complex duration ranges
        String[] bucketRanges = { "1m30s-2m", "2m-2m11.072s", "2m11.072s-3m", "3m-5m" };
        int[] counts = { 25, 25, 25, 25 }; // Total: 100 requests

        for (int i = 0; i < bucketRanges.length; i++) {
            ByteLabels labels = ByteLabels.fromStrings("service", "test", "bucketId", "b" + (i + 1), "bucket", bucketRanges[i]);

            List<Sample> samples = List.of(new FloatSample(timestamp, counts[i]));
            TimeSeries series = new TimeSeries(samples, labels, timestamp, timestamp, 1000L, null);
            input.add(series);
        }

        // Execute - should not throw and should process correctly
        List<TimeSeries> result = stage.process(input);

        // Verify - should have 1 result series for P50
        assertEquals(1, result.size());
        TimeSeries p50Series = result.get(0);

        // P50 of 100 = 50th value, falls in the 2m-2m11.072s bucket (after 25+25=50)
        SampleList samples = p50Series.getSamples();
        assertEquals(1, samples.size());
        // Upper bound of 2m11.072s bucket is 131072ms
        assertEquals(131072.0, samples.getValue(0), 0.001);
    }

    /**
     * Test that value ranges like "10-20" still work through fallback mechanism.
     * This ensures backward compatibility is preserved.
     */
    public void testValueRangeFallbackWithEnhancedParser() {
        // Test numeric value ranges (should fallback from duration parsing to value parsing)
        HistogramPercentileStage.BucketInfo bucket1 = new HistogramPercentileStage.BucketInfo("b1", "10-20");
        assertEquals(10.0, bucket1.getLowerBound(), 0.001);
        assertEquals(20.0, bucket1.getUpperBound(), 0.001);

        HistogramPercentileStage.BucketInfo bucket2 = new HistogramPercentileStage.BucketInfo("b2", "0-100");
        assertEquals(0.0, bucket2.getLowerBound(), 0.001);
        assertEquals(100.0, bucket2.getUpperBound(), 0.001);

        HistogramPercentileStage.BucketInfo bucket3 = new HistogramPercentileStage.BucketInfo("b3", "1.5-3.7");
        assertEquals(1.5, bucket3.getLowerBound(), 0.001);
        assertEquals(3.7, bucket3.getUpperBound(), 0.001);

        // Test mixed scenario - histogram with both duration and value ranges
        HistogramPercentileStage stage = new HistogramPercentileStage("bucketId", "bucket", List.of(50.0f));

        List<TimeSeries> input = new ArrayList<>();
        long timestamp = 1000L;

        // Mix duration ranges and value ranges
        String[] bucketRanges = { "10ms-50ms", "10-20", "100ms-200ms", "50-100" };
        int[] counts = { 25, 25, 25, 25 };

        for (int i = 0; i < bucketRanges.length; i++) {
            ByteLabels labels = ByteLabels.fromStrings("metric", "mixed", "bucketId", "b" + (i + 1), "bucket", bucketRanges[i]);

            List<Sample> samples = List.of(new FloatSample(timestamp, counts[i]));
            TimeSeries series = new TimeSeries(samples, labels, timestamp, timestamp, 1000L, null);
            input.add(series);
        }

        // Execute - should process mixed duration and value ranges correctly
        List<TimeSeries> result = stage.process(input);
        assertNotNull(result); // Should succeed without throwing exceptions
    }

}
