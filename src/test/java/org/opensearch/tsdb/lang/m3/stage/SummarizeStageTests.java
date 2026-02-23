/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.lang.m3.common.WindowAggregationType;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import static org.opensearch.tsdb.TestUtils.findSeriesByLabel;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

public class SummarizeStageTests extends AbstractWireSerializingTestCase<SummarizeStage> {

    @Override
    protected Writeable.Reader<SummarizeStage> instanceReader() {
        return SummarizeStage::readFrom;
    }

    @Override
    protected SummarizeStage createTestInstance() {
        long interval = randomLongBetween(1000, 3600000); // 1s to 1h
        WindowAggregationType[] functions = {
            WindowAggregationType.SUM,
            WindowAggregationType.AVG,
            WindowAggregationType.MAX,
            WindowAggregationType.MIN,
            WindowAggregationType.LAST,
            WindowAggregationType.STDDEV,
            WindowAggregationType.withPercentile(50),
            WindowAggregationType.withPercentile(90),
            WindowAggregationType.withPercentile(95),
            WindowAggregationType.withPercentile(99) };
        WindowAggregationType function = functions[randomIntBetween(0, functions.length - 1)];
        boolean alignToFrom = randomBoolean();
        long referenceTimeConstant = randomLong();
        return new SummarizeStage(interval, function, alignToFrom).setReferenceTimeConstant(referenceTimeConstant);
    }

    /**
     * Create test input with dense, sparse, and empty time series.
     * Interval = 300ms, samples at [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
     */
    private List<TimeSeries> createTestInput() {
        // Dense series: all data points present [10,20,30,40,50,60,70,80,90,100]
        List<Sample> denseSamples = List.of(
            new FloatSample(100L, 10.0),
            new FloatSample(200L, 20.0),
            new FloatSample(300L, 30.0),
            new FloatSample(400L, 40.0),
            new FloatSample(500L, 50.0),
            new FloatSample(600L, 60.0),
            new FloatSample(700L, 70.0),
            new FloatSample(800L, 80.0),
            new FloatSample(900L, 90.0),
            new FloatSample(1000L, 100.0)
        );

        // Sparse series: data at [100,300,500,700,900], missing at [200,400,600,800,1000]
        List<Sample> sparseSamples = List.of(
            new FloatSample(100L, 10.0),
            new FloatSample(300L, 30.0),
            new FloatSample(500L, 50.0),
            new FloatSample(700L, 70.0),
            new FloatSample(900L, 90.0)
        );

        // Empty series: no data points
        List<Sample> emptySamples = List.of();

        ByteLabels labels1 = ByteLabels.fromStrings("type", "dense");
        ByteLabels labels2 = ByteLabels.fromStrings("type", "sparse");
        ByteLabels labels3 = ByteLabels.fromStrings("type", "empty");

        TimeSeries denseSeries = new TimeSeries(denseSamples, labels1, 100L, 1000L, 100L, null);
        TimeSeries sparseSeries = new TimeSeries(sparseSamples, labels2, 100L, 1000L, 100L, null);
        TimeSeries emptySeries = new TimeSeries(emptySamples, labels3, 100L, 1000L, 100L, null);

        return List.of(denseSeries, sparseSeries, emptySeries);
    }

    public void testSummarizeSum() {
        SummarizeStage stage = new SummarizeStage(300, WindowAggregationType.SUM, false);
        List<TimeSeries> result = stage.process(createTestInput());

        assertEquals(3, result.size());

        // Dense result: 3 buckets [0-300), [300-600), [600-900), [900-1200)
        // Bucket 0-300: [10,20] -> 30
        // Bucket 300-600: [30,40,50] -> 120
        // Bucket 600-900: [60,70,80] -> 210
        // Bucket 900-1200: [90,100] -> 190
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(0L, 30.0),
            new FloatSample(300L, 120.0),
            new FloatSample(600L, 210.0),
            new FloatSample(900L, 190.0)
        );
        assertSamplesEqual("Sum Dense", expectedDense, denseResult.getSamples().toList());

        // Sparse result: [10], [30+50], [70], [90] -> 10, 80, 70, 90
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(
            new FloatSample(0L, 10.0),
            new FloatSample(300L, 80.0),
            new FloatSample(600L, 70.0),
            new FloatSample(900L, 90.0)
        );
        assertSamplesEqual("Sum Sparse", expectedSparse, sparseResult.getSamples().toList());

        // Empty result: no samples
        TimeSeries emptyResult = findSeriesByLabel(result, "type", "empty");
        assertEquals(0, emptyResult.getSamples().size());
    }

    public void testSummarizeAvg() {
        SummarizeStage stage = new SummarizeStage(300, WindowAggregationType.AVG, false);
        List<TimeSeries> result = stage.process(createTestInput());

        assertEquals(3, result.size());

        // Dense result: avg([10,20])=15, avg([30,40,50])=40, avg([60,70,80])=70, avg([90,100])=95
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(0L, 15.0),
            new FloatSample(300L, 40.0),
            new FloatSample(600L, 70.0),
            new FloatSample(900L, 95.0)
        );
        assertSamplesEqual("Avg Dense", expectedDense, denseResult.getSamples().toList());

        // Sparse result: avg([10])=10, avg([30,50])=40, avg([70])=70, avg([90])=90
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(
            new FloatSample(0L, 10.0),
            new FloatSample(300L, 40.0),
            new FloatSample(600L, 70.0),
            new FloatSample(900L, 90.0)
        );
        assertSamplesEqual("Avg Sparse", expectedSparse, sparseResult.getSamples().toList());
    }

    public void testSummarizeMax() {
        SummarizeStage stage = new SummarizeStage(300, WindowAggregationType.MAX, false);
        List<TimeSeries> result = stage.process(createTestInput());

        assertEquals(3, result.size());

        // Dense result: max([10,20])=20, max([30,40,50])=50, max([60,70,80])=80, max([90,100])=100
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(0L, 20.0),
            new FloatSample(300L, 50.0),
            new FloatSample(600L, 80.0),
            new FloatSample(900L, 100.0)
        );
        assertSamplesEqual("Max Dense", expectedDense, denseResult.getSamples().toList());

        // Sparse result: max([10])=10, max([30,50])=50, max([70])=70, max([90])=90
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(
            new FloatSample(0L, 10.0),
            new FloatSample(300L, 50.0),
            new FloatSample(600L, 70.0),
            new FloatSample(900L, 90.0)
        );
        assertSamplesEqual("Max Sparse", expectedSparse, sparseResult.getSamples().toList());
    }

    public void testSummarizeMin() {
        SummarizeStage stage = new SummarizeStage(300, WindowAggregationType.MIN, false);
        List<TimeSeries> result = stage.process(createTestInput());

        assertEquals(3, result.size());

        // Dense result: min([10,20])=10, min([30,40,50])=30, min([60,70,80])=60, min([90,100])=90
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(0L, 10.0),
            new FloatSample(300L, 30.0),
            new FloatSample(600L, 60.0),
            new FloatSample(900L, 90.0)
        );
        assertSamplesEqual("Min Dense", expectedDense, denseResult.getSamples().toList());

        // Sparse result: min([10])=10, min([30,50])=30, min([70])=70, min([90])=90
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(
            new FloatSample(0L, 10.0),
            new FloatSample(300L, 30.0),
            new FloatSample(600L, 70.0),
            new FloatSample(900L, 90.0)
        );
        assertSamplesEqual("Min Sparse", expectedSparse, sparseResult.getSamples().toList());
    }

    public void testSummarizeLast() {
        SummarizeStage stage = new SummarizeStage(300, WindowAggregationType.LAST, false);
        List<TimeSeries> result = stage.process(createTestInput());

        assertEquals(3, result.size());

        // Dense result: last([10,20])=20, last([30,40,50])=50, last([60,70,80])=80, last([90,100])=100
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(0L, 20.0),
            new FloatSample(300L, 50.0),
            new FloatSample(600L, 80.0),
            new FloatSample(900L, 100.0)
        );
        assertSamplesEqual("Last Dense", expectedDense, denseResult.getSamples().toList());

        // Sparse result: last([10])=10, last([30,50])=50, last([70])=70, last([90])=90
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(
            new FloatSample(0L, 10.0),
            new FloatSample(300L, 50.0),
            new FloatSample(600L, 70.0),
            new FloatSample(900L, 90.0)
        );
        assertSamplesEqual("Last Sparse", expectedSparse, sparseResult.getSamples().toList());
    }

    public void testSummarizeP50() {
        SummarizeStage stage = new SummarizeStage(300, WindowAggregationType.withPercentile(50), false);
        List<TimeSeries> result = stage.process(createTestInput());

        assertEquals(3, result.size());

        // Dense result: p50([10,20])=10, p50([30,40,50])=40, p50([60,70,80])=70, p50([90,100])=90
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(0L, 10.0),
            new FloatSample(300L, 40.0),
            new FloatSample(600L, 70.0),
            new FloatSample(900L, 90.0)
        );
        assertSamplesEqual("P50 Dense", expectedDense, denseResult.getSamples().toList());
    }

    public void testSummarizeStdDev() {
        SummarizeStage stage = new SummarizeStage(300, WindowAggregationType.STDDEV, false);
        List<TimeSeries> result = stage.process(createTestInput());

        assertEquals(3, result.size());

        // Dense result: stddev([10,20])≈7.071, stddev([30,40,50])=10, stddev([60,70,80])=10, stddev([90,100])≈7.071
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(0L, 7.0710678118654755),
            new FloatSample(300L, 10.0),
            new FloatSample(600L, 10.0),
            new FloatSample(900L, 7.0710678118654755)
        );
        assertSamplesEqual("StdDev Dense", expectedDense, denseResult.getSamples().toList());
    }

    public void testSummarizeAlignToFrom() {
        SummarizeStage stage = new SummarizeStage(300, WindowAggregationType.SUM, true);
        List<TimeSeries> result = stage.process(createTestInput());

        assertEquals(3, result.size());

        // With alignToFrom=true, buckets align to series start (100)
        // Buckets: [100-400), [400-700), [700-1000), [1000-1300)
        // Dense: [10,20,30] -> 60, [40,50,60] -> 150, [70,80,90] -> 240, [100] -> 100
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(100L, 60.0),
            new FloatSample(400L, 150.0),
            new FloatSample(700L, 240.0),
            new FloatSample(1000L, 100.0)
        );
        assertSamplesEqual("AlignToFrom Dense", expectedDense, denseResult.getSamples().toList());

        // Sparse: [10,30] -> 40, [50] -> 50, [70,90] -> 160, [] -> no bucket
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(new FloatSample(100L, 40.0), new FloatSample(400L, 50.0), new FloatSample(700L, 160.0));
        assertSamplesEqual("AlignToFrom Sparse", expectedSparse, sparseResult.getSamples().toList());
    }

    public void testSummarizeIntervalSmallerThanResolution() {
        SummarizeStage stage = new SummarizeStage(500, WindowAggregationType.SUM, false);

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));

        // Series has 1000ms resolution, but interval is 500ms
        TimeSeries series = new TimeSeries(samples, ByteLabels.fromStrings("host", "h1"), 1000L, 2000L, 1000L, null);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> stage.process(List.of(series)));
        assertTrue(e.getMessage().contains("must be >= series resolution"));
    }

    public void testConstructorInvalidInterval() {
        expectThrows(IllegalArgumentException.class, () -> new SummarizeStage(0, WindowAggregationType.SUM, false));
        expectThrows(IllegalArgumentException.class, () -> new SummarizeStage(-1000, WindowAggregationType.SUM, false));
    }

    public void testConstructorInvalidFunction() {
        expectThrows(IllegalArgumentException.class, () -> new SummarizeStage(1000, null, false));
    }

    public void testFromArgsMissingFunction() {
        Map<String, Object> args = Map.of("interval", 5000, "alignToFrom", false);
        expectThrows(IllegalArgumentException.class, () -> SummarizeStage.fromArgs(args));
    }

    public void testFromArgsMissingAlignToFrom() {
        Map<String, Object> args = Map.of("interval", 5000, "function", "sum");
        expectThrows(IllegalArgumentException.class, () -> SummarizeStage.fromArgs(args));
    }

    public void testFromArgsWithAllParameters() {
        Map<String, Object> args = Map.of("interval", 10000, "function", "avg", "alignToFrom", true, "referenceTimeConstant", 123456789L);

        SummarizeStage stage = SummarizeStage.fromArgs(args);
        assertNotNull(stage);
    }

    public void testFromArgsNullArgs() {
        expectThrows(IllegalArgumentException.class, () -> SummarizeStage.fromArgs(null));
    }

    public void testFromArgsMissingIntervalRequired() {
        Map<String, Object> args = Map.of("function", "sum");
        expectThrows(IllegalArgumentException.class, () -> SummarizeStage.fromArgs(args));
    }

    public void testFromArgsInvalidIntervalType() {
        expectThrows(IllegalArgumentException.class, () -> SummarizeStage.fromArgs(Map.of("interval", true, "function", "sum")));
        expectThrows(IllegalArgumentException.class, () -> SummarizeStage.fromArgs(Map.of("interval", "5m", "function", "sum")));
        expectThrows(IllegalArgumentException.class, () -> SummarizeStage.fromArgs(Map.of("interval", "invalid", "function", "sum")));
    }

    public void testFromArgsInvalidFunction() {
        Map<String, Object> args = Map.of("interval", 1000, "function", "invalid");
        expectThrows(IllegalArgumentException.class, () -> SummarizeStage.fromArgs(args));
    }

    public void testFromArgsInvalidFunctionType() {
        Map<String, Object> args = Map.of("interval", 1000, "function", 123);
        expectThrows(IllegalArgumentException.class, () -> SummarizeStage.fromArgs(args));
    }

    public void testFromArgsInvalidAlignToFromType() {
        Map<String, Object> args = Map.of("interval", 1000, "alignToFrom", "true");
        expectThrows(IllegalArgumentException.class, () -> SummarizeStage.fromArgs(args));
    }

    public void testFromArgsMissingReferenceTimeConstant() {
        Map<String, Object> args = Map.of("interval", 5000, "function", "sum", "alignToFrom", false);
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> SummarizeStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("referenceTimeConstant argument is required"));
    }

    public void testFromArgsInvalidReferenceTimeConstantType() {
        Map<String, Object> args = Map.of("interval", 5000, "function", "sum", "alignToFrom", false, "referenceTimeConstant", "invalid");
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> SummarizeStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("referenceTimeConstant must be a number"));
    }

    public void testToXContent_defaultParameters() throws Exception {
        SummarizeStage stage = new SummarizeStage(60000, WindowAggregationType.SUM, false).setReferenceTimeConstant(0L);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"interval\":60000,\"function\":\"sum\",\"alignToFrom\":false,\"referenceTimeConstant\":0}", json);
    }

    public void testToXContent_withAvgFunction() throws Exception {
        SummarizeStage stage = new SummarizeStage(300000, WindowAggregationType.AVG, false).setReferenceTimeConstant(0L);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"interval\":300000,\"function\":\"avg\",\"alignToFrom\":false,\"referenceTimeConstant\":0}", json);
    }

    public void testToXContent_withAlignToFromTrue() throws Exception {
        SummarizeStage stage = new SummarizeStage(60000, WindowAggregationType.MAX, true);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"interval\":60000,\"function\":\"max\",\"alignToFrom\":true,\"referenceTimeConstant\":0}", json);
    }

    public void testToXContent_withPercentileFunction() throws Exception {
        SummarizeStage stage = new SummarizeStage(120000, WindowAggregationType.withPercentile(95), false).setReferenceTimeConstant(0L);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"interval\":120000,\"function\":\"p95.0\",\"alignToFrom\":false,\"referenceTimeConstant\":0}", json);
    }

    public void testToXContent_withStdDevFunction() throws Exception {
        SummarizeStage stage = new SummarizeStage(180000, WindowAggregationType.STDDEV, true);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"interval\":180000,\"function\":\"stddev\",\"alignToFrom\":true,\"referenceTimeConstant\":0}", json);
    }

    public void testToXContent_withMinFunction() throws Exception {
        SummarizeStage stage = new SummarizeStage(30000, WindowAggregationType.MIN, false).setReferenceTimeConstant(0L);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"interval\":30000,\"function\":\"min\",\"alignToFrom\":false,\"referenceTimeConstant\":0}", json);
    }

    public void testToXContent_withLastFunction() throws Exception {
        SummarizeStage stage = new SummarizeStage(90000, WindowAggregationType.LAST, true);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"interval\":90000,\"function\":\"last\",\"alignToFrom\":true,\"referenceTimeConstant\":0}", json);
    }

    public void testNullInputThrowsException() {
        SummarizeStage stage = new SummarizeStage(30000, WindowAggregationType.SUM, false);
        assertNullInputThrowsException(stage, "summarize");
    }

    public void testEstimateMemoryOverhead() {
        SummarizeStage stage = new SummarizeStage(30000, WindowAggregationType.SUM, false);

        // Null and empty input should return 0
        assertEquals(0L, stage.estimateMemoryOverhead(null));
        assertEquals(0L, stage.estimateMemoryOverhead(List.of()));

        // Create test series with samples
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0), new FloatSample(3000L, 30.0));
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, null);

        // Single series should return positive overhead
        long overhead = stage.estimateMemoryOverhead(List.of(ts));
        assertTrue("Overhead should be positive for non-empty input", overhead > 0);

        // Multiple series should scale
        long doubleOverhead = stage.estimateMemoryOverhead(List.of(ts, ts));
        assertTrue("Overhead should scale with number of series", doubleOverhead > overhead);
    }

}
