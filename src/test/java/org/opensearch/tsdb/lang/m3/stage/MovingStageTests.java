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
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import static org.opensearch.tsdb.TestUtils.findSeriesByLabel;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

public class MovingStageTests extends AbstractWireSerializingTestCase<MovingStage> {

    /**
     * Test moving sum with dense, sparse, and empty time series.
     * Window size = 30ms (3 data points with step=10ms).
     */
    public void testMovingSum() {
        MovingStage stage = new MovingStage(30L, WindowAggregationType.SUM);

        // Dense series: all data points present [1,2,3,4,5,6,7] at [0,10,20,30,40,50,60]
        List<Sample> denseSamples = List.of(
            new FloatSample(0L, 1.0),
            new FloatSample(10L, 2.0),
            new FloatSample(20L, 3.0),
            new FloatSample(30L, 4.0),
            new FloatSample(40L, 5.0),
            new FloatSample(50L, 6.0),
            new FloatSample(60L, 7.0)
        );

        // Sparse series: data at [0,20,40,60], missing at [10,30,50]
        List<Sample> sparseSamples = List.of(
            new FloatSample(0L, 1.0),
            new FloatSample(20L, 3.0),
            new FloatSample(40L, 5.0),
            new FloatSample(60L, 7.0)
        );

        // Empty series: no data points
        List<Sample> emptySamples = List.of();

        ByteLabels labels1 = ByteLabels.fromStrings("type", "dense");
        ByteLabels labels2 = ByteLabels.fromStrings("type", "sparse");
        ByteLabels labels3 = ByteLabels.fromStrings("type", "empty");

        TimeSeries denseSeries = new TimeSeries(denseSamples, labels1, 0L, 60L, 10L, null);
        TimeSeries sparseSeries = new TimeSeries(sparseSamples, labels2, 0L, 60L, 10L, null);
        TimeSeries emptySeries = new TimeSeries(emptySamples, labels3, 0L, 60L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(denseSeries, sparseSeries, emptySeries));

        assertEquals(3, result.size());

        // Dense result: sum of window before each timestamp
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(10L, 1.0),   // sum([1])
            new FloatSample(20L, 3.0),   // sum([1,2])
            new FloatSample(30L, 6.0),   // sum([1,2,3])
            new FloatSample(40L, 9.0),   // sum([2,3,4])
            new FloatSample(50L, 12.0),  // sum([3,4,5])
            new FloatSample(60L, 15.0)   // sum([4,5,6])
        );
        assertSamplesEqual("Sum Dense", expectedDense, denseResult.getSamples().toList());

        // Sparse result
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(
            new FloatSample(10L, 1.0),   // sum([1])
            new FloatSample(20L, 1.0),   // sum([1,null])
            new FloatSample(30L, 4.0),   // sum([1,null,3])
            new FloatSample(40L, 3.0),   // sum([null,3,null])
            new FloatSample(50L, 8.0),   // sum([3,null,5])
            new FloatSample(60L, 5.0)    // sum([null,5,null])
        );
        assertSamplesEqual("Sum Sparse", expectedSparse, sparseResult.getSamples().toList());

        // Empty result: no samples
        TimeSeries emptyResult = findSeriesByLabel(result, "type", "empty");
        assertEquals(0, emptyResult.getSamples().size());
    }

    /**
     * Test moving average with dense, sparse, and empty time series.
     */
    public void testMovingAvg() {
        MovingStage stage = new MovingStage(30L, WindowAggregationType.AVG);

        // Dense series: [10,20,30,40,50,60,70]
        List<Sample> denseSamples = List.of(
            new FloatSample(0L, 10.0),
            new FloatSample(10L, 20.0),
            new FloatSample(20L, 30.0),
            new FloatSample(30L, 40.0),
            new FloatSample(40L, 50.0),
            new FloatSample(50L, 60.0),
            new FloatSample(60L, 70.0)
        );

        // Sparse series: data at [0,20,40,60]
        List<Sample> sparseSamples = List.of(
            new FloatSample(0L, 10.0),
            new FloatSample(20L, 30.0),
            new FloatSample(40L, 50.0),
            new FloatSample(60L, 70.0)
        );

        // Empty series
        List<Sample> emptySamples = List.of();

        ByteLabels labels1 = ByteLabels.fromStrings("type", "dense");
        ByteLabels labels2 = ByteLabels.fromStrings("type", "sparse");
        ByteLabels labels3 = ByteLabels.fromStrings("type", "empty");

        TimeSeries denseSeries = new TimeSeries(denseSamples, labels1, 0L, 60L, 10L, null);
        TimeSeries sparseSeries = new TimeSeries(sparseSamples, labels2, 0L, 60L, 10L, null);
        TimeSeries emptySeries = new TimeSeries(emptySamples, labels3, 0L, 60L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(denseSeries, sparseSeries, emptySeries));

        assertEquals(3, result.size());

        // Dense result
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(10L, 10.0),  // avg([10]) = 10
            new FloatSample(20L, 15.0),  // avg([10,20]) = 15
            new FloatSample(30L, 20.0),  // avg([10,20,30]) = 20
            new FloatSample(40L, 30.0),  // avg([20,30,40]) = 30
            new FloatSample(50L, 40.0),  // avg([30,40,50]) = 40
            new FloatSample(60L, 50.0)   // avg([40,50,60]) = 50
        );
        assertSamplesEqual("Avg Dense", expectedDense, denseResult.getSamples().toList());

        // Sparse result: average only over non-null values
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(
            new FloatSample(10L, 10.0),  // avg([10]) = 10
            new FloatSample(20L, 10.0),  // avg([10]) = 10 (null ignored)
            new FloatSample(30L, 20.0),  // avg([10,30]) = 20
            new FloatSample(40L, 30.0),  // avg([30]) = 30
            new FloatSample(50L, 40.0),  // avg([30,50]) = 40
            new FloatSample(60L, 50.0)   // avg([50]) = 50
        );
        assertSamplesEqual("Avg Sparse", expectedSparse, sparseResult.getSamples().toList());

        // Empty result
        TimeSeries emptyResult = findSeriesByLabel(result, "type", "empty");
        assertEquals(0, emptyResult.getSamples().size());
    }

    /**
     * Test moving min with dense, sparse, and empty time series.
     */
    public void testMovingMin() {
        MovingStage stage = new MovingStage(30L, WindowAggregationType.MIN);

        // Dense series: [9,3,7,1,8,2,5]
        List<Sample> denseSamples = List.of(
            new FloatSample(0L, 9.0),
            new FloatSample(10L, 3.0),
            new FloatSample(20L, 7.0),
            new FloatSample(30L, 1.0),
            new FloatSample(40L, 8.0),
            new FloatSample(50L, 2.0),
            new FloatSample(60L, 5.0)
        );

        // Sparse series: data at [0,20,40,60]
        List<Sample> sparseSamples = List.of(
            new FloatSample(0L, 9.0),
            new FloatSample(20L, 7.0),
            new FloatSample(40L, 8.0),
            new FloatSample(60L, 5.0)
        );

        // Empty series
        List<Sample> emptySamples = List.of();

        ByteLabels labels1 = ByteLabels.fromStrings("type", "dense");
        ByteLabels labels2 = ByteLabels.fromStrings("type", "sparse");
        ByteLabels labels3 = ByteLabels.fromStrings("type", "empty");

        TimeSeries denseSeries = new TimeSeries(denseSamples, labels1, 0L, 60L, 10L, null);
        TimeSeries sparseSeries = new TimeSeries(sparseSamples, labels2, 0L, 60L, 10L, null);
        TimeSeries emptySeries = new TimeSeries(emptySamples, labels3, 0L, 60L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(denseSeries, sparseSeries, emptySeries));

        assertEquals(3, result.size());

        // Dense result
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(10L, 9.0),   // min([9]) = 9
            new FloatSample(20L, 3.0),   // min([9,3]) = 3
            new FloatSample(30L, 3.0),   // min([9,3,7]) = 3
            new FloatSample(40L, 1.0),   // min([3,7,1]) = 1
            new FloatSample(50L, 1.0),   // min([7,1,8]) = 1
            new FloatSample(60L, 1.0)    // min([1,8,2]) = 1
        );
        assertSamplesEqual("Min Dense", expectedDense, denseResult.getSamples().toList());

        // Sparse result: nulls treated as +infinity
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(
            new FloatSample(10L, 9.0),   // min([9]) = 9
            new FloatSample(20L, 9.0),   // min([9,+inf]) = 9
            new FloatSample(30L, 7.0),   // min([9,+inf,7]) = 7
            new FloatSample(40L, 7.0),   // min([+inf,7,+inf]) = 7
            new FloatSample(50L, 7.0),   // min([7,+inf,8]) = 7
            new FloatSample(60L, 8.0)    // min([+inf,8,+inf]) = 8
        );
        assertSamplesEqual("Min Sparse", expectedSparse, sparseResult.getSamples().toList());

        // Empty result
        TimeSeries emptyResult = findSeriesByLabel(result, "type", "empty");
        assertEquals(0, emptyResult.getSamples().size());
    }

    /**
     * Test moving max with dense, sparse, and empty time series.
     */
    public void testMovingMax() {
        MovingStage stage = new MovingStage(30L, WindowAggregationType.MAX);

        // Dense series: [1,8,2,9,3,7,4]
        List<Sample> denseSamples = List.of(
            new FloatSample(0L, 1.0),
            new FloatSample(10L, 8.0),
            new FloatSample(20L, 2.0),
            new FloatSample(30L, 9.0),
            new FloatSample(40L, 3.0),
            new FloatSample(50L, 7.0),
            new FloatSample(60L, 4.0)
        );

        // Sparse series: data at [0,20,40,60]
        List<Sample> sparseSamples = List.of(
            new FloatSample(0L, 1.0),
            new FloatSample(20L, 2.0),
            new FloatSample(40L, 3.0),
            new FloatSample(60L, 4.0)
        );

        // Empty series
        List<Sample> emptySamples = List.of();

        ByteLabels labels1 = ByteLabels.fromStrings("type", "dense");
        ByteLabels labels2 = ByteLabels.fromStrings("type", "sparse");
        ByteLabels labels3 = ByteLabels.fromStrings("type", "empty");

        TimeSeries denseSeries = new TimeSeries(denseSamples, labels1, 0L, 60L, 10L, null);
        TimeSeries sparseSeries = new TimeSeries(sparseSamples, labels2, 0L, 60L, 10L, null);
        TimeSeries emptySeries = new TimeSeries(emptySamples, labels3, 0L, 60L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(denseSeries, sparseSeries, emptySeries));

        assertEquals(3, result.size());

        // Dense result
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(10L, 1.0),   // max([1]) = 1
            new FloatSample(20L, 8.0),   // max([1,8]) = 8
            new FloatSample(30L, 8.0),   // max([1,8,2]) = 8
            new FloatSample(40L, 9.0),   // max([8,2,9]) = 9
            new FloatSample(50L, 9.0),   // max([2,9,3]) = 9
            new FloatSample(60L, 9.0)    // max([9,3,7]) = 9
        );
        assertSamplesEqual("Max Dense", expectedDense, denseResult.getSamples().toList());

        // Sparse result: nulls treated as -infinity
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(
            new FloatSample(10L, 1.0),   // max([1]) = 1
            new FloatSample(20L, 1.0),   // max([1,-inf]) = 1
            new FloatSample(30L, 2.0),   // max([1,-inf,2]) = 2
            new FloatSample(40L, 2.0),   // max([-inf,2,-inf]) = 2
            new FloatSample(50L, 3.0),   // max([2,-inf,3]) = 3
            new FloatSample(60L, 3.0)    // max([-inf,3,-inf]) = 3
        );
        assertSamplesEqual("Max Sparse", expectedSparse, sparseResult.getSamples().toList());

        // Empty result
        TimeSeries emptyResult = findSeriesByLabel(result, "type", "empty");
        assertEquals(0, emptyResult.getSamples().size());
    }

    /**
     * Test moving median with dense, sparse, and empty time series.
     */
    public void testMovingMedian() {
        MovingStage stage = new MovingStage(30L, WindowAggregationType.MEDIAN);

        // Dense series: [1,9,3,7,5,2,8]
        List<Sample> denseSamples = List.of(
            new FloatSample(0L, 1.0),
            new FloatSample(10L, 9.0),
            new FloatSample(20L, 3.0),
            new FloatSample(30L, 7.0),
            new FloatSample(40L, 5.0),
            new FloatSample(50L, 2.0),
            new FloatSample(60L, 8.0)
        );

        // Sparse series: data at [0,20,40,60]
        List<Sample> sparseSamples = List.of(
            new FloatSample(0L, 1.0),
            new FloatSample(20L, 3.0),
            new FloatSample(40L, 5.0),
            new FloatSample(60L, 8.0)
        );

        // Empty series
        List<Sample> emptySamples = List.of();

        ByteLabels labels1 = ByteLabels.fromStrings("type", "dense");
        ByteLabels labels2 = ByteLabels.fromStrings("type", "sparse");
        ByteLabels labels3 = ByteLabels.fromStrings("type", "empty");

        TimeSeries denseSeries = new TimeSeries(denseSamples, labels1, 0L, 60L, 10L, null);
        TimeSeries sparseSeries = new TimeSeries(sparseSamples, labels2, 0L, 60L, 10L, null);
        TimeSeries emptySeries = new TimeSeries(emptySamples, labels3, 0L, 60L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(denseSeries, sparseSeries, emptySeries));

        assertEquals(3, result.size());

        // Dense result
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(10L, 1.0),   // median([1]) = 1
            new FloatSample(20L, 1.0),   // median([1,9]) = 1 (midpoint=0)
            new FloatSample(30L, 3.0),   // median([1,9,3]) = 3 (sorted: [1,3,9], midpoint=1)
            new FloatSample(40L, 7.0),   // median([9,3,7]) = 7 (sorted: [3,7,9], midpoint=1)
            new FloatSample(50L, 5.0),   // median([3,7,5]) = 5 (sorted: [3,5,7], midpoint=1)
            new FloatSample(60L, 5.0)    // median([7,5,2]) = 5 (sorted: [2,5,7], midpoint=1)
        );
        assertSamplesEqual("Median Dense", expectedDense, denseResult.getSamples().toList());

        // Sparse result: nulls excluded from median calculation
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(
            new FloatSample(10L, 1.0),   // median([1]) = 1
            new FloatSample(20L, 1.0),   // median([1]) = 1 (null excluded)
            new FloatSample(30L, 1.0),   // median([1,3]) = 1 (midpoint=0)
            new FloatSample(40L, 3.0),   // median([3]) = 3
            new FloatSample(50L, 3.0),   // median([3,5]) = 3 (midpoint=0)
            new FloatSample(60L, 5.0)    // median([5]) = 5
        );
        assertSamplesEqual("Median Sparse", expectedSparse, sparseResult.getSamples().toList());

        // Empty result
        TimeSeries emptyResult = findSeriesByLabel(result, "type", "empty");
        assertEquals(0, emptyResult.getSamples().size());
    }

    /**
     * Test window size validation - should throw when window < step.
     */
    public void testWindowSizeTooSmall() {
        MovingStage stage = new MovingStage(5L, WindowAggregationType.SUM);

        List<Sample> samples = List.of(new FloatSample(10L, 1.0), new FloatSample(20L, 2.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "metric1");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 20L, 10L, null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> stage.process(List.of(timeSeries)));
        assertTrue(exception.getMessage().contains("windowSize should not be smaller than stepSize"));
    }

    /**
     * Test XContent serialization.
     */
    public void testToXContent() throws IOException {
        MovingStage stage = new MovingStage(300000L, WindowAggregationType.AVG); // 5 minutes in millis

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            stage.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();

            String json = builder.toString();
            assertEquals("{\"interval\":300000,\"function\":\"avg\"}", json);
        }
    }

    /**
     * Test factory creation from args with interval.
     */
    public void testFromArgs() {
        Map<String, Object> args = Map.of("interval", 60000L, "function", "sum");

        PipelineStage stage = PipelineStageFactory.createWithArgs("moving", args);
        assertNotNull(stage);
        assertTrue(stage instanceof MovingStage);
        assertEquals("moving", stage.getName());
    }

    /**
     * Test factory creation with time_interval string (for convenience).
     */
    public void testFromArgsWithTimeString() {
        Map<String, Object> args = Map.of("time_interval", "1m", "function", "sum");

        MovingStage stage = MovingStage.fromArgs(args);
        assertNotNull(stage);
        assertEquals("moving", stage.getName());
    }

    /**
     * Test factory creation without required parameter.
     */
    public void testFromArgsMissingParameter() {
        Map<String, Object> args = Map.of("function", "sum");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> MovingStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("requires 'interval' or 'time_interval' parameter"));
    }

    @Override
    protected Writeable.Reader<MovingStage> instanceReader() {
        return MovingStage::readFrom;
    }

    @Override
    protected MovingStage createTestInstance() {
        long intervalMillis = randomIntBetween(1, 60) * 60000L; // Random minutes in milliseconds
        WindowAggregationType[] functions = {
            WindowAggregationType.SUM,
            WindowAggregationType.AVG,
            WindowAggregationType.MAX,
            WindowAggregationType.MIN,
            WindowAggregationType.MEDIAN };
        return new MovingStage(intervalMillis, randomFrom(functions));
    }

    public void testNullInputThrowsException() {
        MovingStage stage = new MovingStage(30L, WindowAggregationType.SUM);
        assertNullInputThrowsException(stage, "moving");
    }

    /**
     * Test estimateMemoryOverhead returns reasonable values.
     */
    public void testEstimateMemoryOverhead() {
        MovingStage stage = new MovingStage(30L, WindowAggregationType.SUM);

        // Null and empty input should return 0
        assertEquals(0L, stage.estimateMemoryOverhead(null));
        assertEquals(0L, stage.estimateMemoryOverhead(List.of()));

        // Create test series with samples
        List<Sample> samples = List.of(new FloatSample(0L, 1.0), new FloatSample(10L, 2.0), new FloatSample(20L, 3.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        TimeSeries ts = new TimeSeries(samples, labels, 0L, 20L, 10L, null);

        // Single series should return positive overhead
        long overhead = stage.estimateMemoryOverhead(List.of(ts));
        assertTrue("Overhead should be positive for non-empty input", overhead > 0);

        // Multiple series should scale
        long doubleOverhead = stage.estimateMemoryOverhead(List.of(ts, ts));
        assertTrue("Overhead should scale with number of series", doubleOverhead > overhead);
    }

    /**
     * Test estimateMemoryOverhead for MEDIAN includes TreeMap overhead.
     */
    public void testEstimateMemoryOverheadWithMedian() {
        MovingStage sumStage = new MovingStage(30L, WindowAggregationType.SUM);
        MovingStage medianStage = new MovingStage(30L, WindowAggregationType.MEDIAN);

        List<Sample> samples = List.of(new FloatSample(0L, 1.0), new FloatSample(10L, 2.0), new FloatSample(20L, 3.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        TimeSeries ts = new TimeSeries(samples, labels, 0L, 20L, 10L, null);
        List<TimeSeries> input = List.of(ts);

        long sumOverhead = sumStage.estimateMemoryOverhead(input);
        long medianOverhead = medianStage.estimateMemoryOverhead(input);

        // MEDIAN uses TreeMap, so should have higher overhead
        assertTrue("MEDIAN should have higher overhead due to TreeMap", medianOverhead > sumOverhead);
    }

    /**
     * Test estimateMemoryOverhead handles zero step gracefully.
     */
    public void testEstimateMemoryOverheadWithZeroStep() {
        MovingStage stage = new MovingStage(30L, WindowAggregationType.SUM);

        List<Sample> samples = List.of(new FloatSample(0L, 1.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        // Step of 0 should use default window size estimation
        TimeSeries ts = new TimeSeries(samples, labels, 0L, 0L, 0L, null);

        long overhead = stage.estimateMemoryOverhead(List.of(ts));
        assertTrue("Should handle zero step gracefully", overhead > 0);
    }
}
