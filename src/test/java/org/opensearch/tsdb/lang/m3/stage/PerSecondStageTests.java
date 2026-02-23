/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

public class PerSecondStageTests extends AbstractWireSerializingTestCase<PerSecondStage> {

    /**
     * Test case 1: Linearly increasing series.
     * Input: 0,1,2,3 → Output: 1,1,1 (per second rate of change)
     * Example from docs: mockFetch 0,1,2,3 city:atlanta name:actions | perSecond
     */
    public void testLinearlyIncreasingSeries() {
        PerSecondStage stage = new PerSecondStage();
        // Values 0,1,2,3 at timestamps 0s,10s,20s,30s (10 second intervals)
        List<Sample> samples = List.of(
            new FloatSample(0L, 0.0),
            new FloatSample(10000L, 1.0),
            new FloatSample(20000L, 2.0),
            new FloatSample(30000L, 3.0)
        );
        ByteLabels labels = ByteLabels.fromStrings("city", "atlanta", "name", "actions");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 0L, 30000L, 10000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(3, resultSeries.getSamples().size());

        // Rate calculations: (1-0)/10s = 0.1/s, (2-1)/10s = 0.1/s, (3-2)/10s = 0.1/s
        List<Sample> expectedSamples = List.of(new FloatSample(10000L, 0.1), new FloatSample(20000L, 0.1), new FloatSample(30000L, 0.1));
        assertSamplesEqual("Linearly increasing series", expectedSamples, resultSeries.getSamples().toList());
    }

    /**
     * Test case 2: Nonlinear increasing series.
     * Input: 0,10,30,60 → Output: 1,2,3 (per second rate of change)
     * Example from docs: mockFetch 0,10,30,60 city:atlanta name:actions | perSecond
     */
    public void testNonlinearIncreasingSeries() {
        PerSecondStage stage = new PerSecondStage();
        // Values 0,10,30,60 at timestamps 0s,10s,20s,30s (10 second intervals)
        List<Sample> samples = List.of(
            new FloatSample(0L, 0.0),
            new FloatSample(10000L, 10.0),
            new FloatSample(20000L, 30.0),
            new FloatSample(30000L, 60.0)
        );
        ByteLabels labels = ByteLabels.fromStrings("city", "atlanta", "name", "actions");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 0L, 30000L, 10000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(3, resultSeries.getSamples().size());

        // Rate calculations: (10-0)/10s = 1/s, (30-10)/10s = 2/s, (60-30)/10s = 3/s
        List<Sample> expectedSamples = List.of(new FloatSample(10000L, 1.0), new FloatSample(20000L, 2.0), new FloatSample(30000L, 3.0));
        assertSamplesEqual("Nonlinear increasing series", expectedSamples, resultSeries.getSamples().toList());
    }

    /**
     * Test case 3: Missing data points (NaN/null values).
     * Input: 0, [missing], 2, 3 → Output: [no rate for missing], 0.1, 0.1
     * Example from docs: mockFetch 0,NaN,2,3 city:atlanta name:actions | perSecond
     * Note: We skip the timestamp instead of using NaN (timestamp 10s is missing)
     */
    public void testWithMissingDataPoints() {
        PerSecondStage stage = new PerSecondStage();
        // Values 0,2,3 at timestamps 0s,[10s missing],20s,30s
        List<Sample> samples = List.of(
            new FloatSample(0L, 0.0),
            // 10000L timestamp is missing (null/NaN in original data)
            new FloatSample(20000L, 2.0),
            new FloatSample(30000L, 3.0)
        );
        ByteLabels labels = ByteLabels.fromStrings("city", "atlanta", "name", "actions");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 0L, 30000L, 10000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(2, resultSeries.getSamples().size());

        // Rate calculations: (2-0)/20s = 0.1/s, (3-2)/10s = 0.1/s
        List<Sample> expectedSamples = List.of(
            new FloatSample(20000L, 0.1),  // rate between 0s and 20s
            new FloatSample(30000L, 0.1)   // rate between 20s and 30s
        );
        assertSamplesEqual("Missing data points test", expectedSamples, resultSeries.getSamples().toList());
    }

    /**
     * Test case 4: Counter reset (negative rate).
     * Tests handling of counter resets where the value decreases, which should be skipped.
     */
    public void testCounterReset() {
        PerSecondStage stage = new PerSecondStage();
        List<Sample> samples = List.of(
            new FloatSample(1000L, 100.0),  // t=1s, v=100
            new FloatSample(2000L, 50.0),   // t=2s, v=50 -> negative rate, should be skipped
            new FloatSample(3000L, 60.0)    // t=3s, v=60 -> rate = (60-50)/(3-2) = 10.0/s
        );
        ByteLabels labels = ByteLabels.fromStrings("name", "counter_with_reset");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(1, resultSeries.getSamples().size()); // Only the valid positive rate

        List<Sample> expectedSamples = List.of(new FloatSample(3000L, 10.0));
        assertSamplesEqual("Counter reset test", expectedSamples, resultSeries.getSamples().toList());
    }

    /**
     * Test case 6: Single sample (insufficient data).
     * Tests handling of time series with only one sample (can't calculate rate).
     */
    public void testSingleSample() {
        PerSecondStage stage = new PerSecondStage();
        List<Sample> samples = List.of(new FloatSample(1000L, 42.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "single_metric");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(0, resultSeries.getSamples().size()); // Empty result - can't calculate rate with one point

        assertSamplesEqual("Single sample test", List.of(), resultSeries.getSamples().toList());
    }

    /**
     * Test case 7: Empty time series.
     * Tests handling of completely empty time series.
     */
    public void testEmptyTimeSeries() {
        PerSecondStage stage = new PerSecondStage();
        List<Sample> samples = List.of();
        ByteLabels labels = ByteLabels.fromStrings("name", "empty_metric");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(0, resultSeries.getSamples().size());

        assertSamplesEqual("Empty time series test", List.of(), resultSeries.getSamples().toList());
    }

    /**
     * Test case 9: Multiple time series processing.
     * Tests processing multiple time series at once.
     */
    public void testMultipleTimeSeries() {
        PerSecondStage stage = new PerSecondStage();

        // First time series
        List<Sample> samples1 = List.of(
            new FloatSample(1000L, 10.0),
            new FloatSample(2000L, 30.0)  // rate = 20.0/s
        );
        ByteLabels labels1 = ByteLabels.fromStrings("name", "ts1");
        TimeSeries ts1 = new TimeSeries(samples1, labels1, 1000L, 2000L, 1000L, null);

        // Second time series
        List<Sample> samples2 = List.of(
            new FloatSample(1000L, 100.0),
            new FloatSample(3000L, 150.0)  // rate = 25.0/s
        );
        ByteLabels labels2 = ByteLabels.fromStrings("name", "ts2");
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 1000L, 3000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(ts1, ts2));

        assertEquals(2, result.size());

        // First time series result
        List<Sample> expectedSamples1 = List.of(new FloatSample(2000L, 20.0));
        assertSamplesEqual("First time series", expectedSamples1, result.get(0).getSamples().toList());

        // Second time series result
        List<Sample> expectedSamples2 = List.of(new FloatSample(3000L, 25.0));
        assertSamplesEqual("Second time series", expectedSamples2, result.get(1).getSamples().toList());
    }

    /**
     * Test case 10: Zero rate (no change in value).
     * Tests handling of constant values (zero rate).
     */
    public void testZeroRate() {
        PerSecondStage stage = new PerSecondStage();
        List<Sample> samples = List.of(
            new FloatSample(1000L, 50.0),  // t=1s, v=50
            new FloatSample(2000L, 50.0)   // t=2s, v=50 -> rate = 0/1 = 0.0/s
        );
        ByteLabels labels = ByteLabels.fromStrings("name", "constant_metric");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(1, resultSeries.getSamples().size());

        List<Sample> expectedSamples = List.of(new FloatSample(2000L, 0.0));
        assertSamplesEqual("Zero rate test", expectedSamples, resultSeries.getSamples().toList());
    }

    /**
     * Test with empty input list.
     */
    public void testWithEmptyInput() {
        PerSecondStage stage = new PerSecondStage();
        List<TimeSeries> result = stage.process(List.of());
        assertTrue(result.isEmpty());
    }

    /**
     * Test getName().
     */
    public void testGetName() {
        PerSecondStage stage = new PerSecondStage();
        assertEquals("per_second", stage.getName());
    }

    /**
     * Test fromArgs() method.
     */
    public void testFromArgs() {
        Map<String, Object> args = Map.of();
        PerSecondStage stage = PerSecondStage.fromArgs(args);
        assertNotNull(stage);
        assertEquals("per_second", stage.getName());
    }

    /**
     * Test fromArgs() with unexpected arguments (should throw exception).
     */
    public void testFromArgsWithArguments() {
        Map<String, Object> args = Map.of("unexpected", "value");
        assertThrows(IllegalArgumentException.class, () -> PerSecondStage.fromArgs(args));
    }

    /**
     * Test PipelineStageFactory integration.
     */
    public void testPipelineStageFactory() {
        assertTrue(PipelineStageFactory.isStageTypeSupported(PerSecondStage.NAME));
        PipelineStage stage = PipelineStageFactory.createWithArgs(PerSecondStage.NAME, Map.of());
        assertTrue(stage instanceof PerSecondStage);
    }

    /**
     * Test PipelineStageFactory.readFrom(StreamInput).
     */
    public void testPipelineStageFactoryReadFrom_StreamInput() throws Exception {
        PerSecondStage original = new PerSecondStage();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Factory variant that reads stage name first
            out.writeString(PerSecondStage.NAME);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                PipelineStage restored = PipelineStageFactory.readFrom(in);
                assertNotNull(restored);
                assertTrue(restored instanceof PerSecondStage);
            }
        }
    }

    /**
     * Test toXContent().
     */
    public void testToXContent() throws IOException {
        PerSecondStage stage = new PerSecondStage();
        try (XContentBuilder builder = XContentFactory.jsonBuilder().startObject()) {
            stage.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            String json = builder.toString();
            assertEquals("{}", json); // No parameters for perSecond
        }
    }

    /**
     * Test supportConcurrentSegmentSearch().
     */
    public void testSupportConcurrentSegmentSearch() {
        PerSecondStage stage = new PerSecondStage();
        assertFalse(stage.supportConcurrentSegmentSearch());
    }

    /**
     * Test equals() and hashCode().
     */
    public void testEqualsAndHashCode() {
        PerSecondStage stage1 = new PerSecondStage();
        PerSecondStage stage2 = new PerSecondStage();

        assertEquals(stage1, stage2);
        assertEquals(stage1.hashCode(), stage2.hashCode());

        assertNotEquals(stage1, null);
        assertNotEquals(stage1, new Object());
    }

    @Override
    protected PerSecondStage createTestInstance() {
        return new PerSecondStage();
    }

    @Override
    protected Writeable.Reader<PerSecondStage> instanceReader() {
        return PerSecondStage::readFrom;
    }

    public void testNullInputThrowsException() {
        PerSecondStage stage = new PerSecondStage();
        assertNullInputThrowsException(stage, "per_second");
    }

    public void testEstimateMemoryOverhead() {
        PerSecondStage stage = new PerSecondStage();

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
