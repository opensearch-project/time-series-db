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
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;
import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;

/**
 * Unit tests for PerSecondRateStage.
 */
public class PerSecondRateStageTests extends AbstractWireSerializingTestCase<PerSecondRateStage> {

    @Override
    protected Writeable.Reader<PerSecondRateStage> instanceReader() {
        return PerSecondRateStage::new;
    }

    @Override
    protected PerSecondRateStage createTestInstance() {
        return new PerSecondRateStage(randomLongBetween(1000, 60000), randomLongBetween(1, 1000));
    }

    /**
     * Test: Regular increasing series
     * Input: 0, 10, 20, 30, 40, 50, 60, 70, 80 at timestamps 0, 10000, 20000, ..., 80000
     * Output: rate=1/s starting from timestamp 10000 (first value has no previous, so skipped)
     */
    public void testRegularIncreasingSeries() {
        List<Sample> input = List.of(
            new FloatSample(0, 0f),
            new FloatSample(10000, 10f),
            new FloatSample(20000, 20f),
            new FloatSample(30000, 30f),
            new FloatSample(40000, 40f),
            new FloatSample(50000, 50f),
            new FloatSample(60000, 60f),
            new FloatSample(70000, 70f),
            new FloatSample(80000, 80f)
        );
        Map<String, String> labelsMap = Map.of("__name__", "actions", "city", "atlanta");
        Labels labels = ByteLabels.fromMap(labelsMap);
        TimeSeries series = new TimeSeries(input, labels, 0, 80000, 10000, null);

        // Expected: NaN NaN 1 1 1 1 1 1 (first 2 NaNs skipped, 6 valid values)
        List<Sample> expected = List.of(
            new FloatSample(20000, 1f),  // First output at index 2
            new FloatSample(30000, 1f),
            new FloatSample(40000, 1f),
            new FloatSample(50000, 1f),
            new FloatSample(60000, 1f),
            new FloatSample(70000, 1f),
            new FloatSample(80000, 1f)
        );

        PerSecondRateStage stage = new PerSecondRateStage(10000, 1000); // 10s interval, milliseconds
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        assertSamplesEqual("Regular increasing series", expected, result.get(0).getSamples().toList());
    }

    /**
     * Test: Exponentially increasing series
     * Input: 0, 10, 20, 40, 80, 160 at timestamps 0, 10000, 20000, 30000, 40000, 50000
     * Expected: NaN NaN 1 1 2 4 (first 2 NaNs skipped)
     */
    public void testExponentiallyIncreasingSeries() {
        List<Sample> input = List.of(
            new FloatSample(0, 0f),
            new FloatSample(10000, 10f),
            new FloatSample(20000, 20f),
            new FloatSample(30000, 40f),
            new FloatSample(40000, 80f),
            new FloatSample(50000, 160f)
        );
        Map<String, String> labelsMap = Map.of("__name__", "actions", "city", "atlanta");
        Labels labels = ByteLabels.fromMap(labelsMap);
        TimeSeries series = new TimeSeries(input, labels, 0, 50000, 10000, null);

        List<Sample> expected = List.of(
            new FloatSample(20000, 1f),
            new FloatSample(30000, 1f),
            new FloatSample(40000, 2f),
            new FloatSample(50000, 4f)
        );

        PerSecondRateStage stage = new PerSecondRateStage(10000, 1000);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        assertSamplesEqual("Exponentially increasing series", expected, result.get(0).getSamples().toList());
    }

    /**
     * Test: Regular increasing series with reset
     * Input: 0, 10, 20, 30, 0, 10, 20, 30 at timestamps 0, 10000, ..., 70000
     * Expected: NaN NaN 1 1 1 0 1 1 (moving average shifts the reset by 1 position)
     */
    public void testRegularIncreasingSeriesWithReset() {
        List<Sample> input = List.of(
            new FloatSample(0, 0f),
            new FloatSample(10000, 10f),
            new FloatSample(20000, 20f),
            new FloatSample(30000, 30f),
            new FloatSample(40000, 0f),   // Reset
            new FloatSample(50000, 10f),
            new FloatSample(60000, 20f),
            new FloatSample(70000, 30f)
        );
        Map<String, String> labelsMap = Map.of("__name__", "actions", "city", "atlanta");
        Labels labels = ByteLabels.fromMap(labelsMap);
        TimeSeries series = new TimeSeries(input, labels, 0, 70000, 10000, null);

        List<Sample> expected = List.of(
            new FloatSample(20000, 1f),
            new FloatSample(30000, 1f),
            new FloatSample(40000, 1f),  // Moving avg of scratchBuf[3]=1
            new FloatSample(50000, 0f),  // Moving avg of scratchBuf[4]=0 (reset)
            new FloatSample(60000, 1f),
            new FloatSample(70000, 1f)
        );

        PerSecondRateStage stage = new PerSecondRateStage(10000, 1000);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        assertSamplesEqual("Regular increasing series with reset", expected, result.get(0).getSamples().toList());
    }

    /**
     * Test: Regular increasing series with 20s interval
     * Input: 0, 10, 20, 30, 40, 50, 60, 70, 80 at timestamps 0, 10000, ..., 80000
     * Expected: NaN NaN NaN 1 1 1 1 1 1 (windowSizeInSteps=2, first 3 NaNs skipped)
     */
    public void testRegularIncreasingSeriesWith20sInterval() {
        List<Sample> input = List.of(
            new FloatSample(0, 0f),
            new FloatSample(10000, 10f),
            new FloatSample(20000, 20f),
            new FloatSample(30000, 30f),
            new FloatSample(40000, 40f),
            new FloatSample(50000, 50f),
            new FloatSample(60000, 60f),
            new FloatSample(70000, 70f),
            new FloatSample(80000, 80f)
        );
        Map<String, String> labelsMap = Map.of("__name__", "actions", "city", "atlanta");
        Labels labels = ByteLabels.fromMap(labelsMap);
        TimeSeries series = new TimeSeries(input, labels, 0, 80000, 10000, null);

        List<Sample> expected = List.of(
            new FloatSample(30000, 1f),  // First output at index 3 (windowSizeInSteps=2)
            new FloatSample(40000, 1f),
            new FloatSample(50000, 1f),
            new FloatSample(60000, 1f),
            new FloatSample(70000, 1f),
            new FloatSample(80000, 1f)
        );

        PerSecondRateStage stage = new PerSecondRateStage(20000, 1000); // 20s interval, milliseconds
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        assertSamplesEqual("Regular increasing series with 20s interval", expected, result.get(0).getSamples().toList());
    }

    /**
     * Test: Mixed series with missing values
     * Input: 0, 10, null, 20, 30 at timestamps 0, 10000, 20000, 30000, 40000
     * Expected output: NaN NaN 1 NaN NaN (only position 2 at timestamp 20000 has value 1)
     */
    public void testMixedSeriesWithMissingValues() {
        List<Sample> input = new ArrayList<>();
        input.add(new FloatSample(0, 0f));
        input.add(new FloatSample(10000, 10f));
        input.add(new FloatSample(20000, Double.NaN));  // Missing at timestamp 20000
        input.add(new FloatSample(30000, 20f));
        input.add(new FloatSample(40000, 30f));

        Map<String, String> labelsMap = Map.of("__name__", "actions", "city", "atlanta");
        Labels labels = ByteLabels.fromMap(labelsMap);
        TimeSeries series = new TimeSeries(input, labels, 0, 40000, 10000, null);

        // Only timestamp 20000 (position 2) should have a value of 1
        List<Sample> expected = List.of(new FloatSample(20000, 1f));

        PerSecondRateStage stage = new PerSecondRateStage(10000, 1000);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        assertSamplesEqual("Mixed series with missing values", expected, result.get(0).getSamples().toList());
    }

    public void testConstructorInvalidInterval() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new PerSecondRateStage(0, 1000));
        assertEquals("Interval must be positive, got: 0", exception.getMessage());

        assertThrows(IllegalArgumentException.class, () -> new PerSecondRateStage(-1000, 1000));
    }

    public void testConstructorInvalidUnitsPerSecond() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> new PerSecondRateStage(10000, 0));
        assertEquals("Units per second must be positive, got: 0", exception.getMessage());

        assertThrows(IllegalArgumentException.class, () -> new PerSecondRateStage(10000, -1000));
    }

    public void testInvalidIntervalNotDivisible() {
        List<Sample> input = List.of(new FloatSample(0, 0f), new FloatSample(10000, 10f), new FloatSample(20000, 20f));
        Map<String, String> labelsMap = Map.of("__name__", "actions", "city", "atlanta");
        Labels labels = ByteLabels.fromMap(labelsMap);
        TimeSeries series = new TimeSeries(input, labels, 0, 20000, 10000, null);

        PerSecondRateStage stage = new PerSecondRateStage(15000, 1000); // 15s interval (not divisible), milliseconds

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> stage.process(List.of(series)));
        assertTrue(exception.getMessage().contains("must be >= series resolution"));
        assertTrue(exception.getMessage().contains("evenly divisible"));
    }

    public void testInvalidIntervalLessThanResolution() {
        List<Sample> input = List.of(new FloatSample(0, 0f), new FloatSample(10000, 10f), new FloatSample(20000, 20f));
        Map<String, String> labelsMap = Map.of("__name__", "actions", "city", "atlanta");
        Labels labels = ByteLabels.fromMap(labelsMap);
        TimeSeries series = new TimeSeries(input, labels, 0, 20000, 10000, null);

        PerSecondRateStage stage = new PerSecondRateStage(5000, 1000); // 5s interval (< resolution), milliseconds

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> stage.process(List.of(series)));
        assertTrue(exception.getMessage().contains("must be >= series resolution"));
    }

    public void testNullInputThrowsException() {
        PerSecondRateStage stage = new PerSecondRateStage(10000, 1000);
        assertNullInputThrowsException(stage, "per_second_rate");
    }

    public void testEmptyInput() {
        PerSecondRateStage stage = new PerSecondRateStage(10000, 1000);
        List<TimeSeries> result = stage.process(List.of());
        assertTrue(result.isEmpty());
    }

    public void testFromArgsValid() {
        Map<String, Object> args = Map.of("interval", 10000L, "unitsPerSecond", 1000L);
        PerSecondRateStage stage = PerSecondRateStage.fromArgs(args);
        assertNotNull(stage);
    }

    public void testFromArgsMissingInterval() {
        Map<String, Object> args = Map.of("unitsPerSecond", 1000L);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> PerSecondRateStage.fromArgs(args));
        assertEquals("interval argument is required", exception.getMessage());
    }

    public void testFromArgsMissingUnitsPerSecond() {
        Map<String, Object> args = Map.of("interval", 10000L);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> PerSecondRateStage.fromArgs(args));
        assertEquals("unitsPerSecond argument is required", exception.getMessage());
    }

    public void testFromArgsInvalidIntervalType() {
        Map<String, Object> args = Map.of("interval", "10s", "unitsPerSecond", 1000L);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> PerSecondRateStage.fromArgs(args));
        assertEquals("interval must be a number", exception.getMessage());
    }

    public void testFromArgsInvalidUnitsPerSecondType() {
        Map<String, Object> args = Map.of("interval", 10000L, "unitsPerSecond", "1000");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> PerSecondRateStage.fromArgs(args));
        assertEquals("unitsPerSecond must be a number", exception.getMessage());
    }

    public void testFromArgsNullArgs() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> PerSecondRateStage.fromArgs(null));
        assertEquals("Args cannot be null", exception.getMessage());
    }

    public void testEquals() {
        PerSecondRateStage stage1 = new PerSecondRateStage(10000, 1000);
        PerSecondRateStage stage2 = new PerSecondRateStage(10000, 1000);

        assertEquals("Equal PerSecondRateStages should be equal", stage1, stage2);

        PerSecondRateStage stageDiffInterval = new PerSecondRateStage(20000, 1000);
        assertNotEquals("Different intervals should not be equal", stage1, stageDiffInterval);

        PerSecondRateStage stageDiffUnits = new PerSecondRateStage(10000, 2000);
        assertNotEquals("Different unitsPerSecond should not be equal", stage1, stageDiffUnits);

        assertEquals("Stage should equal itself", stage1, stage1);

        assertNotEquals("Stage should not equal null", null, stage1);

        assertNotEquals("Stage should not equal different class", "string", stage1);
    }

    public void testHashCode() {
        PerSecondRateStage stage1 = new PerSecondRateStage(10000, 1000);
        PerSecondRateStage stage2 = new PerSecondRateStage(10000, 1000);
        PerSecondRateStage stage3 = new PerSecondRateStage(20000, 1000);
        PerSecondRateStage stage4 = new PerSecondRateStage(10000, 2000);

        assertEquals("Equal stages should have equal hashCodes", stage1.hashCode(), stage2.hashCode());
        assertNotEquals("Different intervals should have different hashCodes", stage1.hashCode(), stage3.hashCode());
        assertNotEquals("Different unitsPerSecond should have different hashCodes", stage1.hashCode(), stage4.hashCode());
    }

    public void testToXContent_defaultParameters() throws Exception {
        PerSecondRateStage stage = new PerSecondRateStage(10000, 1000);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"interval\":10000,\"unitsPerSecond\":1000}", json);
    }

    public void testToXContent_differentInterval() throws Exception {
        PerSecondRateStage stage = new PerSecondRateStage(60000, 1000);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"interval\":60000,\"unitsPerSecond\":1000}", json);
    }

    public void testToXContent_differentUnitsPerSecond() throws Exception {
        PerSecondRateStage stage = new PerSecondRateStage(10000, 1000000);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"interval\":10000,\"unitsPerSecond\":1000000}", json);
    }
}
