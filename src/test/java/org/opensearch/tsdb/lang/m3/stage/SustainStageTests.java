/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import static org.opensearch.tsdb.TestUtils.findSeriesByLabel;

/**
 * Unit tests for SustainStage.
 */
public class SustainStageTests extends AbstractWireSerializingTestCase<SustainStage> {

    /**
     * Comprehensive test with various time series patterns:
     * - Long sustained series: tests that ALL contiguous qualified samples are kept
     * - Dense series with insufficient samples
     * - Series with NaN breaking the sustained window
     * - Series with NaN after sustained window
     * - Series with multiple sustained windows
     * - Empty series
     * Duration: 3000ms, Step: 1000ms → required samples = 3000/1000 = 3
     */
    public void testSustainComprehensive() {
        SustainStage stage = new SustainStage(3000);

        // Long sustained series: [1, 2, 3, 4, 5, 6, 7] - tests multiple contiguous qualified samples
        // This validates that the stage keeps ALL samples once sustained threshold is met
        List<Sample> longSustainedSamples = List.of(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 2.0),
            new FloatSample(3000L, 3.0),
            new FloatSample(4000L, 4.0),
            new FloatSample(5000L, 5.0),
            new FloatSample(6000L, 6.0),
            new FloatSample(7000L, 7.0)
        );

        // Short series: [1, 2] - insufficient samples to meet duration
        List<Sample> shortSamples = List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0));

        // Series with NaN breaking window: [1, NaN, 3, 4]
        List<Sample> nanBreakSamples = List.of(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, Double.NaN),
            new FloatSample(3000L, 3.0),
            new FloatSample(4000L, 4.0)
        );

        // Series with NaN after sustained window: [1, 2, 3, NaN]
        List<Sample> nanAfterSamples = List.of(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 2.0),
            new FloatSample(3000L, 3.0),
            new FloatSample(4000L, Double.NaN)
        );

        // Series with multiple sustained windows: [1, 2, 3, NaN, 5, 6, 7, NaN, 9]
        List<Sample> multiWindowSamples = List.of(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 2.0),
            new FloatSample(3000L, 3.0),
            new FloatSample(4000L, Double.NaN),
            new FloatSample(5000L, 5.0),
            new FloatSample(6000L, 6.0),
            new FloatSample(7000L, 7.0),
            new FloatSample(8000L, Double.NaN),
            new FloatSample(9000L, 9.0)
        );

        // Empty series
        List<Sample> emptySamples = List.of();

        Labels longSustainedLabels = ByteLabels.fromStrings("type", "long_sustained");
        Labels shortLabels = ByteLabels.fromStrings("type", "short");
        Labels nanBreakLabels = ByteLabels.fromStrings("type", "nan_break");
        Labels nanAfterLabels = ByteLabels.fromStrings("type", "nan_after");
        Labels multiWindowLabels = ByteLabels.fromStrings("type", "multi_window");
        Labels emptyLabels = ByteLabels.fromStrings("type", "empty");

        TimeSeries longSustainedSeries = new TimeSeries(longSustainedSamples, longSustainedLabels, 1000L, 7000L, 1000L, "long_sustained");
        TimeSeries shortSeries = new TimeSeries(shortSamples, shortLabels, 1000L, 2000L, 1000L, "short");
        TimeSeries nanBreakSeries = new TimeSeries(nanBreakSamples, nanBreakLabels, 1000L, 4000L, 1000L, "nan_break");
        TimeSeries nanAfterSeries = new TimeSeries(nanAfterSamples, nanAfterLabels, 1000L, 4000L, 1000L, "nan_after");
        TimeSeries multiWindowSeries = new TimeSeries(multiWindowSamples, multiWindowLabels, 1000L, 9000L, 1000L, "multi_window");
        TimeSeries emptySeries = new TimeSeries(emptySamples, emptyLabels, 1000L, 1000L, 1000L, "empty");

        List<TimeSeries> result = stage.process(
            List.of(longSustainedSeries, shortSeries, nanBreakSeries, nanAfterSeries, multiWindowSeries, emptySeries)
        );

        assertEquals(6, result.size());

        // Long sustained result: [3, 4, 5, 6, 7] - all samples with 3+ sample prefix kept
        TimeSeries longSustainedResult = findSeriesByLabel(result, "type", "long_sustained");
        List<Sample> expectedLongSustained = List.of(
            new FloatSample(3000L, 3.0),
            new FloatSample(4000L, 4.0),
            new FloatSample(5000L, 5.0),
            new FloatSample(6000L, 6.0),
            new FloatSample(7000L, 7.0)
        );
        assertSamplesEqual("Long sustained series", expectedLongSustained, longSustainedResult.getSamples().toList());

        // Short result: [] - no samples meet requirement
        TimeSeries shortResult = findSeriesByLabel(result, "type", "short");
        assertTrue("Short series should be empty", shortResult.getSamples().isEmpty());

        // NaN break result: [] - NaN resets counter, so samples 3 and 4 only have 2-sample prefix
        TimeSeries nanBreakResult = findSeriesByLabel(result, "type", "nan_break");
        assertTrue("NaN break series should be empty", nanBreakResult.getSamples().isEmpty());

        // NaN after result: [3] - sample at 3000 has 3-sample prefix [1,2,3]
        TimeSeries nanAfterResult = findSeriesByLabel(result, "type", "nan_after");
        List<Sample> expectedNanAfter = List.of(new FloatSample(3000L, 3.0));
        assertSamplesEqual("NaN after series", expectedNanAfter, nanAfterResult.getSamples().toList());

        // Multi-window result: [3, 7] - two sustained windows meet requirement
        TimeSeries multiWindowResult = findSeriesByLabel(result, "type", "multi_window");
        List<Sample> expectedMultiWindow = List.of(new FloatSample(3000L, 3.0), new FloatSample(7000L, 7.0));
        assertSamplesEqual("Multi-window series", expectedMultiWindow, multiWindowResult.getSamples().toList());

        // Empty result: no samples
        TimeSeries emptyResult = findSeriesByLabel(result, "type", "empty");
        assertEquals(0, emptyResult.getSamples().size());
    }

    public void testConstructor() {
        SustainStage sustainStage = new SustainStage(5000);
        assertEquals(5000, sustainStage.getDuration());
        assertEquals("sustain", sustainStage.getName());
    }

    public void testConstructorWithNegativeDuration() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new SustainStage(-1));
        assertTrue(exception.getMessage().contains("non-negative"));
    }

    public void testProcessWithZeroDuration() {
        SustainStage sustainStage = new SustainStage(0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries timeSeries = new TimeSeries(Arrays.asList(new FloatSample(1000L, 1.0)), labels, 1000L, 1000L, 1000L, "test-series");

        List<TimeSeries> result = sustainStage.process(Arrays.asList(timeSeries));

        assertEquals(1, result.size());
    }

    /**
     * Test: Exception thrown when time series has invalid step (zero or negative)
     */
    public void testProcessWithInvalidStep() {
        SustainStage sustainStage = new SustainStage(3000);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));

        // Test with step = 0
        TimeSeries timeSeriesZeroStep = new TimeSeries(
            Arrays.asList(new FloatSample(1000L, 1.0)),
            labels,
            1000L,
            1000L,
            0L,  // Invalid: zero step
            "test-series"
        );

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> sustainStage.process(Arrays.asList(timeSeriesZeroStep))
        );
        assertTrue(exception.getMessage().contains("step must be positive"));

        // Test with step < 0
        TimeSeries timeSeriesNegativeStep = new TimeSeries(
            Arrays.asList(new FloatSample(1000L, 1.0)),
            labels,
            1000L,
            1000L,
            -1000L,  // Invalid: negative step
            "test-series"
        );

        exception = expectThrows(IllegalStateException.class, () -> sustainStage.process(Arrays.asList(timeSeriesNegativeStep)));
        assertTrue(exception.getMessage().contains("step must be positive"));
    }

    public void testFromArgs() {
        Map<String, Object> args = Map.of("duration", 5000);
        SustainStage sustainStage = SustainStage.fromArgs(args);
        assertEquals(5000, sustainStage.getDuration());
    }

    public void testFromArgsWithDurationString() {
        assertEquals(30000, SustainStage.fromArgs(Map.of("duration", "30s")).getDuration());
        assertEquals(300000, SustainStage.fromArgs(Map.of("duration", "5m")).getDuration());
        assertEquals(3600000, SustainStage.fromArgs(Map.of("duration", "1h")).getDuration());
        assertEquals(172800000, SustainStage.fromArgs(Map.of("duration", "2d")).getDuration());
    }

    public void testFromArgsWithMissingDuration() {
        Map<String, Object> emptyArgs = Map.of();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SustainStage.fromArgs(emptyArgs));
        assertTrue(exception.getMessage().contains("duration"));
    }

    public void testFromArgsWithNullDuration() {
        Map<String, Object> nullArgs = new HashMap<>();
        nullArgs.put("duration", null);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SustainStage.fromArgs(nullArgs));
        assertTrue(exception.getMessage().contains("cannot be null"));
    }

    public void testFromArgsWithInvalidDurationType() {
        Map<String, Object> invalidArgs = Map.of("duration", new Object());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SustainStage.fromArgs(invalidArgs));
        assertTrue(exception.getMessage().contains("string or number") || exception.getMessage().contains("Invalid"));
    }

    public void testFromArgsWithInvalidDurationString() {
        Map<String, Object> invalidArgs = Map.of("duration", "not_a_valid_duration");
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SustainStage.fromArgs(invalidArgs));
        assertTrue(
            exception.getMessage().contains("duration")
                || exception.getMessage().contains("format")
                || exception.getMessage().contains("Invalid")
        );
    }

    public void testWriteToAndReadFrom() throws IOException {
        SustainStage originalStage = new SustainStage(10000);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                SustainStage readStage = SustainStage.readFrom(in);
                assertEquals(10000, readStage.getDuration());
                assertEquals("sustain", readStage.getName());
            }
        }
    }

    public void testAnnotationPresent() {
        assertTrue(SustainStage.class.isAnnotationPresent(PipelineStageAnnotation.class));
    }

    public void testAnnotationValue() {
        PipelineStageAnnotation annotation = SustainStage.class.getAnnotation(PipelineStageAnnotation.class);
        assertNotNull(annotation);
        assertEquals("sustain", annotation.name());
    }

    public void testFactoryCreateWithArgs() {
        String stageName = "sustain";
        Map<String, Object> args = Map.of("duration", "3s");

        PipelineStage stage = PipelineStageFactory.createWithArgs(stageName, args);

        assertNotNull(stage);
        assertTrue(stage instanceof SustainStage);
        assertEquals("sustain", stage.getName());
        assertEquals(3000, ((SustainStage) stage).getDuration());
    }

    public void testFactoryRegistration() {
        Set<String> supportedStages = PipelineStageFactory.getSupportedStageTypes();
        assertTrue(supportedStages.contains("sustain"));
    }

    public void testUnaryPipelineStageNullInput() {
        SustainStage stage = new SustainStage(1);
        TestUtils.assertNullInputThrowsException(stage, "sustain");
    }

    public void testEquals() {
        SustainStage stage1 = new SustainStage(5000);
        SustainStage stage2 = new SustainStage(5000);

        assertEquals(stage1, stage2);

        SustainStage stageDifferent = new SustainStage(10000);
        assertNotEquals(stage1, stageDifferent);

        assertEquals(stage1, stage1);
        assertNotEquals(null, stage1);
        assertNotEquals("string", stage1);
    }

    public void testHashCode() {
        SustainStage stage1 = new SustainStage(5000);
        SustainStage stage2 = new SustainStage(5000);
        assertEquals(stage1.hashCode(), stage2.hashCode());
    }

    @Override
    protected Writeable.Reader<SustainStage> instanceReader() {
        return SustainStage::readFrom;
    }

    @Override
    protected SustainStage createTestInstance() {
        return new SustainStage(randomIntBetween(0, 1000) * 1000L);
    }
}
