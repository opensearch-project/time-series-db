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
import org.opensearch.tsdb.TestUtils;
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

public class UnaryFallbackSeriesStageTests extends AbstractWireSerializingTestCase<FallbackSeriesUnaryStage> {

    /**
     * Test that when input is non-empty, it is returned unchanged.
     */
    public void testNonEmptyInputReturnsInput() {
        FallbackSeriesUnaryStage stage = new FallbackSeriesUnaryStage(1.0, 0L, 20L, 10L);

        List<Sample> inputSamples = List.of(new FloatSample(0L, 5.0), new FloatSample(10L, 6.0), new FloatSample(20L, 7.0));

        ByteLabels inputLabels = ByteLabels.fromStrings("name", "input");
        TimeSeries inputSeries = new TimeSeries(inputSamples, inputLabels, 0L, 20L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(inputSeries));

        assertEquals(1, result.size());
        assertEquals("input", result.get(0).getLabels().get("name"));
        assertSamplesEqual("Input samples should match", inputSamples, result.get(0).getSamples().toList());
    }

    /**
     * Test that when input is empty, a constant series is returned.
     * Note: maxTimestamp is exclusive, so samples are generated in [minTimestamp, maxTimestamp).
     */
    public void testEmptyInputReturnsConstantSeries() {
        double fallbackValue = 2.5;
        long minTimestamp = 0L;
        long maxTimestamp = 30L;
        long step = 10L;

        FallbackSeriesUnaryStage stage = new FallbackSeriesUnaryStage(fallbackValue, minTimestamp, maxTimestamp, step);

        List<TimeSeries> result = stage.process(List.of());

        assertEquals(1, result.size());
        TimeSeries constantSeries = result.get(0);
        // Verify labels are empty (no "fallback" label)
        assertEquals("", constantSeries.getLabels().get("fallback"));
        assertEquals(minTimestamp, constantSeries.getMinTimestamp());
        // maxTimestamp in TimeSeries is the last sample timestamp (20), not the exclusive upper bound (30)
        assertEquals(20L, constantSeries.getMaxTimestamp());
        assertEquals(step, constantSeries.getStep());
        // Verify alias is formatted with 3 decimal places
        assertEquals("2.500", constantSeries.getAlias());

        // Verify samples are generated correctly (maxTimestamp is exclusive, so no sample at 30)
        List<Sample> expectedSamples = List.of(
            new FloatSample(0L, fallbackValue),
            new FloatSample(10L, fallbackValue),
            new FloatSample(20L, fallbackValue)
        );
        assertSamplesEqual("Constant series samples should match", expectedSamples, constantSeries.getSamples().toList());
    }

    /**
     * Test that when input is null, an exception is thrown.
     */
    public void testNullInputThrowsException() {
        FallbackSeriesUnaryStage stage = new FallbackSeriesUnaryStage(3.0, 0L, 10L, 5L);
        TestUtils.assertNullInputThrowsException(stage, "fallback_series_unary");
    }

    /**
     * Test constant series generation with different step sizes.
     * Note: maxTimestamp is exclusive, so samples are generated in [minTimestamp, maxTimestamp).
     */
    public void testConstantSeriesWithDifferentSteps() {
        // Test with step = 5, max=10 (exclusive) -> samples at 0, 5
        FallbackSeriesUnaryStage stage1 = new FallbackSeriesUnaryStage(1.0, 0L, 10L, 5L);
        List<TimeSeries> result1 = stage1.process(List.of());
        assertEquals(2, result1.get(0).getSamples().size()); // 0, 5 (10 is excluded)

        // Test with step = 3, max=9 (exclusive) -> samples at 0, 3, 6
        FallbackSeriesUnaryStage stage2 = new FallbackSeriesUnaryStage(1.0, 0L, 9L, 3L);
        List<TimeSeries> result2 = stage2.process(List.of());
        assertEquals(3, result2.get(0).getSamples().size()); // 0, 3, 6 (9 is excluded)
    }

    /**
     * Test constant series generation with negative values.
     */
    public void testConstantSeriesWithNegativeValue() {
        FallbackSeriesUnaryStage stage = new FallbackSeriesUnaryStage(-5.0, 0L, 20L, 10L);

        List<TimeSeries> result = stage.process(List.of());

        assertEquals(1, result.size());
        TimeSeries constantSeries = result.get(0);
        assertEquals(-5.0, constantSeries.getSamples().getValue(0), 0.001);
    }

    /**
     * Test constant series generation with zero value.
     */
    public void testConstantSeriesWithZeroValue() {
        FallbackSeriesUnaryStage stage = new FallbackSeriesUnaryStage(0.0, 0L, 10L, 5L);

        List<TimeSeries> result = stage.process(List.of());

        assertEquals(1, result.size());
        TimeSeries constantSeries = result.get(0);
        assertEquals(0.0, constantSeries.getSamples().getValue(0), 0.001);
        // Verify alias formatting: 0 -> "0.000"
        assertEquals("0.000", constantSeries.getAlias());
    }

    /**
     * Test getName returns the correct name.
     */
    public void testGetName() {
        FallbackSeriesUnaryStage stage = new FallbackSeriesUnaryStage(1.0, 0L, 10L, 5L);
        assertEquals(FallbackSeriesUnaryStage.NAME, stage.getName());
    }

    /**
     * Test XContent serialization.
     */
    public void testToXContent() throws IOException {
        FallbackSeriesUnaryStage stage = new FallbackSeriesUnaryStage(2.5, 0L, 20L, 10L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stage.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();

        assertTrue(json.contains("\"fallbackValue\":2.5"));
        assertTrue(json.contains("\"minTimestamp\":0"));
        assertTrue(json.contains("\"maxTimestamp\":20"));
        assertTrue(json.contains("\"step\":10"));
    }

    /**
     * Test fromArgs with valid arguments.
     */
    public void testFromArgs() {
        Map<String, Object> args = Map.of("fallbackValue", 3.0, "minTimestamp", 0, "maxTimestamp", 30, "step", 15);

        FallbackSeriesUnaryStage stage = FallbackSeriesUnaryStage.fromArgs(args);

        assertNotNull(stage);
        // Verify by checking the stage processes correctly with the expected values
        List<TimeSeries> result = stage.process(List.of());
        assertEquals(1, result.size());
        TimeSeries constantSeries = result.get(0);
        assertEquals(0L, constantSeries.getMinTimestamp());
        // maxTimestamp in TimeSeries is the last sample timestamp (15), not the exclusive upper bound (30)
        assertEquals(15L, constantSeries.getMaxTimestamp());
        assertEquals(15L, constantSeries.getStep());
        assertEquals(3.0, constantSeries.getSamples().getValue(0), 0.001);
    }

    /**
     * Test that FallbackSeriesUnaryStage can be serialized to JSON (toXContent) and
     * deserialized back (fromArgs) without losing information.
     *
     * <p>This test is critical because FallbackSeriesUnaryStage had a bug where it used
     * direct (Long) casts which failed when JSON parsing returned Integer values.</p>
     */
    public void testXContentDeserialization() throws IOException {
        // Create original stage with various numeric types
        FallbackSeriesUnaryStage original = new FallbackSeriesUnaryStage(1.5, 1000000000L, 2000000000L, 60000L);

        // Serialize to JSON and parse back to args Map
        Map<String, Object> args = PipelineStageTestUtils.serializeToArgs(original);

        // Deserialize from args
        FallbackSeriesUnaryStage deserialized = FallbackSeriesUnaryStage.fromArgs(args);

        // Verify round-trip preserves all fields
        assertEquals("Stage names should match", original.getName(), deserialized.getName());
        assertEquals("Stages should be equal after round-trip", original, deserialized);
    }

    /**
     * Test that fromArgs correctly handles both Integer and Long types from JSON parsing.
     * This verifies the fix for the Number type casting bug.
     */
    public void testFromArgsHandlesIntegerAndLong() {
        // Simulate JSON parser returning Integer for values that fit in int range
        Map<String, Object> argsWithInteger = Map.of(
            "fallbackValue",
            1.5,
            "minTimestamp",
            1000, // Integer (fits in int)
            "maxTimestamp",
            2000, // Integer (fits in int)
            "step",
            60 // Integer (fits in int)
        );

        FallbackSeriesUnaryStage stageFromInteger = FallbackSeriesUnaryStage.fromArgs(argsWithInteger);
        assertNotNull("Should handle Integer types", stageFromInteger);

        // Simulate JSON parser returning Long for large values
        Map<String, Object> argsWithLong = Map.of(
            "fallbackValue",
            1.5,
            "minTimestamp",
            1000000000L, // Long
            "maxTimestamp",
            2000000000L, // Long
            "step",
            60000L // Long
        );

        FallbackSeriesUnaryStage stageFromLong = FallbackSeriesUnaryStage.fromArgs(argsWithLong);
        assertNotNull("Should handle Long types", stageFromLong);
    }

    /**
     * Test fromArgs rejects missing arguments.
     * Tests each missing field individually to cover all null check branches.
     */
    public void testFromArgsMissingArguments() {
        // Missing fallbackValue
        Map<String, Object> missingFallbackValue = Map.of("minTimestamp", 1000L, "maxTimestamp", 2000L, "step", 60L);
        IllegalArgumentException exception1 = expectThrows(
            IllegalArgumentException.class,
            () -> FallbackSeriesUnaryStage.fromArgs(missingFallbackValue)
        );
        assertTrue(exception1.getMessage().contains("requires fallbackValue, minTimestamp, maxTimestamp, and step"));

        // Missing minTimestamp
        Map<String, Object> missingMinTimestamp = Map.of("fallbackValue", 1.0, "maxTimestamp", 2000L, "step", 60L);
        IllegalArgumentException exception2 = expectThrows(
            IllegalArgumentException.class,
            () -> FallbackSeriesUnaryStage.fromArgs(missingMinTimestamp)
        );
        assertTrue(exception2.getMessage().contains("requires fallbackValue, minTimestamp, maxTimestamp, and step"));

        // Missing maxTimestamp
        Map<String, Object> missingMaxTimestamp = Map.of("fallbackValue", 1.0, "minTimestamp", 1000L, "step", 60L);
        IllegalArgumentException exception3 = expectThrows(
            IllegalArgumentException.class,
            () -> FallbackSeriesUnaryStage.fromArgs(missingMaxTimestamp)
        );
        assertTrue(exception3.getMessage().contains("requires fallbackValue, minTimestamp, maxTimestamp, and step"));

        // Missing step
        Map<String, Object> missingStep = Map.of("fallbackValue", 1.0, "minTimestamp", 1000L, "maxTimestamp", 2000L);
        IllegalArgumentException exception4 = expectThrows(
            IllegalArgumentException.class,
            () -> FallbackSeriesUnaryStage.fromArgs(missingStep)
        );
        assertTrue(exception4.getMessage().contains("requires fallbackValue, minTimestamp, maxTimestamp, and step"));
    }

    /**
     * Test fromArgs rejects null arguments map.
     * This covers the null check branch (line 174).
     */
    public void testFromArgsNullArguments() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> FallbackSeriesUnaryStage.fromArgs(null));
        assertEquals("FallbackSeriesUnaryStage requires arguments", exception.getMessage());
    }

    /**
     * Test fromArgs handles invalid field types gracefully.
     * This covers the instanceof Number false branch (line 184).
     */
    public void testFromArgsInvalidFieldTypes() {
        // Test with non-Number fallbackValue
        Map<String, Object> invalidFallbackValue = Map.of(
            "fallbackValue",
            "not a number",
            "minTimestamp",
            1000L,
            "maxTimestamp",
            2000L,
            "step",
            60L
        );
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> FallbackSeriesUnaryStage.fromArgs(invalidFallbackValue)
        );
        assertTrue(exception.getMessage().contains("requires"));
    }

    /**
     * Test PipelineStageFactory can create FallbackSeriesUnaryStage.
     */
    public void testPipelineStageFactory() {
        Map<String, Object> args = Map.of("fallbackValue", 1.0, "minTimestamp", 0L, "maxTimestamp", 10L, "step", 5L);

        PipelineStage stage = PipelineStageFactory.createWithArgs(FallbackSeriesUnaryStage.NAME, args);

        assertNotNull(stage);
        assertTrue(stage instanceof FallbackSeriesUnaryStage);
        // Verify by checking the stage processes correctly
        List<TimeSeries> result = ((FallbackSeriesUnaryStage) stage).process(List.of());
        assertEquals(1, result.size());
        assertEquals(1.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    @Override
    protected Writeable.Reader<FallbackSeriesUnaryStage> instanceReader() {
        return FallbackSeriesUnaryStage::readFrom;
    }

    @Override
    protected FallbackSeriesUnaryStage createTestInstance() {
        return new FallbackSeriesUnaryStage(
            randomDouble(),
            randomLongBetween(0L, 1000L),
            randomLongBetween(1000L, 2000L),
            randomLongBetween(1L, 100L)
        );
    }

    @Override
    protected FallbackSeriesUnaryStage mutateInstance(FallbackSeriesUnaryStage instance) {
        // Extract values by processing an empty input to create a constant series
        List<TimeSeries> result = instance.process(List.of());
        TimeSeries constantSeries = result.get(0);
        double fallbackValue = constantSeries.getSamples().getValue(0);

        return new FallbackSeriesUnaryStage(
            fallbackValue + 1.0,
            constantSeries.getMinTimestamp() + 1L,
            constantSeries.getMaxTimestamp() + 1L,
            constantSeries.getStep() + 1L
        );
    }
}
