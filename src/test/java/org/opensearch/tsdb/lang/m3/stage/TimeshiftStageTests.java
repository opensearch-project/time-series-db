/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TimeshiftStageTests extends AbstractWireSerializingTestCase<TimeshiftStage> {

    // ========== Constructor Tests ==========

    public void testConstructor() {
        // test case: shift millis, expected shift millis, description
        Object[][] testCases = {
            { 0L, 0L, "Zero shift" },
            { 1000L, 1000L, "1 second shift" },
            { 60000L, 60000L, "1 minute shift" },
            { 3600000L, 3600000L, "1 hour shift" },
            { 86400000L, 86400000L, "1 day shift" },
            { -1000L, 1000L, "Negative 1 second (should become positive)" },
            { -60000L, 60000L, "Negative 1 minute (should become positive)" },
            { Long.MAX_VALUE / 2, Long.MAX_VALUE / 2, "Large positive value" }, };

        for (Object[] testCase : testCases) {
            long inputShiftMillis = (Long) testCase[0];
            long expectedShiftMillis = (Long) testCase[1];
            String description = (String) testCase[2];

            // Act
            TimeshiftStage timeshiftStage = new TimeshiftStage(inputShiftMillis);

            // Assert
            assertEquals("Failed for " + description, expectedShiftMillis, timeshiftStage.getShiftMillis());
            assertEquals("Failed for " + description, "timeshift", timeshiftStage.getName());
        }
    }

    // ========== Process Method Tests ==========

    public void testProcessWithSingleTimeSeries() {
        // Arrange
        TimeshiftStage timeshiftStage = new TimeshiftStage(TimeValue.timeValueHours(1).getMillis());
        Labels labels = ByteLabels.fromStrings("service", "api");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0), new FloatSample(3000L, 30.0));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = timeshiftStage.process(List.of(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        TimeSeries shiftedTimeSeries = result.get(0);
        assertEquals(3, shiftedTimeSeries.getSamples().size());
        assertEquals(3601000L, shiftedTimeSeries.getSamples().getTimestamp(0));
        assertEquals(3602000L, shiftedTimeSeries.getSamples().getTimestamp(1));
        assertEquals(3603000L, shiftedTimeSeries.getSamples().getTimestamp(2));
        assertEquals(10.0, shiftedTimeSeries.getSamples().getValue(0), 0.001);
        assertEquals(20.0, shiftedTimeSeries.getSamples().getValue(1), 0.001);
        assertEquals(30.0, shiftedTimeSeries.getSamples().getValue(2), 0.001);
        assertEquals(labels, shiftedTimeSeries.getLabels());
        assertEquals("test-series", shiftedTimeSeries.getAlias());
    }

    public void testProcessWithMultipleTimeSeries() {
        // Arrange
        TimeshiftStage timeshiftStage = new TimeshiftStage(TimeValue.timeValueMinutes(30).getMillis());
        Labels labels1 = ByteLabels.fromStrings("service", "api");
        Labels labels2 = ByteLabels.fromStrings("service", "db");

        TimeSeries timeSeries1 = new TimeSeries(List.of(new FloatSample(1000L, 5.0)), labels1, 1000L, 1000L, 1000L, "api-series");
        TimeSeries timeSeries2 = new TimeSeries(List.of(new FloatSample(2000L, 10.0)), labels2, 2000L, 2000L, 1000L, "db-series");

        // Act
        List<TimeSeries> result = timeshiftStage.process(List.of(timeSeries1, timeSeries2));

        // Assert
        assertEquals(2, result.size());
        assertEquals(1801000L, result.get(0).getSamples().getTimestamp(0)); // 1000 + 30*60*1000
        assertEquals(1802000L, result.get(1).getSamples().getTimestamp(0)); // 2000 + 30*60*1000
        assertEquals(5.0, result.get(0).getSamples().getValue(0), 0.001);
        assertEquals(10.0, result.get(1).getSamples().getValue(0), 0.001);
    }

    public void testProcessWithEmptyInput() {
        // Arrange
        TimeshiftStage timeshiftStage = new TimeshiftStage(TimeValue.timeValueHours(1).getMillis());

        // Act
        List<TimeSeries> result = timeshiftStage.process(List.of());

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testProcessWithNullInput() {
        TimeshiftStage timeshiftStage = new TimeshiftStage(TimeValue.timeValueHours(1).getMillis());
        TestUtils.assertNullInputThrowsException(timeshiftStage, "timeshift");
    }

    public void testProcessWithEmptyTimeSeries() {
        // Arrange
        TimeshiftStage timeshiftStage = new TimeshiftStage(TimeValue.timeValueHours(1).getMillis());
        Labels labels = ByteLabels.fromStrings("service", "api");
        TimeSeries emptyTimeSeries = new TimeSeries(Collections.emptyList(), labels, 1000L, 2000L, 1000L, "empty");

        // Act
        List<TimeSeries> result = timeshiftStage.process(List.of(emptyTimeSeries));

        // Assert
        assertEquals(1, result.size());
        assertTrue(result.get(0).getSamples().isEmpty());
        assertEquals(labels, result.get(0).getLabels());
        assertEquals("empty", result.get(0).getAlias());
    }

    public void testProcessWithSmallShift() {
        // Arrange
        TimeshiftStage timeshiftStage = new TimeshiftStage(100);
        Labels labels = ByteLabels.fromStrings("service", "api");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = timeshiftStage.process(List.of(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        assertEquals(1100L, result.get(0).getSamples().getTimestamp(0)); // 1000 + 100
        assertEquals(10.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testProcessWithZeroShift() {
        // Arrange
        TimeshiftStage timeshiftStage = new TimeshiftStage(0);
        Labels labels = ByteLabels.fromStrings("service", "api");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = timeshiftStage.process(List.of(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getSamples().size());
        assertEquals(1000L, result.get(0).getSamples().getTimestamp(0));
        assertEquals(2000L, result.get(0).getSamples().getTimestamp(1));
        assertEquals(10.0, result.get(0).getSamples().getValue(0), 0.001);
        assertEquals(20.0, result.get(0).getSamples().getValue(1), 0.001);
    }

    // ========== FromArgs Method Tests ==========

    public void testFromArgsValid() {
        // Arrange
        Map<String, Object> args = Map.of(TimeshiftStage.SHIFT_AMOUNT_ARG, "2h");

        // Act
        TimeshiftStage timeshiftStage = TimeshiftStage.fromArgs(args);

        // Assert
        assertEquals(TimeValue.timeValueHours(2).getMillis(), timeshiftStage.getShiftMillis());
        assertEquals("timeshift", timeshiftStage.getName());
    }

    public void testFromArgsWithInteger() {
        // Arrange
        Map<String, Object> args = Map.of(TimeshiftStage.SHIFT_AMOUNT_ARG, 3600000L); // 1 hour in milliseconds

        // Act
        TimeshiftStage timeshiftStage = TimeshiftStage.fromArgs(args);

        // Assert
        assertEquals(TimeValue.timeValueHours(1).getMillis(), timeshiftStage.getShiftMillis());
        assertEquals("timeshift", timeshiftStage.getName());
    }

    public void testFromArgsWithFloat() {
        // Arrange
        Map<String, Object> args = Map.of(TimeshiftStage.SHIFT_AMOUNT_ARG, 1800000.0); // 30 minutes in milliseconds

        // Act
        TimeshiftStage timeshiftStage = TimeshiftStage.fromArgs(args);

        // Assert
        assertEquals(TimeValue.timeValueMinutes(30).getMillis(), timeshiftStage.getShiftMillis());
        assertEquals("timeshift", timeshiftStage.getName());
    }

    public void testFromArgsWithNullArgs() {
        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> TimeshiftStage.fromArgs(null));
        assertTrue(exception.getMessage().contains("Timeshift stage requires 'shift_amount' argument"));
    }

    public void testFromArgsWithMissingShiftAmount() {
        // Arrange
        Map<String, Object> args = Map.of("other_key", "value");

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> TimeshiftStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Timeshift stage requires 'shift_amount' argument"));
    }

    public void testFromArgsWithNullShiftAmount() {
        // Arrange
        Map<String, Object> args = new HashMap<>();
        args.put(TimeshiftStage.SHIFT_AMOUNT_ARG, null);

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> TimeshiftStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Shift amount cannot be null"));
    }

    public void testFromArgsWithInvalidType() {
        // Arrange
        Map<String, Object> args = Map.of(TimeshiftStage.SHIFT_AMOUNT_ARG, new Object());

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> TimeshiftStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Shift amount must be a string or number"));
        assertTrue(exception.getMessage().contains("Object"));
    }

    public void testFromArgsWithInvalidStringFormat() {
        // Arrange
        Map<String, Object> args = Map.of(TimeshiftStage.SHIFT_AMOUNT_ARG, "invalid_format");

        // Act & Assert
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> TimeshiftStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid shift amount format"));
        assertTrue(exception.getMessage().contains("invalid_format"));
    }

    // ========== Serialization Tests ==========

    public void testSerializationRoundtripComprehensive() throws IOException {
        TimeValue[] testShifts = {
            TimeValue.timeValueMillis(0),           // Zero
            TimeValue.timeValueSeconds(1),          // 1 second
            TimeValue.timeValueMinutes(1),          // 1 minute
            TimeValue.timeValueHours(1),            // 1 hour
            TimeValue.timeValueDays(1),             // 1 day
            TimeValue.timeValueMinutes(30),         // 30 minutes
            TimeValue.timeValueMillis(123456),      // Custom milliseconds
            TimeValue.timeValueMillis(1000000),     // Large value
            TimeValue.timeValueMillis(1000000000)   // Very large value
        };

        for (TimeValue shiftAmount : testShifts) {
            testSerializationRoundtripForShift(shiftAmount);
        }
    }

    private void testSerializationRoundtripForShift(TimeValue shiftAmount) throws IOException {
        // Arrange
        TimeshiftStage originalStage = new TimeshiftStage(shiftAmount.getMillis());

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write the stage to stream
            originalStage.writeTo(out);

            // Read it back
            try (StreamInput in = out.bytes().streamInput()) {
                TimeshiftStage deserializedStage = TimeshiftStage.readFrom(in);

                // Assert - verify all properties match
                assertEquals(
                    "Shift amount should match for value " + shiftAmount,
                    shiftAmount.getMillis(),
                    deserializedStage.getShiftMillis()
                );
                assertEquals("Name should match", originalStage.getName(), deserializedStage.getName());
                assertEquals(
                    "supportConcurrentSegmentSearch should match",
                    originalStage.supportConcurrentSegmentSearch(),
                    deserializedStage.supportConcurrentSegmentSearch()
                );
                assertEquals("isCoordinatorOnly should match", originalStage.isCoordinatorOnly(), deserializedStage.isCoordinatorOnly());

                // Verify the deserialized stage behaves the same as original
                List<TimeSeries> emptyInput = new ArrayList<>();
                List<TimeSeries> originalResult = originalStage.process(emptyInput);
                List<TimeSeries> deserializedResult = deserializedStage.process(emptyInput);
                assertEquals("Process results should match for empty input", originalResult.size(), deserializedResult.size());
            }
        }
    }

    public void testSerializationStreamPosition() throws IOException {
        TimeshiftStage stage1 = new TimeshiftStage(TimeValue.timeValueHours(1).getMillis());
        TimeshiftStage stage2 = new TimeshiftStage(TimeValue.timeValueHours(2).getMillis());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write two stages to the same stream
            stage1.writeTo(out);
            stage2.writeTo(out);

            // Read them back in order
            try (StreamInput in = out.bytes().streamInput()) {
                TimeshiftStage readStage1 = TimeshiftStage.readFrom(in);
                TimeshiftStage readStage2 = TimeshiftStage.readFrom(in);

                // Verify correct order and values
                assertEquals("First stage shift amount should match", TimeValue.timeValueHours(1).getMillis(), readStage1.getShiftMillis());
                assertEquals(
                    "Second stage shift amount should match",
                    TimeValue.timeValueHours(2).getMillis(),
                    readStage2.getShiftMillis()
                );
            }
        }
    }

    public void testSerializationDataIntegrity() throws IOException {
        TimeshiftStage originalStage = new TimeshiftStage(123456789);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            // Verify the output contains data
            assertTrue("Stream should contain data", out.size() > 0);
            assertEquals("Stream should contain exactly 8 bytes for long", 8, out.size());

            try (StreamInput in = out.bytes().streamInput()) {
                TimeshiftStage readStage = TimeshiftStage.readFrom(in);

                // Verify exact precision is maintained
                assertEquals("Exact shift amount precision should be maintained", 123456789L, readStage.getShiftMillis());

                // Verify stream is fully consumed
                assertEquals("Stream should be fully consumed", -1, in.read());
            }
        }
    }

    // ========== Factory Integration Tests ==========

    public void testFactoryCreateWithArgs() {
        String stageName = "timeshift";
        Map<String, Object> args = Map.of(TimeshiftStage.SHIFT_AMOUNT_ARG, "1h");

        PipelineStage stage = PipelineStageFactory.createWithArgs(stageName, args);

        assertNotNull(stage);
        assertTrue(stage instanceof TimeshiftStage);
        assertEquals("timeshift", stage.getName());
        assertEquals(TimeValue.timeValueHours(1).getMillis(), ((TimeshiftStage) stage).getShiftMillis());
    }

    public void testFactoryCreateWithComplexArgs() {

        Map<String, Object> stringArgs = Map.of(TimeshiftStage.SHIFT_AMOUNT_ARG, "30m");
        PipelineStage stringStage = PipelineStageFactory.createWithArgs("timeshift", stringArgs);
        assertEquals(TimeValue.timeValueMinutes(30).getMillis(), ((TimeshiftStage) stringStage).getShiftMillis());

        Map<String, Object> smallArgs = Map.of(TimeshiftStage.SHIFT_AMOUNT_ARG, "30s");
        PipelineStage smallStage = PipelineStageFactory.createWithArgs("timeshift", smallArgs);
        assertEquals(TimeValue.timeValueSeconds(30).getMillis(), ((TimeshiftStage) smallStage).getShiftMillis());

        Map<String, Object> numericArgs = Map.of(TimeshiftStage.SHIFT_AMOUNT_ARG, 3600000L);
        PipelineStage numericStage = PipelineStageFactory.createWithArgs("timeshift", numericArgs);
        assertEquals(TimeValue.timeValueHours(1).getMillis(), ((TimeshiftStage) numericStage).getShiftMillis());
    }

    public void testFactoryRegistration() {
        Set<String> supportedStages = PipelineStageFactory.getSupportedStageTypes();

        // Verify TimeshiftStage is registered
        assertTrue(supportedStages.contains("timeshift"));

        // Verify the class has the annotation
        assertTrue(TimeshiftStage.class.isAnnotationPresent(PipelineStageAnnotation.class));
        PipelineStageAnnotation annotation = TimeshiftStage.class.getAnnotation(PipelineStageAnnotation.class);
        assertEquals("timeshift", annotation.name());
    }

    // ========== Reduce Method Tests ==========

    public void testReduceMethod() {
        TimeshiftStage stage = new TimeshiftStage(TimeValue.timeValueHours(1).getMillis());

        // Create mock TimeSeriesProvider instances
        List<TimeSeriesProvider> aggregations = List.of(
            createMockTimeSeriesProvider("provider1"),
            createMockTimeSeriesProvider("provider2")
        );

        UnsupportedOperationException exception = assertThrows(
            UnsupportedOperationException.class,
            () -> stage.reduce(aggregations, false)
        );

        // Assert the complete error message for better test precision
        assertEquals(
            "Unary pipeline stage 'TimeshiftStage' does not support reduce function. "
                + "This method should only be called for global aggregations.",
            exception.getMessage()
        );
    }

    public void testIsGlobalAggregation() {
        TimeshiftStage stage = new TimeshiftStage(TimeValue.timeValueHours(1).getMillis());
        assertFalse("Mapper stages should not be global aggregations", stage.isGlobalAggregation());
    }

    // Helper methods for creating mock providers
    private TimeSeriesProvider createMockTimeSeriesProvider(String name) {
        return new TimeSeriesProvider() {
            @Override
            public List<TimeSeries> getTimeSeries() {
                return Collections.emptyList();
            }

            @Override
            public TimeSeriesProvider createReduced(List<TimeSeries> reducedTimeSeries) {
                return this;
            }
        };
    }

    // ========== XContent Serialization Tests ==========

    public void testToXContent() throws IOException {
        TimeshiftStage stage = new TimeshiftStage(TimeValue.timeValueMinutes(30).getMillis());
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue("JSON should contain shift_amount field", json.contains(TimeshiftStage.SHIFT_AMOUNT_ARG));
        assertTrue("JSON should contain time value", json.contains("30m") || json.contains("1800000"));
    }

    /**
     * Test equals method for TimeshiftStage.
     */
    public void testEquals() {
        TimeshiftStage stage1 = new TimeshiftStage(1000L);
        TimeshiftStage stage2 = new TimeshiftStage(1000L);

        assertEquals("Equal TimeshiftStages should be equal", stage1, stage2);

        TimeshiftStage stageDifferent = new TimeshiftStage(2000L);
        assertNotEquals("Different shift millis should not be equal", stage1, stageDifferent);

        assertEquals("Stage should equal itself", stage1, stage1);

        assertNotEquals("Stage should not equal null", null, stage1);

        assertNotEquals("Stage should not equal different class", "string", stage1);

        TimeshiftStage stageZero1 = new TimeshiftStage(0L);
        TimeshiftStage stageZero2 = new TimeshiftStage(0L);
        assertEquals("Zero shift millis should be equal", stageZero1, stageZero2);

        TimeshiftStage stageLarge1 = new TimeshiftStage(Long.MAX_VALUE);
        TimeshiftStage stageLarge2 = new TimeshiftStage(Long.MAX_VALUE);
        assertEquals("Large shift millis should be equal", stageLarge1, stageLarge2);
    }

    @Override
    protected Writeable.Reader<TimeshiftStage> instanceReader() {
        return TimeshiftStage::readFrom;
    }

    @Override
    protected TimeshiftStage createTestInstance() {
        return new TimeshiftStage(randomLongBetween(-100000, 100000));
    }
}
