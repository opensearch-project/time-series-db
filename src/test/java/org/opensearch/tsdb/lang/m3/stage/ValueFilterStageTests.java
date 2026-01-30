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
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.lang.m3.common.ValueFilterType;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ValueFilterStageTests extends AbstractWireSerializingTestCase<ValueFilterStage> {

    // ========== Constructor Tests ==========

    public void testConstructorWithOperatorAndTargetValue() {
        // Arrange & Act
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterType.EQ, 10.5);

        // Assert
        assertEquals(ValueFilterType.EQ, filterStage.getOperator());
        assertEquals(10.5, filterStage.getTargetValue(), 0.001);
        assertEquals(ValueFilterStage.NAME, filterStage.getName());
    }

    // ========== Process Method Tests ==========

    public void testProcessWithNullInput() {
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterType.EQ, 10.0);
        TestUtils.assertNullInputThrowsException(filterStage, "value_filter");
    }

    public void testProcessWithEmptyInput() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterType.EQ, 10.0);
        List<TimeSeries> input = new ArrayList<>();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testProcessEqOperator() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterType.EQ, 10.0);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertEquals(10.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testProcessNeOperator() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterType.NE, 10.0);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getSamples().size());
        assertEquals(5.0, result.get(0).getSamples().getValue(0), 0.001);
        assertEquals(15.0, result.get(0).getSamples().getValue(1), 0.001);
    }

    public void testProcessGtOperator() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterType.GT, 10.0);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertEquals(15.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testProcessGeOperator() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterType.GE, 10.0);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getSamples().size());
        assertEquals(10.0, result.get(0).getSamples().getValue(0), 0.001);
        assertEquals(15.0, result.get(0).getSamples().getValue(1), 0.001);
    }

    public void testProcessLtOperator() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterType.LT, 10.0);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertEquals(5.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testProcessLeOperator() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterType.LE, 10.0);
        List<TimeSeries> input = createTestTimeSeries();

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).getSamples().size());
        assertEquals(5.0, result.get(0).getSamples().getValue(0), 0.001);
        assertEquals(10.0, result.get(0).getSamples().getValue(1), 0.001);
    }

    public void testProcessWithNaNValues() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterType.EQ, 10.0);
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, 10.0),
            new FloatSample(2000L, Double.NaN),
            new FloatSample(3000L, 15.0)
        );
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertEquals(10.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testProcessWithNullSamples() {
        // Arrange
        ValueFilterStage filterStage = new ValueFilterStage(ValueFilterType.EQ, 10.0);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0), null, new FloatSample(3000L, 15.0));
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = filterStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertEquals(10.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    // ========== Serialization Tests ==========

    public void testWriteToAndReadFrom() throws IOException {
        // Arrange
        ValueFilterStage original = new ValueFilterStage(ValueFilterType.GT, 5.0);
        BytesStreamOutput output = new BytesStreamOutput();

        // Act
        original.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        ValueFilterStage deserialized = ValueFilterStage.readFrom(input);

        // Assert
        assertEquals(original.getOperator(), deserialized.getOperator());
        assertEquals(original.getTargetValue(), deserialized.getTargetValue(), 0.001);
    }

    public void testToXContent() throws IOException {
        ValueFilterStage stage = new ValueFilterStage(ValueFilterType.GE, 10.5);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue("JSON should contain operator field", json.contains(ValueFilterStage.OPERATOR_ARG));
        assertTrue("JSON should contain targetValue field", json.contains(ValueFilterStage.TARGET_VALUE_ARG));
        assertTrue("JSON should contain ge", json.contains("ge"));
        assertTrue("JSON should contain 10.5", json.contains("10.5"));
    }

    /**
     * Test round-trip serialization: toXContent -> parse -> fromArgs
     * This ensures that the JSON produced by toXContent can be parsed back
     * using fromArgs to recreate an equivalent stage.
     */
    public void testToXContentRoundTrip() throws IOException {
        // Test all operator types to ensure round-trip works for each
        ValueFilterType[] operators = ValueFilterType.values();

        for (ValueFilterType operator : operators) {
            // Create original stage
            ValueFilterStage original = new ValueFilterStage(operator, 42.5);

            // Serialize to JSON
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            original.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();

            // Parse the JSON back to a map
            String json = builder.toString();
            Map<String, Object> args;
            try (var parser = createParser(org.opensearch.common.xcontent.json.JsonXContent.jsonXContent, json)) {
                args = parser.map();
            }

            // Recreate the stage from the parsed args
            ValueFilterStage deserialized = ValueFilterStage.fromArgs(args);

            // Verify they're equal
            assertEquals(
                "Round-trip failed for operator " + operator + ": operators don't match",
                original.getOperator(),
                deserialized.getOperator()
            );
            assertEquals(
                "Round-trip failed for operator " + operator + ": target values don't match",
                original.getTargetValue(),
                deserialized.getTargetValue(),
                0.001
            );
            assertEquals("Round-trip failed for operator " + operator + ": stages not equal", original, deserialized);
        }
    }

    // ========== FromArgs Tests ==========

    public void testFromArgsValid() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.OPERATOR_ARG, "gt", ValueFilterStage.TARGET_VALUE_ARG, 10.5);

        // Act
        ValueFilterStage filterStage = ValueFilterStage.fromArgs(args);

        // Assert
        assertEquals(ValueFilterType.GT, filterStage.getOperator());
        assertEquals(10.5, filterStage.getTargetValue(), 0.001);
    }

    public void testFromArgsWithSymbolOperator() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.OPERATOR_ARG, ">=", ValueFilterStage.TARGET_VALUE_ARG, 10.5);

        // Act
        ValueFilterStage filterStage = ValueFilterStage.fromArgs(args);

        // Assert
        assertEquals(ValueFilterType.GE, filterStage.getOperator());
        assertEquals(10.5, filterStage.getTargetValue(), 0.001);
    }

    public void testFromArgsMissingOperator() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.TARGET_VALUE_ARG, 10.5);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("ValueFilter stage requires '" + ValueFilterStage.OPERATOR_ARG + "' argument"));
    }

    public void testFromArgsMissingTargetValue() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.OPERATOR_ARG, "eq");

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("ValueFilter stage requires '" + ValueFilterStage.TARGET_VALUE_ARG + "' argument"));
    }

    public void testFromArgsNullArgs() {
        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(null));
        assertTrue(exception.getMessage().contains("ValueFilter stage requires '" + ValueFilterStage.OPERATOR_ARG + "' argument"));
    }

    public void testFromArgsNullOperator() {
        // Arrange
        Map<String, Object> args = new HashMap<>();
        args.put(ValueFilterStage.OPERATOR_ARG, null);
        args.put(ValueFilterStage.TARGET_VALUE_ARG, 10.5);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertEquals("Operator cannot be null", exception.getMessage());
    }

    public void testFromArgsNullTargetValue() {
        // Arrange
        Map<String, Object> args = new HashMap<>();
        args.put(ValueFilterStage.OPERATOR_ARG, "eq");
        args.put(ValueFilterStage.TARGET_VALUE_ARG, null);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertEquals("Target value cannot be null", exception.getMessage());
    }

    public void testFromArgsInvalidOperatorType() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.OPERATOR_ARG, 123, ValueFilterStage.TARGET_VALUE_ARG, 10.5);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid type for '" + ValueFilterStage.OPERATOR_ARG + "' argument"));
    }

    public void testFromArgsInvalidTargetValueType() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.OPERATOR_ARG, "eq", ValueFilterStage.TARGET_VALUE_ARG, "invalid");

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid type for '" + ValueFilterStage.TARGET_VALUE_ARG + "' argument"));
    }

    public void testFromArgsInvalidOperator() {
        // Arrange
        Map<String, Object> args = Map.of(ValueFilterStage.OPERATOR_ARG, "invalid", ValueFilterStage.TARGET_VALUE_ARG, 10.5);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ValueFilterStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Unknown filter function"));
    }

    // ========== Helper Methods ==========

    private List<TimeSeries> createTestTimeSeries() {
        // Create time series with values: [5.0, 10.0, 15.0]
        TimeSeries series = createTimeSeries(Arrays.asList(5.0, 10.0, 15.0));
        return Arrays.asList(series);
    }

    private TimeSeries createTimeSeries(List<Double> values) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            samples.add(new FloatSample(1000L + i * 1000L, values.get(i)));
        }
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        return new TimeSeries(samples, labels, 1000L, 1000L + (values.size() - 1) * 1000L, 1000L, "test");
    }

    /**
     * Test equals method for ValueFilterStage.
     */
    public void testEquals() {
        ValueFilterStage stage1 = new ValueFilterStage(ValueFilterType.GT, 10.5);
        ValueFilterStage stage2 = new ValueFilterStage(ValueFilterType.GT, 10.5);

        assertEquals("Equal ValueFilterStages should be equal", stage1, stage2);

        ValueFilterStage stageDiffOp = new ValueFilterStage(ValueFilterType.LT, 10.5);
        assertNotEquals("Different operators should not be equal", stage1, stageDiffOp);

        ValueFilterStage stageDiffValue = new ValueFilterStage(ValueFilterType.GT, 15.0);
        assertNotEquals("Different target values should not be equal", stage1, stageDiffValue);

        assertEquals("Stage should equal itself", stage1, stage1);

        assertNotEquals("Stage should not equal null", null, stage1);

        assertNotEquals("Stage should not equal different class", "string", stage1);

        for (ValueFilterType op : ValueFilterType.values()) {
            ValueFilterStage stage3 = new ValueFilterStage(op, 5.0);
            ValueFilterStage stage4 = new ValueFilterStage(op, 5.0);
            assertEquals("Stages with same operator " + op + " should be equal", stage3, stage4);
        }
    }

    @Override
    protected Writeable.Reader<ValueFilterStage> instanceReader() {
        return ValueFilterStage::readFrom;
    }

    @Override
    protected ValueFilterStage createTestInstance() {
        return new ValueFilterStage(randomFrom(ValueFilterType.values()), randomDoubleBetween(-1000.0, 1000.0, true));
    }
}
