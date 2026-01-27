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
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DivideScalarStageTests extends AbstractWireSerializingTestCase<DivideScalarStage> {

    public void testConstructor() {
        // Arrange
        double divisor = 2.5;

        // Act
        DivideScalarStage divideScalarStage = new DivideScalarStage(divisor);

        // Assert
        assertEquals(2.5, divideScalarStage.getDivisor(), 0.001);
        assertEquals("divideScalar", divideScalarStage.getName());
    }

    public void testConstructorThrowsOnZeroDivisor() {
        // Test that constructor throws exception for zero divisor
        expectThrows(IllegalArgumentException.class, () -> new DivideScalarStage(0.0));
    }

    public void testConstructorThrowsOnNaNDivisor() {
        // Test that constructor throws exception for NaN divisor
        expectThrows(IllegalArgumentException.class, () -> new DivideScalarStage(Double.NaN));
    }

    public void testProcessWithSingleTimeSeries() {
        // Arrange
        DivideScalarStage divideScalarStage = new DivideScalarStage(2.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0), new FloatSample(3000L, 30.0));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = divideScalarStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        TimeSeries dividedTimeSeries = result.get(0);
        assertEquals(3, dividedTimeSeries.getSamples().size());
        assertEquals(5.0, dividedTimeSeries.getSamples().get(0).getValue(), 0.001); // 10.0 / 2.0
        assertEquals(10.0, dividedTimeSeries.getSamples().get(1).getValue(), 0.001); // 20.0 / 2.0
        assertEquals(15.0, dividedTimeSeries.getSamples().get(2).getValue(), 0.001); // 30.0 / 2.0
        assertEquals(labels, dividedTimeSeries.getLabels());
        assertEquals("test-series", dividedTimeSeries.getAlias());
    }

    public void testProcessWithMultipleTimeSeries() {
        // Arrange
        DivideScalarStage divideScalarStage = new DivideScalarStage(4.0);
        Labels labels1 = ByteLabels.fromMap(Map.of("service", "api"));
        Labels labels2 = ByteLabels.fromMap(Map.of("service", "db"));

        TimeSeries timeSeries1 = new TimeSeries(Arrays.asList(new FloatSample(1000L, 8.0)), labels1, 1000L, 1000L, 1000L, "api-series");
        TimeSeries timeSeries2 = new TimeSeries(Arrays.asList(new FloatSample(2000L, 16.0)), labels2, 2000L, 2000L, 1000L, "db-series");

        // Act
        List<TimeSeries> result = divideScalarStage.process(Arrays.asList(timeSeries1, timeSeries2));

        // Assert
        assertEquals(2, result.size());
        assertEquals(2.0, result.get(0).getSamples().get(0).getValue(), 0.001); // 8.0 / 4.0
        assertEquals(4.0, result.get(1).getSamples().get(0).getValue(), 0.001); // 16.0 / 4.0
    }

    public void testProcessWithEmptyInput() {
        // Arrange
        DivideScalarStage divideScalarStage = new DivideScalarStage(2.0);

        // Act
        List<TimeSeries> result = divideScalarStage.process(Arrays.asList());

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testProcessWithNegativeDivisor() {
        // Arrange
        DivideScalarStage divideScalarStage = new DivideScalarStage(-2.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries inputTimeSeries = new TimeSeries(Arrays.asList(new FloatSample(1000L, 6.0)), labels, 1000L, 1000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = divideScalarStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        assertEquals(-3.0, result.get(0).getSamples().get(0).getValue(), 0.001); // 6.0 / -2.0
    }

    public void testProcessThrowsOnNaNInput() {
        // Arrange
        DivideScalarStage divideScalarStage = new DivideScalarStage(2.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries inputTimeSeries = new TimeSeries(
            Arrays.asList(new FloatSample(1000L, Double.NaN)),
            labels,
            1000L,
            1000L,
            1000L,
            "test-series"
        );

        // Act & Assert
        expectThrows(IllegalArgumentException.class, () -> divideScalarStage.process(Arrays.asList(inputTimeSeries)));
    }

    public void testFromArgs() {
        // Arrange
        Map<String, Object> args = Map.of("divisor", 5.0);

        // Act
        DivideScalarStage divideScalarStage = DivideScalarStage.fromArgs(args);

        // Assert
        assertEquals(5.0, divideScalarStage.getDivisor(), 0.001);
    }

    public void testFromArgsWithInteger() {
        // Arrange
        Map<String, Object> args = Map.of("divisor", 3);

        // Act
        DivideScalarStage divideScalarStage = DivideScalarStage.fromArgs(args);

        // Assert
        assertEquals(3.0, divideScalarStage.getDivisor(), 0.001);
    }

    public void testFromArgsThrowsOnZero() {
        // Test with zero divisor value
        Map<String, Object> zeroArgs = Map.of("divisor", 0.0);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> DivideScalarStage.fromArgs(zeroArgs));
        assertTrue("Should throw exception for zero divisor", exception.getMessage().contains("Division by zero is not allowed"));
    }

    public void testFromArgsThrowsOnNaN() {
        // Test with NaN divisor value
        Map<String, Object> nanArgs = Map.of("divisor", Double.NaN);

        Exception exception = expectThrows(IllegalArgumentException.class, () -> DivideScalarStage.fromArgs(nanArgs));
        assertTrue("Should throw exception for NaN divisor", exception.getMessage().contains("Divisor cannot be NaN"));
    }

    public void testFromArgsWithMissingDivisor() {
        // Test with missing divisor key
        Map<String, Object> emptyArgs = Map.of();

        Exception exception = expectThrows(Exception.class, () -> DivideScalarStage.fromArgs(emptyArgs));
        assertNotNull("Should throw exception for missing divisor", exception);
    }

    public void testFromArgsWithNullDivisor() {
        // Test with null divisor value - Map.of doesn't allow null values, so use HashMap
        Map<String, Object> nullArgs = new HashMap<>();
        nullArgs.put("divisor", null);

        Exception exception = expectThrows(Exception.class, () -> DivideScalarStage.fromArgs(nullArgs));
        assertNotNull("Should throw exception for null divisor", exception);
    }

    public void testFromArgsWithInvalidDivisorType() {
        // Test with invalid divisor type
        Map<String, Object> invalidArgs = Map.of("divisor", "not_a_number");

        Exception exception = expectThrows(Exception.class, () -> DivideScalarStage.fromArgs(invalidArgs));
        assertNotNull("Should throw exception for invalid divisor type", exception);
    }

    public void testWriteToAndReadFrom() throws IOException {
        // Arrange
        DivideScalarStage originalStage = new DivideScalarStage(3.5);

        // Act & Assert
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                DivideScalarStage readStage = DivideScalarStage.readFrom(in);

                // Verify the deserialized stage has the same divisor
                assertEquals(3.5, readStage.getDivisor(), 0.001);
                assertEquals("divideScalar", readStage.getName());
            }
        }
    }

    public void testSerializationRoundtripComprehensive() throws IOException {
        // Test various divisor values for comprehensive serialization testing
        // Note: Exclude zero as it's not allowed for DivideScalarStage
        double[] testDivisors = {
            1.0,           // One
            -1.0,          // Negative one
            2.5,           // Positive decimal
            -3.7,          // Negative decimal
            1000.0,        // Large positive
            -1000.0,       // Large negative
            0.001,         // Small positive
            -0.001,        // Small negative
            Double.MAX_VALUE,    // Maximum value
            Double.MIN_VALUE,    // Minimum positive value
            -Double.MAX_VALUE    // Minimum value (most negative)
        };

        for (double divisor : testDivisors) {
            testSerializationRoundtripForDivisor(divisor);
        }
    }

    private void testSerializationRoundtripForDivisor(double divisor) throws IOException {
        // Arrange
        DivideScalarStage originalStage = new DivideScalarStage(divisor);

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write the stage to stream
            originalStage.writeTo(out);

            // Read it back
            try (StreamInput in = out.bytes().streamInput()) {
                DivideScalarStage deserializedStage = DivideScalarStage.readFrom(in);

                // Assert - verify all properties match
                assertEquals("Divisor should match for value " + divisor, divisor, deserializedStage.getDivisor(), 0.0);
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

    public void testSupportConcurrentSegmentSearch() {
        // Arrange
        DivideScalarStage divideScalarStage = new DivideScalarStage(2.0);

        // Act & Assert
        assertTrue(divideScalarStage.supportConcurrentSegmentSearch());
    }

    public void testGetName() {
        // Arrange
        DivideScalarStage divideScalarStage = new DivideScalarStage(1.0);

        // Act & Assert
        assertEquals("divideScalar", divideScalarStage.getName());
    }

    public void testGetDivisor() {
        // Arrange
        DivideScalarStage divideScalarStage = new DivideScalarStage(4.2);

        // Act & Assert
        assertEquals(4.2, divideScalarStage.getDivisor(), 0.001);
    }

    public void testProcessPreservesMetadata() {
        // Arrange
        DivideScalarStage divideScalarStage = new DivideScalarStage(2.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api", "region", "us-east"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "preserved-alias");

        // Act
        List<TimeSeries> result = divideScalarStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        TimeSeries dividedTimeSeries = result.get(0);
        assertEquals(labels, dividedTimeSeries.getLabels());
        assertEquals("preserved-alias", dividedTimeSeries.getAlias());
        assertEquals(1000L, dividedTimeSeries.getMinTimestamp());
        assertEquals(2000L, dividedTimeSeries.getMaxTimestamp());
        assertEquals(1000L, dividedTimeSeries.getStep());
    }

    // ========== Annotation Tests ==========

    public void testAnnotationPresent() {
        // Verify DivideScalarStage has the PipelineStageAnnotation
        assertTrue(
            "DivideScalarStage should have PipelineStageAnnotation",
            DivideScalarStage.class.isAnnotationPresent(PipelineStageAnnotation.class)
        );
    }

    public void testAnnotationValue() {
        // Get the annotation and verify its value
        PipelineStageAnnotation annotation = DivideScalarStage.class.getAnnotation(PipelineStageAnnotation.class);
        assertNotNull("Annotation should not be null", annotation);
        assertEquals("Annotation name should be 'divideScalar'", "divideScalar", annotation.name());
    }

    public void testAnnotationConsistencyWithStageName() {
        // Verify that the annotation name matches the stage's getName() result
        PipelineStageAnnotation annotation = DivideScalarStage.class.getAnnotation(PipelineStageAnnotation.class);
        DivideScalarStage stage = new DivideScalarStage(1.0);
        assertEquals("Annotation name should match stage getName()", annotation.name(), stage.getName());
    }

    public void testAnnotationConsistencyWithStaticName() {
        // Verify that the annotation name matches the static NAME constant
        PipelineStageAnnotation annotation = DivideScalarStage.class.getAnnotation(PipelineStageAnnotation.class);
        try {
            java.lang.reflect.Field nameField = DivideScalarStage.class.getField("NAME");
            String staticName = (String) nameField.get(null);
            assertEquals("Annotation name should match static NAME constant", annotation.name(), staticName);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("DivideScalarStage should have public static NAME field");
        }
    }

    // ========== Factory Integration Tests ==========

    public void testFactoryCreateWithArgs() {
        // Test that the factory can create DivideScalarStage using createWithArgs
        String stageName = "divideScalar";
        Map<String, Object> args = Map.of("divisor", 2.0);

        PipelineStage stage = PipelineStageFactory.createWithArgs(stageName, args);

        assertNotNull(stage);
        assertTrue(stage instanceof DivideScalarStage);
        assertEquals("divideScalar", stage.getName());
        assertEquals(2.0, ((DivideScalarStage) stage).getDivisor(), 0.001);
    }

    public void testFactoryCreateWithComplexArgs() {
        // Test factory with various argument types

        // Test with integer that should be converted to double
        Map<String, Object> intArgs = Map.of("divisor", 5);
        PipelineStage intStage = PipelineStageFactory.createWithArgs("divideScalar", intArgs);
        assertEquals(5.0, ((DivideScalarStage) intStage).getDivisor(), 0.001);

        // Test with negative value
        Map<String, Object> negativeArgs = Map.of("divisor", -2.5);
        PipelineStage negativeStage = PipelineStageFactory.createWithArgs("divideScalar", negativeArgs);
        assertEquals(-2.5, ((DivideScalarStage) negativeStage).getDivisor(), 0.001);
    }

    public void testFactoryThrowsOnZeroDivisor() {
        // Test with zero divisor via factory
        Map<String, Object> zeroArgs = Map.of("divisor", 0.0);
        expectThrows(Exception.class, () -> PipelineStageFactory.createWithArgs("divideScalar", zeroArgs));
    }

    public void testFactoryRegistration() {
        // Test that DivideScalarStage is properly registered via annotation
        Set<String> supportedStages = PipelineStageFactory.getSupportedStageTypes();

        // Verify DivideScalarStage is registered
        assertTrue(supportedStages.contains("divideScalar"));

        // Verify the class has the annotation
        assertTrue(DivideScalarStage.class.isAnnotationPresent(PipelineStageAnnotation.class));
        PipelineStageAnnotation annotation = DivideScalarStage.class.getAnnotation(PipelineStageAnnotation.class);
        assertEquals("divideScalar", annotation.name());
    }

    // ========== Interface Compliance Tests ==========

    public void testUnaryPipelineStageInterface() {
        // Verify DivideScalarStage implements UnaryPipelineStage
        DivideScalarStage divideScalarStage = new DivideScalarStage(2.0);
        assertTrue("DivideScalarStage should implement UnaryPipelineStage", divideScalarStage instanceof UnaryPipelineStage);
    }

    public void testPipelineStageInterface() {
        // Verify DivideScalarStage implements PipelineStage
        DivideScalarStage divideScalarStage = new DivideScalarStage(2.0);
        assertTrue("DivideScalarStage should implement PipelineStage", divideScalarStage instanceof PipelineStage);
    }

    public void testUnaryPipelineStageNullInput() {
        DivideScalarStage stage = new DivideScalarStage(1.0);
        TestUtils.assertNullInputThrowsException(stage, "divideScalar");
    }

    /**
     * Test equals method for DivideScalarStage.
     */
    public void testEquals() {
        DivideScalarStage stage1 = new DivideScalarStage(2.5);
        DivideScalarStage stage2 = new DivideScalarStage(2.5);

        assertEquals("Equal DivideScalarStages should be equal", stage1, stage2);

        DivideScalarStage stageDifferent = new DivideScalarStage(3.0);
        assertNotEquals("Different divisors should not be equal", stage1, stageDifferent);

        assertEquals("Stage should equal itself", stage1, stage1);

        assertNotEquals("Stage should not equal null", null, stage1);

        assertNotEquals("Stage should not equal different class", "string", stage1);

        DivideScalarStage stageNegative1 = new DivideScalarStage(-1.5);
        DivideScalarStage stageNegative2 = new DivideScalarStage(-1.5);
        assertEquals("Negative divisors should be equal", stageNegative1, stageNegative2);
    }

    @Override
    protected Writeable.Reader<DivideScalarStage> instanceReader() {
        return DivideScalarStage::readFrom;
    }

    @Override
    protected DivideScalarStage createTestInstance() {
        double divisor;
        do {
            divisor = randomDoubleBetween(-1000.0, 1000.0, false); // false excludes zero
        } while (Double.isNaN(divisor)); // Also exclude NaN
        return new DivideScalarStage(divisor);
    }
}
