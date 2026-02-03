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
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ScaleStageTests extends AbstractWireSerializingTestCase<ScaleStage> {

    public void testConstructor() {
        // Arrange
        double factor = 2.5;

        // Act
        ScaleStage scaleStage = new ScaleStage(factor);

        // Assert
        assertEquals(2.5, scaleStage.getFactor(), 0.001);
        assertEquals("scale", scaleStage.getName());
    }

    public void testProcessWithSingleTimeSeries() {
        // Arrange
        ScaleStage scaleStage = new ScaleStage(2.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = scaleStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        TimeSeries scaledTimeSeries = result.get(0);
        assertEquals(3, scaledTimeSeries.getSamples().size());
        assertEquals(2.0, scaledTimeSeries.getSamples().getValue(0), 0.001);
        assertEquals(4.0, scaledTimeSeries.getSamples().getValue(1), 0.001);
        assertEquals(6.0, scaledTimeSeries.getSamples().getValue(2), 0.001);
        assertEquals(labels, scaledTimeSeries.getLabels());
        assertEquals("test-series", scaledTimeSeries.getAlias());
    }

    public void testProcessWithMultipleTimeSeries() {
        // Arrange
        ScaleStage scaleStage = new ScaleStage(0.5);
        Labels labels1 = ByteLabels.fromMap(Map.of("service", "api"));
        Labels labels2 = ByteLabels.fromMap(Map.of("service", "db"));

        TimeSeries timeSeries1 = new TimeSeries(Arrays.asList(new FloatSample(1000L, 4.0)), labels1, 1000L, 1000L, 1000L, "api-series");
        TimeSeries timeSeries2 = new TimeSeries(Arrays.asList(new FloatSample(2000L, 8.0)), labels2, 2000L, 2000L, 1000L, "db-series");

        // Act
        List<TimeSeries> result = scaleStage.process(Arrays.asList(timeSeries1, timeSeries2));

        // Assert
        assertEquals(2, result.size());
        assertEquals(2.0, result.get(0).getSamples().getValue(0), 0.001);
        assertEquals(4.0, result.get(1).getSamples().getValue(0), 0.001);
    }

    public void testProcessWithEmptyInput() {
        // Arrange
        ScaleStage scaleStage = new ScaleStage(2.0);

        // Act
        List<TimeSeries> result = scaleStage.process(Arrays.asList());

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testProcessWithZeroFactor() {
        // Arrange
        ScaleStage scaleStage = new ScaleStage(0.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries inputTimeSeries = new TimeSeries(Arrays.asList(new FloatSample(1000L, 5.0)), labels, 1000L, 1000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = scaleStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        assertEquals(0.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testProcessWithNegativeFactor() {
        // Arrange
        ScaleStage scaleStage = new ScaleStage(-1.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries inputTimeSeries = new TimeSeries(Arrays.asList(new FloatSample(1000L, 3.0)), labels, 1000L, 1000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = scaleStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        assertEquals(-3.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testFromArgs() {
        // Arrange
        Map<String, Object> args = Map.of("factor", 3.0);

        // Act
        ScaleStage scaleStage = ScaleStage.fromArgs(args);

        // Assert
        assertEquals(3.0, scaleStage.getFactor(), 0.001);
    }

    public void testFromArgsWithInteger() {
        // Arrange
        Map<String, Object> args = Map.of("factor", 2);

        // Act
        ScaleStage scaleStage = ScaleStage.fromArgs(args);

        // Assert
        assertEquals(2.0, scaleStage.getFactor(), 0.001);
    }

    public void testFromArgsWithMissingFactor() {
        // Test with missing factor key
        Map<String, Object> emptyArgs = Map.of();

        Exception exception = expectThrows(Exception.class, () -> ScaleStage.fromArgs(emptyArgs));
        assertNotNull("Should throw exception for missing factor", exception);
    }

    public void testFromArgsWithNullFactor() {
        // Test with null factor value - Map.of doesn't allow null values, so use HashMap
        Map<String, Object> nullArgs = new HashMap<>();
        nullArgs.put("factor", null);

        Exception exception = expectThrows(Exception.class, () -> ScaleStage.fromArgs(nullArgs));
        assertNotNull("Should throw exception for null factor", exception);
    }

    public void testFromArgsWithInvalidFactorType() {
        // Test with invalid factor type
        Map<String, Object> invalidArgs = Map.of("factor", "not_a_number");

        Exception exception = expectThrows(Exception.class, () -> ScaleStage.fromArgs(invalidArgs));
        assertNotNull("Should throw exception for invalid factor type", exception);
    }

    public void testWriteToAndReadFrom() throws IOException {
        // Arrange
        ScaleStage originalStage = new ScaleStage(3.5);

        // Act & Assert
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                ScaleStage readStage = ScaleStage.readFrom(in);

                // Verify the deserialized stage has the same factor
                assertEquals(3.5, readStage.getFactor(), 0.001);
                assertEquals("scale", readStage.getName());
            }
        }
    }

    public void testSerializationRoundtripComprehensive() throws IOException {
        // Test various factor values for comprehensive serialization testing
        double[] testFactors = {
            0.0,           // Zero
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

        for (double factor : testFactors) {
            testSerializationRoundtripForFactor(factor);
        }
    }

    private void testSerializationRoundtripForFactor(double factor) throws IOException {
        // Arrange
        ScaleStage originalStage = new ScaleStage(factor);

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write the stage to stream
            originalStage.writeTo(out);

            // Read it back
            try (StreamInput in = out.bytes().streamInput()) {
                ScaleStage deserializedStage = ScaleStage.readFrom(in);

                // Assert - verify all properties match
                assertEquals("Factor should match for value " + factor, factor, deserializedStage.getFactor(), 0.0);
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
        // Test that serialization correctly handles stream position
        ScaleStage stage1 = new ScaleStage(1.5);
        ScaleStage stage2 = new ScaleStage(2.5);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Write two stages to the same stream
            stage1.writeTo(out);
            stage2.writeTo(out);

            // Read them back in order
            try (StreamInput in = out.bytes().streamInput()) {
                ScaleStage readStage1 = ScaleStage.readFrom(in);
                ScaleStage readStage2 = ScaleStage.readFrom(in);

                // Verify correct order and values
                assertEquals("First stage factor should match", 1.5, readStage1.getFactor(), 0.001);
                assertEquals("Second stage factor should match", 2.5, readStage2.getFactor(), 0.001);
            }
        }
    }

    public void testSerializationEmptyStream() throws IOException {
        // Test reading from an empty stream should fail appropriately
        try (BytesStreamOutput out = new BytesStreamOutput(); StreamInput in = out.bytes().streamInput()) {

            expectThrows(Exception.class, () -> ScaleStage.readFrom(in));
        }
    }

    public void testSerializationDataIntegrity() throws IOException {
        // Test that serialized data maintains integrity
        ScaleStage originalStage = new ScaleStage(42.123456789);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            // Verify the output contains data
            assertTrue("Stream should contain data", out.size() > 0);
            assertEquals("Stream should contain exactly 8 bytes for double", 8, out.size());

            try (StreamInput in = out.bytes().streamInput()) {
                ScaleStage readStage = ScaleStage.readFrom(in);

                // Verify exact precision is maintained
                assertEquals("Exact factor precision should be maintained", 42.123456789, readStage.getFactor(), 0.0);

                // Verify stream is fully consumed
                assertEquals("Stream should be fully consumed", -1, in.read());
            }
        }
    }

    public void testSerializationBehaviorValidation() throws IOException {
        // Test that writeTo and readFrom are properly paired
        ScaleStage originalStage = new ScaleStage(7.89);

        // Test that writeTo actually writes data
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);

            // Verify data was written
            assertTrue("WriteTo should write data to stream", out.size() > 0);

            // Test that readFrom can read what writeTo wrote
            try (StreamInput in = out.bytes().streamInput()) {
                ScaleStage readStage = ScaleStage.readFrom(in);

                // Verify functional equivalence
                assertNotSame("Should be different instances", originalStage, readStage);
                assertEquals("Should have same factor", originalStage.getFactor(), readStage.getFactor(), 0.0);
                assertEquals("Should have same name", originalStage.getName(), readStage.getName());

                // Verify behavioral equivalence with actual data
                Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
                List<Sample> samples = Arrays.asList(new FloatSample(1000L, 5.0));
                TimeSeries inputSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, "test");
                List<TimeSeries> input = Arrays.asList(inputSeries);

                List<TimeSeries> originalResult = originalStage.process(input);
                List<TimeSeries> readResult = readStage.process(input);

                assertEquals("Results should have same size", originalResult.size(), readResult.size());
                assertEquals(
                    "Results should have same scaled value",
                    originalResult.get(0).getSamples().getValue(0),
                    readResult.get(0).getSamples().getValue(0),
                    0.001
                );
            }
        }
    }

    public void testSupportConcurrentSegmentSearch() {
        // Arrange
        ScaleStage scaleStage = new ScaleStage(2.0);

        // Act & Assert
        assertTrue(scaleStage.supportConcurrentSegmentSearch());
    }

    public void testGetName() {
        // Arrange
        ScaleStage scaleStage = new ScaleStage(1.0);

        // Act & Assert
        assertEquals("scale", scaleStage.getName());
    }

    public void testGetFactor() {
        // Arrange
        ScaleStage scaleStage = new ScaleStage(4.2);

        // Act & Assert
        assertEquals(4.2, scaleStage.getFactor(), 0.001);
    }

    public void testProcessPreservesMetadata() {
        // Arrange
        ScaleStage scaleStage = new ScaleStage(2.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api", "region", "us-east"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "preserved-alias");

        // Act
        List<TimeSeries> result = scaleStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        TimeSeries scaledTimeSeries = result.get(0);
        assertEquals(labels, scaledTimeSeries.getLabels());
        assertEquals("preserved-alias", scaledTimeSeries.getAlias());
        assertEquals(1000L, scaledTimeSeries.getMinTimestamp());
        assertEquals(2000L, scaledTimeSeries.getMaxTimestamp());
        assertEquals(1000L, scaledTimeSeries.getStep());
    }

    // ========== Annotation Tests ==========

    public void testAnnotationPresent() {
        // Verify ScaleStage has the PipelineStageAnnotation
        assertTrue("ScaleStage should have PipelineStageAnnotation", ScaleStage.class.isAnnotationPresent(PipelineStageAnnotation.class));
    }

    public void testAnnotationValue() {
        // Get the annotation and verify its value
        PipelineStageAnnotation annotation = ScaleStage.class.getAnnotation(PipelineStageAnnotation.class);
        assertNotNull("Annotation should not be null", annotation);
        assertEquals("Annotation name should be 'scale'", "scale", annotation.name());
    }

    public void testAnnotationConsistencyWithStageName() {
        // Verify that the annotation name matches the stage's getName() result
        PipelineStageAnnotation annotation = ScaleStage.class.getAnnotation(PipelineStageAnnotation.class);
        ScaleStage stage = new ScaleStage(1.0);
        assertEquals("Annotation name should match stage getName()", annotation.name(), stage.getName());
    }

    public void testAnnotationConsistencyWithStaticName() {
        // Verify that the annotation name matches the static NAME constant
        PipelineStageAnnotation annotation = ScaleStage.class.getAnnotation(PipelineStageAnnotation.class);
        try {
            java.lang.reflect.Field nameField = ScaleStage.class.getField("NAME");
            String staticName = (String) nameField.get(null);
            assertEquals("Annotation name should match static NAME constant", annotation.name(), staticName);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            fail("ScaleStage should have public static NAME field");
        }
    }

    // ========== Factory Integration Tests ==========

    public void testFactoryCreateWithArgs() {
        // Test that the factory can create ScaleStage using createWithArgs
        String stageName = "scale";
        Map<String, Object> args = Map.of("factor", 2.0);

        PipelineStage stage = PipelineStageFactory.createWithArgs(stageName, args);

        assertNotNull(stage);
        assertTrue(stage instanceof ScaleStage);
        assertEquals("scale", stage.getName());
        assertEquals(2.0, ((ScaleStage) stage).getFactor(), 0.001);
    }

    public void testFactoryCreateWithComplexArgs() {
        // Test factory with various argument types

        // Test with integer that should be converted to double
        Map<String, Object> intArgs = Map.of("factor", 5);
        PipelineStage intStage = PipelineStageFactory.createWithArgs("scale", intArgs);
        assertEquals(5.0, ((ScaleStage) intStage).getFactor(), 0.001);

        // Test with negative value
        Map<String, Object> negativeArgs = Map.of("factor", -2.5);
        PipelineStage negativeStage = PipelineStageFactory.createWithArgs("scale", negativeArgs);
        assertEquals(-2.5, ((ScaleStage) negativeStage).getFactor(), 0.001);

        // Test with zero
        Map<String, Object> zeroArgs = Map.of("factor", 0.0);
        PipelineStage zeroStage = PipelineStageFactory.createWithArgs("scale", zeroArgs);
        assertEquals(0.0, ((ScaleStage) zeroStage).getFactor(), 0.001);
    }

    public void testFactoryRegistration() {
        // Test that ScaleStage is properly registered via annotation
        Set<String> supportedStages = PipelineStageFactory.getSupportedStageTypes();

        // Verify ScaleStage is registered
        assertTrue(supportedStages.contains("scale"));

        // Verify the class has the annotation
        assertTrue(ScaleStage.class.isAnnotationPresent(PipelineStageAnnotation.class));
        PipelineStageAnnotation annotation = ScaleStage.class.getAnnotation(PipelineStageAnnotation.class);
        assertEquals("scale", annotation.name());
    }

    // ========== Interface Compliance Tests ==========

    public void testUnaryPipelineStageInterface() {
        // Verify ScaleStage implements UnaryPipelineStage
        ScaleStage scaleStage = new ScaleStage(2.0);
        assertTrue("ScaleStage should implement UnaryPipelineStage", scaleStage instanceof UnaryPipelineStage);
    }

    public void testPipelineStageInterface() {
        // Verify ScaleStage implements PipelineStage
        ScaleStage scaleStage = new ScaleStage(2.0);
        assertTrue("ScaleStage should implement PipelineStage", scaleStage instanceof PipelineStage);
    }

    public void testPolymorphicUsage() {
        // Test that ScaleStage can be used polymorphically
        ScaleStage scaleStage = new ScaleStage(1.0);

        // As PipelineStage
        PipelineStage pipelineStage = scaleStage;
        assertNotNull("Should work as PipelineStage", pipelineStage);
        assertEquals("Should have same name", scaleStage.getName(), pipelineStage.getName());

        // As UnaryPipelineStage
        UnaryPipelineStage unaryStage = scaleStage;
        assertNotNull("Should work as UnaryPipelineStage", unaryStage);
        assertEquals("Should have same name", scaleStage.getName(), unaryStage.getName());

        // All should reference the same object
        assertSame("Should be same object", scaleStage, pipelineStage);
        assertSame("Should be same object", scaleStage, unaryStage);
        assertSame("Should be same object", pipelineStage, unaryStage);
    }

    public void testConcurrentSegmentSearchSupport() {
        // Test the concurrent segment search capability
        ScaleStage stage = new ScaleStage(2.0);
        boolean supportsConcurrent = stage.supportConcurrentSegmentSearch();
        assertTrue("ScaleStage should support concurrent segment search", supportsConcurrent);
    }

    public void testIsCoordinatorOnly() {
        // Test the default coordinator-only behavior from UnaryPipelineStage
        ScaleStage stage = new ScaleStage(2.0);

        // ScaleStage should use the default UnaryPipelineStage implementation
        boolean isCoordinatorOnly = stage.isCoordinatorOnly();
        assertFalse("ScaleStage should not be coordinator-only by default", isCoordinatorOnly);
    }

    public void testUnaryPipelineStageDefaults() {
        // Test that ScaleStage properly inherits UnaryPipelineStage default behaviors
        ScaleStage stage = new ScaleStage(1.0);

        // Test isCoordinatorOnly default
        assertFalse("UnaryPipelineStage default isCoordinatorOnly should be false", stage.isCoordinatorOnly());

        // Test that ScaleStage overrides supportConcurrentSegmentSearch from the default
        // Note: ScaleStage overrides the default (false) to return true
        assertTrue("ScaleStage should override default supportConcurrentSegmentSearch", stage.supportConcurrentSegmentSearch());
    }

    public void testUnaryPipelineStageInterfaceContract() {
        // Verify that ScaleStage implements the UnaryPipelineStage interface contract
        ScaleStage stage = new ScaleStage(2.0);

        // Cast to interface to test polymorphic behavior
        UnaryPipelineStage unaryStage = stage;

        // Test interface methods
        assertFalse("Interface default isCoordinatorOnly should be false", unaryStage.isCoordinatorOnly());
        assertTrue("ScaleStage should support concurrent search through interface", unaryStage.supportConcurrentSegmentSearch());

        // Test that getName works through interface
        assertEquals("Interface getName should work", "scale", unaryStage.getName());
    }

    // ========== Reflection Tests ==========

    public void testReflectionMethods() throws Exception {
        // Test that ScaleStage has the required static methods for factory registration
        Class<ScaleStage> stageClass = ScaleStage.class;

        // Verify fromArgs method exists
        java.lang.reflect.Method fromArgsMethod = stageClass.getMethod("fromArgs", Map.class);
        assertNotNull(fromArgsMethod);
        assertTrue(java.lang.reflect.Modifier.isStatic(fromArgsMethod.getModifiers()));

        // Verify readFrom method exists
        java.lang.reflect.Method readFromMethod = stageClass.getMethod("readFrom", org.opensearch.core.common.io.stream.StreamInput.class);
        assertNotNull(readFromMethod);
        assertTrue(java.lang.reflect.Modifier.isStatic(readFromMethod.getModifiers()));
    }

    public void testFactoryReflectionUsage() throws Exception {
        // Test that the factory reflection mechanism works correctly
        Map<String, Object> args = Map.of("factor", 1.5);

        java.lang.reflect.Method fromArgsMethod = ScaleStage.class.getMethod("fromArgs", Map.class);
        Object result = fromArgsMethod.invoke(null, args);

        assertNotNull(result);
        assertTrue(result instanceof ScaleStage);
        assertEquals(1.5, ((ScaleStage) result).getFactor(), 0.001);
    }

    public void testReduceMethod() {
        // Test the reduce method from UnaryPipelineStage interface
        ScaleStage stage = new ScaleStage(2.0);

        // Create mock TimeSeriesProvider instances
        List<TimeSeriesProvider> aggregations = Arrays.asList(
            createMockTimeSeriesProvider("provider1"),
            createMockTimeSeriesProvider("provider2")
        );

        // Test the reduce method - should throw UnsupportedOperationException for unary stages
        UnsupportedOperationException exception = assertThrows(
            UnsupportedOperationException.class,
            () -> stage.reduce(aggregations, false)
        );

        assertTrue("Exception message should contain class name", exception.getMessage().contains("ScaleStage"));
        assertTrue("Exception message should mention reduce function", exception.getMessage().contains("reduce function"));
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

    // ========== UnaryPipelineStage Interface Tests ==========

    public void testUnaryPipelineStageInheritance() {
        // Verify UnaryPipelineStage extends PipelineStage
        assertTrue("UnaryPipelineStage should extend PipelineStage", PipelineStage.class.isAssignableFrom(UnaryPipelineStage.class));
    }

    public void testUnaryPipelineStageContract() {
        // Test that UnaryPipelineStage provides the expected interface
        ScaleStage stage = new ScaleStage(1.5);

        // Should have process method from PipelineStage
        List<TimeSeries> emptyInput = Collections.emptyList();
        List<TimeSeries> result = stage.process(emptyInput);
        assertNotNull("process() should return non-null result", result);
        assertTrue("process() should return empty list for empty input", result.isEmpty());

        // Should have getName method from PipelineStage
        String name = stage.getName();
        assertNotNull("getName() should return non-null", name);
        assertEquals("getName() should return 'scale'", "scale", name);

        // Should have supportConcurrentSegmentSearch method
        boolean supportsConcurrent = stage.supportConcurrentSegmentSearch();
        assertTrue("ScaleStage should support concurrent segment search", supportsConcurrent);
    }

    public void testUnaryPipelineStageNullInput() {
        ScaleStage stage = new ScaleStage(1.0);
        TestUtils.assertNullInputThrowsException(stage, "scale");
    }

    public void testUnaryPipelineStageComposition() {
        // Test that multiple unary stages can be composed
        ScaleStage stage1 = new ScaleStage(2.0);
        ScaleStage stage2 = new ScaleStage(3.0);

        // Create real test data
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0), new FloatSample(3000L, 30.0));
        Labels labels = ByteLabels.fromMap(Map.of("metric", "cpu_usage"));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "cpu");
        List<TimeSeries> input = Arrays.asList(inputTimeSeries);

        // Test first stage (scale by 2.0)
        List<TimeSeries> result1 = stage1.process(input);
        assertNotNull("First stage should return non-null result", result1);
        assertEquals("First stage should return one time series", 1, result1.size());

        // Verify first stage scaling: 10.0 * 2.0 = 20.0, 20.0 * 2.0 = 40.0, 30.0 * 2.0 = 60.0
        List<Sample> scaledSamples1 = result1.get(0).getSamples().toList();
        assertEquals("First sample should be scaled by 2.0", 20.0, scaledSamples1.get(0).getValue(), 0.001);
        assertEquals("Second sample should be scaled by 2.0", 40.0, scaledSamples1.get(1).getValue(), 0.001);
        assertEquals("Third sample should be scaled by 2.0", 60.0, scaledSamples1.get(2).getValue(), 0.001);

        // Test second stage (scale by 3.0)
        List<TimeSeries> result2 = stage2.process(input);
        assertNotNull("Second stage should return non-null result", result2);
        assertEquals("Second stage should return one time series", 1, result2.size());

        // Verify second stage scaling: 10.0 * 3.0 = 30.0, 20.0 * 3.0 = 60.0, 30.0 * 3.0 = 90.0
        List<Sample> scaledSamples2 = result2.get(0).getSamples().toList();
        assertEquals("First sample should be scaled by 3.0", 30.0, scaledSamples2.get(0).getValue(), 0.001);
        assertEquals("Second sample should be scaled by 3.0", 60.0, scaledSamples2.get(1).getValue(), 0.001);
        assertEquals("Third sample should be scaled by 3.0", 90.0, scaledSamples2.get(2).getValue(), 0.001);
    }

    public void testUnaryPipelineStageCharacteristics() {
        // Test characteristics specific to unary stages
        ScaleStage stage = new ScaleStage(2.5);

        // Create real test data
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 4.0), new FloatSample(2000L, 8.0));
        Labels labels = ByteLabels.fromMap(Map.of("metric", "memory_usage"));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "memory");
        List<TimeSeries> input = Arrays.asList(inputTimeSeries);

        // Test unary stage processing with real data
        List<TimeSeries> output = stage.process(input);

        assertNotNull("Output should not be null", output);
        assertEquals("Should return one time series", 1, output.size());

        // Verify scaling: 4.0 * 2.5 = 10.0, 8.0 * 2.5 = 20.0
        List<Sample> scaledSamples = output.get(0).getSamples().toList();
        assertEquals("First sample should be scaled by 2.5", 10.0, scaledSamples.get(0).getValue(), 0.001);
        assertEquals("Second sample should be scaled by 2.5", 20.0, scaledSamples.get(1).getValue(), 0.001);
    }

    public void testFactoryIntegration() {
        // Test that ScaleStage works with the factory
        Map<String, Object> args = Map.of("factor", 2.0);
        PipelineStage stage = PipelineStageFactory.createWithArgs("scale", args);

        assertNotNull("Factory should create stage", stage);
        assertTrue("Factory should create UnaryPipelineStage", stage instanceof UnaryPipelineStage);
        assertTrue("Factory should create ScaleStage", stage instanceof ScaleStage);

        // Should work as UnaryPipelineStage
        UnaryPipelineStage unaryStage = (UnaryPipelineStage) stage;
        assertNotNull("Should work as UnaryPipelineStage", unaryStage);
    }

    /**
     * Test equals method for ScaleStage.
     */
    public void testEquals() {
        ScaleStage stage1 = new ScaleStage(2.5);
        ScaleStage stage2 = new ScaleStage(2.5);

        assertEquals("Equal ScaleStages should be equal", stage1, stage2);

        ScaleStage stageDifferent = new ScaleStage(3.0);
        assertNotEquals("Different factors should not be equal", stage1, stageDifferent);

        assertEquals("Stage should equal itself", stage1, stage1);

        assertNotEquals("Stage should not equal null", null, stage1);

        assertNotEquals("Stage should not equal different class", "string", stage1);

        ScaleStage stageZero1 = new ScaleStage(0.0);
        ScaleStage stageZero2 = new ScaleStage(0.0);
        assertEquals("Zero factors should be equal", stageZero1, stageZero2);

        ScaleStage stageNegative1 = new ScaleStage(-1.5);
        ScaleStage stageNegative2 = new ScaleStage(-1.5);
        assertEquals("Negative factors should be equal", stageNegative1, stageNegative2);
    }

    @Override
    protected Writeable.Reader<ScaleStage> instanceReader() {
        return ScaleStage::readFrom;
    }

    @Override
    protected ScaleStage createTestInstance() {
        return new ScaleStage(randomDoubleBetween(-1000.0, 1000.0, true));
    }
}
