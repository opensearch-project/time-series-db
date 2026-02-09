/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.Writeable;
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
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;

public class SqrtStageTests extends AbstractWireSerializingTestCase<SqrtStage> {

    public void testDefaultConstructor() {
        // Arrange & Act
        SqrtStage sqrtStage = new SqrtStage();

        // Assert
        assertEquals("sqrt", sqrtStage.getName());
    }

    public void testMappingInputOutput() {
        // Arrange
        SqrtStage sqrtStage = new SqrtStage();
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, 4.0),     // sqrt(4) = 2
            new FloatSample(2000L, 9.0),     // sqrt(9) = 3
            new FloatSample(3000L, 0.0),     // sqrt(0) = 0
            new FloatSample(4000L, -1.0),    // sqrt(-1) = NaN
            new FloatSample(5000L, 16.0),    // sqrt(16) = 4
            new FloatSample(6000L, Double.NaN),
            new FloatSample(7000L, Double.POSITIVE_INFINITY),
            new FloatSample(8000L, 0.25)     // sqrt(0.25) = 0.5
        );
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 8000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = sqrtStage.process(List.of(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        TimeSeries sqrtTimeSeries = result.getFirst();

        List<Sample> expectedSamples = Arrays.asList(
            new FloatSample(1000L, 2.0),                        // sqrt(4) = 2
            new FloatSample(2000L, 3.0),                        // sqrt(9) = 3
            new FloatSample(3000L, 0.0),                        // sqrt(0) = 0
            new FloatSample(4000L, Double.NaN),                 // sqrt(-1) = NaN
            new FloatSample(5000L, 4.0),                        // sqrt(16) = 4
            new FloatSample(6000L, Double.NaN),                 // NaN input = NaN
            new FloatSample(7000L, Double.POSITIVE_INFINITY),   // +Infinity input = +Infinity
            new FloatSample(8000L, 0.5)                         // sqrt(0.25) = 0.5
        );
        assertSamplesEqual("Sqrt mapping values", expectedSamples, sqrtTimeSeries.getSamples(), 1e-10);
    }

    public void testSqrtEdgeCases() {
        // Arrange
        SqrtStage sqrtStage = new SqrtStage();
        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, 0.0),           // sqrt(0) should be 0
            new FloatSample(2000L, 1.0),           // sqrt(1) should be 1
            new FloatSample(3000L, 4.0),           // sqrt(4) should be 2
            new FloatSample(4000L, -5.0),          // sqrt(negative) should be NaN
            new FloatSample(5000L, 0.01)           // sqrt(0.01) should be 0.1
        );
        TimeSeries inputSeries = new TimeSeries(samples, labels, 1000L, 5000L, 1000L, "test");

        // Act
        List<TimeSeries> result = sqrtStage.process(List.of(inputSeries));

        // Assert
        List<Sample> expectedSamples = Arrays.asList(
            new FloatSample(1000L, 0.0),       // sqrt(0) = 0
            new FloatSample(2000L, 1.0),       // sqrt(1) = 1
            new FloatSample(3000L, 2.0),       // sqrt(4) = 2
            new FloatSample(4000L, Double.NaN), // sqrt(-5) = NaN
            new FloatSample(5000L, 0.1)        // sqrt(0.01) = 0.1
        );
        assertSamplesEqual("Sqrt edge cases", expectedSamples, result.getFirst().getSamples(), 1e-10);
    }

    public void testProcessWithEmptyInput() {
        // Arrange
        SqrtStage sqrtStage = new SqrtStage();

        // Act
        List<TimeSeries> result = sqrtStage.process(List.of());

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testSupportConcurrentSegmentSearch() {
        // Arrange
        SqrtStage sqrtStage = new SqrtStage();

        // Act & Assert
        assertTrue(sqrtStage.supportConcurrentSegmentSearch());
    }

    public void testGetName() {
        // Arrange
        SqrtStage sqrtStage = new SqrtStage();

        // Act & Assert
        assertEquals("sqrt", sqrtStage.getName());
    }

    public void testReduceMethod() {
        // Test the reduce method from UnaryPipelineStage interface
        SqrtStage stage = new SqrtStage();

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

        assertTrue("Exception message should contain class name", exception.getMessage().contains("SqrtStage"));
        assertTrue("Exception message should mention reduce function", exception.getMessage().contains("reduce function"));
    }

    public void testMonotonicProperty() {
        // Test that sqrt preserves order for non-negative values
        SqrtStage sqrtStage = new SqrtStage();
        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 4.0),
            new FloatSample(3000L, 9.0),
            new FloatSample(4000L, 16.0)
        );
        TimeSeries inputSeries = new TimeSeries(samples, labels, 1000L, 4000L, 1000L, "test");

        // Act
        List<TimeSeries> result = sqrtStage.process(List.of(inputSeries));

        // Assert - sqrt values should be monotonically increasing
        List<Sample> resultSamples = result.getFirst().getSamples().toList();
        for (int i = 1; i < resultSamples.size(); i++) {
            assertTrue(
                "Sqrt should preserve order for non-negative values",
                resultSamples.get(i).getValue() > resultSamples.get(i - 1).getValue()
            );
        }
    }

    public void testNonNegativeOutput() {
        // Test that all valid outputs are non-negative
        SqrtStage sqrtStage = new SqrtStage();
        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, 0.0),
            new FloatSample(2000L, 0.25),
            new FloatSample(3000L, 1.0),
            new FloatSample(4000L, 4.0),
            new FloatSample(5000L, 100.0)
        );
        TimeSeries inputSeries = new TimeSeries(samples, labels, 1000L, 5000L, 1000L, "test");

        // Act
        List<TimeSeries> result = sqrtStage.process(Arrays.asList(inputSeries));

        // Assert all values are non-negative
        for (Sample sample : result.get(0).getSamples()) {
            assertTrue("All sqrt values should be non-negative", sample.getValue() >= 0.0);
        }
    }

    public void testNullInputThrowsException() {
        SqrtStage stage = new SqrtStage();
        TestUtils.assertNullInputThrowsException(stage, "sqrt");
    }

    public void testToXContent() throws Exception {
        SqrtStage stage = new SqrtStage();
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{}", json); // No parameters for sqrt stage
    }

    public void testFromArgsWithEmptyMap() {
        Map<String, Object> args = new HashMap<>();
        SqrtStage stage = SqrtStage.fromArgs(args);

        assertEquals("sqrt", stage.getName());
    }

    public void testFromArgsWithNullMap() {
        SqrtStage stage = SqrtStage.fromArgs(null);

        assertEquals("sqrt", stage.getName());
    }

    public void testCreateWithArgsSqrtStage() {
        // Test creating SqrtStage
        Map<String, Object> args = Map.of();
        PipelineStage stage = PipelineStageFactory.createWithArgs("sqrt", args);

        assertNotNull(stage);
        assertTrue(stage instanceof SqrtStage);
        assertEquals("sqrt", stage.getName());
    }

    public void testEquals() {
        SqrtStage stage1 = new SqrtStage();
        SqrtStage stage2 = new SqrtStage();

        assertEquals(stage1, stage1);

        assertEquals(stage1, stage2);
        assertEquals(stage2, stage1);

        assertNotEquals(stage1, null);

        assertNotEquals(stage1, new Object());
    }

    public void testHashCode() {
        SqrtStage stage1 = new SqrtStage();
        SqrtStage stage2 = new SqrtStage();

        assertEquals(stage1.hashCode(), stage2.hashCode());
    }

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

    @Override
    protected Writeable.Reader instanceReader() {
        return SqrtStage::readFrom;
    }

    @Override
    protected SqrtStage createTestInstance() {
        return new SqrtStage();
    }
}
