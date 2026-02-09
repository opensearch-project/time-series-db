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

public class LogarithmStageTests extends AbstractWireSerializingTestCase<LogarithmStage> {

    public void testDefaultConstructor() {
        // Arrange & Act
        LogarithmStage logarithmStage = new LogarithmStage();

        // Assert
        assertEquals("logarithm", logarithmStage.getName());
    }

    public void testMappingInputOutput() {
        // Arrange
        LogarithmStage logarithmStage = new LogarithmStage();
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, 1.0),     // log10(1) = 0
            new FloatSample(2000L, 10.0),    // log10(10) = 1
            new FloatSample(3000L, 100.0),   // log10(100) = 2
            new FloatSample(4000L, -1.0),    // log10(-1) = NaN
            new FloatSample(5000L, 0.0),     // log10(0) = -Infinity
            new FloatSample(6000L, Double.NaN),
            new FloatSample(7000L, Double.POSITIVE_INFINITY),
            new FloatSample(8000L, 0.1)      // log10(0.1) = -1
        );
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 8000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = logarithmStage.process(List.of(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        TimeSeries logTimeSeries = result.getFirst();

        List<Sample> expectedSamples = Arrays.asList(
            new FloatSample(1000L, 0.0),                        // log10(1) = 0
            new FloatSample(2000L, 1.0),                        // log10(10) = 1
            new FloatSample(3000L, 2.0),                        // log10(100) = 2
            new FloatSample(4000L, Double.NaN),                 // log10(-1) = NaN
            new FloatSample(5000L, Double.NEGATIVE_INFINITY),   // log10(0) = -Infinity
            new FloatSample(6000L, Double.NaN),                 // NaN input = NaN
            new FloatSample(7000L, Double.POSITIVE_INFINITY),   // +Infinity input = +Infinity
            new FloatSample(8000L, -1.0)                        // log10(0.1) = -1
        );
        assertSamplesEqual("Logarithm mapping values", expectedSamples, logTimeSeries.getSamples(), 1e-10);
    }

    public void testLogarithmEdgeCases() {
        // Arrange
        LogarithmStage logarithmStage = new LogarithmStage();
        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, 1.0),           // log10(1) should be 0
            new FloatSample(2000L, 10.0),          // log10(10) should be 1
            new FloatSample(3000L, 0.1),           // log10(0.1) should be -1
            new FloatSample(4000L, -5.0),          // log10(negative) should be NaN
            new FloatSample(5000L, 0.0)            // log10(0) should be -Infinity
        );
        TimeSeries inputSeries = new TimeSeries(samples, labels, 1000L, 5000L, 1000L, "test");

        // Act
        List<TimeSeries> result = logarithmStage.process(List.of(inputSeries));

        // Assert
        List<Sample> expectedSamples = Arrays.asList(
            new FloatSample(1000L, 0.0),                        // log10(1) = 0
            new FloatSample(2000L, 1.0),                        // log10(10) = 1
            new FloatSample(3000L, -1.0),                       // log10(0.1) = -1
            new FloatSample(4000L, Double.NaN),                 // log10(-5) = NaN
            new FloatSample(5000L, Double.NEGATIVE_INFINITY)    // log10(0) = -Infinity
        );
        assertSamplesEqual("Logarithm edge cases", expectedSamples, result.getFirst().getSamples(), 1e-10);
    }

    public void testProcessWithEmptyInput() {
        // Arrange
        LogarithmStage logarithmStage = new LogarithmStage();

        // Act
        List<TimeSeries> result = logarithmStage.process(List.of());

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testSupportConcurrentSegmentSearch() {
        // Arrange
        LogarithmStage logarithmStage = new LogarithmStage();

        // Act & Assert
        assertTrue(logarithmStage.supportConcurrentSegmentSearch());
    }

    public void testGetName() {
        // Arrange
        LogarithmStage logarithmStage = new LogarithmStage();

        // Act & Assert
        assertEquals("logarithm", logarithmStage.getName());
    }

    public void testReduceMethod() {
        // Test the reduce method from UnaryPipelineStage interface
        LogarithmStage stage = new LogarithmStage();

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

        assertTrue("Exception message should contain class name", exception.getMessage().contains("LogarithmStage"));
        assertTrue("Exception message should mention reduce function", exception.getMessage().contains("reduce function"));
    }

    public void testMonotonicProperty() {
        // Test that logarithm preserves order for positive values
        LogarithmStage logarithmStage = new LogarithmStage();
        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 10.0),
            new FloatSample(3000L, 100.0),
            new FloatSample(4000L, 1000.0)
        );
        TimeSeries inputSeries = new TimeSeries(samples, labels, 1000L, 4000L, 1000L, "test");

        // Act
        List<TimeSeries> result = logarithmStage.process(List.of(inputSeries));

        // Assert - logarithm values should be monotonically increasing
        List<Sample> resultSamples = result.getFirst().getSamples().toList();
        for (int i = 1; i < resultSamples.size(); i++) {
            assertTrue("Logarithm should preserve order", resultSamples.get(i).getValue() > resultSamples.get(i - 1).getValue());
        }
    }

    public void testNullInputThrowsException() {
        LogarithmStage stage = new LogarithmStage();
        TestUtils.assertNullInputThrowsException(stage, "logarithm");
    }

    public void testToXContent() throws Exception {
        LogarithmStage stage = new LogarithmStage();
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{}", json); // No parameters for logarithm stage
    }

    public void testFromArgsWithEmptyMap() {
        Map<String, Object> args = new HashMap<>();
        LogarithmStage stage = LogarithmStage.fromArgs(args);

        assertEquals("logarithm", stage.getName());
    }

    public void testFromArgsWithNullMap() {
        LogarithmStage stage = LogarithmStage.fromArgs(null);

        assertEquals("logarithm", stage.getName());
    }

    public void testCreateWithArgsLogarithmStage() {
        // Test creating LogarithmStage
        Map<String, Object> args = Map.of();
        PipelineStage stage = PipelineStageFactory.createWithArgs("logarithm", args);

        assertNotNull(stage);
        assertTrue(stage instanceof LogarithmStage);
        assertEquals("logarithm", stage.getName());
    }

    public void testEquals() {
        LogarithmStage stage1 = new LogarithmStage();
        LogarithmStage stage2 = new LogarithmStage();

        assertEquals(stage1, stage1);

        assertEquals(stage1, stage2);
        assertEquals(stage2, stage1);

        assertNotEquals(stage1, null);

        assertNotEquals(stage1, new Object());
    }

    public void testHashCode() {
        LogarithmStage stage1 = new LogarithmStage();
        LogarithmStage stage2 = new LogarithmStage();

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
        return LogarithmStage::readFrom;
    }

    @Override
    protected LogarithmStage createTestInstance() {
        return new LogarithmStage();
    }
}
