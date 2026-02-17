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

public class AbsStageTests extends AbstractWireSerializingTestCase<AbsStage> {

    public void testDefaultConstructor() {
        // Arrange & Act
        AbsStage absStage = new AbsStage();

        // Assert
        assertEquals("abs", absStage.getName());
    }

    public void testMappingInputOutput() {
        // Arrange
        AbsStage absStage = new AbsStage();
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, -1.5),
            new FloatSample(2000L, 2.3),
            new FloatSample(3000L, Double.NaN),
            new FloatSample(4000L, Double.NEGATIVE_INFINITY),
            new FloatSample(5000L, Double.POSITIVE_INFINITY),
            new FloatSample(6000L, Double.MIN_VALUE),
            new FloatSample(7000L, Double.MAX_VALUE),
            new FloatSample(8000L, 0.0)
        );
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 8000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = absStage.process(List.of(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        TimeSeries absTimeSeries = result.getFirst();
        assertEquals(8, absTimeSeries.getSamples().size());
        assertEquals(1.5, absTimeSeries.getSamples().getValue(0), 0.0);
        assertEquals(2.3, absTimeSeries.getSamples().getValue(1), 0.0);
        assertEquals(Double.NaN, absTimeSeries.getSamples().getValue(2), 0.0);
        assertEquals(Double.POSITIVE_INFINITY, absTimeSeries.getSamples().getValue(3), 0.0);
        assertEquals(Double.POSITIVE_INFINITY, absTimeSeries.getSamples().getValue(4), 0.0);
        assertEquals(Double.MIN_VALUE, absTimeSeries.getSamples().getValue(5), 0.0);
        assertEquals(Double.MAX_VALUE, absTimeSeries.getSamples().getValue(6), 0.0);
        assertEquals(0.0, absTimeSeries.getSamples().getValue(7), 0.0);
    }

    public void testProcessWithEmptyInput() {
        // Arrange
        AbsStage absStage = new AbsStage();

        // Act
        List<TimeSeries> result = absStage.process(List.of());

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testSupportConcurrentSegmentSearch() {
        // Arrange
        AbsStage absStage = new AbsStage();

        // Act & Assert
        assertTrue(absStage.supportConcurrentSegmentSearch());
    }

    public void testGetName() {
        // Arrange
        AbsStage absStage = new AbsStage();

        // Act & Assert
        assertEquals("abs", absStage.getName());
    }

    public void testReduceMethod() {
        // Test the reduce method from UnaryPipelineStage interface
        AbsStage stage = new AbsStage();

        // Create mock TimeSeriesProvider instances
        List<TimeSeriesProvider> aggregations = Arrays.asList(
            createMockTimeSeriesProvider("provider1"),
            createMockTimeSeriesProvider("provider2")
        );

        // Test the reduce method - should throw UnsupportedOperationException for unary stages
        UnsupportedOperationException exception = assertThrows(
            UnsupportedOperationException.class,
            () -> stage.reduce(aggregations, false, null)
        );

        assertTrue("Exception message should contain class name", exception.getMessage().contains("AbsStage"));
        assertTrue("Exception message should mention reduce function", exception.getMessage().contains("reduce function"));
    }

    public void testIdempotentProperty() {
        // Test that applying abs multiple times has same effect
        AbsStage absStage = new AbsStage();
        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        TimeSeries inputSeries = new TimeSeries(List.of(new FloatSample(1000L, -3.14159)), labels, 1000L, 1000L, 1000L, "test");

        // Apply abs once
        List<TimeSeries> firstAbs = absStage.process(List.of(inputSeries));

        // Apply abs again to the result
        List<TimeSeries> secondAbs = absStage.process(firstAbs);

        // Results should be identical
        assertEquals(firstAbs.getFirst().getSamples().getValue(0), secondAbs.getFirst().getSamples().getValue(0), 0.000);
    }

    public void testNonNegativeOutput() {
        // Test that all outputs are non-negative
        AbsStage absStage = new AbsStage();
        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, -5.0),
            new FloatSample(2000L, -0.1),
            new FloatSample(3000L, 0.0),
            new FloatSample(4000L, 2.5)
        );
        TimeSeries inputSeries = new TimeSeries(samples, labels, 1000L, 4000L, 1000L, "test");

        // Act
        List<TimeSeries> result = absStage.process(Arrays.asList(inputSeries));

        // Assert all values are non-negative
        for (Sample sample : result.get(0).getSamples()) {
            assertTrue("All values should be non-negative", sample.getValue() >= 0.0);
        }
    }

    public void testNullInputThrowsException() {
        AbsStage stage = new AbsStage();
        TestUtils.assertNullInputThrowsException(stage, "abs");
    }

    public void testToXContent() throws Exception {
        AbsStage stage = new AbsStage();
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{}", json); // No parameters for abs stage
    }

    public void testFromArgsWithEmptyMap() {
        Map<String, Object> args = new HashMap<>();
        AbsStage stage = AbsStage.fromArgs(args);

        assertEquals("abs", stage.getName());
    }

    public void testFromArgsWithNullMap() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> AbsStage.fromArgs(null));

        assertEquals("Args cannot be null", exception.getMessage());
    }

    public void testCreateWithArgsAbsStage() {
        // Test creating AbsStage
        Map<String, Object> args = Map.of();
        PipelineStage stage = PipelineStageFactory.createWithArgs("abs", args);

        assertNotNull(stage);
        assertTrue(stage instanceof AbsStage);
        assertEquals("abs", stage.getName());
    }

    public void testEquals() {
        AbsStage stage1 = new AbsStage();
        AbsStage stage2 = new AbsStage();

        assertEquals(stage1, stage1);

        assertEquals(stage1, stage2);
        assertEquals(stage2, stage1);

        assertNotEquals(stage1, null);

        assertNotEquals(stage1, new Object());
    }

    public void testHashCode() {
        AbsStage stage1 = new AbsStage();
        AbsStage stage2 = new AbsStage();

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
        return AbsStage::readFrom;
    }

    @Override
    protected AbsStage createTestInstance() {
        return new AbsStage();
    }
}
