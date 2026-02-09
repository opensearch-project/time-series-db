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
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;

public class OffsetStageTests extends AbstractWireSerializingTestCase<OffsetStage> {

    public void testConstructor() {
        // Arrange
        double offset = 10.5;

        // Act
        OffsetStage offsetStage = new OffsetStage(offset);

        // Assert
        assertEquals(10.5, offsetStage.getOffset(), 0.001);
        assertEquals("offset", offsetStage.getName());
    }

    public void testMappingInputOutputPositiveOffset() {
        // Arrange
        OffsetStage offsetStage = new OffsetStage(5.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, 1.0),     // 1 + 5 = 6
            new FloatSample(2000L, -2.0),    // -2 + 5 = 3
            new FloatSample(3000L, 0.0),     // 0 + 5 = 5
            new FloatSample(4000L, 10.5),    // 10.5 + 5 = 15.5
            new FloatSample(5000L, Double.NaN),
            new FloatSample(6000L, Double.POSITIVE_INFINITY),
            new FloatSample(7000L, Double.NEGATIVE_INFINITY)
        );
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 7000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = offsetStage.process(List.of(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        TimeSeries offsetTimeSeries = result.getFirst();

        List<Sample> expectedSamples = Arrays.asList(
            new FloatSample(1000L, 6.0),                        // 1 + 5 = 6
            new FloatSample(2000L, 3.0),                        // -2 + 5 = 3
            new FloatSample(3000L, 5.0),                        // 0 + 5 = 5
            new FloatSample(4000L, 15.5),                       // 10.5 + 5 = 15.5
            new FloatSample(5000L, Double.NaN),                 // NaN + 5 = NaN
            new FloatSample(6000L, Double.POSITIVE_INFINITY),   // +Infinity + 5 = +Infinity
            new FloatSample(7000L, Double.NEGATIVE_INFINITY)    // -Infinity + 5 = -Infinity
        );
        assertSamplesEqual("Positive offset mapping values", expectedSamples, offsetTimeSeries.getSamples(), 1e-10);
    }

    public void testMappingInputOutputNegativeOffset() {
        // Arrange
        OffsetStage offsetStage = new OffsetStage(-3.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, 10.0),    // 10 - 3 = 7
            new FloatSample(2000L, 0.0),     // 0 - 3 = -3
            new FloatSample(3000L, -2.0),    // -2 - 3 = -5
            new FloatSample(4000L, 3.5)      // 3.5 - 3 = 0.5
        );
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 4000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = offsetStage.process(List.of(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        TimeSeries offsetTimeSeries = result.getFirst();

        List<Sample> expectedSamples = Arrays.asList(
            new FloatSample(1000L, 7.0),    // 10 - 3 = 7
            new FloatSample(2000L, -3.0),   // 0 - 3 = -3
            new FloatSample(3000L, -5.0),   // -2 - 3 = -5
            new FloatSample(4000L, 0.5)     // 3.5 - 3 = 0.5
        );
        assertSamplesEqual("Negative offset mapping values", expectedSamples, offsetTimeSeries.getSamples(), 1e-10);
    }

    public void testMappingInputOutputZeroOffset() {
        // Arrange
        OffsetStage offsetStage = new OffsetStage(0.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 5.0), new FloatSample(2000L, -3.0), new FloatSample(3000L, 0.0));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = offsetStage.process(List.of(inputTimeSeries));

        // Assert - values should remain unchanged
        assertEquals(1, result.size());
        TimeSeries offsetTimeSeries = result.getFirst();

        List<Sample> expectedSamples = Arrays.asList(
            new FloatSample(1000L, 5.0),    // 5 + 0 = 5
            new FloatSample(2000L, -3.0),   // -3 + 0 = -3
            new FloatSample(3000L, 0.0)     // 0 + 0 = 0
        );
        assertSamplesEqual("Zero offset mapping values", expectedSamples, offsetTimeSeries.getSamples(), 1e-10);
    }

    public void testProcessWithMultipleTimeSeries() {
        // Arrange
        OffsetStage offsetStage = new OffsetStage(100.0);
        Labels labels1 = ByteLabels.fromMap(Map.of("service", "api"));
        Labels labels2 = ByteLabels.fromMap(Map.of("service", "db"));

        TimeSeries timeSeries1 = new TimeSeries(Arrays.asList(new FloatSample(1000L, 5.0)), labels1, 1000L, 1000L, 1000L, "api-series");
        TimeSeries timeSeries2 = new TimeSeries(Arrays.asList(new FloatSample(2000L, -10.0)), labels2, 2000L, 2000L, 1000L, "db-series");

        // Act
        List<TimeSeries> result = offsetStage.process(Arrays.asList(timeSeries1, timeSeries2));

        // Assert
        assertEquals(2, result.size());
        assertEquals(105.0, result.get(0).getSamples().getSample(0).getValue(), 1e-10);
        assertEquals(90.0, result.get(1).getSamples().getSample(0).getValue(), 1e-10);
        assertEquals(labels1, result.get(0).getLabels());
        assertEquals(labels2, result.get(1).getLabels());
    }

    public void testProcessWithEmptyInput() {
        // Arrange
        OffsetStage offsetStage = new OffsetStage(50.0);

        // Act
        List<TimeSeries> result = offsetStage.process(List.of());

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testSupportConcurrentSegmentSearch() {
        // Arrange
        OffsetStage offsetStage = new OffsetStage(1.0);

        // Act & Assert
        assertTrue(offsetStage.supportConcurrentSegmentSearch());
    }

    public void testGetName() {
        // Arrange
        OffsetStage offsetStage = new OffsetStage(2.0);

        // Act & Assert
        assertEquals("offset", offsetStage.getName());
    }

    public void testReduceMethod() {
        // Test the reduce method from UnaryPipelineStage interface
        OffsetStage stage = new OffsetStage(1.0);

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

        assertTrue("Exception message should contain class name", exception.getMessage().contains("OffsetStage"));
        assertTrue("Exception message should mention reduce function", exception.getMessage().contains("reduce function"));
    }

    public void testLinearTransformationProperty() {
        // Test that offset preserves relative differences
        OffsetStage offsetStage = new OffsetStage(10.0);
        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, 5.0), new FloatSample(3000L, 10.0));
        TimeSeries inputSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test");

        // Act
        List<TimeSeries> result = offsetStage.process(List.of(inputSeries));

        // Assert - relative differences should be preserved
        List<Sample> resultSamples = result.getFirst().getSamples().toList();
        double diff1 = resultSamples.get(1).getValue() - resultSamples.get(0).getValue();
        double diff2 = resultSamples.get(2).getValue() - resultSamples.get(1).getValue();

        assertEquals(4.0, diff1, 1e-10); // Original diff: 5 - 1 = 4
        assertEquals(5.0, diff2, 1e-10); // Original diff: 10 - 5 = 5
    }

    public void testCommutativeProperty() {
        // Test that multiple offset operations can be combined
        OffsetStage offset1 = new OffsetStage(3.0);
        OffsetStage offset2 = new OffsetStage(7.0);
        OffsetStage combinedOffset = new OffsetStage(10.0);

        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        TimeSeries inputSeries = new TimeSeries(List.of(new FloatSample(1000L, 5.0)), labels, 1000L, 1000L, 1000L, "test");

        // Apply offsets separately
        List<TimeSeries> result1 = offset1.process(List.of(inputSeries));
        List<TimeSeries> result2 = offset2.process(result1);

        // Apply combined offset
        List<TimeSeries> combinedResult = combinedOffset.process(List.of(inputSeries));

        // Results should be the same
        assertEquals(
            result2.getFirst().getSamples().getSample(0).getValue(),
            combinedResult.getFirst().getSamples().getSample(0).getValue(),
            1e-10
        );
    }

    public void testNullInputThrowsException() {
        OffsetStage stage = new OffsetStage(1.0);
        TestUtils.assertNullInputThrowsException(stage, "offset");
    }

    public void testToXContent() throws Exception {
        OffsetStage stage = new OffsetStage(42.5);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertTrue("JSON should contain offset value", json.contains("42.5"));
        assertTrue("JSON should contain offset key", json.contains("offset"));
    }

    public void testFromArgs() {
        Map<String, Object> args = Map.of("offset", 123.45);
        OffsetStage stage = OffsetStage.fromArgs(args);

        assertEquals("offset", stage.getName());
        assertEquals(123.45, stage.getOffset(), 1e-10);
    }

    public void testFromArgsWithInteger() {
        Map<String, Object> args = Map.of("offset", 100);
        OffsetStage stage = OffsetStage.fromArgs(args);

        assertEquals("offset", stage.getName());
        assertEquals(100.0, stage.getOffset(), 1e-10);
    }

    public void testCreateWithArgsOffsetStage() {
        // Test creating OffsetStage through factory
        Map<String, Object> args = Map.of("offset", 5.5);
        PipelineStage stage = PipelineStageFactory.createWithArgs("offset", args);

        assertNotNull(stage);
        assertTrue(stage instanceof OffsetStage);
        assertEquals("offset", stage.getName());
        assertEquals(5.5, ((OffsetStage) stage).getOffset(), 1e-10);
    }

    public void testEquals() {
        OffsetStage stage1 = new OffsetStage(2.5);
        OffsetStage stage2 = new OffsetStage(2.5);
        OffsetStage stage3 = new OffsetStage(3.0);

        assertEquals(stage1, stage1);
        assertEquals(stage1, stage2);
        assertEquals(stage2, stage1);

        assertNotEquals(stage1, stage3);
        assertNotEquals(stage1, null);
        assertNotEquals(stage1, new Object());
    }

    public void testHashCode() {
        OffsetStage stage1 = new OffsetStage(2.5);
        OffsetStage stage2 = new OffsetStage(2.5);
        OffsetStage stage3 = new OffsetStage(3.0);

        assertEquals(stage1.hashCode(), stage2.hashCode());
        assertNotEquals(stage1.hashCode(), stage3.hashCode());
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
        return OffsetStage::readFrom;
    }

    @Override
    protected OffsetStage createTestInstance() {
        return new OffsetStage(randomDouble());
    }
}
