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
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;

public class RemoveEmptyStageTests extends AbstractWireSerializingTestCase<RemoveEmptyStage> {

    public void testDefaultConstructor() {
        // Arrange & Act
        RemoveEmptyStage removeEmptyStage = new RemoveEmptyStage();

        // Assert
        assertEquals("remove_empty", removeEmptyStage.getName());
    }

    public void testMappingInputOutput() {
        // Arrange
        RemoveEmptyStage removeEmptyStage = new RemoveEmptyStage();
        Labels labels = ByteLabels.fromStrings("service", "api");
        List<Sample> samples1 = Arrays.asList(new FloatSample(1000L, 2.0));
        List<Sample> samples2 = Collections.emptyList();
        TimeSeries inputTimeSeries1 = new TimeSeries(samples1, labels, 1000L, 8000L, 1000L, "test-series");
        TimeSeries inputTimeSeries2 = new TimeSeries(samples2, labels, 1000L, 8000L, 1000L, "test-series");

        // Act
        List<TimeSeries> firstResult = removeEmptyStage.process(List.of(inputTimeSeries1, inputTimeSeries2));

        // Assert
        assertEquals(1, firstResult.size());
        TimeSeries timeSeries = firstResult.getFirst();
        assertEquals(1, timeSeries.getSamples().size());
        assertEquals(2.0, timeSeries.getSamples().getValue(0), 0.0);

        // Test Idempotent
        // Act
        List<TimeSeries> secondResult = removeEmptyStage.process(List.of(inputTimeSeries1, inputTimeSeries2));

        // Assert
        assertEquals(1, secondResult.size());
        timeSeries = secondResult.getFirst();
        assertEquals(1, timeSeries.getSamples().size());
        assertEquals(2.0, timeSeries.getSamples().getValue(0), 0.0);

    }

    public void testRemoveSeriesWithAllNaNValues() {
        // Arrange
        RemoveEmptyStage removeEmptyStage = new RemoveEmptyStage();
        Labels labels = ByteLabels.fromStrings("service", "api");

        // Series with all NaN values
        List<Sample> allNaNSamples = Arrays.asList(
            new FloatSample(1000L, Double.NaN),
            new FloatSample(2000L, Double.NaN),
            new FloatSample(3000L, Double.NaN)
        );
        TimeSeries allNaNSeries = new TimeSeries(allNaNSamples, labels, 1000L, 3000L, 1000L, "all-nan-series");

        // Series with some valid values
        List<Sample> validSamples = Arrays.asList(
            new FloatSample(1000L, 2.0),
            new FloatSample(2000L, Double.NaN),
            new FloatSample(3000L, 4.0)
        );
        TimeSeries validSeries = new TimeSeries(validSamples, labels, 1000L, 3000L, 1000L, "valid-series");

        // Series with empty samples
        TimeSeries emptySeries = new TimeSeries(Collections.emptyList(), labels, 1000L, 3000L, 1000L, "empty-series");

        // Act
        List<TimeSeries> result = removeEmptyStage.process(List.of(allNaNSeries, validSeries, emptySeries));

        // Assert - only the valid series should remain
        assertEquals(1, result.size());
        TimeSeries remainingSeries = result.get(0);
        assertEquals("valid-series", remainingSeries.getAlias());
        List<Sample> expectedSamples = Arrays.asList(
            new FloatSample(1000L, 2.0),
            new FloatSample(2000L, Double.NaN),
            new FloatSample(3000L, 4.0)
        );
        assertSamplesEqual("Remaining series should contain valid samples", expectedSamples, remainingSeries.getSamples().toList(), 0.001);
    }

    public void testProcessWithEmptyInput() {
        // Arrange
        RemoveEmptyStage removeEmptyStage = new RemoveEmptyStage();

        // Act
        List<TimeSeries> result = removeEmptyStage.process(List.of());

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testSupportConcurrentSegmentSearch() {
        // Arrange
        RemoveEmptyStage removeEmptyStage = new RemoveEmptyStage();

        // Act & Assert
        assertFalse(removeEmptyStage.supportConcurrentSegmentSearch());
    }

    public void testGetName() {
        // Arrange
        RemoveEmptyStage removeEmptyStage = new RemoveEmptyStage();

        // Act & Assert
        assertEquals("remove_empty", removeEmptyStage.getName());
    }

    public void testNullInputThrowsException() {
        RemoveEmptyStage stage = new RemoveEmptyStage();
        TestUtils.assertNullInputThrowsException(stage, "remove_empty");
    }

    public void testToXContent() throws Exception {
        RemoveEmptyStage stage = new RemoveEmptyStage();
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{}", json); // No parameters for removeEmpty stage
    }

    public void testFromArgsWithEmptyMap() {
        Map<String, Object> args = new HashMap<>();
        RemoveEmptyStage stage = RemoveEmptyStage.fromArgs(args);

        assertEquals("remove_empty", stage.getName());
    }

    public void testFromArgsWithNullMap() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> RemoveEmptyStage.fromArgs(null));

        assertEquals("Args cannot be null", exception.getMessage());
    }

    public void testCreateWithArgsRemoveEmptyStage() {
        // Test creating removeEmptyStage
        Map<String, Object> args = Map.of();
        PipelineStage stage = PipelineStageFactory.createWithArgs("remove_empty", args);

        assertNotNull(stage);
        assertTrue(stage instanceof RemoveEmptyStage);
        assertEquals("remove_empty", stage.getName());
    }

    public void testEquals() {
        RemoveEmptyStage stage1 = new RemoveEmptyStage();
        RemoveEmptyStage stage2 = new RemoveEmptyStage();

        assertEquals(stage1, stage1);

        assertEquals(stage1, stage2);
        assertEquals(stage2, stage1);

        assertNotEquals(stage1, null);

        assertNotEquals(stage1, new Object());
    }

    public void testHashCode() {
        RemoveEmptyStage stage1 = new RemoveEmptyStage();
        RemoveEmptyStage stage2 = new RemoveEmptyStage();

        assertEquals(stage1.hashCode(), stage2.hashCode());
    }

    @Override
    protected Writeable.Reader instanceReader() {
        return RemoveEmptyStage::readFrom;
    }

    @Override
    protected RemoveEmptyStage createTestInstance() {
        return new RemoveEmptyStage();
    }
}
