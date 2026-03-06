/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;

public class TimestampStageTests extends AbstractWireSerializingTestCase<TimestampStage> {

    public void testProcessWithNullInput() {
        TimestampStage stage = new TimestampStage();
        TestUtils.assertNullInputThrowsException(stage, "timestamp");
    }

    public void testProcessWithEmptyInput() {
        TimestampStage stage = new TimestampStage();
        List<TimeSeries> input = new ArrayList<>();
        List<TimeSeries> result = stage.process(input);
        assertTrue(result.isEmpty());
    }

    public void testProcessWithEmptySamples() {
        TimestampStage stage = new TimestampStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        TimeSeries series = new TimeSeries(new ArrayList<>(), labels, 1000L, 1000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));
        assertEquals(1, result.size());
        assertTrue(result.get(0).getSamples().isEmpty());
        assertEquals(labels, result.get(0).getLabels());
    }

    /**
     * Values are replaced with step start time in seconds (minTimestamp + i * step) / 1000.
     * Input sample values are ignored.
     */
    public void testProcessReplacesValuesWithStepStartInSeconds() {
        TimestampStage stage = new TimestampStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "metric");
        long minTs = 1735696800000L;  // 2025-01-01T02:00:00Z in ms
        long step = 60_000L;           // 1 minute
        List<Sample> samples = List.of(
            new FloatSample(minTs, 42.0),
            new FloatSample(minTs + step, 99.0),
            new FloatSample(minTs + 2 * step, -1.0)
        );
        TimeSeries series = new TimeSeries(samples, labels, minTs, minTs + 2 * step, step, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> expected = List.of(
            new FloatSample(minTs, minTs / 1000.0),
            new FloatSample(minTs + step, (minTs + step) / 1000.0),
            new FloatSample(minTs + 2 * step, (minTs + 2 * step) / 1000.0)
        );
        assertSamplesEqual("Step start in seconds", expected, result.get(0).getSamples());
        assertEquals(labels, result.get(0).getLabels());
        assertEquals(minTs, result.get(0).getMinTimestamp());
        assertEquals(minTs + 2 * step, result.get(0).getMaxTimestamp());
        assertEquals(step, result.get(0).getStep());
    }

    /**
     * Uses series step grid (minTimestamp, step), not actual sample timestamps.
     * So output timestamps and values are minTimestamp + i * step.
     */
    public void testProcessUsesStepGridNotSampleTimestamps() {
        TimestampStage stage = new TimestampStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "metric");
        long minTs = 1000L;
        long step = 500L;
        List<Sample> samples = List.of(new FloatSample(1001L, 1.0), new FloatSample(1502L, 2.0), new FloatSample(2000L, 3.0));
        TimeSeries series = new TimeSeries(samples, labels, minTs, 2000L, step, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());

        List<Sample> expected = List.of(new FloatSample(1000L, 1.0), new FloatSample(1500L, 1.5), new FloatSample(2000L, 2.0));
        assertSamplesEqual("Grid-aligned timestamps", expected, result.get(0).getSamples());
    }

    public void testProcessMultipleSeries() {
        TimestampStage stage = new TimestampStage();
        ByteLabels labels1 = ByteLabels.fromStrings("name", "a");
        ByteLabels labels2 = ByteLabels.fromStrings("name", "b");
        TimeSeries s1 = new TimeSeries(
            List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0)),
            labels1,
            1000L,
            2000L,
            1000L,
            null
        );
        TimeSeries s2 = new TimeSeries(List.of(new FloatSample(5000L, 50.0)), labels2, 5000L, 5000L, 1000L, "alias-b");
        List<TimeSeries> result = stage.process(List.of(s1, s2));

        assertEquals(2, result.size());
        assertSamplesEqual("Series 1", List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0)), result.get(0).getSamples());
        assertEquals("alias-b", result.get(1).getAlias());
        assertSamplesEqual("Series 2", List.of(new FloatSample(5000L, 5.0)), result.get(1).getSamples());
    }

    public void testGetName() {
        TimestampStage stage = new TimestampStage();
        assertEquals("timestamp", stage.getName());
    }

    public void testSupportConcurrentSegmentSearch() {
        TimestampStage stage = new TimestampStage();
        assertTrue(stage.supportConcurrentSegmentSearch());
    }

    public void testIsCoordinatorOnly() {
        TimestampStage stage = new TimestampStage();
        assertFalse(stage.isCoordinatorOnly());
    }

    public void testFromArgs() {
        TimestampStage stage = TimestampStage.fromArgs(Map.of());
        assertNotNull(stage);
        assertEquals("timestamp", stage.getName());

        stage = TimestampStage.fromArgs(Map.of("ignored", "value"));
        assertNotNull(stage);
        assertEquals("timestamp", stage.getName());
    }

    @Override
    protected TimestampStage createTestInstance() {
        return new TimestampStage();
    }

    @Override
    protected Writeable.Reader<TimestampStage> instanceReader() {
        return TimestampStage::readFrom;
    }
}
