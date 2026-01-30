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
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import static org.opensearch.tsdb.TestUtils.findSeriesByLabel;

/**
 * Unit tests for ScaleToSecondsStage.
 */
public class ScaleToSecondsStageTests extends AbstractWireSerializingTestCase<ScaleToSecondsStage> {

    @Override
    protected Writeable.Reader<ScaleToSecondsStage> instanceReader() {
        return ScaleToSecondsStage::readFrom;
    }

    @Override
    protected ScaleToSecondsStage createTestInstance() {
        return new ScaleToSecondsStage(randomLongBetween(1, 3600));
    }

    public void testScaleToSecondsMultipleSeries() {
        ScaleToSecondsStage stage = new ScaleToSecondsStage(1);

        // Series 1: 10s step (scaleFactor = 1/10 = 0.1)
        List<Sample> samples1 = Arrays.asList(
            new FloatSample(0L, 10.0),
            new FloatSample(10000L, 20.0),
            new FloatSample(20000L, 30.0),
            new FloatSample(30000L, 40.0)
        );
        ByteLabels labels1 = ByteLabels.fromMap(Map.of("id", "series1"));
        TimeSeries series1 = new TimeSeries(samples1, labels1, 0L, 30000L, 10000L, null);

        // Series 2: 20s step (scaleFactor = 1/20 = 0.05)
        List<Sample> samples2 = Arrays.asList(new FloatSample(0L, 100.0), new FloatSample(20000L, 200.0), new FloatSample(40000L, 300.0));
        ByteLabels labels2 = ByteLabels.fromMap(Map.of("id", "series2"));
        TimeSeries series2 = new TimeSeries(samples2, labels2, 0L, 40000L, 20000L, null);

        // Series 3: 10s step with sparse samples (missing data at 10s and 30s)
        List<Sample> samples3 = Arrays.asList(new FloatSample(0L, 50.0), new FloatSample(20000L, 60.0));
        ByteLabels labels3 = ByteLabels.fromMap(Map.of("id", "series3"));
        TimeSeries series3 = new TimeSeries(samples3, labels3, 0L, 30000L, 10000L, null);

        // Series 4: Empty series with 10s step
        List<Sample> samples4 = List.of();
        ByteLabels labels4 = ByteLabels.fromMap(Map.of("id", "series4"));
        TimeSeries series4 = new TimeSeries(samples4, labels4, 0L, 30000L, 10000L, null);

        List<TimeSeries> result = stage.process(Arrays.asList(series1, series2, series3, series4));

        assertEquals(4, result.size());

        // Verify series1: 10s step, scaleFactor = 0.1
        TimeSeries resultSeries1 = findSeriesByLabel(result, "id", "series1");
        List<Sample> expectedSamples1 = Arrays.asList(
            new FloatSample(0L, 1.0),   // 10 * 0.1 = 1
            new FloatSample(10000L, 2.0),   // 20 * 0.1 = 2
            new FloatSample(20000L, 3.0),   // 30 * 0.1 = 3
            new FloatSample(30000L, 4.0)    // 40 * 0.1 = 4
        );
        assertSamplesEqual("Series1 with 10s step", expectedSamples1, resultSeries1.getSamples().toList());

        // Verify series2: 20s step, scaleFactor = 0.05
        TimeSeries resultSeries2 = findSeriesByLabel(result, "id", "series2");
        List<Sample> expectedSamples2 = Arrays.asList(
            new FloatSample(0L, 5.0),       // 100 * 0.05 = 5
            new FloatSample(20000L, 10.0),  // 200 * 0.05 = 10
            new FloatSample(40000L, 15.0)   // 300 * 0.05 = 15
        );
        assertSamplesEqual("Series2 with 20s step", expectedSamples2, resultSeries2.getSamples().toList());

        // Verify series3: sparse samples with 10s step
        TimeSeries resultSeries3 = findSeriesByLabel(result, "id", "series3");
        List<Sample> expectedSamples3 = Arrays.asList(
            new FloatSample(0L, 5.0),       // 50 * 0.1 = 5
            new FloatSample(20000L, 6.0)    // 60 * 0.1 = 6
        );
        assertSamplesEqual("Series3 with sparse samples", expectedSamples3, resultSeries3.getSamples().toList());

        // Verify series4: empty series
        TimeSeries resultSeries4 = findSeriesByLabel(result, "id", "series4");
        assertTrue("Series4 should be empty", resultSeries4.getSamples().isEmpty());
    }

    public void testScaleToSecondsEmptyInput() {
        ScaleToSecondsStage stage = new ScaleToSecondsStage(5);
        List<TimeSeries> result = stage.process(List.of());
        assertTrue(result.isEmpty());
    }

    public void testScaleToSecondsNullInput() {
        ScaleToSecondsStage stage = new ScaleToSecondsStage(5);
        TestUtils.assertNullInputThrowsException(stage, "scale_to_seconds");
    }

    public void testConstructorInvalidSeconds() {
        expectThrows(IllegalArgumentException.class, () -> new ScaleToSecondsStage(0));
        expectThrows(IllegalArgumentException.class, () -> new ScaleToSecondsStage(-5));
    }

    public void testGetName() {
        ScaleToSecondsStage stage = new ScaleToSecondsStage(10);
        assertEquals("scale_to_seconds", stage.getName());
    }

    public void testFromArgs() {
        Map<String, Object> args = Map.of("seconds", 10L);
        ScaleToSecondsStage stage = ScaleToSecondsStage.fromArgs(args);
        assertNotNull(stage);
    }

    public void testFromArgsNullArgs() {
        expectThrows(IllegalArgumentException.class, () -> ScaleToSecondsStage.fromArgs(null));
    }

    public void testToXContent() throws Exception {
        ScaleToSecondsStage stage = new ScaleToSecondsStage(5);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"seconds\":5.0}", json);
    }

    public void testEquals() {
        ScaleToSecondsStage stage1 = new ScaleToSecondsStage(10);
        ScaleToSecondsStage stage2 = new ScaleToSecondsStage(10);
        ScaleToSecondsStage stage3 = new ScaleToSecondsStage(20);

        assertEquals(stage1, stage2);
        assertNotEquals(stage1, stage3);
        assertNotEquals(stage1, null);
        assertNotEquals(stage1, "not a stage");
    }

    public void testHashCode() {
        ScaleToSecondsStage stage1 = new ScaleToSecondsStage(10);
        ScaleToSecondsStage stage2 = new ScaleToSecondsStage(10);
        ScaleToSecondsStage stage3 = new ScaleToSecondsStage(20);

        assertEquals(stage1.hashCode(), stage2.hashCode());
        assertNotEquals(stage1.hashCode(), stage3.hashCode());
    }

    public void testSupportConcurrentSegmentSearch() {
        ScaleToSecondsStage stage = new ScaleToSecondsStage(5);
        assertTrue(stage.supportConcurrentSegmentSearch());
    }
}
