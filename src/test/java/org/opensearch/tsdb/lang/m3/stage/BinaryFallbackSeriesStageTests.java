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
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;

public class BinaryFallbackSeriesStageTests extends AbstractWireSerializingTestCase<FallbackSeriesBinaryStage> {

    /**
     * Test that when left series is non-empty, it is returned.
     */
    public void testNonEmptyLeftReturnsLeft() {
        FallbackSeriesBinaryStage stage = new FallbackSeriesBinaryStage("fallback_ref");

        List<Sample> leftSamples = List.of(new FloatSample(0L, 1.0), new FloatSample(10L, 2.0), new FloatSample(20L, 3.0));

        List<Sample> rightSamples = List.of(new FloatSample(0L, 10.0), new FloatSample(10L, 20.0), new FloatSample(20L, 30.0));

        ByteLabels leftLabels = ByteLabels.fromStrings("name", "left");
        ByteLabels rightLabels = ByteLabels.fromStrings("name", "right");

        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 0L, 20L, 10L, null);
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 0L, 20L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(leftSeries), List.of(rightSeries));

        assertEquals(1, result.size());
        assertEquals("left", result.get(0).getLabels().get("name"));
        assertSamplesEqual("Left series samples should match", leftSamples, result.get(0).getSamples().toList());
    }

    /**
     * Test that when left series is empty, right series is returned.
     */
    public void testEmptyLeftReturnsRight() {
        FallbackSeriesBinaryStage stage = new FallbackSeriesBinaryStage("fallback_ref");

        List<Sample> rightSamples = List.of(new FloatSample(0L, 10.0), new FloatSample(10L, 20.0), new FloatSample(20L, 30.0));

        ByteLabels rightLabels = ByteLabels.fromStrings("name", "fallback");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 0L, 20L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(), List.of(rightSeries));

        assertEquals(1, result.size());
        assertEquals("fallback", result.get(0).getLabels().get("name"));
        assertSamplesEqual("Right series samples should match", rightSamples, result.get(0).getSamples().toList());
    }

    /**
     * Test that null inputs throw exceptions.
     */
    public void testNullInputsThrowExceptions() {
        FallbackSeriesBinaryStage stage = new FallbackSeriesBinaryStage("fallback_ref");

        List<Sample> samples = List.of(new FloatSample(0L, 5.0), new FloatSample(10L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        TimeSeries series = new TimeSeries(samples, labels, 0L, 10L, 10L, null);

        TestUtils.assertBinaryNullInputsThrowExceptions(stage, List.of(series), "fallback_series_binary");
    }

    /**
     * Test that when left has multiple series, all are returned.
     */
    public void testMultipleLeftSeriesReturnsAll() {
        FallbackSeriesBinaryStage stage = new FallbackSeriesBinaryStage("fallback_ref");

        List<Sample> samples1 = List.of(new FloatSample(0L, 1.0));
        List<Sample> samples2 = List.of(new FloatSample(0L, 2.0));
        List<Sample> rightSamples = List.of(new FloatSample(0L, 10.0));

        TimeSeries series1 = new TimeSeries(samples1, ByteLabels.fromStrings("id", "1"), 0L, 0L, 10L, null);
        TimeSeries series2 = new TimeSeries(samples2, ByteLabels.fromStrings("id", "2"), 0L, 0L, 10L, null);
        TimeSeries rightSeries = new TimeSeries(rightSamples, ByteLabels.fromStrings("id", "fallback"), 0L, 0L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(series1, series2), List.of(rightSeries));

        assertEquals(2, result.size());
    }

    /**
     * Test getRightOpReferenceName returns the correct reference.
     */
    public void testGetRightOpReferenceName() {
        String refName = "test_reference";
        FallbackSeriesBinaryStage stage = new FallbackSeriesBinaryStage(refName);
        assertEquals(refName, stage.getRightOpReferenceName());
    }

    /**
     * Test getName returns the correct name.
     */
    public void testGetName() {
        FallbackSeriesBinaryStage stage = new FallbackSeriesBinaryStage("ref");
        assertEquals(FallbackSeriesBinaryStage.NAME, stage.getName());
    }

    /**
     * Test XContent serialization.
     */
    public void testToXContent() throws IOException {
        FallbackSeriesBinaryStage stage = new FallbackSeriesBinaryStage("test_ref");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stage.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();

        assertTrue(json.contains("\"right_op_reference\":\"test_ref\""));
    }

    /**
     * Test fromArgs with valid arguments.
     */
    public void testFromArgs() {
        Map<String, Object> args = Map.of("right_op_reference", "test_ref");
        FallbackSeriesBinaryStage stage = FallbackSeriesBinaryStage.fromArgs(args);

        assertNotNull(stage);
        assertEquals("test_ref", stage.getRightOpReferenceName());
    }

    /**
     * Test fromArgs rejects missing right_op_reference.
     */
    public void testFromArgsMissingRightOpReference() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> FallbackSeriesBinaryStage.fromArgs(Map.of())
        );
        assertTrue(exception.getMessage().contains("right_op_reference"));
    }

    /**
     * Test PipelineStageFactory can create FallbackSeriesBinaryStage.
     */
    public void testPipelineStageFactory() {
        Map<String, Object> args = Map.of("right_op_reference", "test_ref");
        PipelineStage stage = PipelineStageFactory.createWithArgs(FallbackSeriesBinaryStage.NAME, args);

        assertNotNull(stage);
        assertTrue(stage instanceof FallbackSeriesBinaryStage);
        assertEquals("test_ref", ((FallbackSeriesBinaryStage) stage).getRightOpReferenceName());
    }

    @Override
    protected Writeable.Reader<FallbackSeriesBinaryStage> instanceReader() {
        return FallbackSeriesBinaryStage::readFrom;
    }

    @Override
    protected FallbackSeriesBinaryStage createTestInstance() {
        return new FallbackSeriesBinaryStage("test_ref_" + randomInt());
    }

    @Override
    protected FallbackSeriesBinaryStage mutateInstance(FallbackSeriesBinaryStage instance) {
        return new FallbackSeriesBinaryStage(instance.getRightOpReferenceName() + "_mutated");
    }
}
