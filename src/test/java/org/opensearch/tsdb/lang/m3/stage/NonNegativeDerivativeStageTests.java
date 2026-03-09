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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;

public class NonNegativeDerivativeStageTests extends OpenSearchTestCase {

    public void testProcessWithNullInput() {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage();
        TestUtils.assertNullInputThrowsException(stage, "non_negative_derivative");
    }

    public void testProcessWithEmptyInput() {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage();
        List<TimeSeries> result = stage.process(new ArrayList<>());
        assertTrue(result.isEmpty());
    }

    public void testProcessIncreasingValues() {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "counter");
        List<Sample> samples = List.of(
            new FloatSample(1000L, 10.0),
            new FloatSample(2000L, 30.0),
            new FloatSample(3000L, 40.0),
            new FloatSample(4000L, 50.0)
        );
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 4000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> expected = List.of(new FloatSample(2000L, 20.0), new FloatSample(3000L, 10.0), new FloatSample(4000L, 10.0));
        assertSamplesEqual("Increasing", expected, result.get(0).getSamples().toList());
    }

    public void testProcessDecreasingWithoutMaxValue() {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "counter");
        List<Sample> samples = List.of(new FloatSample(1000L, 50.0), new FloatSample(2000L, 30.0), new FloatSample(3000L, 40.0));
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> out = result.get(0).getSamples().toList();
        assertEquals(1, out.size());
        assertEquals(10.0, out.get(0).getValue(), 0.001);
    }

    public void testProcessCounterWrapWithMaxValue() {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage(100.0);
        ByteLabels labels = ByteLabels.fromStrings("name", "counter");
        List<Sample> samples = List.of(new FloatSample(1000L, 98.0), new FloatSample(2000L, 5.0), new FloatSample(3000L, 15.0));
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> out = result.get(0).getSamples().toList();
        assertEquals(2, out.size());
        assertEquals(8.0, out.get(0).getValue(), 0.001);
        assertEquals(10.0, out.get(1).getValue(), 0.001);
    }

    public void testProcessSkipsGapInTimestamps() {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "x");
        long step = 1000L;
        List<Sample> samples = List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 3.0), new FloatSample(4000L, 7.0));
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 4000L, step, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertEquals(2000L, result.get(0).getSamples().getTimestamp(0));
        assertEquals(2.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testProcessNaNPreviousValue() {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "x");
        List<Sample> samples = List.of(new FloatSample(1000L, Double.NaN), new FloatSample(2000L, 5.0), new FloatSample(3000L, 8.0));
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> out = result.get(0).getSamples().toList();
        assertEquals(1, out.size());
        assertEquals(3.0, out.get(0).getValue(), 0.001);
    }

    /** Current value NaN: sample is skipped. */
    public void testProcessNaNCurrentValue() {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "x");
        List<Sample> samples = List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, Double.NaN), new FloatSample(3000L, 9.0));
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));
        assertEquals(1, result.size());
        List<Sample> out = result.get(0).getSamples().toList();
        assertEquals(0, out.size());
    }

    public void testFromArgsNoMaxValue() {
        NonNegativeDerivativeStage stage = NonNegativeDerivativeStage.fromArgs(Map.of());
        assertFalse(stage.hasMaxValue());
        assertTrue(Double.isNaN(stage.getMaxValue()));
    }

    public void testFromArgsWithMaxValue() {
        NonNegativeDerivativeStage stage = NonNegativeDerivativeStage.fromArgs(Map.of("max_value", 255.0));
        assertTrue(stage.hasMaxValue());
        assertEquals(255.0, stage.getMaxValue(), 0.001);
    }

    public void testFromArgsNullThrows() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> NonNegativeDerivativeStage.fromArgs(null));
        assertTrue(e.getMessage().contains("Args cannot be null"));
    }

    public void testSingleSample() {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "x");
        TimeSeries series = new TimeSeries(List.of(new FloatSample(1000L, 1.0)), labels, 1000L, 1000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).getSamples().size());
    }

    /** Series with empty samples: pass through unchanged. */
    public void testProcessWithEmptySamples() {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "x");
        TimeSeries series = new TimeSeries(new ArrayList<>(), labels, 1000L, 1000L, 1000L, "alias");
        List<TimeSeries> result = stage.process(List.of(series));
        assertEquals(1, result.size());
        assertTrue(result.get(0).getSamples().isEmpty());
        assertEquals("alias", result.get(0).getAlias());
    }

    /** Negative diff with maxValue set but maxValue < currentValue: sample is skipped. */
    public void testProcessNegativeDiffMaxValueBelowCurrent() {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage(10.0);
        ByteLabels labels = ByteLabels.fromStrings("name", "c");
        List<Sample> samples = List.of(new FloatSample(1000L, 50.0), new FloatSample(2000L, 20.0));
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));
        assertEquals(1, result.size());
        assertEquals(0, result.get(0).getSamples().size());
    }

    /** fromArgs with string value uses Double.parseDouble. */
    public void testFromArgsWithMaxValueAsString() {
        NonNegativeDerivativeStage stage = NonNegativeDerivativeStage.fromArgs(Map.of("max_value", "100"));
        assertTrue(stage.hasMaxValue());
        assertEquals(100.0, stage.getMaxValue(), 0.001);
    }

    public void testToXContentWithMaxValue() throws IOException {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage(50.0);
        try (XContentBuilder builder = org.opensearch.common.xcontent.json.JsonXContent.contentBuilder()) {
            builder.startObject();
            stage.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }
    }

    public void testToXContentWithoutMaxValue() throws IOException {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage();
        try (XContentBuilder builder = org.opensearch.common.xcontent.json.JsonXContent.contentBuilder()) {
            builder.startObject();
            stage.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }
    }

    public void testEstimateMemoryOverhead() {
        NonNegativeDerivativeStage stage = new NonNegativeDerivativeStage();
        assertEquals(0L, stage.estimateMemoryOverhead(null));
        assertEquals(0L, stage.estimateMemoryOverhead(List.of()));
        ByteLabels labels = ByteLabels.fromStrings("name", "x");
        TimeSeries ts = new TimeSeries(
            List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0)),
            labels,
            1000L,
            2000L,
            1000L,
            null
        );
        assertTrue(stage.estimateMemoryOverhead(List.of(ts)) > 0);
    }

    public void testEqualsAndHashCode() {
        NonNegativeDerivativeStage a = new NonNegativeDerivativeStage(10.0);
        NonNegativeDerivativeStage same = new NonNegativeDerivativeStage(10.0);
        NonNegativeDerivativeStage other = new NonNegativeDerivativeStage(20.0);

        assertTrue(a.equals(a));
        assertTrue(a.equals(same));
        assertFalse(a.equals(null));
        assertFalse(a.equals("not a stage"));
        assertFalse(a.equals(other));
        assertEquals(a.hashCode(), same.hashCode());
        assertTrue(a.hashCode() != other.hashCode());
    }

    public void testSerializationRoundtrip() throws IOException {
        NonNegativeDerivativeStage original = new NonNegativeDerivativeStage(123.45);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                NonNegativeDerivativeStage deserialized = NonNegativeDerivativeStage.readFrom(in);
                assertEquals(original, deserialized);
                assertEquals(123.45, deserialized.getMaxValue(), 0.001);
            }
        }
    }
}
