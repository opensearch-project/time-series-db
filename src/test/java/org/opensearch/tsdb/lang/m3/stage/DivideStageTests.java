/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DivideStageTests extends AbstractWireSerializingTestCase<DivideStage> {

    public void testSingleRightSeries() {
        DivideStage stage = new DivideStage("right_series");

        // Left series with normal values, NaN, and mismatched timestamps
        List<Sample> leftSamples = Arrays.asList(
            new FloatSample(1000L, 100.0),     // normal division
            new FloatSample(2000L, 200.0),     // divide by zero -> NaN
            new FloatSample(3000L, Float.NaN), // NaN numerator -> NaN
            new FloatSample(4000L, 400.0)      // missing in right series
        );
        ByteLabels leftLabels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 4000L, 1000L, "api-series");

        // Right series with normal values, zero, NaN, and mismatched timestamps
        List<Sample> rightSamples = Arrays.asList(
            new FloatSample(1000L, 10.0),      // normal division
            new FloatSample(2000L, 0.0),       // zero denominator -> NaN
            new FloatSample(3000L, 30.0),      // NaN numerator -> NaN
            new FloatSample(5000L, Float.NaN)  // not in left series
        );
        ByteLabels rightLabels = ByteLabels.fromMap(Map.of("service", "total"));
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 5000L, 1000L, "total-series");

        List<TimeSeries> result = stage.process(Arrays.asList(leftSeries), Arrays.asList(rightSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        // After pairwise normalization, NaN values are skipped, so we only get non-NaN buckets
        // Left series has NaN at 3000L, so that timestamp is skipped after normalization
        assertEquals(2, resultSeries.getSamples().size());

        List<Sample> samples = resultSeries.getSamples().toList();

        // 1000L: 100.0/10.0 = 10.0
        assertEquals(1000L, samples.get(0).getTimestamp());
        assertEquals(10.0, samples.get(0).getValue(), 0.001);

        // 2000L: 200.0/0.0 = NaN (zero denominator)
        // Note: divide by zero produces NaN in the operation, not from normalization
        assertEquals(2000L, samples.get(1).getTimestamp());
        assertTrue("Should be NaN when denominator is zero", Double.isNaN(samples.get(1).getValue()));
    }

    public void testMultipleRightSeries() {
        DivideStage stage = new DivideStage("right_series");

        // Left series with normal values and NaN
        List<Sample> leftSamples = Arrays.asList(
            new FloatSample(1000L, 500.0),      // normal division
            new FloatSample(2000L, 1000.0),     // divide by zero -> NaN
            new FloatSample(3000L, Float.NaN),  // NaN numerator -> NaN
            new FloatSample(4000L, 2000.0)      // missing in right series
        );
        ByteLabels leftLabels = ByteLabels.fromMap(Map.of("service", "api", "instance", "server1"));
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 4000L, 1000L, "api-series");

        // Right series 1: matching labels with normal values, zero, and NaN
        List<Sample> rightSamples1 = Arrays.asList(
            new FloatSample(1000L, 100.0),      // normal division
            new FloatSample(2000L, 0.0),        // zero denominator -> NaN
            new FloatSample(3000L, 300.0),      // NaN numerator -> NaN
            new FloatSample(5000L, Float.NaN)   // not in left series
        );
        ByteLabels rightLabels1 = ByteLabels.fromMap(Map.of("service", "api", "instance", "server1"));
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 5000L, 1000L, "right-series-1");

        // Right series 2: non-matching labels (should be ignored)
        List<Sample> rightSamples2 = Arrays.asList(new FloatSample(1000L, 500.0));
        ByteLabels rightLabels2 = ByteLabels.fromMap(Map.of("service", "db", "instance", "server2"));
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 1000L, 1000L, "right-series-2");

        List<TimeSeries> result = stage.process(Arrays.asList(leftSeries), Arrays.asList(rightSeries1, rightSeries2));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        // After pairwise normalization, NaN values are skipped
        // Left series has NaN at 3000L, so that timestamp is skipped after normalization
        assertEquals(2, resultSeries.getSamples().size());

        // Verify that result has the original labels plus the type:ratios label
        assertEquals("ratios", resultSeries.getLabels().get("type"));
        assertEquals("api", resultSeries.getLabels().get("service"));
        assertEquals("server1", resultSeries.getLabels().get("instance"));

        List<Sample> samples = resultSeries.getSamples().toList();

        // 1000L: 500.0/100.0 = 5.0
        assertEquals(1000L, samples.get(0).getTimestamp());
        assertEquals(5.0, samples.get(0).getValue(), 0.001);

        // 2000L: 1000.0/0.0 = NaN (zero denominator)
        // Note: divide by zero produces NaN in the operation, not from normalization
        assertEquals(2000L, samples.get(1).getTimestamp());
        assertTrue("Should be NaN when denominator is zero", Double.isNaN(samples.get(1).getValue()));
    }

    public void testSelectiveLabelMatching() {
        // Test selective label matching with specific label keys
        List<String> labelKeys = Arrays.asList("service");
        DivideStage stage = new DivideStage("right_series", labelKeys);

        List<Sample> leftSamples = Arrays.asList(new FloatSample(1000L, 500.0));
        ByteLabels leftLabels = ByteLabels.fromMap(Map.of("service", "api", "instance", "server1", "region", "us-east"));
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 1000L, 1000L, "left-series");

        // Right series 1: matches on "service" label only
        List<Sample> rightSamples1 = Arrays.asList(new FloatSample(1000L, 100.0));
        ByteLabels rightLabels1 = ByteLabels.fromMap(Map.of("service", "api", "instance", "server2", "region", "us-west"));
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 1000L, 1000L, "right-series-1");

        // Right series 2: different "service" label (should not match)
        List<Sample> rightSamples2 = Arrays.asList(new FloatSample(1000L, 200.0));
        ByteLabels rightLabels2 = ByteLabels.fromMap(Map.of("service", "db", "instance", "server1", "region", "us-east"));
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 1000L, 1000L, "right-series-2");

        List<TimeSeries> result = stage.process(Arrays.asList(leftSeries), Arrays.asList(rightSeries1, rightSeries2));

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertEquals(5.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testSingleRightSeriesWithLabelMatching() {
        // When labelKeys are specified, if it is a single right series, we ignore labels
        List<String> labelKeys = Arrays.asList("service");
        DivideStage stage = new DivideStage("right_series", labelKeys);

        List<Sample> leftSamples1 = Arrays.asList(new FloatSample(1000L, 250.0));
        ByteLabels leftLabels1 = ByteLabels.fromMap(Map.of("service", "api", "instance", "server1"));
        TimeSeries leftSeries1 = new TimeSeries(leftSamples1, leftLabels1, 1000L, 1000L, 1000L, "left-series-1");

        List<Sample> leftSamples2 = Arrays.asList(new FloatSample(1000L, 500.0));
        ByteLabels leftLabels2 = ByteLabels.fromMap(Map.of("service", "db", "instance", "server2"));
        TimeSeries leftSeries2 = new TimeSeries(leftSamples2, leftLabels2, 1000L, 1000L, 1000L, "left-series-2");

        List<Sample> rightSamples = Arrays.asList(new FloatSample(1000L, 50.0));
        ByteLabels rightLabels = ByteLabels.fromMap(Map.of("service", "api", "instance", "server3"));
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 1000L, 1000L, "right-series");

        List<TimeSeries> result = stage.process(Arrays.asList(leftSeries1, leftSeries2), Arrays.asList(rightSeries));

        assertEquals(2, result.size());
        assertEquals(5.0, result.get(0).getSamples().getValue(0), 0.001);
        assertEquals(10.0, result.get(1).getSamples().getValue(0), 0.001);
    }

    public void testSelectiveLabelMatchingWithMultipleKeysException() {
        // Test selective label matching with multiple label tag
        List<String> labelTag = Arrays.asList("service"); // Match on both service and region
        DivideStage stage = new DivideStage("right_series", labelTag);

        // Left series with labels: service=api, instance=server1, region=us-east
        List<Sample> leftSamples = Arrays.asList(new FloatSample(1000L, 50.0));
        ByteLabels leftLabels = ByteLabels.fromMap(Map.of("service", "api", "instance", "server1", "region", "us-east"));
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 1000L, 1000L, "left-series");

        // Right series 1: service=api, instance=server2, region=us-east (should match - same service and region labels)
        List<Sample> rightSamples1 = Arrays.asList(new FloatSample(1000L, 200.0));
        ByteLabels rightLabels1 = ByteLabels.fromMap(Map.of("service", "api", "instance", "server2", "region", "us-east"));
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 1000L, 1000L, "right-series-1");

        // Right series 2: service=api, instance=server1, region=us-west (should not match - different region)
        List<Sample> rightSamples2 = Arrays.asList(new FloatSample(1000L, 300.0));
        ByteLabels rightLabels2 = ByteLabels.fromMap(Map.of("service", "api", "instance", "server1", "region", "us-west"));
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 1000L, 1000L, "right-series-2");

        List<TimeSeries> left = Arrays.asList(leftSeries);
        List<TimeSeries> right = Arrays.asList(rightSeries1, rightSeries2);
        // Should throw exception
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> stage.process(left, right));
        assertTrue(exception.getMessage().contains("bucket for divide must have exactly one divisor, got 2"));
    }

    public void testEmptyInputs() {
        DivideStage stage = new DivideStage("right_series");

        TimeSeries leftSeries = new TimeSeries(
            Arrays.asList(new FloatSample(1000L, 100.0)),
            ByteLabels.fromMap(Map.of("service", "api")),
            1000L,
            1000L,
            1000L,
            "left-series"
        );

        TimeSeries rightSeries = new TimeSeries(
            Arrays.asList(new FloatSample(1000L, 10.0)),
            ByteLabels.fromMap(Map.of("service", "api")),
            1000L,
            1000L,
            1000L,
            "right-series"
        );

        assertTrue(stage.process(new ArrayList<>(), Arrays.asList(rightSeries)).isEmpty());
        assertTrue(stage.process(Arrays.asList(leftSeries), new ArrayList<>()).isEmpty());
    }

    public void testFactoryAndSerialization() throws IOException {
        Map<String, Object> args = Map.of("right_op_reference", "series2");
        DivideStage stage = DivideStage.fromArgs(args);
        assertEquals("series2", stage.getRightOpReferenceName());
        assertEquals("divide", stage.getName());

        DivideStage nullStage = DivideStage.fromArgs(new HashMap<>());
        assertNull(nullStage.getRightOpReferenceName());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stage.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                DivideStage readStage = DivideStage.readFrom(in);
                assertEquals("series2", readStage.getRightOpReferenceName());
                assertEquals("divide", readStage.getName());
            }
        }
    }

    public void testFactoryAndSerializationWithLabelKeys() throws IOException {
        Map<String, Object> args = Map.of("right_op_reference", "series2", "labels", Arrays.asList("service", "region"));
        DivideStage stage = DivideStage.fromArgs(args);
        assertEquals("series2", stage.getRightOpReferenceName());
        assertEquals(Arrays.asList("service", "region"), stage.getLabelKeys());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stage.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                DivideStage readStage = DivideStage.readFrom(in);
                assertEquals("series2", readStage.getRightOpReferenceName());
                assertEquals(Arrays.asList("service", "region"), readStage.getLabelKeys());
            }
        }
    }

    public void testToXContent() throws IOException {
        DivideStage stageWithoutLabels = new DivideStage("test_reference");
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            stageWithoutLabels.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }

        DivideStage stageWithLabels = new DivideStage("test_reference", Arrays.asList("service", "region"));
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            stageWithLabels.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }
    }

    public void testEquals() {
        DivideStage stage1 = new DivideStage("test_ref");
        DivideStage stage2 = new DivideStage("test_ref");
        assertEquals(stage1, stage2);

        DivideStage stageDiffRef = new DivideStage("different_ref");
        assertNotEquals(stage1, stageDiffRef);

        DivideStage stageNull1 = new DivideStage(null);
        DivideStage stageNull2 = new DivideStage(null);
        assertEquals(stageNull1, stageNull2);

        assertNotEquals(stage1, stageNull1);
        assertEquals(stage1, stage1);
        assertNotEquals(null, stage1);
        assertNotEquals("string", stage1);

        DivideStage stageWithLabels1 = new DivideStage("ref", Arrays.asList("service", "region"));
        DivideStage stageWithLabels2 = new DivideStage("ref", Arrays.asList("service", "region"));
        assertEquals(stageWithLabels1, stageWithLabels2);

        DivideStage stageWithDiffLabels = new DivideStage("ref", Arrays.asList("service", "zone"));
        assertNotEquals(stageWithLabels1, stageWithDiffLabels);
    }

    /**
     * Test DivideStage with null left input throws exception.
     */
    public void testNullLeftInputThrowsException() {
        DivideStage stage = new DivideStage("right_series");
        List<TimeSeries> nonNullInput = List.of(
            new TimeSeries(List.of(new FloatSample(1000L, 10.0)), ByteLabels.emptyLabels(), 1000L, 1000L, 1000L, null)
        );
        TestUtils.assertNullLeftInputThrowsException(stage, nonNullInput, "divide");
    }

    /**
     * Test DivideStage with null right input throws exception.
     */
    public void testNullRightInputThrowsException() {
        DivideStage stage = new DivideStage("right_series");
        List<TimeSeries> nonNullInput = List.of(
            new TimeSeries(List.of(new FloatSample(1000L, 10.0)), ByteLabels.emptyLabels(), 1000L, 1000L, 1000L, null)
        );
        TestUtils.assertNullRightInputThrowsException(stage, nonNullInput, "divide");
    }

    @Override
    protected Writeable.Reader<DivideStage> instanceReader() {
        return DivideStage::readFrom;
    }

    @Override
    protected DivideStage createTestInstance() {
        return new DivideStage(randomAlphaOfLengthBetween(3, 10), randomBoolean() ? null : Arrays.asList("service", "region"));
    }
}
