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

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import static org.opensearch.tsdb.TestUtils.findSeriesByLabel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;

public class AsPercentStageTests extends AbstractWireSerializingTestCase<AsPercentStage> {

    public void testSingleRightSeries() {
        AsPercentStage stage = new AsPercentStage("right_series");

        // Left series with timestamps 1000L, 2000L, 3000L
        List<Sample> leftSamples = Arrays.asList(
            new FloatSample(1000L, 10.0),  // matching timestamp
            new FloatSample(2000L, 20.0),  // matching timestamp
            new FloatSample(3000L, 30.0)   // missing in right series
        );
        ByteLabels leftLabels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 3000L, 1000L, "api-series");

        // Right series with timestamps 1000L, 2000L, 4000L (4000L not in left)
        List<Sample> rightSamples = Arrays.asList(
            new FloatSample(1000L, 100.0), // matching timestamp
            new FloatSample(2000L, 200.0), // matching timestamp
            new FloatSample(4000L, 400.0)  // not in left series
        );
        ByteLabels rightLabels = ByteLabels.fromMap(Map.of("service", "total"));
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 4000L, 1000L, "total-series");

        List<TimeSeries> left = Arrays.asList(leftSeries);
        List<TimeSeries> right = Arrays.asList(rightSeries);
        List<TimeSeries> result = stage.process(left, right);

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);

        // Should only include matching timestamps: 1000L, 2000L
        assertEquals(2, resultSeries.getSamples().size());

        List<Sample> samples = resultSeries.getSamples().toList();

        // 1000L: 10.0/100.0 * 100 = 10.0%
        assertEquals(1000L, samples.get(0).getTimestamp());
        assertEquals(10.0, samples.get(0).getValue(), 0.001);

        // 2000L: 20.0/200.0 * 100 = 10.0%
        assertEquals(2000L, samples.get(1).getTimestamp());
        assertEquals(10.0, samples.get(1).getValue(), 0.001);
    }

    public void testMultipleRightSeries() {
        AsPercentStage stage = new AsPercentStage("right_series");

        // Left series with timestamps 1000L, 2000L, 3000L, 5000L
        List<Sample> leftSamples = Arrays.asList(
            new FloatSample(1000L, 25.0),  // matching timestamp
            new FloatSample(2000L, 50.0),  // matching timestamp
            new FloatSample(3000L, 75.0),  // missing in right series
            new FloatSample(5000L, 100.0)  // missing in right series
        );
        ByteLabels leftLabels = ByteLabels.fromMap(Map.of("service", "api", "instance", "server1"));
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 5000L, 1000L, "api-series");

        // Right series with matching labels
        // Start at 500L with step 1000L: timestamps are 500, 1500, 2500, 3500, 4500
        // After normalization, these will align with left series buckets
        List<Sample> rightSamples = Arrays.asList(
            new FloatSample(500L, 75.0),   // falls into [500, 1500) bucket
            new FloatSample(1500L, 200.0), // falls into [1500, 2500) bucket
            new FloatSample(2500L, 250.0), // not matching with left series
            new FloatSample(3500L, 350.0), // not matching with left series
            new FloatSample(4500L, 450.0)  // not matching with left series
        );
        ByteLabels rightLabels = ByteLabels.fromMap(Map.of("service", "api", "instance", "server1"));
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 500L, 4500L, 1000L, "total-series");

        // Right series with non-matching labels (should be ignored)
        List<Sample> rightSamples2 = Arrays.asList(new FloatSample(1000L, 500.0));
        ByteLabels rightLabels2 = ByteLabels.fromMap(Map.of("service", "db", "instance", "server2"));
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 1000L, 1000L, "db-series");

        List<TimeSeries> left = Arrays.asList(leftSeries);
        List<TimeSeries> right = Arrays.asList(rightSeries, rightSeries2);
        List<TimeSeries> result = stage.process(left, right);

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);

        // With BATCH normalization, series are normalized to common grid starting at 500L
        // Grid buckets: [500, 1500), [1500, 2500), [2500, 3500), [3500, 4500), [4500, 5500)
        // After normalization and matching, we get samples where both left and right have data
        // 500L (bucket [500, 1500)): left avg=25.0, right avg=75.0 → 25.0/75.0 * 100 = 33.33%
        // 1500L (bucket [1500, 2500)): left avg=50.0, right avg=200.0 → 50.0/200.0 * 100 = 25.0%
        // 2500L (bucket [2500, 3500)): left avg=75.0, right avg=250.0 → 75.0/250.0 * 100 = 30.0%
        // 4500L (bucket [4500, 5500)): left avg=100.0, right avg=450.0 → 100.0/450.0 * 100 = 22.22%
        List<Sample> expectedSamples = Arrays.asList(
            new FloatSample(500L, 33.33),
            new FloatSample(1500L, 25.0),
            new FloatSample(2500L, 30.0),
            new FloatSample(4500L, 22.22)
        );
        assertSamplesEqual("Multiple right series with normalization", expectedSamples, resultSeries.getSamples().toList(), 0.01);

        // Verify that result has the original labels plus the type:ratios label
        assertEquals("ratios", resultSeries.getLabels().get("type"));
        assertEquals("api", resultSeries.getLabels().get("service"));
        assertEquals("server1", resultSeries.getLabels().get("instance"));
    }

    public void testMisalignedStepSizesAndStartTimes() {
        AsPercentStage stage = new AsPercentStage("right_series");

        // Left series with 10s step size starting at 0L
        List<Sample> leftSamples = Arrays.asList(
            new FloatSample(0L, 10.0),
            new FloatSample(10000L, 20.0),
            new FloatSample(20000L, 30.0),
            new FloatSample(30000L, 40.0),
            new FloatSample(40000L, 50.0)
        );
        ByteLabels leftLabels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 0L, 40000L, 10000L, "api-series");

        // Right series with 20s step size starting at 5000L
        List<Sample> rightSamples = Arrays.asList(
            new FloatSample(5000L, 100.0),
            new FloatSample(25000L, 200.0),
            new FloatSample(45000L, 300.0)
        );
        ByteLabels rightLabels = ByteLabels.fromMap(Map.of("service", "total"));
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 5000L, 45000L, 20000L, "total-series");

        List<TimeSeries> left = Arrays.asList(leftSeries);
        List<TimeSeries> right = Arrays.asList(rightSeries);
        List<TimeSeries> result = stage.process(left, right);

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);

        // With BATCH normalization:
        // - LCM(10000, 20000) = 20000ms (20 seconds)
        // - Min start = 0L, Max end = 45000L
        // - Range = 45000 - 0 = 45000ms
        // - Adjusted range (divisible by 20000): 40000ms (45000 - 5000 remainder)
        // - Buckets: [0, 20000), [20000, 40000), [40000, 60000)
        // - Bucket [0, 20000): left avg(10, 20) = 15.0, right avg(100) = 100.0 → 15.0%
        // - Bucket [20000, 40000): left avg(30, 40) = 35.0, right avg(200) = 200.0 → 17.5%
        // - Bucket [40000, 60000): left avg(50) = 50.0, right avg(300) = 300.0 → 16.67%

        List<Sample> expectedSamples = Arrays.asList(
            new FloatSample(0L, 15.0f),
            new FloatSample(20000L, 17.5f),
            new FloatSample(40000L, 16.67f)
        );
        assertSamplesEqual("Misaligned step sizes and start times", expectedSamples, resultSeries.getSamples().toList(), 0.01);

        // Verify the normalized step size is LCM(10000, 20000) = 20000
        assertEquals(20000L, resultSeries.getStep());

        // Verify labels
        assertEquals("ratios", resultSeries.getLabels().get("type"));
        assertEquals("api", resultSeries.getLabels().get("service"));
    }

    public void testNoMatchingLabels() {
        AsPercentStage stage = new AsPercentStage("right_series");

        List<Sample> leftSamples = Arrays.asList(new FloatSample(1000L, 10.0));
        ByteLabels leftLabels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 1000L, 1000L, "api-series");

        List<Sample> rightSamples1 = Arrays.asList(new FloatSample(1000L, 100.0));
        ByteLabels rightLabels1 = ByteLabels.fromMap(Map.of("service", "db"));
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 1000L, 1000L, "db-series");

        List<Sample> rightSamples2 = Arrays.asList(new FloatSample(1000L, 150.0));
        ByteLabels rightLabels2 = ByteLabels.fromMap(Map.of("service", "db2"));
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 1000L, 1000L, "db-series");

        List<TimeSeries> left = Arrays.asList(leftSeries);
        List<TimeSeries> right = Arrays.asList(rightSeries1, rightSeries2);
        List<TimeSeries> result = stage.process(left, right);
        assertTrue(result.isEmpty());
    }

    public void testZeroDenominatorAndMissingSeries() {
        AsPercentStage stage = new AsPercentStage("right_series");

        // Test with zero right values (should return NaN)
        List<Sample> leftSamples = Arrays.asList(new FloatSample(1000L, 10.0));
        ByteLabels leftLabels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 1000L, 1000L, "api-series");

        List<Sample> rightSamples = Arrays.asList(new FloatSample(1000L, 0.0));
        ByteLabels rightLabels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 1000L, 1000L, "total-series");

        List<TimeSeries> result = stage.process(Arrays.asList(leftSeries), Arrays.asList(rightSeries));
        assertEquals("Zero right values should return series with NaN", 1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertTrue("Should be NaN when right value is zero", Double.isNaN(result.get(0).getSamples().getValue(0)));
        assertEquals(1000L, result.get(0).getSamples().getTimestamp(0));

        // Test with empty inputs
        result = stage.process(new ArrayList<>(), Arrays.asList(rightSeries));
        assertTrue("Empty left input should return empty result", result.isEmpty());

        result = stage.process(Arrays.asList(leftSeries), new ArrayList<>());
        assertTrue("Empty right input should return empty result", result.isEmpty());
    }

    public void testSelectiveLabelMatching() {
        // Test selective label matching with specific label tag
        List<String> labelTag = Arrays.asList("service"); // Only match on "service" label
        AsPercentStage stage = new AsPercentStage("right_series", labelTag);

        // Left series with labels: service=api, instance=server1, region=us-east
        List<Sample> leftSamples = Arrays.asList(new FloatSample(1000L, 25.0));
        ByteLabels leftLabels = ByteLabels.fromMap(Map.of("service", "api", "instance", "server1", "region", "us-east"));
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 1000L, 1000L, "left-series");

        // Right series 1: service=api, instance=server2, region=us-west (should match - same service label)
        List<Sample> rightSamples1 = Arrays.asList(new FloatSample(1000L, 100.0));
        ByteLabels rightLabels1 = ByteLabels.fromMap(Map.of("service", "api", "instance", "server2", "region", "us-west"));
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 1000L, 1000L, "right-series-1");

        // Right series 2: service=db, instance=server1, region=us-east (should not match - different service)
        List<Sample> rightSamples2 = Arrays.asList(new FloatSample(1000L, 200.0));
        ByteLabels rightLabels2 = ByteLabels.fromMap(Map.of("service", "db", "instance", "server1", "region", "us-east"));
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 1000L, 1000L, "right-series-2");

        List<TimeSeries> left = Arrays.asList(leftSeries);
        List<TimeSeries> right = Arrays.asList(rightSeries1, rightSeries2);
        List<TimeSeries> result = stage.process(left, right);

        // Should match with rightSeries1 (same service), not rightSeries2
        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(1, resultSeries.getSamples().size());
        assertEquals(25.0, resultSeries.getSamples().getValue(0), 0.001); // 25.0/100.0 * 100 = 25.0%
        assertEquals(1000L, resultSeries.getSamples().getTimestamp(0));
    }

    public void testSelectiveLabelMatchingWithMultipleKeys() {
        // Test selective label matching with multiple label tag
        List<String> labelTag = Arrays.asList("service", "region"); // Match on both service and region
        AsPercentStage stage = new AsPercentStage("right_series", labelTag);

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
        List<TimeSeries> result = stage.process(left, right);

        // Should match with rightSeries1 (same service and region), not rightSeries2
        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(1, resultSeries.getSamples().size());
        assertEquals(25.0, resultSeries.getSamples().getValue(0), 0.001); // 50.0/200.0 * 100 = 25.0%
        assertEquals(1000L, resultSeries.getSamples().getTimestamp(0));
    }

    public void testSelectiveLabelMatchingWithMultipleKeysException() {
        // Test selective label matching with multiple label tag
        List<String> labelTag = Arrays.asList("service"); // Match on both service and region
        AsPercentStage stage = new AsPercentStage("right_series", labelTag);

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
        assertTrue(exception.getMessage().contains("bucket for asPercent/ratio must have exactly one divisor, got 2"));
    }

    public void testFactoryAndSerialization() throws IOException {
        // Test fromArgs
        Map<String, Object> args = Map.of("right_op_reference", "series2");
        AsPercentStage stage = AsPercentStage.fromArgs(args);
        assertEquals("series2", stage.getRightOpReferenceName());
        assertEquals("as_percent", stage.getName());

        // Test fromArgs with null/missing reference
        AsPercentStage nullStage = AsPercentStage.fromArgs(new HashMap<>());
        assertNull(nullStage.getRightOpReferenceName());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stage.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                AsPercentStage readStage = AsPercentStage.readFrom(in);
                assertEquals("series2", readStage.getRightOpReferenceName());
                assertEquals("as_percent", readStage.getName());
            }
        }
    }

    public void testFactoryAndSerializationWithLabelTag() throws IOException {
        // Test fromArgs with label tag
        Map<String, Object> args = Map.of("right_op_reference", "series2", "labels", Arrays.asList("service", "region"));
        AsPercentStage stage = AsPercentStage.fromArgs(args);
        assertEquals("series2", stage.getRightOpReferenceName());
        assertEquals("as_percent", stage.getName());
        assertEquals(stage.getLabelKeys(), Arrays.asList("service", "region"));

        // Test serialization with label tag
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stage.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                AsPercentStage readStage = AsPercentStage.readFrom(in);
                assertEquals("series2", readStage.getRightOpReferenceName());
                assertEquals("as_percent", readStage.getName());
                assertEquals(readStage.getLabelKeys(), Arrays.asList("service", "region"));
            }
        }
    }

    public void testSingleRightSeriesWithLabelMatching() {
        // Test that labelKeys are still applied even when right side has single series
        List<String> labelKeys = Arrays.asList("service"); // Only match on "service" label
        AsPercentStage stage = new AsPercentStage("right_series", labelKeys);

        // Left series 1: service=api, instance=server1
        List<Sample> leftSamples1 = Arrays.asList(new FloatSample(1000L, 25.0));
        ByteLabels leftLabels1 = ByteLabels.fromMap(Map.of("service", "api", "instance", "server1"));
        TimeSeries leftSeries1 = new TimeSeries(leftSamples1, leftLabels1, 1000L, 1000L, 1000L, "left-series-1");

        // Left series 2: service=db, instance=server2 (should not match)
        List<Sample> leftSamples2 = Arrays.asList(new FloatSample(1000L, 50.0));
        ByteLabels leftLabels2 = ByteLabels.fromMap(Map.of("service", "db", "instance", "server2"));
        TimeSeries leftSeries2 = new TimeSeries(leftSamples2, leftLabels2, 1000L, 1000L, 1000L, "left-series-2");

        // Single right series: service=api, instance=server3 (should match leftSeries1 only)
        List<Sample> rightSamples = Arrays.asList(new FloatSample(1000L, 100.0));
        ByteLabels rightLabels = ByteLabels.fromMap(Map.of("service", "api", "instance", "server3"));
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 1000L, 1000L, "right-series");

        List<TimeSeries> left = Arrays.asList(leftSeries1, leftSeries2);
        List<TimeSeries> right = Arrays.asList(rightSeries);
        List<TimeSeries> result = stage.process(left, right);

        // Should only find one matching series
        assertEquals("Should process all series since labels are ignored if it is a single right series", 2, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(1, resultSeries.getSamples().size());
        assertEquals(25.0, resultSeries.getSamples().getValue(0), 0.001); // 25.0/100.0 * 100 = 25.0%
        assertEquals(1000L, resultSeries.getSamples().getTimestamp(0));
        resultSeries = result.get(1);
        assertEquals(1, resultSeries.getSamples().size());
        assertEquals(50.0, resultSeries.getSamples().getValue(0), 0.001); // 25.0/100.0 * 100 = 25.0%
        assertEquals(1000L, resultSeries.getSamples().getTimestamp(0));
    }

    public void testEdgeCases() {
        AsPercentStage stage = new AsPercentStage("right_series");

        // Test case 1: Empty left input
        List<TimeSeries> emptyLeft = new ArrayList<>();
        List<TimeSeries> rightList = Arrays.asList(
            new TimeSeries(
                Arrays.asList(new FloatSample(1000L, 100.0)),
                ByteLabels.fromMap(Map.of("service", "api")),
                1000L,
                1000L,
                1000L,
                "right-series"
            )
        );
        List<TimeSeries> result1 = stage.process(emptyLeft, rightList);
        assertTrue("Should return empty list when left input is empty", result1.isEmpty());

        // Test case 2: Empty right input
        List<TimeSeries> leftList = Arrays.asList(
            new TimeSeries(
                Arrays.asList(new FloatSample(1000L, 10.0)),
                ByteLabels.fromMap(Map.of("service", "api")),
                1000L,
                1000L,
                1000L,
                "left-series"
            )
        );
        List<TimeSeries> emptyRight = new ArrayList<>();
        List<TimeSeries> result2 = stage.process(leftList, emptyRight);
        assertTrue("Should return empty list when right input is empty", result2.isEmpty());
    }

    public void testToXContent() throws IOException {
        // Test toXContent without labelKeys
        AsPercentStage stageWithoutLabels = new AsPercentStage("test_reference");
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            stageWithoutLabels.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }

        // Test toXContent with labelKeys
        List<String> labelKeys = Arrays.asList("service", "region");
        AsPercentStage stageWithLabels = new AsPercentStage("test_reference", labelKeys);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            stageWithLabels.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        }
    }

    /**
     * Test equals method for AsPercentStage.
     */
    public void testEquals() {
        AsPercentStage stage1 = new AsPercentStage("test_ref");
        AsPercentStage stage2 = new AsPercentStage("test_ref");

        assertEquals("Equal AsPercentStages should be equal", stage1, stage2);

        AsPercentStage stageDiffRef = new AsPercentStage("different_ref");
        assertNotEquals("Different reference names should not be equal", stage1, stageDiffRef);

        AsPercentStage stageNull1 = new AsPercentStage(null);
        AsPercentStage stageNull2 = new AsPercentStage(null);
        assertEquals("Null reference names should be equal", stageNull1, stageNull2);

        assertNotEquals("Null vs non-null reference names should not be equal", stage1, stageNull1);
        assertNotEquals("Non-null vs null reference names should not be equal", stageNull1, stage1);

        assertEquals("Stage should equal itself", stage1, stage1);

        assertNotEquals("Stage should not equal null", null, stage1);

        assertNotEquals("Stage should not equal different class", "string", stage1);

        List<String> labelKeys = Arrays.asList("service", "region");
        AsPercentStage stageWithLabels1 = new AsPercentStage("ref", labelKeys);
        AsPercentStage stageWithLabels2 = new AsPercentStage("ref", labelKeys);
        assertEquals("Stages with same reference and label keys should be equal", stageWithLabels1, stageWithLabels2);

        List<String> differentLabelKeys = Arrays.asList("service", "zone");
        AsPercentStage stageWithDiffLabels = new AsPercentStage("ref", differentLabelKeys);
        assertNotEquals("Stages with different label keys should not be equal", stageWithLabels1, stageWithDiffLabels);
    }

    @Override
    protected Writeable.Reader<AsPercentStage> instanceReader() {
        return AsPercentStage::readFrom;
    }

    @Override
    protected AsPercentStage createTestInstance() {
        return new AsPercentStage(randomAlphaOfLengthBetween(3, 10), randomBoolean() ? null : Arrays.asList("service", "region"));
    }

    public void testNullInputsThrowExceptions() {
        AsPercentStage stage = new AsPercentStage("right_series");
        List<TimeSeries> nonNullInput = List.of(
            new TimeSeries(List.of(new FloatSample(1000L, 10.0)), ByteLabels.emptyLabels(), 1000L, 1000L, 1000L, null)
        );
        TestUtils.assertBinaryNullInputsThrowExceptions(stage, nonNullInput, "as_percent");
    }

    public void testCommonTagKeyExtraction() {
        // Test that common tag keys are extracted when no label keys are specified
        // Left inputs: 2 time series with multiple tags, including cluster:clusterA and cluster:clusterB
        // Right inputs: 2 series with only cluster tag
        AsPercentStage stage = new AsPercentStage("right_series");

        // Left series 1: cluster=clusterA, service=api, instance=server1
        List<Sample> leftSamples1 = Arrays.asList(new FloatSample(1000L, 20.0), new FloatSample(2000L, 40.0));
        ByteLabels leftLabels1 = ByteLabels.fromMap(Map.of("cluster", "clusterA", "service", "api", "instance", "server1"));
        TimeSeries leftSeries1 = new TimeSeries(leftSamples1, leftLabels1, 1000L, 2000L, 1000L, "left-series-1");

        // Left series 2: cluster=clusterB, service=api, instance=server2
        List<Sample> leftSamples2 = Arrays.asList(new FloatSample(1000L, 30.0), new FloatSample(2000L, 60.0));
        ByteLabels leftLabels2 = ByteLabels.fromMap(Map.of("cluster", "clusterB", "service", "api", "instance", "server2"));
        TimeSeries leftSeries2 = new TimeSeries(leftSamples2, leftLabels2, 1000L, 2000L, 1000L, "left-series-2");

        // Right series 1: cluster=clusterA (only cluster tag)
        List<Sample> rightSamples1 = Arrays.asList(new FloatSample(1000L, 100.0), new FloatSample(2000L, 200.0));
        ByteLabels rightLabels1 = ByteLabels.fromMap(Map.of("cluster", "clusterA"));
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 2000L, 1000L, "right-series-1");

        // Right series 2: cluster=clusterB (only cluster tag)
        List<Sample> rightSamples2 = Arrays.asList(new FloatSample(1000L, 150.0), new FloatSample(2000L, 300.0));
        ByteLabels rightLabels2 = ByteLabels.fromMap(Map.of("cluster", "clusterB"));
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 2000L, 1000L, "right-series-2");

        List<TimeSeries> left = Arrays.asList(leftSeries1, leftSeries2);
        List<TimeSeries> right = Arrays.asList(rightSeries1, rightSeries2);
        List<TimeSeries> result = stage.process(left, right);

        // Should match leftSeries1 with rightSeries1 (clusterA) and leftSeries2 with rightSeries2 (clusterB)
        assertEquals(2, result.size());

        // Verify result for clusterA
        TimeSeries resultClusterA = findSeriesByLabel(result, "cluster", "clusterA");
        List<Sample> expectedSamplesA = Arrays.asList(
            new FloatSample(1000L, 20.0),  // 20.0/100.0 * 100 = 20.0%
            new FloatSample(2000L, 20.0)   // 40.0/200.0 * 100 = 20.0%
        );
        assertSamplesEqual("ClusterA samples", expectedSamplesA, resultClusterA.getSamples().toList(), 0.001);
        assertEquals("ratios", resultClusterA.getLabels().get("type"));
        assertEquals("clusterA", resultClusterA.getLabels().get("cluster"));
        assertEquals("api", resultClusterA.getLabels().get("service"));
        assertEquals("server1", resultClusterA.getLabels().get("instance"));

        // Verify result for clusterB
        TimeSeries resultClusterB = findSeriesByLabel(result, "cluster", "clusterB");
        List<Sample> expectedSamplesB = Arrays.asList(
            new FloatSample(1000L, 20.0),  // 30.0/150.0 * 100 = 20.0%
            new FloatSample(2000L, 20.0)   // 60.0/300.0 * 100 = 20.0%
        );
        assertSamplesEqual("ClusterB samples", expectedSamplesB, resultClusterB.getSamples().toList(), 0.001);
        assertEquals("ratios", resultClusterB.getLabels().get("type"));
        assertEquals("clusterB", resultClusterB.getLabels().get("cluster"));
        assertEquals("api", resultClusterB.getLabels().get("service"));
        assertEquals("server2", resultClusterB.getLabels().get("instance"));
    }
}
