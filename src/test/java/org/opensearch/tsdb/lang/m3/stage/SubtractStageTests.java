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
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;

public class SubtractStageTests extends AbstractWireSerializingTestCase<SubtractStage> {
    public void testSingleRightSeries() {
        SubtractStage stage = new SubtractStage("right_series");

        // Left series with timestamps 1000L, 2000L, 3000L
        List<Sample> leftSamples = List.of(
            new FloatSample(1000L, 10.0),  // matching timestamp
            new FloatSample(2000L, 20.0),  // matching timestamp
            new FloatSample(3000L, 30.0)   // missing in right series
        );
        ByteLabels leftLabels = ByteLabels.fromStrings("service", "api");
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 3000L, 1000L, "api-series");

        // Right series with timestamps 1000L, 2000L, 4000L (4000L not in left)
        List<Sample> rightSamples = List.of(
            new FloatSample(1000L, 1), // matching timestamp
            new FloatSample(2000L, 2), // matching timestamp
            new FloatSample(4000L, 4)  // not in left series
        );
        ByteLabels rightLabels = ByteLabels.fromStrings("service", "total");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 4000L, 1000L, "total-series");

        List<TimeSeries> left = List.of(leftSeries);
        List<TimeSeries> right = List.of(rightSeries);
        List<TimeSeries> result = stage.process(left, right);

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertSamplesEqual(
            "Single Right Series with keepNans = false",
            List.of(
                new FloatSample(1000L, 9.0),// 10 - 1
                new FloatSample(2000L, 18.0),// 20 - 2
                new FloatSample(3000L, 30.0), // 30 -0
                new FloatSample(4000L, -4.0)// 0 - 4
            ),
            resultSeries.getSamples().toList()
        );

        SubtractStage stageWithKeepNans = new SubtractStage("right_series", true, Collections.emptyList());
        result = stageWithKeepNans.process(left, right);

        assertEquals(1, result.size());
        resultSeries = result.get(0);
        assertSamplesEqual(
            "Single Right Series with keepNans = true",
            List.of(
                new FloatSample(1000L, 9.0),// 10 - 1
                new FloatSample(2000L, 18.0)// 20 - 2
            ),
            resultSeries.getSamples().toList()
        );
    }

    public void testMultipleRightSeries() {
        SubtractStage stage = new SubtractStage("right_series");

        // Left series with timestamps 1000L, 2000L, 3000L, 5000L
        List<Sample> leftSamples = List.of(
            new FloatSample(1000L, 25.0),  // matching timestamp
            new FloatSample(2000L, 50.0),  // matching timestamp
            new FloatSample(3000L, 75.0),  // missing in right series
            new FloatSample(7000L, 100.0)  // missing in right series
        );
        ByteLabels leftLabels = ByteLabels.fromStrings("service", "api", "instance", "server1");
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 5000L, 1000L, "api-series");

        // Right series with matching labels
        List<Sample> rightSamples = List.of(
            new FloatSample(500L, 50.0),   // not in left series
            new FloatSample(1000L, 5), // matching timestamp
            new FloatSample(2000L, 10), // matching timestamp
            new FloatSample(4000L, 400.0)  // not in left series
        );
        ByteLabels rightLabels = ByteLabels.fromStrings("service", "api", "instance", "server1");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 500L, 4000L, 1000L, "total-series");

        // Right series with non-matching labels (should be ignored)
        List<Sample> rightSamples2 = List.of(new FloatSample(1000L, 500.0));
        ByteLabels rightLabels2 = ByteLabels.fromStrings("service", "db", "instance", "server2");
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 1000L, 1000L, "db-series");

        List<TimeSeries> left = List.of(leftSeries);
        List<TimeSeries> right = List.of(rightSeries, rightSeries2);
        List<TimeSeries> result = stage.process(left, right);

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);

        assertSamplesEqual(
            "Multiple Right Series with keepNans = false",
            List.of(
                new FloatSample(500L, -50.0), // 0 - 50
                new FloatSample(1000L, 20.0), // 25 - 5
                new FloatSample(2000L, 40.0), // 50 - 10
                new FloatSample(3000L, 75.0), // 75 - 0
                new FloatSample(4000L, -400.0), // 0 - 400
                new FloatSample(7000L, 100.0) // 100 - 0
            ),
            resultSeries.getSamples().toList()
        );

        SubtractStage stageWithKeepNans = new SubtractStage("right_series", true, Collections.emptyList());
        result = stageWithKeepNans.process(left, right);

        assertEquals(1, result.size());
        resultSeries = result.get(0);
        assertSamplesEqual(
            "Multiple Right Series with keepNans = true",
            List.of(new FloatSample(1000L, 20.0), new FloatSample(2000L, 40.0)),
            resultSeries.getSamples().toList()
        );
    }

    public void testNoMatchingLabels() {
        SubtractStage stage = new SubtractStage("right_series");

        List<Sample> leftSamples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels leftLabels = ByteLabels.fromStrings("service", "api");
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 1000L, 1000L, "api-series");

        List<Sample> rightSamples1 = List.of(new FloatSample(1000L, 100.0));
        ByteLabels rightLabels1 = ByteLabels.fromStrings("service", "db");
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 1000L, 1000L, "db-series");

        List<Sample> rightSamples2 = List.of(new FloatSample(1000L, 150.0));
        ByteLabels rightLabels2 = ByteLabels.fromStrings("service", "db2");
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 1000L, 1000L, "db-series");

        List<TimeSeries> left = List.of(leftSeries);
        List<TimeSeries> right = List.of(rightSeries1, rightSeries2);
        List<TimeSeries> result = stage.process(left, right);
        assertTrue(result.isEmpty());
    }

    public void testSelectiveLabelMatching() {
        // Test selective label matching with specific label tag
        List<String> labelTag = List.of("service"); // Only match on "service" label
        SubtractStage stage = new SubtractStage("right_series", false, labelTag);

        // Left series with labels: service=api, instance=server1, region=us-east
        List<Sample> leftSamples = List.of(new FloatSample(1000L, 25.0));
        ByteLabels leftLabels = ByteLabels.fromStrings("service", "api", "instance", "server1", "region", "us-east");
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 1000L, 1000L, "left-series");

        // Right series 1: service=api, instance=server2, region=us-west (should match - same service label)
        List<Sample> rightSamples1 = List.of(new FloatSample(1000L, 1));
        ByteLabels rightLabels1 = ByteLabels.fromStrings("service", "api", "instance", "server2", "region", "us-west");
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 1000L, 1000L, "right-series-1");

        // Right series 2: service=db, instance=server1, region=us-east (should not match - different service)
        List<Sample> rightSamples2 = List.of(new FloatSample(1000L, 2));
        ByteLabels rightLabels2 = ByteLabels.fromStrings("service", "db", "instance", "server1", "region", "us-east");
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 1000L, 1000L, "right-series-2");

        List<TimeSeries> left = List.of(leftSeries);
        List<TimeSeries> right = List.of(rightSeries1, rightSeries2);
        List<TimeSeries> result = stage.process(left, right);

        // Should match with rightSeries1 (same service), not rightSeries2
        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertSamplesEqual(
            "Selective Label Matching with keepNans = false",
            List.of(new FloatSample(1000L, 24.0)),
            resultSeries.getSamples().toList()
        );
    }

    public void testSelectiveLabelMatchingWithMultipleKeys() {
        // Test selective label matching with multiple label tag
        List<String> labelTag = List.of("service", "region"); // Match on both service and region
        SubtractStage stage = new SubtractStage("right_series", false, labelTag);

        // Left series with labels: service=api, instance=server1, region=us-east
        List<Sample> leftSamples = List.of(new FloatSample(1000L, 50.0));
        ByteLabels leftLabels = ByteLabels.fromStrings("service", "api", "instance", "server1", "region", "us-east");
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 1000L, 1000L, "left-series");

        // Right series 1: service=api, instance=server2, region=us-east (should match - same service and region labels)
        List<Sample> rightSamples1 = List.of(new FloatSample(1000L, 1));
        ByteLabels rightLabels1 = ByteLabels.fromStrings("service", "api", "instance", "server2", "region", "us-east");
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 1000L, 1000L, "right-series-1");

        // Right series 2: service=api, instance=server1, region=us-west (should not match - different region)
        List<Sample> rightSamples2 = List.of(new FloatSample(1000L, 2));
        ByteLabels rightLabels2 = ByteLabels.fromStrings("service", "api", "instance", "server1", "region", "us-west");
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 1000L, 1000L, "right-series-2");

        List<TimeSeries> left = List.of(leftSeries);
        List<TimeSeries> right = List.of(rightSeries1, rightSeries2);
        List<TimeSeries> result = stage.process(left, right);

        // Should match with rightSeries1 (same service and region), not rightSeries2
        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertSamplesEqual(
            "Selective Multiple Label Matching with keepNans = false",
            List.of(new FloatSample(1000L, 49.0)),
            resultSeries.getSamples().toList()
        );
    }

    public void testSelectiveLabelMatchingWithMerge() {
        // Test selective label matching with multiple label tag
        List<String> labelTag = List.of("service", "region"); // Match on both service and region
        SubtractStage stage = new SubtractStage("right_series", false, labelTag);

        // Left series with labels: service=api, instance=server1, region=us-east
        List<Sample> leftSamples = List.of(new FloatSample(1000L, 50.0), new FloatSample(2000L, 100.0), new FloatSample(3000L, 150.0));
        ByteLabels leftLabels = ByteLabels.fromStrings("service", "api", "instance", "server1", "region", "us-east");
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 1000L, 1000L, "left-series");

        // Right series 1: service=api, instance=server2, region=us-east (should match - same service and region labels)
        List<Sample> rightSamples1 = List.of(new FloatSample(1000L, 1), new FloatSample(2000L, 2), new FloatSample(3000L, 3));
        ByteLabels rightLabels1 = ByteLabels.fromStrings("service", "api", "instance", "server2", "region", "us-east");
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 1000L, 1000L, "right-series-1");

        // Right series 2: service=api, instance=server1, region=us-west (should not match - different region)
        List<Sample> rightSamples2 = List.of(new FloatSample(1000L, 2), new FloatSample(2000L, 4), new FloatSample(3000L, 8));
        ByteLabels rightLabels2 = ByteLabels.fromStrings("service", "api", "instance", "server1", "region", "us-west");
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 1000L, 1000L, "right-series-2");

        // Right series 3: service=api, instance=server2, region=us-east (should match - same service and region labels)
        List<Sample> rightSamples3 = List.of(new FloatSample(1000L, 3), new FloatSample(2000L, 9), new FloatSample(3000L, 27));
        ByteLabels rightLabels3 = ByteLabels.fromStrings("service", "api", "instance", "server2", "region", "us-east");
        TimeSeries rightSeries3 = new TimeSeries(rightSamples3, rightLabels3, 1000L, 1000L, 1000L, "right-series-2");

        List<TimeSeries> left = List.of(leftSeries);
        List<TimeSeries> right = List.of(rightSeries1, rightSeries2, rightSeries3);
        List<TimeSeries> result = stage.process(left, right);

        // Should match with rightSeries1 and rightSeries3 (same service and region), not rightSeries2
        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertSamplesEqual(
            "Selective Label Matching with Merge and with keepNans = false",
            List.of(
                new FloatSample(1000L, 46.0), // 50 -1 - 3
                new FloatSample(2000L, 89.0), // 100 - 2 - 9
                new FloatSample(3000L, 120.0)  // 150 - 3 - 27
            ),
            resultSeries.getSamples().toList()
        );
    }

    public void testFactoryAndSerialization() throws IOException {
        // Test fromArgs
        Map<String, Object> args = Map.of("right_op_reference", "series2");
        SubtractStage stage = SubtractStage.fromArgs(args);
        assertEquals("series2", stage.getRightOpReferenceName());
        assertFalse(stage.isKeepNaNs());
        assertEquals("subtract", stage.getName());

        // Test fromArgs with null/missing reference
        SubtractStage nullStage = SubtractStage.fromArgs(new HashMap<>());
        assertNull(nullStage.getRightOpReferenceName());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stage.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                SubtractStage readStage = SubtractStage.readFrom(in);
                assertEquals("series2", readStage.getRightOpReferenceName());
                assertFalse(readStage.isKeepNaNs());
                assertEquals("subtract", readStage.getName());
            }
        }
    }

    public void testFactoryAndSerializationWithLabelTag() throws IOException {
        // Test fromArgs with label tag
        Map<String, Object> args = Map.of("right_op_reference", "series2", "keep_nans", true, "labels", List.of("service", "region"));
        SubtractStage stage = SubtractStage.fromArgs(args);
        assertEquals("series2", stage.getRightOpReferenceName());
        assertEquals("subtract", stage.getName());
        assertTrue(stage.isKeepNaNs());
        assertEquals(stage.getLabelKeys(), List.of("service", "region"));

        // Test serialization with label tag
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            stage.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                SubtractStage readStage = SubtractStage.readFrom(in);
                assertEquals("series2", readStage.getRightOpReferenceName());
                assertEquals("subtract", readStage.getName());
                assertEquals(readStage.getLabelKeys(), List.of("service", "region"));
                assertTrue(readStage.isKeepNaNs());
            }
        }
    }

    public void testSingleRightSeriesWithLabelMatching() {
        // Test that labelKeys are still applied even when right side has single series
        List<String> labelKeys = List.of("service"); // Only match on "service" label
        SubtractStage stage = new SubtractStage("right_series", true, labelKeys);

        // Left series 1: service=api, instance=server1
        List<Sample> leftSamples1 = List.of(new FloatSample(1000L, 25.0));
        ByteLabels leftLabels1 = ByteLabels.fromStrings("service", "api", "instance", "server1");
        TimeSeries leftSeries1 = new TimeSeries(leftSamples1, leftLabels1, 1000L, 1000L, 1000L, "left-series-1");

        // Left series 2: service=db, instance=server2 (should not match)
        List<Sample> leftSamples2 = List.of(new FloatSample(1000L, 50.0));
        ByteLabels leftLabels2 = ByteLabels.fromStrings("service", "db", "instance", "server2");
        TimeSeries leftSeries2 = new TimeSeries(leftSamples2, leftLabels2, 1000L, 1000L, 1000L, "left-series-2");

        // Single right series: service=api, instance=server3 (should match leftSeries1 only)
        List<Sample> rightSamples = List.of(new FloatSample(1000L, 5));
        ByteLabels rightLabels = ByteLabels.fromStrings("service", "api", "instance", "server3");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 1000L, 1000L, "right-series");

        List<TimeSeries> left = List.of(leftSeries1, leftSeries2);
        List<TimeSeries> right = List.of(rightSeries);
        List<TimeSeries> result = stage.process(left, right);

        // Should only find one matching series
        assertEquals("Should process all series since labels are ignored if it is a single right series", 2, result.size());

        TimeSeries resultSeries = result.get(0);
        assertEquals(1, resultSeries.getSamples().size());
        assertEquals(20, resultSeries.getSamples().getValue(0), 0.001); // 25 - 5
        assertEquals(1000L, resultSeries.getSamples().getTimestamp(0));
        resultSeries = result.get(1);
        assertEquals(1, resultSeries.getSamples().size());
        assertEquals(45, resultSeries.getSamples().getValue(0), 0.001); // 50 - 5
        assertEquals(1000L, resultSeries.getSamples().getTimestamp(0));
    }

    public void testEdgeCases() {
        SubtractStage stage = new SubtractStage("right_series");

        // Test case 1: Empty left input
        List<TimeSeries> emptyLeft = new ArrayList<>();
        List<TimeSeries> rightList = List.of(
            new TimeSeries(
                List.of(new FloatSample(1000L, 100.0)),
                ByteLabels.fromStrings("service", "api"),
                1000L,
                1000L,
                1000L,
                "right-series"
            )
        );
        List<TimeSeries> result1 = stage.process(emptyLeft, rightList);
        assertTrue("Should return empty list when left input is empty", result1.isEmpty());

        // Test case 2: Empty right input
        List<TimeSeries> leftList = List.of(
            new TimeSeries(
                List.of(new FloatSample(1000L, 10.0)),
                ByteLabels.fromStrings("service", "api"),
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

    public void testNaNValuesAreTreatedAsZero() {
        // Test that when keepNaNs=false, NaN values are treated as 0.0
        SubtractStage stage = new SubtractStage("right_series", false, Collections.emptyList());

        // Left series with NaN values
        List<Sample> leftSamples = List.of(
            new FloatSample(1000L, 10.0),      // normal value
            new FloatSample(2000L, Double.NaN), // NaN -> treated as 0.0
            new FloatSample(3000L, 30.0),      // normal value
            new FloatSample(4000L, Double.NaN)  // NaN -> treated as 0.0
        );
        ByteLabels leftLabels = ByteLabels.fromStrings("service", "api");
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 4000L, 1000L, "left-series");

        // Right series with NaN values
        List<Sample> rightSamples = List.of(
            new FloatSample(1000L, 5.0),       // normal value
            new FloatSample(2000L, 2.0),       // normal value
            new FloatSample(3000L, Double.NaN), // NaN -> treated as 0.0
            new FloatSample(4000L, Double.NaN)  // NaN -> treated as 0.0
        );
        ByteLabels rightLabels = ByteLabels.fromStrings("service", "api");
        TimeSeries rightSeries = new TimeSeries(rightSamples, rightLabels, 1000L, 4000L, 1000L, "right-series");

        List<TimeSeries> left = List.of(leftSeries);
        List<TimeSeries> right = List.of(rightSeries);
        List<TimeSeries> result = stage.process(left, right);

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);

        // Verify NaN handling:
        // 1000L: 10.0 - 5.0 = 5.0 (both normal values)
        // 2000L: NaN - 2.0 = 0.0 - 2.0 = -2.0 (left NaN treated as 0.0)
        // 3000L: 30.0 - NaN = 30.0 - 0.0 = 30.0 (right NaN treated as 0.0)
        // 4000L: NaN - NaN = null (both NaN -> both null -> no sample)
        assertSamplesEqual(
            "NaN values should be treated as 0.0 when keepNaNs=false, except when both are NaN",
            List.of(
                new FloatSample(1000L, 5.0),   // 10.0 - 5.0
                new FloatSample(2000L, -2.0),  // NaN -> 0.0 - 2.0
                new FloatSample(3000L, 30.0)   // 30.0 - NaN -> 0.0
            ),
            resultSeries.getSamples().toList()
        );
    }

    private void verifyXContent(SubtractStage stage, String expectedJson) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertEquals(expectedJson, json);
    }

    public void testToXContent() throws IOException {
        // Test toXContent without labelKeys
        SubtractStage stageWithoutLabels = new SubtractStage("test_reference");
        verifyXContent(stageWithoutLabels, "{\"right_op_reference\":\"test_reference\",\"keep_nans\":false}");

        // Test toXContent with labelKeys
        List<String> labelKeys = List.of("service", "region");
        SubtractStage stageWithLabels = new SubtractStage("test_reference", true, labelKeys);
        verifyXContent(
            stageWithLabels,
            "{\"right_op_reference\":\"test_reference\",\"keep_nans\":true,\"labels\":[\"service\",\"region\"]}"
        );
    }

    /**
     * Test equals method for SubtractStage.
     */
    public void testEquals() {
        // test with default constructor
        SubtractStage stage1 = new SubtractStage("test_ref");
        SubtractStage stage2 = new SubtractStage("test_ref");

        assertEquals("Equal SubtractStages should be equal", stage1, stage2);

        SubtractStage stageDiffRef = new SubtractStage("different_ref");
        assertNotEquals("Different reference names should not be equal", stage1, stageDiffRef);

        SubtractStage stageNull1 = new SubtractStage(null);
        SubtractStage stageNull2 = new SubtractStage(null);
        assertEquals("Null reference names should be equal", stageNull1, stageNull2);

        assertNotEquals("Null vs non-null reference names should not be equal", stage1, stageNull1);
        assertNotEquals("Non-null vs null reference names should not be equal", stageNull1, stage1);

        assertEquals("Stage should equal itself", stage1, stage1);

        assertNotEquals("Stage should not equal null", null, stage1);

        assertNotEquals("Stage should not equal different class", "string", stage1);

        List<String> labelKeys = List.of("service", "region");
        SubtractStage stageWithLabels1 = new SubtractStage("ref", true, labelKeys);
        SubtractStage stageWithLabels2 = new SubtractStage("ref", true, labelKeys);
        assertEquals("Stages with same reference and label keys should be equal", stageWithLabels1, stageWithLabels2);

        SubtractStage stageWithLabels3 = new SubtractStage("ref", false, labelKeys);
        assertNotEquals(
            "Stages with same reference and label keys but diff keep_nans should not be equal",
            stageWithLabels1,
            stageWithLabels3
        );

        List<String> differentLabelKeys = List.of("service", "zone");
        SubtractStage stageWithDiffLabels = new SubtractStage("ref", true, differentLabelKeys);
        assertNotEquals("Stages with different label keys should not be equal", stageWithLabels1, stageWithDiffLabels);

    }

    @Override
    protected Writeable.Reader<SubtractStage> instanceReader() {
        return SubtractStage::readFrom;
    }

    @Override
    protected SubtractStage createTestInstance() {
        return new SubtractStage(randomAlphaOfLengthBetween(3, 10), randomBoolean(), randomBoolean() ? null : List.of("service", "region"));
    }

    public void testCommonTagKeyExtraction() {
        // Test that common tag keys are extracted when no label keys are specified
        // Left: one series with tag1:a, tag2:bb, tag3:c
        // Right: series 1 with tag1:a, tag2:b and series 2 with tag3:c, tag2:b
        SubtractStage stage = new SubtractStage("right_series", false, null);

        // Left series: tag1:a, tag2:bb, tag3:c
        List<Sample> leftSamples = List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0));
        ByteLabels leftLabels = ByteLabels.fromMap(Map.of("tag1", "a", "tag2", "bb", "tag3", "c"));
        TimeSeries leftSeries = new TimeSeries(leftSamples, leftLabels, 1000L, 3000L, 1000L, "left-series");

        // Right series 1: tag1:a, tag2:b
        List<Sample> rightSamples1 = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0), new FloatSample(3000L, 30.0));
        ByteLabels rightLabels1 = ByteLabels.fromMap(Map.of("tag1", "a", "tag2", "b"));
        TimeSeries rightSeries1 = new TimeSeries(rightSamples1, rightLabels1, 1000L, 3000L, 1000L, "right-series-1");

        // Right series 2: tag3:c, tag2:b
        List<Sample> rightSamples2 = List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 10.0), new FloatSample(3000L, 15.0));
        ByteLabels rightLabels2 = ByteLabels.fromMap(Map.of("tag3", "c", "tag2", "b"));
        TimeSeries rightSeries2 = new TimeSeries(rightSamples2, rightLabels2, 1000L, 3000L, 1000L, "right-series-2");

        List<TimeSeries> left = List.of(leftSeries);
        List<TimeSeries> right = List.of(rightSeries1, rightSeries2);
        List<TimeSeries> result = stage.process(left, right);

        // Common tag keys are tag2
        // Both right series have tag2:b, so they group into the same bucket
        // Since there's only one bucket, tag matching is skipped and left series is processed against merged right series
        // Merged right series: (10+5)=15, (20+10)=30, (30+15)=45
        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);

        // Expected: 1-15=-14, 2-30=-28, 3-45=-42
        List<Sample> expectedSamples = List.of(
            new FloatSample(1000L, -14.0),  // 1 - (10+5)
            new FloatSample(2000L, -28.0),  // 2 - (20+10)
            new FloatSample(3000L, -42.0)   // 3 - (30+15)
        );
        assertSamplesEqual("Subtract with merged right series", expectedSamples, resultSeries.getSamples().toList(), 0.001);

        // Verify labels come from left series
        assertEquals("a", resultSeries.getLabels().get("tag1"));
        assertEquals("bb", resultSeries.getLabels().get("tag2"));
        assertEquals("c", resultSeries.getLabels().get("tag3"));
    }

    /**
     * Test extractGroupLabels with null labels - should return null.
     * This tests the null check at lines 433-435 in AbstractBinaryProjectionStage.
     */
    public void testExtractGroupLabelsWithNullLabels() {
        SubtractStage stage = new SubtractStage("right_series");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries seriesWithNullLabels = new TimeSeries(samples, null, 1000L, 1000L, 1000L, null);

        ByteLabels result = stage.extractGroupLabels(seriesWithNullLabels, null);
        assertNull("extractGroupLabels should return null when series labels are null", result);

        // Also test with empty labelKeys
        result = stage.extractGroupLabels(seriesWithNullLabels, List.of());
        assertNull("extractGroupLabels should return null when series labels are null, even with empty labelKeys", result);

        // Also test with specific labelKeys
        result = stage.extractGroupLabels(seriesWithNullLabels, List.of("service"));
        assertNull("extractGroupLabels should return null when series labels are null, even with specific labelKeys", result);
    }

    /**
     * Test extractGroupLabels with null labelKeys - should return all labels as ByteLabels.
     */
    public void testExtractGroupLabelsWithNullLabelKeys() {
        SubtractStage stage = new SubtractStage("right_series");
        ByteLabels originalLabels = ByteLabels.fromStrings("service", "api", "region", "us-east");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries series = new TimeSeries(samples, originalLabels, 1000L, 1000L, 1000L, null);

        ByteLabels result = stage.extractGroupLabels(series, null);
        assertNotNull("extractGroupLabels should return ByteLabels when labelKeys is null", result);
        assertEquals("api", result.get("service"));
        assertEquals("us-east", result.get("region"));
        // Should return the same instance since it's already ByteLabels
        assertSame("Should return the same ByteLabels instance when already ByteLabels", originalLabels, result);
    }

    /**
     * Test extractGroupLabels with empty labelKeys - should return all labels as ByteLabels.
     */
    public void testExtractGroupLabelsWithEmptyLabelKeys() {
        SubtractStage stage = new SubtractStage("right_series");
        ByteLabels originalLabels = ByteLabels.fromStrings("service", "api", "region", "us-east");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries series = new TimeSeries(samples, originalLabels, 1000L, 1000L, 1000L, null);

        ByteLabels result = stage.extractGroupLabels(series, List.of());
        assertNotNull("extractGroupLabels should return ByteLabels when labelKeys is empty", result);
        assertEquals("api", result.get("service"));
        assertEquals("us-east", result.get("region"));
        // Should return the same instance since it's already ByteLabels
        assertSame("Should return the same ByteLabels instance when already ByteLabels", originalLabels, result);
    }

    /**
     * Test extractGroupLabels with missing required label key - should return null.
     * This tests the case where labelKeys is specified but one of the required labels is missing.
     */
    public void testExtractGroupLabelsWithMissingRequiredLabel() {
        SubtractStage stage = new SubtractStage("right_series");
        ByteLabels labels = ByteLabels.fromStrings("service", "api", "region", "us-east");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        // Request a label key that doesn't exist in the series
        ByteLabels result = stage.extractGroupLabels(series, List.of("missing_label"));
        assertNull("extractGroupLabels should return null when required label key is missing", result);
    }
}
