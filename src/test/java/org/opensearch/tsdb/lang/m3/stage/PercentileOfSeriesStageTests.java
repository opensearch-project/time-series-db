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
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.MultiValueSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import static org.opensearch.tsdb.TestUtils.findSeriesByLabel;
import static org.opensearch.tsdb.TestUtils.findSeriesWithLabels;

public class PercentileOfSeriesStageTests extends AbstractWireSerializingTestCase<PercentileOfSeriesStage> {

    /**
     * Test percentile calculation across multiple time series with varying sparseness.
     * This test verifies:
     * - Multiple percentiles (0, 30, 50, 90, 95, 99, 99.5, 100)
     * - Dense and sparse time series
     * - Correct percentile calculation at each timestamp
     * - Decimal percentile label formatting (99.5 should keep decimal, 99 should not)
     */
    public void testPercentileOfSeries() {
        PercentileOfSeriesStage stage = new PercentileOfSeriesStage(
            List.of(0.0f, 30.0f, 50.0f, 90.0f, 95.0f, 99.0f, 99.5f, 100.0f),
            false // no interpolation
        );

        // Server1: present at all timestamps
        List<Sample> server1Samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 100.0), new FloatSample(3000L, 15.0));

        // Server2: present at all timestamps
        List<Sample> server2Samples = List.of(new FloatSample(1000L, 20.0), new FloatSample(2000L, 200.0), new FloatSample(3000L, 25.0));

        // Server3: present at all timestamps
        List<Sample> server3Samples = List.of(new FloatSample(1000L, 30.0), new FloatSample(2000L, 300.0), new FloatSample(3000L, 35.0));

        // Server4: missing at timestamp 3000 (sparse data)
        List<Sample> server4Samples = List.of(new FloatSample(1000L, 40.0), new FloatSample(2000L, 400.0));

        // Server5: missing at timestamps 2000 and 3000 (very sparse data)
        List<Sample> server5Samples = List.of(new FloatSample(1000L, 50.0));

        ByteLabels labels1 = ByteLabels.fromStrings("instance", "server1");
        ByteLabels labels2 = ByteLabels.fromStrings("instance", "server2");
        ByteLabels labels3 = ByteLabels.fromStrings("instance", "server3");
        ByteLabels labels4 = ByteLabels.fromStrings("instance", "server4");
        ByteLabels labels5 = ByteLabels.fromStrings("instance", "server5");

        TimeSeries series1 = new TimeSeries(server1Samples, labels1, 1000L, 3000L, 1000L, null);
        TimeSeries series2 = new TimeSeries(server2Samples, labels2, 1000L, 3000L, 1000L, null);
        TimeSeries series3 = new TimeSeries(server3Samples, labels3, 1000L, 3000L, 1000L, null);
        TimeSeries series4 = new TimeSeries(server4Samples, labels4, 1000L, 3000L, 1000L, null);
        TimeSeries series5 = new TimeSeries(server5Samples, labels5, 1000L, 3000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(series1, series2, series3, series4, series5));

        // Should have 8 output series (one per percentile: 0, 30, 50, 90, 95, 99, 99.5, 100)
        assertEquals("Should have 8 percentile series", 8, result.size());

        // Find each percentile series by label
        TimeSeries p0Series = findSeriesByLabel(result, "__percentile", "0");
        TimeSeries p30Series = findSeriesByLabel(result, "__percentile", "30");
        TimeSeries p50Series = findSeriesByLabel(result, "__percentile", "50");
        TimeSeries p90Series = findSeriesByLabel(result, "__percentile", "90");
        TimeSeries p95Series = findSeriesByLabel(result, "__percentile", "95");
        TimeSeries p99Series = findSeriesByLabel(result, "__percentile", "99");
        TimeSeries p99_5Series = findSeriesByLabel(result, "__percentile", "99.5");
        TimeSeries p100Series = findSeriesByLabel(result, "__percentile", "100");

        // Verify 0th percentile (minimum)
        // t=1000: [10,20,30,40,50] -> fractionalRank=0.0*5=0.0, ceil=0, <=1 -> 10
        // t=2000: [100,200,300,400] -> fractionalRank=0.0*4=0.0, ceil=0, <=1 -> 100
        // t=3000: [15,25,35] -> fractionalRank=0.0*3=0.0, ceil=0, <=1 -> 15
        List<Sample> expectedP0 = List.of(new FloatSample(1000L, 10.0f), new FloatSample(2000L, 100.0f), new FloatSample(3000L, 15.0f));
        assertSamplesEqual("0th percentile (minimum)", expectedP0, p0Series.getSamples().toList());

        // Verify 30th percentile
        // t=1000: [10,20,30,40,50] -> fractionalRank=0.3*5=1.5, ceil=2, index=1 -> 20
        // t=2000: [100,200,300,400] -> fractionalRank=0.3*4=1.2, ceil=2, index=1 -> 200
        // t=3000: [15,25,35] -> fractionalRank=0.3*3=0.9, ceil=1, <=1 -> 15
        List<Sample> expectedP30 = List.of(new FloatSample(1000L, 20.0f), new FloatSample(2000L, 200.0f), new FloatSample(3000L, 15.0f));
        assertSamplesEqual("30th percentile", expectedP30, p30Series.getSamples().toList());

        // Verify 50th percentile (median)
        // t=1000: [10,20,30,40,50] -> fractionalRank=0.5*5=2.5, ceil=3, index=2 -> 30
        // t=2000: [100,200,300,400] -> fractionalRank=0.5*4=2.0, ceil=2, index=1 -> 200
        // t=3000: [15,25,35] -> fractionalRank=0.5*3=1.5, ceil=2, index=1 -> 25
        List<Sample> expectedP50 = List.of(new FloatSample(1000L, 30.0f), new FloatSample(2000L, 200.0f), new FloatSample(3000L, 25.0f));
        assertSamplesEqual("50th percentile (median)", expectedP50, p50Series.getSamples().toList());

        // Verify 90th percentile
        // t=1000: [10,20,30,40,50] -> fractionalRank=0.9*5=4.5, ceil=5, index=4 -> 50
        // t=2000: [100,200,300,400] -> fractionalRank=0.9*4=3.6, ceil=4, index=3 -> 400
        // t=3000: [15,25,35] -> fractionalRank=0.9*3=2.7, ceil=3, index=2 -> 35
        List<Sample> expectedP90 = List.of(new FloatSample(1000L, 50.0f), new FloatSample(2000L, 400.0f), new FloatSample(3000L, 35.0f));
        assertSamplesEqual("90th percentile", expectedP90, p90Series.getSamples().toList());

        // Verify 95th percentile
        // t=1000: [10,20,30,40,50] -> fractionalRank=0.95*5=4.75, ceil=5, index=4 -> 50
        // t=2000: [100,200,300,400] -> fractionalRank=0.95*4=3.8, ceil=4, index=3 -> 400
        // t=3000: [15,25,35] -> fractionalRank=0.95*3=2.85, ceil=3, index=2 -> 35
        List<Sample> expectedP95 = List.of(new FloatSample(1000L, 50.0f), new FloatSample(2000L, 400.0f), new FloatSample(3000L, 35.0f));
        assertSamplesEqual("95th percentile", expectedP95, p95Series.getSamples().toList());

        // Verify 99th percentile
        // t=1000: [10,20,30,40,50] -> fractionalRank=0.99*5=4.95, ceil=5, index=4 -> 50
        // t=2000: [100,200,300,400] -> fractionalRank=0.99*4=3.96, ceil=4, index=3 -> 400
        // t=3000: [15,25,35] -> fractionalRank=0.99*3=2.97, ceil=3, index=2 -> 35
        List<Sample> expectedP99 = List.of(new FloatSample(1000L, 50.0f), new FloatSample(2000L, 400.0f), new FloatSample(3000L, 35.0f));
        assertSamplesEqual("99th percentile", expectedP99, p99Series.getSamples().toList());

        // Verify 99.5th percentile (validates decimal percentile label formatting)
        // t=1000: [10,20,30,40,50] -> fractionalRank=0.995*5=4.975, ceil=5, index=4 -> 50
        // t=2000: [100,200,300,400] -> fractionalRank=0.995*4=3.98, ceil=4, index=3 -> 400
        // t=3000: [15,25,35] -> fractionalRank=0.995*3=2.985, ceil=3, index=2 -> 35
        List<Sample> expectedP99_5 = List.of(new FloatSample(1000L, 50.0f), new FloatSample(2000L, 400.0f), new FloatSample(3000L, 35.0f));
        assertSamplesEqual("99.5th percentile", expectedP99_5, p99_5Series.getSamples().toList());

        // Verify 100th percentile (maximum)
        // t=1000: [10,20,30,40,50] -> fractionalRank=1.0*5=5.0, ceil=5, index=4 -> 50
        // t=2000: [100,200,300,400] -> fractionalRank=1.0*4=4.0, ceil=4, index=3 -> 400
        // t=3000: [15,25,35] -> fractionalRank=1.0*3=3.0, ceil=3, index=2 -> 35
        List<Sample> expectedP100 = List.of(new FloatSample(1000L, 50.0f), new FloatSample(2000L, 400.0f), new FloatSample(3000L, 35.0f));
        assertSamplesEqual("100th percentile (maximum)", expectedP100, p100Series.getSamples().toList());
    }

    /**
     * Test percentile calculation with a simple two-series case.
     */
    public void testPercentileWithTwoSeries() {
        PercentileOfSeriesStage stage = new PercentileOfSeriesStage(List.of(50.0f), false);

        List<Sample> series1Samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 30.0));

        List<Sample> series2Samples = List.of(new FloatSample(1000L, 20.0), new FloatSample(2000L, 40.0));

        ByteLabels labels1 = ByteLabels.fromStrings("host", "h1");
        ByteLabels labels2 = ByteLabels.fromStrings("host", "h2");

        TimeSeries series1 = new TimeSeries(series1Samples, labels1, 1000L, 2000L, 1000L, null);
        TimeSeries series2 = new TimeSeries(series2Samples, labels2, 1000L, 2000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(series1, series2));

        assertEquals(1, result.size());
        TimeSeries p50Series = result.get(0);

        // t=1000: [10,20] -> median at index 0 (ceil(0.5*2)=1, index=0) -> 10
        // t=2000: [30,40] -> median at index 0 (ceil(0.5*2)=1, index=0) -> 30
        List<Sample> expected = List.of(new FloatSample(1000L, 10.0f), new FloatSample(2000L, 30.0f));
        assertSamplesEqual("50th percentile", expected, p50Series.getSamples().toList());
    }

    /**
     * Test percentile with empty input.
     */
    public void testPercentileWithEmptyInput() {
        PercentileOfSeriesStage stage = new PercentileOfSeriesStage(List.of(50.0f), false);
        List<TimeSeries> result = stage.process(List.of());
        assertEquals(0, result.size());
    }

    /**
     * Test invalid percentile values.
     */
    public void testInvalidPercentiles() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PercentileOfSeriesStage(List.of(-10.0f), false)
        );
        assertTrue(exception.getMessage().contains("must be between 0 and 100"));

        exception = assertThrows(IllegalArgumentException.class, () -> new PercentileOfSeriesStage(List.of(150.0f), false));
        assertTrue(exception.getMessage().contains("must be between 0 and 100"));
    }

    /**
     * Test empty percentiles list.
     */
    public void testEmptyPercentilesList() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new PercentileOfSeriesStage(List.of(), false)
        );
        assertTrue(exception.getMessage().contains("cannot be null or empty"));
    }

    /**
     * Test XContent serialization.
     */
    public void testToXContent() throws IOException {
        PercentileOfSeriesStage stage = new PercentileOfSeriesStage(List.of(50.0f, 95.0f, 99.0f), true);

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            stage.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();

            String json = builder.toString();
            assertTrue(json.contains("\"percentiles\":[50.0,95.0,99.0]"));
            assertTrue(json.contains("\"interpolate\":true"));
        }
    }

    /**
     * Test factory creation from args.
     */
    public void testFromArgs() {
        Map<String, Object> args = Map.of("percentiles", List.of(50, 95, 99), "interpolate", false);

        PipelineStage stage = PipelineStageFactory.createWithArgs("percentile_of_series", args);
        assertNotNull(stage);
        assertTrue(stage instanceof PercentileOfSeriesStage);
        assertEquals("percentile_of_series", stage.getName());
    }

    /**
     * Test factory creation without required parameter.
     */
    public void testFromArgsMissingParameter() {
        Map<String, Object> args = Map.of("interpolate", false);
        expectThrows(IllegalArgumentException.class, () -> PercentileOfSeriesStage.fromArgs(args));
    }

    /**
     * Test fromArgs with null args.
     */
    public void testFromArgsNullArgs() {
        expectThrows(IllegalArgumentException.class, () -> PercentileOfSeriesStage.fromArgs(null));
    }

    /**
     * Test fromArgs with null percentiles value.
     */
    public void testFromArgsNullPercentiles() {
        Map<String, Object> args = new java.util.HashMap<>();
        args.put("percentiles", null);
        expectThrows(IllegalArgumentException.class, () -> PercentileOfSeriesStage.fromArgs(args));
    }

    /**
     * Test fromArgs with percentiles not a list.
     */
    public void testFromArgsPercentilesNotList() {
        Map<String, Object> args = Map.of("percentiles", "50,95");
        expectThrows(IllegalArgumentException.class, () -> PercentileOfSeriesStage.fromArgs(args));
    }

    /**
     * Test fromArgs with percentiles list containing null.
     */
    public void testFromArgsPercentilesContainsNull() {
        Map<String, Object> args = new java.util.HashMap<>();
        List<Object> percentiles = new java.util.ArrayList<>();
        percentiles.add(50);
        percentiles.add(null);
        percentiles.add(95);
        args.put("percentiles", percentiles);
        expectThrows(IllegalArgumentException.class, () -> PercentileOfSeriesStage.fromArgs(args));
    }

    /**
     * Test fromArgs with percentiles list containing non-number.
     */
    public void testFromArgsPercentilesContainsNonNumber() {
        Map<String, Object> args = Map.of("percentiles", List.of(50, "invalid", 95));
        expectThrows(IllegalArgumentException.class, () -> PercentileOfSeriesStage.fromArgs(args));
    }

    /**
     * Test fromArgs with empty percentiles list.
     */
    public void testFromArgsPercentilesEmpty() {
        Map<String, Object> args = Map.of("percentiles", List.of());
        expectThrows(IllegalArgumentException.class, () -> PercentileOfSeriesStage.fromArgs(args));
    }

    /**
     * Test fromArgs with percentile value < 0.
     */
    public void testFromArgsPercentileBelowRange() {
        Map<String, Object> args = Map.of("percentiles", List.of(-1, 50, 95));
        expectThrows(IllegalArgumentException.class, () -> PercentileOfSeriesStage.fromArgs(args));
    }

    /**
     * Test fromArgs with percentile value > 100.
     */
    public void testFromArgsPercentileAboveRange() {
        Map<String, Object> args = Map.of("percentiles", List.of(50, 95, 101));
        expectThrows(IllegalArgumentException.class, () -> PercentileOfSeriesStage.fromArgs(args));
    }

    /**
     * Test fromArgs with interpolate not a boolean.
     */
    public void testFromArgsInterpolateNotBoolean() {
        Map<String, Object> args = Map.of("percentiles", List.of(50, 95), "interpolate", "true");
        expectThrows(IllegalArgumentException.class, () -> PercentileOfSeriesStage.fromArgs(args));
    }

    /**
     * Test fromArgs with interpolate=true.
     */
    public void testFromArgsWithInterpolate() {
        Map<String, Object> args = Map.of("percentiles", List.of(50, 95), "interpolate", true);

        PercentileOfSeriesStage stage = PercentileOfSeriesStage.fromArgs(args);
        assertNotNull(stage);
        assertEquals("percentile_of_series", stage.getName());
    }

    /**
     * Test fromArgs without interpolate (should default to false).
     */
    public void testFromArgsWithoutInterpolate() {
        Map<String, Object> args = Map.of("percentiles", List.of(50, 95));

        PercentileOfSeriesStage stage = PercentileOfSeriesStage.fromArgs(args);
        assertNotNull(stage);
        assertEquals("percentile_of_series", stage.getName());
    }

    /**
     * Test fromArgs with group_by_labels as a single string.
     */
    public void testFromArgsGroupByLabelsSingleString() {
        Map<String, Object> args = Map.of("percentiles", List.of(50, 95), "group_by_labels", "region");

        PercentileOfSeriesStage stage = PercentileOfSeriesStage.fromArgs(args);
        assertNotNull(stage);
        assertEquals("percentile_of_series", stage.getName());
    }

    /**
     * Test fromArgs with group_by_labels as empty list.
     */
    public void testFromArgsGroupByLabelsEmpty() {
        Map<String, Object> args = Map.of("percentiles", List.of(50, 95), "group_by_labels", List.of());

        PercentileOfSeriesStage stage = PercentileOfSeriesStage.fromArgs(args);
        assertNotNull(stage);
        assertEquals("percentile_of_series", stage.getName());
    }

    /**
     * Test fromArgs with group_by_labels as invalid type.
     */
    public void testFromArgsGroupByLabelsInvalidType() {
        Map<String, Object> args = Map.of("percentiles", List.of(50, 95), "group_by_labels", 123);
        expectThrows(IllegalArgumentException.class, () -> PercentileOfSeriesStage.fromArgs(args));
    }

    /**
     * Test fromArgs with all parameters specified.
     */
    public void testFromArgsAllParameters() {
        Map<String, Object> args = Map.of(
            "percentiles",
            List.of(10, 50, 90, 99),
            "interpolate",
            true,
            "group_by_labels",
            List.of("region", "env", "service")
        );

        PercentileOfSeriesStage stage = PercentileOfSeriesStage.fromArgs(args);
        assertNotNull(stage);
        assertEquals("percentile_of_series", stage.getName());
    }

    @Override
    protected Writeable.Reader<PercentileOfSeriesStage> instanceReader() {
        return PercentileOfSeriesStage::readFrom;
    }

    @Override
    protected PercentileOfSeriesStage createTestInstance() {
        int numPercentiles = randomIntBetween(1, 5);
        List<Float> percentiles = new java.util.ArrayList<>();
        for (int i = 0; i < numPercentiles; i++) {
            percentiles.add(randomFloat() * 100);
        }
        boolean interpolate = randomBoolean();

        // Randomly include groupByLabels to test serialization with and without grouping
        if (randomBoolean()) {
            int numLabels = randomIntBetween(1, 3);
            List<String> groupByLabels = new java.util.ArrayList<>();
            for (int i = 0; i < numLabels; i++) {
                groupByLabels.add(randomAlphaOfLength(5));
            }
            return new PercentileOfSeriesStage(percentiles, interpolate, groupByLabels);
        } else {
            return new PercentileOfSeriesStage(percentiles, interpolate);
        }
    }

    /**
     * Test percentile calculation with interpolation and grouping by multiple labels.
     */
    public void testPercentileOfSeriesWithInterpolationAndGrouping() {
        // Create stage with interpolation enabled and grouping by "region" and "env"
        PercentileOfSeriesStage stage = new PercentileOfSeriesStage(
            List.of(0.0f, 30.0f, 50.0f, 70.0f, 90.0f, 100.0f),
            true, // interpolation enabled
            List.of("region", "env")
        );

        // Group 1: region=us-west, env=prod (3 series)
        List<Sample> usWestProd1 = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 100.0));
        List<Sample> usWestProd2 = List.of(new FloatSample(1000L, 20.0), new FloatSample(2000L, 200.0));
        List<Sample> usWestProd3 = List.of(new FloatSample(1000L, 30.0), new FloatSample(2000L, 300.0));

        // Group 2: region=us-west, env=dev (2 series)
        List<Sample> usWestDev1 = List.of(new FloatSample(1000L, 40.0), new FloatSample(2000L, 400.0));
        List<Sample> usWestDev2 = List.of(new FloatSample(1000L, 50.0), new FloatSample(2000L, 500.0));

        // Group 3: region=us-east, env=prod (3 series)
        List<Sample> usEastProd1 = List.of(new FloatSample(1000L, 5.0), new FloatSample(2000L, 50.0));
        List<Sample> usEastProd2 = List.of(new FloatSample(1000L, 15.0), new FloatSample(2000L, 150.0));
        List<Sample> usEastProd3 = List.of(new FloatSample(1000L, 25.0), new FloatSample(2000L, 250.0));

        TimeSeries ts1 = new TimeSeries(
            usWestProd1,
            ByteLabels.fromStrings("region", "us-west", "env", "prod", "host", "h1"),
            1000L,
            2000L,
            1000L,
            null
        );
        TimeSeries ts2 = new TimeSeries(
            usWestProd2,
            ByteLabels.fromStrings("region", "us-west", "env", "prod", "host", "h2"),
            1000L,
            2000L,
            1000L,
            null
        );
        TimeSeries ts3 = new TimeSeries(
            usWestProd3,
            ByteLabels.fromStrings("region", "us-west", "env", "prod", "host", "h3"),
            1000L,
            2000L,
            1000L,
            null
        );
        TimeSeries ts4 = new TimeSeries(
            usWestDev1,
            ByteLabels.fromStrings("region", "us-west", "env", "dev", "host", "h4"),
            1000L,
            2000L,
            1000L,
            null
        );
        TimeSeries ts5 = new TimeSeries(
            usWestDev2,
            ByteLabels.fromStrings("region", "us-west", "env", "dev", "host", "h5"),
            1000L,
            2000L,
            1000L,
            null
        );
        TimeSeries ts6 = new TimeSeries(
            usEastProd1,
            ByteLabels.fromStrings("region", "us-east", "env", "prod", "host", "h6"),
            1000L,
            2000L,
            1000L,
            null
        );
        TimeSeries ts7 = new TimeSeries(
            usEastProd2,
            ByteLabels.fromStrings("region", "us-east", "env", "prod", "host", "h7"),
            1000L,
            2000L,
            1000L,
            null
        );
        TimeSeries ts8 = new TimeSeries(
            usEastProd3,
            ByteLabels.fromStrings("region", "us-east", "env", "prod", "host", "h8"),
            1000L,
            2000L,
            1000L,
            null
        );

        List<TimeSeries> result = stage.process(List.of(ts1, ts2, ts3, ts4, ts5, ts6, ts7, ts8));

        // Should have 3 groups * 6 percentiles = 18 output series
        assertEquals("Should have 18 series (3 groups * 6 percentiles)", 18, result.size());

        // ========== Group 1: region=us-west, env=prod (values: [10,20,30]) ==========

        // 0th percentile: fractionalRank=0.0*3=0.0, ceil=0, rankAsInt=0, <=1 -> first element
        List<TimeSeries> group1P0List = findSeriesWithLabels(result, Map.of("region", "us-west", "env", "prod", "__percentile", "0"));
        assertEquals(1, group1P0List.size());
        assertSamplesEqual(
            "Group1 P0",
            List.of(new FloatSample(1000L, 10.0f), new FloatSample(2000L, 100.0f)),
            group1P0List.get(0).getSamples().toList()
        );

        // 30th percentile: fractionalRank=0.3*3=0.9, ceil=1, rankAsInt=1, <=1 -> first element (no interpolation possible)
        List<TimeSeries> group1P30List = findSeriesWithLabels(result, Map.of("region", "us-west", "env", "prod", "__percentile", "30"));
        assertEquals(1, group1P30List.size());
        assertSamplesEqual(
            "Group1 P30",
            List.of(new FloatSample(1000L, 10.0f), new FloatSample(2000L, 100.0f)),
            group1P30List.get(0).getSamples().toList()
        );

        // 50th percentile: fractionalRank=0.5*3=1.5, ceil=2, rankAsInt=2, index=1 -> 20
        // Interpolation: prevValue=10 (index=0), fraction=1.5-1=0.5 -> 10 + 0.5*(20-10) = 15
        List<TimeSeries> group1P50List = findSeriesWithLabels(result, Map.of("region", "us-west", "env", "prod", "__percentile", "50"));
        assertEquals(1, group1P50List.size());
        assertSamplesEqual(
            "Group1 P50",
            List.of(new FloatSample(1000L, 15.0f), new FloatSample(2000L, 150.0f)),
            group1P50List.get(0).getSamples().toList()
        );

        // 70th percentile: fractionalRank=0.7*3=2.1, ceil=3, rankAsInt=3, index=2 -> 30
        // Interpolation: prevValue=20 (index=1), fraction=2.1-2=0.1 -> 20 + 0.1*(30-20) = 21
        List<TimeSeries> group1P70List = findSeriesWithLabels(result, Map.of("region", "us-west", "env", "prod", "__percentile", "70"));
        assertEquals(1, group1P70List.size());
        assertSamplesEqual(
            "Group1 P70",
            List.of(new FloatSample(1000L, 21.0f), new FloatSample(2000L, 210.0f)),
            group1P70List.get(0).getSamples().toList()
        );

        // 90th percentile: fractionalRank=0.9*3=2.7, ceil=3, rankAsInt=3, index=2 -> 30
        // Interpolation: prevValue=20 (index=1), fraction=2.7-2=0.7 -> 20 + 0.7*(30-20) = 27
        List<TimeSeries> group1P90List = findSeriesWithLabels(result, Map.of("region", "us-west", "env", "prod", "__percentile", "90"));
        assertEquals(1, group1P90List.size());
        assertSamplesEqual(
            "Group1 P90",
            List.of(new FloatSample(1000L, 27.0f), new FloatSample(2000L, 270.0f)),
            group1P90List.get(0).getSamples().toList()
        );

        // 100th percentile: fractionalRank=1.0*3=3.0, ceil=3, rankAsInt=3, index=2 -> 30
        // Interpolation: prevValue=20 (index=1), fraction=3.0-2=1.0 -> 20 + 1.0*(30-20) = 30
        List<TimeSeries> group1P100List = findSeriesWithLabels(result, Map.of("region", "us-west", "env", "prod", "__percentile", "100"));
        assertEquals(1, group1P100List.size());
        assertSamplesEqual(
            "Group1 P100",
            List.of(new FloatSample(1000L, 30.0f), new FloatSample(2000L, 300.0f)),
            group1P100List.get(0).getSamples().toList()
        );

        // ========== Group 2: region=us-west, env=dev (values: [40,50]) ==========

        // 0th percentile: fractionalRank=0.0*2=0.0, ceil=0, rankAsInt=0, <=1 -> first element
        List<TimeSeries> group2P0List = findSeriesWithLabels(result, Map.of("region", "us-west", "env", "dev", "__percentile", "0"));
        assertEquals(1, group2P0List.size());
        assertSamplesEqual(
            "Group2 P0",
            List.of(new FloatSample(1000L, 40.0f), new FloatSample(2000L, 400.0f)),
            group2P0List.get(0).getSamples().toList()
        );

        // 30th percentile: fractionalRank=0.3*2=0.6, ceil=1, rankAsInt=1, <=1 -> first element
        List<TimeSeries> group2P30List = findSeriesWithLabels(result, Map.of("region", "us-west", "env", "dev", "__percentile", "30"));
        assertEquals(1, group2P30List.size());
        assertSamplesEqual(
            "Group2 P30",
            List.of(new FloatSample(1000L, 40.0f), new FloatSample(2000L, 400.0f)),
            group2P30List.get(0).getSamples().toList()
        );

        // 50th percentile: fractionalRank=0.5*2=1.0, ceil=1, rankAsInt=1, <=1 -> first element
        List<TimeSeries> group2P50List = findSeriesWithLabels(result, Map.of("region", "us-west", "env", "dev", "__percentile", "50"));
        assertEquals(1, group2P50List.size());
        assertSamplesEqual(
            "Group2 P50",
            List.of(new FloatSample(1000L, 40.0f), new FloatSample(2000L, 400.0f)),
            group2P50List.get(0).getSamples().toList()
        );

        // 70th percentile: fractionalRank=0.7*2=1.4, ceil=2, rankAsInt=2, index=1 -> 50
        // Interpolation: prevValue=40 (index=0), fraction=1.4-1=0.4 -> 40 + 0.4*(50-40) = 44
        List<TimeSeries> group2P70List = findSeriesWithLabels(result, Map.of("region", "us-west", "env", "dev", "__percentile", "70"));
        assertEquals(1, group2P70List.size());
        assertSamplesEqual(
            "Group2 P70",
            List.of(new FloatSample(1000L, 44.0f), new FloatSample(2000L, 440.0f)),
            group2P70List.get(0).getSamples().toList()
        );

        // 90th percentile: fractionalRank=0.9*2=1.8, ceil=2, rankAsInt=2, index=1 -> 50
        // Interpolation: prevValue=40 (index=0), fraction=1.8-1=0.8 -> 40 + 0.8*(50-40) = 48
        List<TimeSeries> group2P90List = findSeriesWithLabels(result, Map.of("region", "us-west", "env", "dev", "__percentile", "90"));
        assertEquals(1, group2P90List.size());
        assertSamplesEqual(
            "Group2 P90",
            List.of(new FloatSample(1000L, 48.0f), new FloatSample(2000L, 480.0f)),
            group2P90List.get(0).getSamples().toList()
        );

        // 100th percentile: fractionalRank=1.0*2=2.0, ceil=2, rankAsInt=2, index=1 -> 50
        // Interpolation: prevValue=40 (index=0), fraction=2.0-1=1.0 -> 40 + 1.0*(50-40) = 50
        List<TimeSeries> group2P100List = findSeriesWithLabels(result, Map.of("region", "us-west", "env", "dev", "__percentile", "100"));
        assertEquals(1, group2P100List.size());
        assertSamplesEqual(
            "Group2 P100",
            List.of(new FloatSample(1000L, 50.0f), new FloatSample(2000L, 500.0f)),
            group2P100List.get(0).getSamples().toList()
        );

        // ========== Group 3: region=us-east, env=prod (values: [5,15,25]) ==========

        // 0th percentile: fractionalRank=0.0*3=0.0, ceil=0, rankAsInt=0, <=1 -> first element
        List<TimeSeries> group3P0List = findSeriesWithLabels(result, Map.of("region", "us-east", "env", "prod", "__percentile", "0"));
        assertEquals(1, group3P0List.size());
        assertSamplesEqual(
            "Group3 P0",
            List.of(new FloatSample(1000L, 5.0f), new FloatSample(2000L, 50.0f)),
            group3P0List.get(0).getSamples().toList()
        );

        // 30th percentile: fractionalRank=0.3*3=0.9, ceil=1, rankAsInt=1, <=1 -> first element
        List<TimeSeries> group3P30List = findSeriesWithLabels(result, Map.of("region", "us-east", "env", "prod", "__percentile", "30"));
        assertEquals(1, group3P30List.size());
        assertSamplesEqual(
            "Group3 P30",
            List.of(new FloatSample(1000L, 5.0f), new FloatSample(2000L, 50.0f)),
            group3P30List.get(0).getSamples().toList()
        );

        // 50th percentile: fractionalRank=0.5*3=1.5, ceil=2, rankAsInt=2, index=1 -> 15
        // Interpolation: prevValue=5 (index=0), fraction=1.5-1=0.5 -> 5 + 0.5*(15-5) = 10
        List<TimeSeries> group3P50List = findSeriesWithLabels(result, Map.of("region", "us-east", "env", "prod", "__percentile", "50"));
        assertEquals(1, group3P50List.size());
        assertSamplesEqual(
            "Group3 P50",
            List.of(new FloatSample(1000L, 10.0f), new FloatSample(2000L, 100.0f)),
            group3P50List.get(0).getSamples().toList()
        );

        // 70th percentile: fractionalRank=0.7*3=2.1, ceil=3, rankAsInt=3, index=2 -> 25
        // Interpolation: prevValue=15 (index=1), fraction=2.1-2=0.1 -> 15 + 0.1*(25-15) = 16
        List<TimeSeries> group3P70List = findSeriesWithLabels(result, Map.of("region", "us-east", "env", "prod", "__percentile", "70"));
        assertEquals(1, group3P70List.size());
        assertSamplesEqual(
            "Group3 P70",
            List.of(new FloatSample(1000L, 16.0f), new FloatSample(2000L, 160.0f)),
            group3P70List.get(0).getSamples().toList()
        );

        // 90th percentile: fractionalRank=0.9*3=2.7, ceil=3, rankAsInt=3, index=2 -> 25
        // Interpolation: prevValue=15 (index=1), fraction=2.7-2=0.7 -> 15 + 0.7*(25-15) = 22
        List<TimeSeries> group3P90List = findSeriesWithLabels(result, Map.of("region", "us-east", "env", "prod", "__percentile", "90"));
        assertEquals(1, group3P90List.size());
        assertSamplesEqual(
            "Group3 P90",
            List.of(new FloatSample(1000L, 22.0f), new FloatSample(2000L, 220.0f)),
            group3P90List.get(0).getSamples().toList()
        );

        // 100th percentile: fractionalRank=1.0*3=3.0, ceil=3, rankAsInt=3, index=2 -> 25
        // Interpolation: prevValue=15 (index=1), fraction=3.0-2=1.0 -> 15 + 1.0*(25-15) = 25
        List<TimeSeries> group3P100List = findSeriesWithLabels(result, Map.of("region", "us-east", "env", "prod", "__percentile", "100"));
        assertEquals(1, group3P100List.size());
        assertSamplesEqual(
            "Group3 P100",
            List.of(new FloatSample(1000L, 25.0f), new FloatSample(2000L, 250.0f)),
            group3P100List.get(0).getSamples().toList()
        );
    }

    /**
     * Test that PercentileOfSeriesStage can be serialized to JSON (toXContent) and
     * deserialized back (fromArgs) without losing information.
     *
     * <p>This stage uses array fields (percentiles) and boolean fields (interpolate),
     * so this test ensures they are properly serialized and deserialized.</p>
     */
    public void testXContentDeserialization() throws java.io.IOException {
        // Create original stage
        List<Float> percentiles = List.of(50.0f, 95.0f, 99.0f);
        List<String> groupByLabels = List.of("host", "region");
        PercentileOfSeriesStage original = new PercentileOfSeriesStage(percentiles, true, groupByLabels);

        // Serialize to JSON and parse back to args Map
        java.util.Map<String, Object> args = PipelineStageTestUtils.serializeToArgs(original);

        // Deserialize from args
        PercentileOfSeriesStage deserialized = PercentileOfSeriesStage.fromArgs(args);

        // Verify round-trip preserves all fields
        assertEquals("Stage names should match", original.getName(), deserialized.getName());
        assertEquals("Stages should be equal after round-trip", original, deserialized);
    }

    /**
     * Test process with coordinatorExecution=false returns grouped series (MultiValueSample) without percentile expansion.
     */
    public void testProcessAsNonCoordinator() {
        PercentileOfSeriesStage stage = new PercentileOfSeriesStage(List.of(50.0f, 95.0f), false);

        List<Sample> s1 = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));
        List<Sample> s2 = List.of(new FloatSample(1000L, 30.0), new FloatSample(2000L, 40.0));
        TimeSeries ts1 = new TimeSeries(s1, ByteLabels.fromStrings("host", "h1"), 1000L, 2000L, 1000L, null);
        TimeSeries ts2 = new TimeSeries(s2, ByteLabels.fromStrings("host", "h2"), 1000L, 2000L, 1000L, null);

        List<TimeSeries> result = stage.processWithContext(List.of(ts1, ts2), false, null);

        assertEquals(1, result.size());
        TimeSeries grouped = result.get(0);
        List<Sample> samples = grouped.getSamples().toList();
        assertEquals(2, samples.size());
        assertTrue(samples.get(0) instanceof MultiValueSample);
        assertTrue(samples.get(1) instanceof MultiValueSample);
    }

    /**
     * Test reduce with null or empty aggregations throws.
     */
    public void testReduceEmptyOrNullAggregations() {
        PercentileOfSeriesStage stage = new PercentileOfSeriesStage(List.of(50.0f), false);

        assertThrows(IllegalArgumentException.class, () -> stage.reduce(Collections.emptyList(), true));
        assertThrows(IllegalArgumentException.class, () -> stage.reduce(null, true));
    }

    /**
     * Test reduce with isFinalReduce=false returns merged MultiValueSample series without expansion.
     */
    public void testReduceIntermediate() {
        PercentileOfSeriesStage stage = new PercentileOfSeriesStage(List.of(50.0f, 95.0f), false);

        List<Sample> shard1 = List.of(new MultiValueSample(1000L, List.of(10.0, 20.0)), new MultiValueSample(2000L, List.of(30.0, 40.0)));
        List<Sample> shard2 = List.of(new MultiValueSample(1000L, List.of(50.0)), new MultiValueSample(2000L, List.of(60.0)));
        TimeSeries ts1 = new TimeSeries(shard1, ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null);
        TimeSeries ts2 = new TimeSeries(shard2, ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null);
        List<TimeSeriesProvider> aggregations = List.of(
            new InternalTimeSeries("a", List.of(ts1), Map.of()),
            new InternalTimeSeries("b", List.of(ts2), Map.of())
        );

        InternalAggregation result = stage.reduce(aggregations, false);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        List<TimeSeries> series = ((TimeSeriesProvider) result).getTimeSeries();
        assertEquals(1, series.size());
        List<Sample> samples = series.get(0).getSamples().toList();
        assertEquals(2, samples.size());
        assertTrue(samples.get(0) instanceof MultiValueSample);
        assertEquals(List.of(10.0, 20.0, 50.0), ((MultiValueSample) samples.get(0)).getValueList());
    }

    /**
     * Test reduce with isFinalReduce=true expands to percentile series.
     */
    public void testReduceFinal() {
        PercentileOfSeriesStage stage = new PercentileOfSeriesStage(List.of(50.0f, 100.0f), false);

        List<Sample> shard1 = List.of(new MultiValueSample(1000L, List.of(10.0, 30.0)), new MultiValueSample(2000L, List.of(20.0, 40.0)));
        List<Sample> shard2 = List.of(new MultiValueSample(1000L, List.of(50.0)), new MultiValueSample(2000L, List.of(60.0)));
        TimeSeries ts1 = new TimeSeries(shard1, ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null);
        TimeSeries ts2 = new TimeSeries(shard2, ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null);
        List<TimeSeriesProvider> aggregations = List.of(
            new InternalTimeSeries("a", List.of(ts1), Map.of()),
            new InternalTimeSeries("b", List.of(ts2), Map.of())
        );

        InternalAggregation result = stage.reduce(aggregations, true);

        assertNotNull(result);
        assertTrue(result instanceof TimeSeriesProvider);
        List<TimeSeries> series = ((TimeSeriesProvider) result).getTimeSeries();
        assertEquals(2, series.size()); // 50th and 100th percentile
        TimeSeries p50 = findSeriesByLabel(series, "__percentile", "50");
        TimeSeries p100 = findSeriesByLabel(series, "__percentile", "100");
        assertSamplesEqual("P50", List.of(new FloatSample(1000L, 30.0f), new FloatSample(2000L, 40.0f)), p50.getSamples().toList());
        assertSamplesEqual("P100", List.of(new FloatSample(1000L, 50.0f), new FloatSample(2000L, 60.0f)), p100.getSamples().toList());
    }

    /**
     * Test that NaN values in MultiValueSample are filtered out during aggregation (both when
     * creating the first bucket and when merging into an existing bucket). Percentiles are
     * computed only from non-NaN values.
     */
    public void testPercentileOfSeriesFiltersNaNInMultiValueSample() {
        PercentileOfSeriesStage stage = new PercentileOfSeriesStage(List.of(50.0f, 100.0f), false);

        // Shard1: t=1000 has [10, NaN, 30], t=2000 has [20, 40]
        List<Sample> shard1 = List.of(
            new MultiValueSample(1000L, List.of(10.0, Double.NaN, 30.0)),
            new MultiValueSample(2000L, List.of(20.0, 40.0))
        );
        // Shard2: t=1000 has [NaN, 50] (only 50 should count), t=2000 has [60]
        List<Sample> shard2 = List.of(new MultiValueSample(1000L, List.of(Double.NaN, 50.0)), new MultiValueSample(2000L, List.of(60.0)));
        TimeSeries ts1 = new TimeSeries(shard1, ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null);
        TimeSeries ts2 = new TimeSeries(shard2, ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null);
        List<TimeSeriesProvider> aggregations = List.of(
            new InternalTimeSeries("a", List.of(ts1), Map.of()),
            new InternalTimeSeries("b", List.of(ts2), Map.of())
        );

        // Intermediate reduce: merged values at t=1000 should be [10, 30, 50], at t=2000 [20, 40, 60]
        InternalAggregation intermediate = stage.reduce(aggregations, false);
        List<TimeSeries> mergedSeries = ((TimeSeriesProvider) intermediate).getTimeSeries();
        assertEquals(1, mergedSeries.size());
        List<Sample> mergedSamples = mergedSeries.get(0).getSamples().toList();
        assertEquals(List.of(10.0, 30.0, 50.0), ((MultiValueSample) mergedSamples.get(0)).getSortedValueList());
        assertEquals(List.of(20.0, 40.0, 60.0), ((MultiValueSample) mergedSamples.get(1)).getSortedValueList());

        // Final reduce: P50 at t=1000 = median(10,30,50) = 30, at t=2000 = 40; P100 = 50 and 60
        InternalAggregation result = stage.reduce(aggregations, true);
        List<TimeSeries> series = ((TimeSeriesProvider) result).getTimeSeries();
        TimeSeries p50 = findSeriesByLabel(series, "__percentile", "50");
        TimeSeries p100 = findSeriesByLabel(series, "__percentile", "100");
        assertSamplesEqual(
            "P50 excludes NaN",
            List.of(new FloatSample(1000L, 30.0f), new FloatSample(2000L, 40.0f)),
            p50.getSamples().toList()
        );
        assertSamplesEqual(
            "P100 excludes NaN",
            List.of(new FloatSample(1000L, 50.0f), new FloatSample(2000L, 60.0f)),
            p100.getSamples().toList()
        );
    }
}
