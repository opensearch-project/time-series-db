/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;
import org.opensearch.tsdb.query.utils.PercentileUtils;
import org.opensearch.tsdb.lang.m3.utils.BucketParsingUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * HistogramPercentileStage is a UnaryPipelineStage stage that calculates percentiles from histogram buckets.
 * This stage groups time series by all labels except bucketId and bucket range,
 * then calculates the specified percentile across bucket values for each timestamp.
 *
 * Note : It is not an AbstractGroupingStage as group keys are dynamic based on what existing labels are in the input time series.
 */
@PipelineStageAnnotation(name = HistogramPercentileStage.NAME)
public class HistogramPercentileStage implements UnaryPipelineStage {

    /** The name identifier for this stage. */
    public static final String NAME = "histogram_percentile";
    /** The name of bucket_id field when constructing from Args*/
    public static final String BUCKET_ID = "bucket_id";

    /** The name of bucket_id field when constructing from Args*/
    public static final String BUCKET_RANGE = "bucket_range";

    /** The name of the percentiles field **/
    public static final String PERCENTILES = "percentiles";

    private static final String PERCENTILE_LABEL = "histogramPercentile"; // name of the label when generating aggregated time series

    private final String bucketId;
    private final String bucketRange;
    private final List<Float> percentiles; // 0-100
    private final int numPercentiles;

    /**
     * Constructor for histogram percentile calculation.
     *
     * @param bucketId The label name identifying the bucket ID
     * @param bucketRange The label name identifying the bucket range
     * @param percentiles List of percentiles to calculate (0-100)
     * @throws IllegalArgumentException if any percentile is not between 0 and 100, or if list is empty
     */
    public HistogramPercentileStage(String bucketId, String bucketRange, List<Float> percentiles) {
        this.bucketId = bucketId;
        this.bucketRange = bucketRange;

        if (percentiles == null || percentiles.isEmpty()) {
            throw new IllegalArgumentException("Percentiles list cannot be null or empty");
        }

        // Validate all percentiles are in valid range
        for (Float percentile : percentiles) {
            if (percentile == null || percentile < 0 || percentile > 100) {
                throw new IllegalArgumentException("All percentiles must be between 0 and 100 (inclusive), got: " + percentile);
            }
        }

        // Deduplicate and sort percentiles
        SortedSet<Float> sortedPercentiles = new TreeSet<>(percentiles);
        this.percentiles = sortedPercentiles.stream().toList();
        this.numPercentiles = percentiles.size();
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return input;
        }

        // Group time series by all labels except bucketId and bucketRange
        Map<Map<String, String>, List<TimeSeries>> groups = groupTimeSeriesByNonBucketLabels(input);

        List<TimeSeries> result = new ArrayList<>();

        for (Map.Entry<Map<String, String>, List<TimeSeries>> entry : groups.entrySet()) {
            Map<String, String> groupLabels = entry.getKey();
            List<TimeSeries> groupSeries = entry.getValue();

            List<TimeSeries> processedSeries = processGroup(groupSeries, groupLabels);
            result.addAll(processedSeries);
        }

        return result;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(BUCKET_ID, bucketId);
        builder.field(BUCKET_RANGE, bucketRange);
        builder.startArray(PERCENTILES);
        for (Float percentile : percentiles) {
            builder.value(percentile);
        }
        builder.endArray();
    }

    @Override
    public boolean isCoordinatorOnly() {
        return true;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(bucketId);
        out.writeString(bucketRange);
        out.writeCollection(percentiles, StreamOutput::writeFloat);
    }

    /**
     * Create a HistogramPercentileStage instance from the input stream for deserialization.
     * @param in The input stream to read from
     * @return HistogramPercentileStage instance
     * @throws IOException if an error occurs during deserialization
     */
    public static HistogramPercentileStage readFrom(StreamInput in) throws IOException {
        String bucketId = in.readString();
        String bucketRange = in.readString();
        List<Float> percentiles = in.readList(StreamInput::readFloat);

        return new HistogramPercentileStage(bucketId, bucketRange, percentiles);
    }

    /**
     * Create a HistogramPercentile from arguments map.
     *
     * @param args Map of argument names to values
     * @return HistogramPercentile instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static HistogramPercentileStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }

        Object bucketIdObj = args.get(BUCKET_ID);
        Object bucketRangeObj = args.get(BUCKET_RANGE);
        Object percentilesObj = args.get(PERCENTILES);

        if (bucketIdObj == null || bucketRangeObj == null || percentilesObj == null) {
            throw new IllegalArgumentException("bucketId, bucketRange, and percentiles arguments are required");
        }

        try {
            @SuppressWarnings("unchecked")
            List<Object> percentilesList = (List<Object>) percentilesObj;
            List<Float> percentiles = new ArrayList<>();

            for (Object percentileObj : percentilesList) {
                if (percentileObj == null || !(percentileObj instanceof Number)) {
                    throw new IllegalArgumentException("percentile values must be non-null numbers");
                }
                percentiles.add(((Number) percentileObj).floatValue());
            }

            return new HistogramPercentileStage((String) bucketIdObj, (String) bucketRangeObj, percentiles);
        } catch (ClassCastException e) {
            throw new IllegalArgumentException(
                "Invalid argument types: bucketId and bucketRange must be strings, percentiles must be a list",
                e
            );
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        HistogramPercentileStage that = (HistogramPercentileStage) obj;
        return Objects.equals(bucketId, that.bucketId)
            && Objects.equals(bucketRange, that.bucketRange)
            && Objects.equals(percentiles, that.percentiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bucketId, bucketRange, percentiles);
    }

    /**
     * Group time series by all labels except bucketId and bucketRange.
     * Example Input :
     *  [{job="api", instance="1", bucketRange="10-20", bucketId="a"} -> [0s : 1, 10s : 2, 20s : 3],
     *  {job="api", instance="1", bucketRange="20-30", bucketId="b"} -> [0s : 1, 10s : 2, 20s : 3],
     *  {job="api", instance="2", bucketRange="10-20", bucketId="a"} -> [0s : 1, 10s : 2, 20s : 3],
     *  {job="api", instance="2", bucketRange="20-30", bucketId="b"} -> [0s : 1, 10s : 2, 20s : 3]]
     * Example Output :
     * {job="api", instance="1"} -> [{job="api", instance="1",bucketRange="10-20", bucketId="a"} -> [0s : 1, 10s : 2, 20s : 3],
     *                               {job="api", instance="1",bucketRange="20-30", bucketId="b"} -> [0s : 1, 10s : 2, 20s : 3]],
     *{job="api", instance="2"} -> [{job="api", instance="2",bucketRange="10-20", bucketId="a"} -> [0s : 1, 10s : 2, 20s : 3],
     *                              {job="api", instance="2", bucketRange="20-30", bucketId="b"} -> [0s : 1, 10s : 2, 20s : 3]]
     */
    private Map<Map<String, String>, List<TimeSeries>> groupTimeSeriesByNonBucketLabels(List<TimeSeries> input) {
        Map<Map<String, String>, List<TimeSeries>> groups = new HashMap<>();

        for (TimeSeries series : input) {
            Labels seriesLabels = series.getLabels();
            if (seriesLabels == null) {
                continue; // Skip series without labels
            }

            // Create a map of all labels except bucketId and bucketRange
            Map<String, String> groupLabelMap = new HashMap<>();

            // Get map view of all labels and iterate through them
            Map<String, String> allLabels = seriesLabels.toMapView();
            for (Map.Entry<String, String> entry : allLabels.entrySet()) {
                String labelName = entry.getKey();
                String labelValue = entry.getValue();

                // Include all labels except bucketId and bucketRange
                if (!bucketId.equals(labelName) && !bucketRange.equals(labelName)) {
                    groupLabelMap.put(labelName, labelValue);
                }
            }

            // ByteLabels groupLabels = ByteLabels.fromMap(groupLabelMap);
            groups.computeIfAbsent(groupLabelMap, k -> new ArrayList<>()).add(series);
        }

        return groups;
    }

    /**
     * Process a group of time series to calculate histogram percentiles.
     * * Example Input :
     *     [{job="api", instance="1",bucketRange="10-20", bucketId="a"} -> [0s : 1, 10s : 2, 20s : 3],
     *     {job="api", instance="1",bucketRange="20-30", bucketId="b"} -> [0s :  2, 10s : 5, 20s : 1]],
     */
    private List<TimeSeries> processGroup(List<TimeSeries> groupSeries, Map<String, String> groupLabels) {
        if (groupSeries.isEmpty()) {
            return new ArrayList<>();
        }

        // Collect all samples by timestamp, organizing by bucket information
        // e.g.
        // 0s -> {BucketInfo("a","10-20") -> 1, BucketInfo("b","20-30") -> 2}
        // 10s -> {BucketInfo("a","10-20") -> 2, BucketInfo("b","20-30") -> 5}
        // 20s -> {BucketInfo("a","10-20") -> 3, BucketInfo("b","20-30") -> 1}
        Map<Long, Map<BucketParsingUtils.BucketInfo, Double>> timestampToBuckets = new TreeMap<>();

        for (TimeSeries series : groupSeries) {
            Map<String, String> seriesLabelsMap = series.getLabels().toMapView();
            if (!seriesLabelsMap.containsKey(bucketId) || !seriesLabelsMap.containsKey(bucketRange)) {
                // Skip series with missing required labels instead of throwing exception
                continue;
            }

            String bucketIdValue = seriesLabelsMap.get(bucketId);
            String bucketRangeValue = seriesLabelsMap.get(bucketRange);

            BucketParsingUtils.BucketInfo bucketInfo;
            try {
                bucketInfo = new BucketParsingUtils.BucketInfo(bucketIdValue, bucketRangeValue);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "Failed to parse bucket range '"
                        + bucketRangeValue
                        + "' for bucketId '"
                        + bucketIdValue
                        + "' in histogram percentile calculation: "
                        + e.getMessage(),
                    e
                );
            }

            for (Sample sample : series.getSamples()) {
                long timestamp = sample.getTimestamp();
                double value = sample.getValue();

                // Cardinality of bucketInfoValueMap depends on how many unique buckets are defined in the histogram
                Map<BucketParsingUtils.BucketInfo, Double> bucketInfoValueMap = timestampToBuckets.computeIfAbsent(
                    timestamp,
                    k -> new HashMap<>()
                );
                if (bucketInfoValueMap.containsKey(bucketInfo)) {
                    throw new IllegalStateException("already seen range" + bucketInfo + " Histogram buckets may have changed");
                } else {
                    bucketInfoValueMap.put(bucketInfo, value);
                }
            }
        }

        // If no valid buckets were found, return empty result
        if (timestampToBuckets.isEmpty()) {
            return new ArrayList<>();
        }

        // Create metadata from first series
        TimeSeries firstSeries = groupSeries.getFirst();

        // Create one time series for each percentile
        List<TimeSeries> result = new ArrayList<>(numPercentiles);
        int sampleSize = timestampToBuckets.size();

        for (Float percentile : percentiles) {
            // Calculate percentiles for each timestamp
            List<Sample> percentileSamples = new ArrayList<>(sampleSize);
            for (Map.Entry<Long, Map<BucketParsingUtils.BucketInfo, Double>> entry : timestampToBuckets.entrySet()) {
                long timestamp = entry.getKey();
                Map<BucketParsingUtils.BucketInfo, Double> buckets = entry.getValue();

                double percentileValue = calculatePercentile(buckets, percentile);
                if (Double.isNaN(percentileValue)) {
                    // No valid buckets for this timestamp, skip
                    continue;
                }
                percentileSamples.add(new FloatSample(timestamp, percentileValue));
            }

            // Create labels with percentile information (format: histogramPercentile:pXX)
            String percentileValue = "p" + PercentileUtils.formatPercentile(percentile);
            Labels finalLabels = ByteLabels.fromMap(groupLabels).withLabel(PERCENTILE_LABEL, percentileValue);

            result.add(
                new TimeSeries(
                    percentileSamples,
                    finalLabels,
                    firstSeries.getMinTimestamp(),
                    firstSeries.getMaxTimestamp(),
                    firstSeries.getStep(),
                    firstSeries.getAlias()
                )
            );
        }
        return result;
    }

    /**
     * Calculate percentile from histogram buckets using upper bounds.
     * This implements histogram percentile calculation where each bucket contributes
     * its count to the bucket's upper bound value for percentile calculation.
     *
     * Example Input:
     *   Buckets = {BucketInfo("a","10-20") -> 1, BucketInfo("b","20-30") -> 2}, percentile=95.0
     * Example Output:
     *  30
     */
    private double calculatePercentile(Map<BucketParsingUtils.BucketInfo, Double> buckets, float percentile) {
        if (buckets.isEmpty()) {
            return 0.0;
        }

        // Create a list of (upperBound, count) pairs and sort by upper bound
        int bucketCount = buckets.size();
        List<BucketValuePair> bucketValues = new ArrayList<>(bucketCount);
        double totalCount = 0.0;
        for (Map.Entry<BucketParsingUtils.BucketInfo, Double> entry : buckets.entrySet()) {
            BucketParsingUtils.BucketInfo bucketInfo = entry.getKey();
            double count = entry.getValue();
            // Use upper bound of the bucket range as the value
            double upperBound = bucketInfo.getUpperBound();
            totalCount += count;
            bucketValues.add(new BucketValuePair(upperBound, count));
        }
        // Calculate total count
        if (totalCount == 0) {
            // all buckets are empty e.g. Buckets = {BucketInfo("a","10-20") -> 0, BucketInfo("b","20-30") -> 0}
            return Double.NaN;
        }

        // Sort by upper bound
        bucketValues.sort((a, b) -> Double.compare(a.upperBound, b.upperBound));

        // Calculate target count for the percentile
        double targetCount = (percentile / 100.0) * totalCount;

        // Find the bucket that contains the percentile
        double cumulativeCount = 0.0;
        for (BucketValuePair bucketValue : bucketValues) {
            cumulativeCount += bucketValue.count;
            if (cumulativeCount >= targetCount) {
                // Return the upper bound of this bucket
                return bucketValue.upperBound;
            }
        }

        // If we somehow didn't find it (should not happen if buckets are non-empty), return the highest upper bound
        throw new IllegalStateException(
            "Could not find target bucket. Cumulative count " + cumulativeCount + " must be greater than target count " + targetCount
        );
    }

    /**
     * Helper class to pair bucket upper bound with its count.
     */
    private static class BucketValuePair {
        final double upperBound;
        final double count;

        BucketValuePair(double upperBound, double count) {
            this.upperBound = upperBound;
            this.count = count;
        }
    }

}
