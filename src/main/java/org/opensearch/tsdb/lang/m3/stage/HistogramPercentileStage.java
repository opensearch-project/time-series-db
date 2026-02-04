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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.opensearch.tsdb.metrics.TSDBMetricsConstants.NANOS_PER_MILLI;

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
        Map<Long, Map<BucketInfo, Double>> timestampToBuckets = new TreeMap<>();

        for (TimeSeries series : groupSeries) {
            Map<String, String> seriesLabelsMap = series.getLabels().toMapView();
            if (!seriesLabelsMap.containsKey(bucketId) || !seriesLabelsMap.containsKey(bucketRange)) {
                // Skip series with missing required labels instead of throwing exception
                continue;
            }

            String bucketIdValue = seriesLabelsMap.get(bucketId);
            String bucketRangeValue = seriesLabelsMap.get(bucketRange);

            BucketInfo bucketInfo;
            try {
                bucketInfo = new BucketInfo(bucketIdValue, bucketRangeValue);
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
                Map<BucketInfo, Double> bucketInfoValueMap = timestampToBuckets.computeIfAbsent(timestamp, k -> new HashMap<>());
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
            for (Map.Entry<Long, Map<BucketInfo, Double>> entry : timestampToBuckets.entrySet()) {
                long timestamp = entry.getKey();
                Map<BucketInfo, Double> buckets = entry.getValue();

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
    private double calculatePercentile(Map<BucketInfo, Double> buckets, float percentile) {
        if (buckets.isEmpty()) {
            return 0.0;
        }

        // Create a list of (upperBound, count) pairs and sort by upper bound
        int bucketCount = buckets.size();
        List<BucketValuePair> bucketValues = new ArrayList<>(bucketCount);
        double totalCount = 0.0;
        for (Map.Entry<BucketInfo, Double> entry : buckets.entrySet()) {
            BucketInfo bucketInfo = entry.getKey();
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

    /**
     * Interface to represent histogram bucket ranges (duration or value based).
     */
    private interface HistogramRange {
        /**
         * Returns the lower bound as float64
         */
        double lower();

        /**
         * Returns the upper bound as float64
         */
        double upper();

    }

    /**
     * Implementation for value-based histogram ranges (e.g., "10-20").
     */
    private static class HistogramValueRange implements HistogramRange {
        private final double low;
        private final double high;

        public HistogramValueRange(double low, double high) {
            this.low = low;
            this.high = high;
        }

        @Override
        public double lower() {
            return low;
        }

        @Override
        public double upper() {
            return high;
        }
    }

    /**
     * Implementation for duration-based histogram ranges (e.g., "10ms-20ms").
     */
    private static class HistogramDurationRange implements HistogramRange {
        private final Duration low;
        private final Duration high;

        public HistogramDurationRange(Duration low, Duration high) {
            this.low = low;
            this.high = high;
        }

        @Override
        public double lower() {
            // cannot use low.toMillis() directly as that wil discard everything under 1ms for small durations
            return low.toNanos() / NANOS_PER_MILLI; // Convert to milliseconds
        }

        @Override
        public double upper() {
            // cannot use high.toMillis() directly as that wil discard everything under 1ms for small durations
            return high.toNanos() / NANOS_PER_MILLI; // Convert to milliseconds
        }

    }

    /**
     * Helper class to represent bucket information for grouping.
     */
    static class BucketInfo {
        private final String bucketId;
        private final String bucketRange;
        private final HistogramRange parsedRange;
        private static final Pattern DURATION_PATTERN = Pattern.compile("(-?\\d+(?:\\.\\d+)?)(ns|us|µs|ms|s|m|h|d)");
        private static final Map<String, Function<Double, Duration>> unitMap = Map.of(
            "ns",
            value -> Duration.ofNanos(Math.round(value)),
            "us",
            value -> Duration.ofNanos(Math.round(value * 1000)), // 1 microsecond = 1000 nanoseconds
            "µs",
            value -> Duration.ofNanos(Math.round(value * 1000)), // 1 microsecond = 1000 nanoseconds
            "ms",
            value -> Duration.ofNanos(Math.round(value * 1_000_000)),
            "s",
            value -> Duration.ofNanos(Math.round(value * 1_000_000_000)),
            "m",
            value -> Duration.ofNanos(Math.round(value * 60_000_000_000L)),
            "h",
            value -> Duration.ofNanos(Math.round(value * 3_600_000_000_000L))
        );

        public BucketInfo(String bucketId, String bucketRange) throws IllegalArgumentException {
            this.bucketId = bucketId;
            this.bucketRange = bucketRange;
            this.parsedRange = parseBucket(bucketRange);
        }

        public double getUpperBound() {
            return parsedRange.upper();
        }

        public double getLowerBound() {
            return parsedRange.lower();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BucketInfo that = (BucketInfo) o;
            return bucketId.equals(that.bucketId) && bucketRange.equals(that.bucketRange);
        }

        @Override
        public int hashCode() {
            return bucketId.hashCode() * 31 + bucketRange.hashCode();
        }

        @Override
        public String toString() {
            return "BucketInfo{bucketId='" + bucketId + "', bucketRange='" + bucketRange + "', upper=" + getUpperBound() + "}";
        }

        /**
         * Parse bucket range string into HistogramRange.
         * Supports both duration ranges (e.g., "10ms-20ms") and value ranges (e.g., "10-20").
         */
        private static HistogramRange parseBucket(String bucketRange) throws IllegalArgumentException {
            if (bucketRange == null || bucketRange.trim().isEmpty()) {
                throw new IllegalArgumentException("Bucket range cannot be null or empty");
            }

            // Handle single value or infinity cases
            if (bucketRange.equals("infinity") || bucketRange.equals("+Inf")) {
                return new HistogramValueRange(0, Double.POSITIVE_INFINITY);
            }

            // Find the delimiter dash, handling negative numbers correctly
            int delimiterIndex = findDelimiterDash(bucketRange);
            if (delimiterIndex == -1) {
                throw new IllegalArgumentException("Invalid bucket range format: " + bucketRange + ". Expected format: 'low-high'");
            }

            String left = bucketRange.substring(0, delimiterIndex).trim();
            String right = bucketRange.substring(delimiterIndex + 1).trim();

            // Try parsing as duration range first
            try {
                return parseDurationRange(left, right);
            } catch (IllegalArgumentException de) {
                // If duration parsing fails, try value range
                try {
                    return parseValueRange(left, right);
                } catch (IllegalArgumentException ve) {
                    throw new IllegalArgumentException(
                        "Cannot parse as duration range: " + de.getMessage() + "; cannot parse as value range: " + ve.getMessage()
                    );
                }
            }
        }

        /**
         * Find the delimiter dash in a bucket range string, properly handling negative numbers.
         * Examples:
         * - "10-20" -> index 2
         * - "-10-5" -> index 3 (skip initial negative sign)
         * - "-10--5" -> index 3 (skip initial negative sign)
         * - "1.5s-2.5s" -> index 4
         */
        private static int findDelimiterDash(String bucketRange) {
            // Start from index 1 to skip potential negative sign at the beginning
            int startIndex = bucketRange.startsWith("-") ? 1 : 0;

            // Look for a dash that's not immediately after a numeric character followed by 'e' or 'E'
            // (to handle scientific notation like 1e-5)
            for (int i = startIndex; i < bucketRange.length(); i++) {
                if (bucketRange.charAt(i) == '-') {
                    // Check if this dash is part of scientific notation
                    if (i > 0 && (bucketRange.charAt(i - 1) == 'e' || bucketRange.charAt(i - 1) == 'E')) {
                        continue; // Skip scientific notation dashes
                    }
                    return i; // Found delimiter dash
                }
            }
            return -1; // No delimiter dash found
        }

        /**
         * Parse value range (e.g., "10-20").
         */
        private static HistogramRange parseValueRange(String left, String right) throws IllegalArgumentException {
            try {
                double low;
                double high;
                if ("-Inf".equals(left) || "-infinity".equals(left)) {
                    low = Double.NEGATIVE_INFINITY;
                } else {
                    low = Double.parseDouble(left);
                }

                if ("infinity".equals(right) || "+Inf".equals(right)) {
                    high = low; // For infinity buckets, upper bound equals lower bound
                } else {
                    high = Double.parseDouble(right);
                    if (high <= low) {
                        throw new IllegalArgumentException("High value " + high + " must exceed low value " + low);
                    }
                }

                return new HistogramValueRange(low, high);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Cannot parse value range: " + left + "-" + right, e);
            }
        }

        /**
         * Parse duration range (e.g., "10ms-20ms").
         */
        private static HistogramRange parseDurationRange(String left, String right) throws IllegalArgumentException {
            try {
                Duration low;

                // Handle special cases
                if ("0".equals(left)) {
                    low = Duration.ZERO;
                } else if ("-Inf".equals(left) || "-infinity".equals(left)) {
                    // negative infinity : use the minimal duration possible (–9223372036854775808 nanoseconds (~–292.47 years))
                    low = Duration.ofNanos(Long.MIN_VALUE);
                } else {
                    low = parseDuration(left);
                }

                Duration high;
                if ("infinity".equals(right) || "+Inf".equals(right)) {
                    // positive infinity : set high=low which is the current m3 behavior
                    high = low; // For infinity buckets
                } else {
                    high = parseDuration(right);

                    if (high.compareTo(low) <= 0) {
                        throw new IllegalArgumentException("High duration " + high + " must exceed low duration " + low);
                    }
                }

                return new HistogramDurationRange(low, high);
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot parse duration range: " + left + "-" + right, e);
            }
        }

        /**
         * Parse a duration string supporting both single-unit (e.g., "1ms", "10s") and
         * multi-unit Go-style formats (e.g., "2m11.072s", "1h30m").
         * Supports the specific postfixes: "ns", "us"/"µs", "ms", "s", "m", "h"
         */
        public static Duration parseDuration(String durationString) throws IllegalArgumentException {
            if (durationString == null || durationString.trim().isEmpty()) {
                throw new IllegalArgumentException("Duration string cannot be null or empty");
            }

            // Handle the __ → µ replacement workaround for bad data
            String cleanedDuration = durationString.replace("__", "µ");

            // Find all duration components and validate for Go-compatibility
            Matcher matcher = DURATION_PATTERN.matcher(cleanedDuration);
            Duration total = Duration.ZERO;
            Set<String> seenUnits = new HashSet<>(); // Track units to prevent duplicates
            boolean hasMatches = false;
            int lastEnd = 0;

            while (matcher.find()) {
                hasMatches = true;

                // Check for gaps between matches (invalid characters)
                if (matcher.start() > lastEnd) {
                    String gap = cleanedDuration.substring(lastEnd, matcher.start());
                    if (!gap.isEmpty()) {
                        throw new IllegalArgumentException("Invalid characters '" + gap + "' in duration: " + durationString);
                    }
                }
                lastEnd = matcher.end();

                double value = Double.parseDouble(matcher.group(1));
                String unit = matcher.group(2);

                // Reject duplicate units (Go-compatible behavior)
                if (!seenUnits.add(unit)) {
                    throw new IllegalArgumentException("Duplicate time unit '" + unit + "' in duration: " + durationString);
                }

                Function<Double, Duration> durationCreator = unitMap.get(unit);
                if (durationCreator == null) {
                    throw new IllegalArgumentException("Unknown time unit: " + unit);
                }

                Duration component = durationCreator.apply(value);
                total = total.plus(component);
            }

            if (!hasMatches) {
                throw new IllegalArgumentException("Invalid duration string format: " + durationString);
            }

            // Validate that entire string was consumed (no leftover characters at the end)
            if (lastEnd < cleanedDuration.length()) {
                String leftover = cleanedDuration.substring(lastEnd);
                if (!leftover.isEmpty()) {
                    throw new IllegalArgumentException("Invalid characters '" + leftover + "' in duration: " + durationString);
                }
            }

            return total;
        }

    }

}
