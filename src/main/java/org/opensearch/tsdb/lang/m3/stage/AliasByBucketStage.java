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
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;
import org.opensearch.tsdb.lang.m3.utils.BucketParsingUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A pipeline stage that creates aliases based on histogram bucket bounds.
 * Requires an explicit tag name parameter containing bucket range information.
 * Extracts the bucket range from the specified tag, parses it, and uses the upper bound
 * (or lower bound for infinity cases) as the alias. Duration buckets are converted to milliseconds.
 *
 * Supports both 'aliasByBucket' and 'aliasByHistogramBucket' function names.
 */
@PipelineStageAnnotation(name = AliasByBucketStage.NAME)
public class AliasByBucketStage implements UnaryPipelineStage {

    /** The primary name identifier for this pipeline stage. */
    public static final String NAME = "aliasByBucket";

    /** The alternative name identifier for this pipeline stage. */
    public static final String ALTERNATIVE_NAME = "aliasByHistogramBucket";

    private static final String TAG_NAME_ARG = "tag_name";

    private final String tagName;

    /**
     * Creates a new AliasByBucketStage with the specified tag name.
     *
     * @param tagName the tag name containing bucket range information (required, cannot be null or empty)
     * @throws IllegalArgumentException if tagName is null or empty
     */
    public AliasByBucketStage(String tagName) {
        if (tagName == null || tagName.trim().isEmpty()) {
            throw new IllegalArgumentException("Tag name is required for AliasByBucket stage");
        }
        this.tagName = tagName.trim();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }

        List<TimeSeries> result = new ArrayList<>(input.size());
        for (TimeSeries ts : input) {
            String bucketAlias = extractBucketAlias(ts);

            // Create new TimeSeries with bucket-based alias
            TimeSeries newTs = new TimeSeries(
                ts.getSamples(),
                ts.getLabels(),
                ts.getMinTimestamp(),
                ts.getMaxTimestamp(),
                ts.getStep(),
                bucketAlias
            );
            result.add(newTs);
        }
        return result;
    }

    /**
     * Extract bucket alias from the time series.
     *
     * @param timeSeries the time series containing bucket information
     * @return the bucket alias based on the upper (or lower for infinity) bound
     * @throws IllegalArgumentException if the required bucket tag doesn't exist in the series
     */
    private String extractBucketAlias(TimeSeries timeSeries) {
        Labels labels = timeSeries.getLabels();
        if (labels == null) {
            throw new IllegalArgumentException("Series has no labels");
        }

        // Check if the bucket tag exists in the series
        if (!labels.has(tagName)) {
            String seriesAlias = timeSeries.getAlias();
            String seriesDescription = seriesAlias != null ? "series '" + seriesAlias + "'" : "series";
            throw new IllegalArgumentException("Required bucket tag '" + tagName + "' does not exist in " + seriesDescription);
        }

        String bucketRangeValue = labels.get(tagName);
        if (bucketRangeValue == null || bucketRangeValue.isEmpty()) {
            String seriesAlias = timeSeries.getAlias();
            String seriesDescription = seriesAlias != null ? "series '" + seriesAlias + "'" : "series";
            throw new IllegalArgumentException("Bucket tag '" + tagName + "' exists but has no value in " + seriesDescription);
        }

        BucketParsingUtils.BucketInfo bucketInfo = new BucketParsingUtils.BucketInfo("", bucketRangeValue);

        double upperBound = bucketInfo.getUpperBound();
        double lowerBound = bucketInfo.getLowerBound();

        // Use upper bound, but fall back to lower bound if upper bound is infinity
        if (Double.isInfinite(upperBound)) {
            if (Double.isInfinite(lowerBound)) {
                return "infinity";
            } else {
                return formatBound(lowerBound);
            }
        } else {
            return formatBound(upperBound);
        }
    }

    /**
     * Format a numeric bound value to string, handling duration conversion.
     * Duration ranges are already converted to milliseconds by BucketParsingUtils,
     * so we format them as integers when they represent whole milliseconds.
     *
     * @param bound the bound value to format
     * @return the formatted string
     */
    private String formatBound(double bound) {
        // If the value is a whole number, format as integer (useful for millisecond durations)
        if (bound == Math.floor(bound) && !Double.isInfinite(bound) && !Double.isNaN(bound)) {
            return String.valueOf((long) bound);
        } else {
            return String.valueOf(bound);
        }
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(TAG_NAME_ARG, tagName);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tagName);
    }

    /**
     * Create an AliasByBucketStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new AliasByBucketStage instance
     * @throws IOException if an I/O error occurs while reading
     */
    public static AliasByBucketStage readFrom(StreamInput in) throws IOException {
        String tagName = in.readString();
        return new AliasByBucketStage(tagName);
    }

    /**
     * Creates a new instance of AliasByBucketStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct an AliasByBucketStage instance
     * @return a new AliasByBucketStage instance initialized with the provided parameters
     * @throws IllegalArgumentException if required tag_name argument is missing
     */
    public static AliasByBucketStage fromArgs(Map<String, Object> args) {
        if (args == null || !args.containsKey(TAG_NAME_ARG)) {
            throw new IllegalArgumentException("Tag name argument '" + TAG_NAME_ARG + "' is required for AliasByBucket stage");
        }

        String tagName = (String) args.get(TAG_NAME_ARG);
        return new AliasByBucketStage(tagName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AliasByBucketStage that = (AliasByBucketStage) obj;
        return Objects.equals(tagName, that.tagName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tagName);
    }

    /**
     * Gets the tag name used by this stage.
     *
     * @return the tag name (never null)
     */
    public String getTagName() {
        return tagName;
    }
}
