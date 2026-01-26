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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A pipeline stage that creates aliases based on histogram bucket bounds.
 * Takes an optional tag name parameter (defaults to common bucket tag names if not provided).
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

    // Common bucket tag names to try if no tag name is specified
    private static final List<String> DEFAULT_BUCKET_TAG_NAMES = Arrays.asList(
        "le",
        "bucket",
        "bucketRange",
        "bucket_range",
        "upper_bound"
    );

    private final String tagName;

    /**
     * Creates a new AliasByBucketStage with the specified tag name.
     *
     * @param tagName the tag name containing bucket range information (can be null to use defaults)
     */
    public AliasByBucketStage(String tagName) {
        this.tagName = tagName;
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
            String bucketAlias = extractBucketAlias(ts.getLabels());

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
     * Extract bucket alias from the time series labels.
     *
     * @param labels the labels containing bucket information
     * @return the bucket alias based on the upper (or lower for infinity) bound
     */
    private String extractBucketAlias(Labels labels) {
        if (labels == null) {
            return null;
        }

        String bucketRangeValue = getBucketRangeValue(labels);
        if (bucketRangeValue == null || bucketRangeValue.isEmpty()) {
            return null;
        }

        try {
            // Create a placeholder bucket ID since we only need the range parsing
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
        } catch (IllegalArgumentException e) {
            // If parsing fails, return the original value
            return bucketRangeValue;
        }
    }

    /**
     * Get the bucket range value from labels, trying the specified tag name or defaults.
     *
     * @param labels the labels to search
     * @return the bucket range value, or null if not found
     */
    private String getBucketRangeValue(Labels labels) {
        if (tagName != null && !tagName.isEmpty()) {
            // Use the specified tag name
            return labels.get(tagName);
        } else {
            // Try default tag names
            Map<String, String> labelsMap = labels.toMapView();
            for (String defaultTagName : DEFAULT_BUCKET_TAG_NAMES) {
                String value = labelsMap.get(defaultTagName);
                if (value != null && !value.isEmpty()) {
                    return value;
                }
            }
            return null;
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
        if (tagName != null) {
            builder.field(TAG_NAME_ARG, tagName);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(tagName);
    }

    /**
     * Create an AliasByBucketStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new AliasByBucketStage instance
     * @throws IOException if an I/O error occurs while reading
     */
    public static AliasByBucketStage readFrom(StreamInput in) throws IOException {
        String tagName = in.readOptionalString();
        return new AliasByBucketStage(tagName);
    }

    /**
     * Creates a new instance of AliasByBucketStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct an AliasByBucketStage instance
     * @return a new AliasByBucketStage instance initialized with the provided parameters
     */
    public static AliasByBucketStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            return new AliasByBucketStage(null); // No tag name specified, use defaults
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
     * @return the tag name, or null if using defaults
     */
    public String getTagName() {
        return tagName;
    }
}
