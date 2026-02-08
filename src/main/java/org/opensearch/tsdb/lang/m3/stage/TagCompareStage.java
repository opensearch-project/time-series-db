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
import org.opensearch.tsdb.lang.m3.common.SemanticVersionComparator;
import org.opensearch.tsdb.lang.m3.common.TagComparisonOperator;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A pipeline stage that filters time series based on tag comparison conditions.
 * These comparisons are done lexicographically for strings and numerically for semantic version tags.
 */
@PipelineStageAnnotation(name = TagCompareStage.NAME)
public class TagCompareStage implements UnaryPipelineStage {

    /** The name identifier for this pipeline stage. */
    public static final String NAME = "tag_compare";
    private static final String OPERATOR_ARG = "operator";
    private static final String TAG_KEY_ARG = "tag_key";
    private static final String COMPARE_VALUE_ARG = "compare_value";

    private final TagComparisonOperator operator;
    private final String tagKey;
    private final String compareValue;

    /**
     * Constructs a new TagCompareStage.
     *
     * @param operator the comparison operator to apply
     * @param tagKey the tag key to compare
     * @param compareValue the value to compare against
     */
    public TagCompareStage(TagComparisonOperator operator, String tagKey, String compareValue) {
        this.operator = operator;
        this.tagKey = tagKey;
        this.compareValue = compareValue;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Get the comparison operator.
     * @return the comparison operator
     */
    public TagComparisonOperator getOperator() {
        return operator;
    }

    /**
     * Get the tag key being compared.
     * @return the tag key
     */
    public String getTagKey() {
        return tagKey;
    }

    /**
     * Get the comparison value.
     * @return the comparison value
     */
    public String getCompareValue() {
        return compareValue;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }

        List<TimeSeries> result = new ArrayList<>();
        for (TimeSeries ts : input) {
            if (shouldIncludeSeries(ts)) {
                result.add(ts);
            }
        }
        return result;
    }

    /**
     * Determines if a time series should be included based on tag comparison.
     *
     * @param ts the time series to evaluate
     * @return true if the series should be included, false otherwise
     */
    private boolean shouldIncludeSeries(TimeSeries ts) {
        Labels seriesLabels = ts.getLabels();

        // If series has no labels or the tag doesn't exist, exclude from results
        if (seriesLabels == null || !seriesLabels.has(tagKey)) {
            return false;
        }

        String seriesValue = seriesLabels.get(tagKey);
        if (seriesValue == null) {
            return false;
        }

        // Apply comparison using semantic version comparator
        return SemanticVersionComparator.applyComparison(seriesValue, operator, compareValue);
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(OPERATOR_ARG, operator.getOperatorString());
        builder.field(TAG_KEY_ARG, tagKey);
        builder.field(COMPARE_VALUE_ARG, compareValue);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(operator.getOperatorString());
        out.writeString(tagKey);
        out.writeString(compareValue);
    }

    /**
     * Create a TagCompareStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new TagCompareStage instance
     * @throws IOException if an I/O error occurs while reading
     */
    public static TagCompareStage readFrom(StreamInput in) throws IOException {
        String operatorString = in.readString();
        String tagKey = in.readString();
        String compareValue = in.readString();

        TagComparisonOperator operator = TagComparisonOperator.fromString(operatorString);
        return new TagCompareStage(operator, tagKey, compareValue);
    }

    /**
     * Creates a new instance of TagCompareStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct a TagCompareStage instance
     * @return a new TagCompareStage instance initialized with the provided parameters
     * @throws IllegalArgumentException if required arguments are missing or invalid
     */
    public static TagCompareStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }

        if (!args.containsKey(OPERATOR_ARG)) {
            throw new IllegalArgumentException("TagCompare stage requires '" + OPERATOR_ARG + "' argument");
        }
        String operatorString = (String) args.get(OPERATOR_ARG);
        if (operatorString == null || operatorString.isEmpty()) {
            throw new IllegalArgumentException("Operator cannot be null or empty");
        }

        TagComparisonOperator operator;
        try {
            operator = TagComparisonOperator.fromString(operatorString);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid comparison operator: " + operatorString, e);
        }

        if (!args.containsKey(TAG_KEY_ARG)) {
            throw new IllegalArgumentException("TagCompare stage requires '" + TAG_KEY_ARG + "' argument");
        }
        String tagKey = (String) args.get(TAG_KEY_ARG);
        if (tagKey == null || tagKey.isEmpty()) {
            throw new IllegalArgumentException("Tag key cannot be null or empty");
        }

        if (!args.containsKey(COMPARE_VALUE_ARG)) {
            throw new IllegalArgumentException("TagCompare stage requires '" + COMPARE_VALUE_ARG + "' argument");
        }
        String compareValue = (String) args.get(COMPARE_VALUE_ARG);
        if (compareValue == null) {
            throw new IllegalArgumentException("Compare value cannot be null");
        }

        return new TagCompareStage(operator, tagKey, compareValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TagCompareStage that = (TagCompareStage) obj;
        return Objects.equals(operator, that.operator)
            && Objects.equals(tagKey, that.tagKey)
            && Objects.equals(compareValue, that.compareValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, tagKey, compareValue);
    }
}
