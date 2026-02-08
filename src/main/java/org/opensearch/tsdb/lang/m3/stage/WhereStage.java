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
import org.opensearch.tsdb.lang.m3.common.WhereOperator;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A pipeline stage that filters time series based on tag equality conditions.
 * Limits the input series to a subset that satisfy the given constraint on the given 2 tags.
 * The currently supported constraints are equality (op = "eq") or not-equality (op = "neq").
 */
@PipelineStageAnnotation(name = WhereStage.NAME)
public class WhereStage implements UnaryPipelineStage {

    /** The name identifier for this pipeline stage. */
    public static final String NAME = "where";
    private static final String OPERATOR_ARG = "operator";
    private static final String TAG_KEY1_ARG = "tag_key1";
    private static final String TAG_KEY2_ARG = "tag_key2";

    private final WhereOperator operator;
    private final String tagKey1;
    private final String tagKey2;

    /**
     * Constructs a new WhereStage.
     *
     * @param operator the comparison operator to apply
     * @param tagKey1 the first tag key to compare
     * @param tagKey2 the second tag key to compare
     */
    public WhereStage(WhereOperator operator, String tagKey1, String tagKey2) {
        this.operator = operator;
        this.tagKey1 = tagKey1;
        this.tagKey2 = tagKey2;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Get the comparison operator.
     * @return the where operator
     */
    public WhereOperator getOperator() {
        return operator;
    }

    /**
     * Get the first tag key.
     * @return the first tag key
     */
    public String getTagKey1() {
        return tagKey1;
    }

    /**
     * Get the second tag key.
     * @return the second tag key
     */
    public String getTagKey2() {
        return tagKey2;
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
     * Determines if a time series should be included based on tag equality condition.
     *
     * @param ts the time series to evaluate
     * @return true if the series should be included, false otherwise
     */
    private boolean shouldIncludeSeries(TimeSeries ts) {
        Labels seriesLabels = ts.getLabels();

        // If series has no labels, exclude from results
        if (seriesLabels == null) {
            return false;
        }

        // If either tag doesn't exist, exclude series from results
        if (!seriesLabels.has(tagKey1) || !seriesLabels.has(tagKey2)) {
            return false;
        }

        String value1 = seriesLabels.get(tagKey1);
        String value2 = seriesLabels.get(tagKey2);

        // Apply the comparison using the operator
        return operator.apply(value1, value2);
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(OPERATOR_ARG, operator.getOperatorString());
        builder.field(TAG_KEY1_ARG, tagKey1);
        builder.field(TAG_KEY2_ARG, tagKey2);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(operator.getOperatorString());
        out.writeString(tagKey1);
        out.writeString(tagKey2);
    }

    /**
     * Create a WhereStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new WhereStage instance
     * @throws IOException if an I/O error occurs while reading
     */
    public static WhereStage readFrom(StreamInput in) throws IOException {
        String operatorString = in.readString();
        String tagKey1 = in.readString();
        String tagKey2 = in.readString();

        WhereOperator operator = WhereOperator.fromString(operatorString);
        return new WhereStage(operator, tagKey1, tagKey2);
    }

    /**
     * Creates a new instance of WhereStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct a WhereStage instance
     * @return a new WhereStage instance initialized with the provided parameters
     * @throws IllegalArgumentException if required arguments are missing or invalid
     */
    public static WhereStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }

        if (!args.containsKey(OPERATOR_ARG)) {
            throw new IllegalArgumentException("Where stage requires '" + OPERATOR_ARG + "' argument");
        }
        String operatorString = (String) args.get(OPERATOR_ARG);
        if (operatorString == null || operatorString.isEmpty()) {
            throw new IllegalArgumentException("Operator cannot be null or empty");
        }

        WhereOperator operator;
        try {
            operator = WhereOperator.fromString(operatorString);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid where operator: " + operatorString, e);
        }

        if (!args.containsKey(TAG_KEY1_ARG)) {
            throw new IllegalArgumentException("Where stage requires '" + TAG_KEY1_ARG + "' argument");
        }
        String tagKey1 = (String) args.get(TAG_KEY1_ARG);
        if (tagKey1 == null || tagKey1.isEmpty()) {
            throw new IllegalArgumentException("First tag key cannot be null or empty");
        }

        if (!args.containsKey(TAG_KEY2_ARG)) {
            throw new IllegalArgumentException("Where stage requires '" + TAG_KEY2_ARG + "' argument");
        }
        String tagKey2 = (String) args.get(TAG_KEY2_ARG);
        if (tagKey2 == null || tagKey2.isEmpty()) {
            throw new IllegalArgumentException("Second tag key cannot be null or empty");
        }

        return new WhereStage(operator, tagKey1, tagKey2);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        WhereStage that = (WhereStage) obj;
        return Objects.equals(operator, that.operator) && Objects.equals(tagKey1, that.tagKey1) && Objects.equals(tagKey2, that.tagKey2);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operator, tagKey1, tagKey2);
    }
}
