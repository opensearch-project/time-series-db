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
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@PipelineStageAnnotation(name = SubtractStage.NAME)
public class SubtractStage extends AbstractBinaryProjectionStage {
    /** The name of this pipeline stage. */
    public static final String NAME = "subtract";
    /** The parameter name for keepNan keys. */
    public static final String KEEP_NANS_PARAM_KEY = "keep_nans";
    private final String rightOperandReferenceName;
    private final List<String> labelKeys;
    private final boolean keepNaNs;

    /**
     * Constructs a new SubtractStage with the specified right operand reference name.
     *
     * @param rightOperandReferenceName the reference for the right operand
     */
    public SubtractStage(String rightOperandReferenceName) {
        this(rightOperandReferenceName, false, null);
    }

    /**
     * Constructs a new SubtractStage with the specified right operand reference name and labels keys.
     *
     * @param rightOperandReferenceName the reference for the right operand
     * @param keepNaNs the flag to keep empty value from right or left operand
     * @param labelKeys the specific label keys to consider for matching, or null for full matching
     */
    public SubtractStage(String rightOperandReferenceName, boolean keepNaNs, List<String> labelKeys) {
        this.rightOperandReferenceName = rightOperandReferenceName;
        this.keepNaNs = keepNaNs;
        this.labelKeys = labelKeys;
    }

    @Override
    protected boolean hasKeepNansOption() {
        return true;
    }

    @Override
    protected NormalizationStrategy getNormalizationStrategy() {
        return NormalizationStrategy.NONE;
    }

    @Override
    protected List<String> getLabelKeys() {
        return labelKeys;
    }

    @Override
    protected boolean shouldExtractCommonTagKeys() {
        return true;
    }

    protected boolean isKeepNaNs() {
        return keepNaNs;
    }

    /**
     * Sum multiple time series into one time series
     * @param rightTimeSeriesList
     * @return
     */
    @Override
    protected TimeSeries mergeMatchingSeries(List<TimeSeries> rightTimeSeriesList) {
        if (rightTimeSeriesList.isEmpty()) {
            return null;
        }
        if (rightTimeSeriesList.size() == 1) {
            return rightTimeSeriesList.getFirst();
        }
        Map<Long, Double> timestampToValue = new HashMap<>();
        for (TimeSeries timeSeries : rightTimeSeriesList) {
            SampleList samples = timeSeries.getSamples();
            for (Sample sample : samples) {
                timestampToValue.merge(sample.getTimestamp(), sample.getValue(), Double::sum);
            }
        }
        List<Sample> samples = timestampToValue.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .map((entry) -> new FloatSample(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());

        TimeSeries firstTimeSeries = rightTimeSeriesList.getFirst();
        return new TimeSeries(
            samples,
            firstTimeSeries.getLabels(),
            firstTimeSeries.getMinTimestamp(),
            firstTimeSeries.getMaxTimestamp(),
            firstTimeSeries.getStep(),
            firstTimeSeries.getAlias()
        );
    }

    @Override
    protected Double processSampleValues(Double leftValue, Double rightValue) {
        // Treat NaN samples as null at the very beginning
        if (leftValue != null && Double.isNaN(leftValue)) {
            leftValue = null;
        }
        if (rightValue != null && Double.isNaN(rightValue)) {
            rightValue = null;
        }

        // Scenario 1: Both samples are null. Regardless keepNans, we should return null.
        if (leftValue == null && rightValue == null) {
            return null;
        }
        // Scenario 2: One sample is null, and we are configured to propagate null (keepNaNs = true).
        if (keepNaNs && (leftValue == null || rightValue == null)) {
            return null;
        }
        // Scenario 3: KeepNans is false, we treat null samples as 0.0
        leftValue = leftValue != null ? leftValue : 0.0;
        rightValue = rightValue != null ? rightValue : 0.0;
        return leftValue - rightValue;
    }

    @Override
    public String getRightOpReferenceName() {
        return rightOperandReferenceName;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(RIGHT_OP_REFERENCE_PARAM_KEY, rightOperandReferenceName);
        builder.field(KEEP_NANS_PARAM_KEY, keepNaNs);
        if (labelKeys != null && !labelKeys.isEmpty()) {
            builder.field(LABELS_PARAM_KEY, labelKeys);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(rightOperandReferenceName);
        out.writeBoolean(keepNaNs);
        out.writeOptionalStringCollection(labelKeys);
    }

    /**
     * Create an SubtractStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new SubtractStage instance
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public static SubtractStage readFrom(StreamInput in) throws IOException {
        String referenceName = in.readString();
        boolean keepNaNs = in.readBoolean();
        List<String> labelTag = in.readOptionalStringList();
        return new SubtractStage(referenceName, keepNaNs, labelTag);
    }

    /**
     * Creates a new instance of SubtractStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct an SubtractStage instance.
     *             The map must include a key for right operand reference with a String value representing
     *             the right operand reference name. Optionally, it can include labelKeys for selective matching.
     * @return a new SubtractStage instance initialized with the provided right operand reference and labelKeys.
     */
    @SuppressWarnings("unchecked")
    public static SubtractStage fromArgs(Map<String, Object> args) {
        String rightOpReference = (String) args.get(RIGHT_OP_REFERENCE_PARAM_KEY);
        Boolean keepNans = (Boolean) args.get(KEEP_NANS_PARAM_KEY);
        List<String> labelTag = (List<String>) args.get(LABELS_PARAM_KEY);
        return new SubtractStage(rightOpReference, keepNans == null ? false : keepNans.booleanValue(), labelTag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rightOperandReferenceName, keepNaNs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        SubtractStage stage = (SubtractStage) obj;
        return Objects.equals(keepNaNs, stage.keepNaNs) && Objects.equals(rightOperandReferenceName, stage.rightOperandReferenceName);
    }
}
