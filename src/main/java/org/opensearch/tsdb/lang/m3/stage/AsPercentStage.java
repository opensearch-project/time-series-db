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
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Binary pipeline projection stage that processes time series to compute percentage values.
 */
@PipelineStageAnnotation(name = AsPercentStage.NAME)
public class AsPercentStage extends AbstractBinaryProjectionStage {
    /** The name of this pipeline stage. */
    public static final String NAME = "as_percent";

    /** The parameter name for label keys. */
    public static final String LABELS_PARAM_KEY = "labels";

    /** The type label value to add to all generated time series. */
    private static final String TYPE_LABEL = "type";
    private static final String RATIOS_VALUE = "ratios";

    private final String rightOperandReferenceName;
    private final List<String> labelKeys;

    /**
     * Constructs a new AsPercentStage with the specified right operand reference name.
     *
     * @param rightOperandReferenceName the reference for the right operand
     */
    public AsPercentStage(String rightOperandReferenceName) {
        this(rightOperandReferenceName, null);
    }

    /**
     * Constructs a new AsPercentStage with the specified right operand reference name and labels keys.
     *
     * @param rightOperandReferenceName the reference for the right operand
     * @param labelKeys the specific label keys to consider for matching, or null for full matching
     */
    public AsPercentStage(String rightOperandReferenceName, List<String> labelKeys) {
        this.rightOperandReferenceName = rightOperandReferenceName;
        this.labelKeys = labelKeys;
    }

    @Override
    public String getRightOpReferenceName() {
        return rightOperandReferenceName;
    }

    @Override
    protected List<String> getLabelKeys() {
        return labelKeys;
    }

    /**
     * Process samples to calculate percentage. Both samples are expected to be available.
     *
     * @param leftSample The left sample
     * @param rightSample The right sample
     * @return A FloatSample with percentage value, or null if right value is 0
     */
    @Override
    protected Sample processSamples(Sample leftSample, Sample rightSample) {
        double leftValue = leftSample.getValue();
        double rightValue = rightSample.getValue();

        // If right value is 0, return NaN
        if (rightValue == 0.0) {
            return new FloatSample(leftSample.getTimestamp(), Float.NaN);
        }

        double percentage = (leftValue / rightValue) * 100.0;
        return new FloatSample(leftSample.getTimestamp(), percentage);
    }

    /**
     * Transform labels to add the type:ratios label to all generated time series.
     * This ensures that all AsPercent stage outputs are tagged with type=ratios.
     *
     * @param originalLabels The original labels from the left series
     * @return The labels with type:ratios added
     */
    @Override
    protected Labels transformLabels(Labels originalLabels) {
        return originalLabels.withLabel(TYPE_LABEL, RATIOS_VALUE);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(RIGHT_OP_REFERENCE_PARAM_KEY, rightOperandReferenceName);
        if (labelKeys != null && !labelKeys.isEmpty()) {
            builder.field(LABELS_PARAM_KEY, labelKeys);
        }
    }

    /**
     * Write stage-specific data to the output stream for serialization.
     */
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(rightOperandReferenceName);
        out.writeOptionalStringCollection(labelKeys);
    }

    /**
     * Create an AsPercentStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new AsPercentStage instance
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public static AsPercentStage readFrom(StreamInput in) throws IOException {
        String referenceName = in.readString();
        List<String> labelTag = in.readOptionalStringList();

        return new AsPercentStage(referenceName, labelTag);
    }

    /**
     * Creates a new instance of AsPercentStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct an AsPercentStage instance.
     *             The map must include a key for right operand reference with a String value representing
     *             the right operand reference name. Optionally, it can include labelKeys for selective matching.
     * @return a new AsPercentStage instance initialized with the provided right operand reference and labelKeys.
     */
    @SuppressWarnings("unchecked")
    public static AsPercentStage fromArgs(Map<String, Object> args) {
        String rightOpReference = (String) args.get(RIGHT_OP_REFERENCE_PARAM_KEY);
        List<String> labelTag = (List<String>) args.get(LABELS_PARAM_KEY);
        return new AsPercentStage(rightOpReference, labelTag);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (rightOperandReferenceName != null ? rightOperandReferenceName.hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }
        AsPercentStage that = (AsPercentStage) obj;
        if (rightOperandReferenceName == null && that.rightOperandReferenceName == null) {
            return true;
        }
        if (rightOperandReferenceName == null || that.rightOperandReferenceName == null) {
            return false;
        }
        return rightOperandReferenceName.equals(that.rightOperandReferenceName);
    }
}
