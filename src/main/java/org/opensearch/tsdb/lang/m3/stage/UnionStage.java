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
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.BinaryPipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Binary pipeline stage that combines two sets of time series by taking their union.
 * This stage concatenates all time series from both left and right operands
 * into a single result list. The left operand is modified to include values from right operand. Note that duplicates
 * from left and right operands are retained.
 */
@PipelineStageAnnotation(name = UnionStage.NAME)
public class UnionStage implements BinaryPipelineStage {
    /** The name of this pipeline stage. */
    public static final String NAME = "union";

    private final String rightOperandReferenceName;

    /**
     * Constructs a new UnionStage with the specified right operand reference name.
     *
     * @param rightOperandReferenceName the reference name for the right operand
     */
    public UnionStage(String rightOperandReferenceName) {
        this.rightOperandReferenceName = rightOperandReferenceName;
    }

    /**
     * Process two lists of time series by taking their union.
     *
     * @param left the left operand time series list. This must be a mutable list.
     * @param right the right operand time series list. This must be a mutable list.
     * @return the left operand list after adding all time series from provided right operand.
     */
    @Override
    public List<TimeSeries> process(List<TimeSeries> left, List<TimeSeries> right) {
        if (left == null) {
            throw new NullPointerException(getName() + " stage received null left input");
        }
        if (right == null) {
            throw new NullPointerException(getName() + " stage received null right input");
        }
        List<TimeSeries> result = new ArrayList<>(left.size() + right.size());
        // Both left and right could be unmodifiable list so create a new list and add all
        result.addAll(left);
        result.addAll(right);
        return result;
    }

    /**
     * Get the reference name for the right operand.
     *
     * @return the right operand reference name
     */
    @Override
    public String getRightOpReferenceName() {
        return rightOperandReferenceName;
    }

    /**
     * Get the name of this pipeline stage.
     *
     * @return the stage name "union"
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Serialize this stage to XContent format.
     *
     * @param builder the XContent builder to write to
     * @param params serialization parameters
     * @throws IOException if an I/O error occurs during serialization
     */
    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(RIGHT_OP_REFERENCE_PARAM_KEY, rightOperandReferenceName);
    }

    /**
     * Write this stage to the output stream for serialization.
     *
     * @param out the stream output to write to
     * @throws IOException if an I/O error occurs while writing to the stream
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(rightOperandReferenceName);
    }

    /**
     * Create a UnionStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new UnionStage instance
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public static UnionStage readFrom(StreamInput in) throws IOException {
        String rightOperandReferenceName = in.readString();
        return new UnionStage(rightOperandReferenceName);
    }

    /**
     * Creates a new instance of UnionStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct a UnionStage instance.
     *             The map must include a key and string value for right operand reference name.
     * @return a new UnionStage instance initialized with the provided right operand reference.
     */
    public static UnionStage fromArgs(Map<String, Object> args) {
        String rightOpReference = (String) args.get(RIGHT_OP_REFERENCE_PARAM_KEY);
        return new UnionStage(rightOpReference);
    }

    @Override
    public int hashCode() {
        return rightOperandReferenceName != null ? rightOperandReferenceName.hashCode() : 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        UnionStage that = (UnionStage) obj;
        if (rightOperandReferenceName == null && that.rightOperandReferenceName == null) {
            return true;
        }
        if (rightOperandReferenceName == null || that.rightOperandReferenceName == null) {
            return false;
        }
        return rightOperandReferenceName.equals(that.rightOperandReferenceName);
    }
}
