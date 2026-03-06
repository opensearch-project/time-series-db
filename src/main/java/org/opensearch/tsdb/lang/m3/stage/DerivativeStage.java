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
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.Map;

/**
 * Pipeline stage that implements M3QL's derivative function.
 *
 * Calculates the derivative, the rate of change of a quantity, from a series of values.
 * Only emits derivative values when consecutive points are exactly step size apart.
 * Each derivative value is the difference between the current and the previous element.
 * Points that don't meet the step size requirement are skipped.
 *
 * Usage: fetch a | derivative
 */
@PipelineStageAnnotation(name = DerivativeStage.NAME)
public class DerivativeStage extends AbstractDerivativeStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "derivative";

    /**
     * Constructor for DerivativeStage.
     */
    public DerivativeStage() {
        // No arguments needed
    }

    @Override
    protected void computeDerivative(double prevValue, double currentValue, long currTimestamp, FloatSampleList.Builder builder) {
        if (!Double.isNaN(prevValue) && !Double.isNaN(currentValue)) {
            builder.add(currTimestamp, currentValue - prevValue);
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // No parameters
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No parameters
    }

    /**
     * Create a DerivativeStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new DerivativeStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static DerivativeStage readFrom(StreamInput in) throws IOException {
        return new DerivativeStage();
    }

    /**
     * Create a DerivativeStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return DerivativeStage instance
     */
    public static DerivativeStage fromArgs(Map<String, Object> args) {
        return new DerivativeStage();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        return obj != null && getClass() == obj.getClass();
    }

    @Override
    public int hashCode() {
        return NAME.hashCode();
    }
}
