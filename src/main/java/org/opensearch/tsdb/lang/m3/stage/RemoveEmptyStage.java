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
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A pipeline stage that removes series with empty samples lists or all NaN values.
 * This implements the removeEmpty function from M3QL
 */
@PipelineStageAnnotation(name = "remove_empty")
public class RemoveEmptyStage implements UnaryPipelineStage {
    /** The name of this pipeline stage. */
    public static final String NAME = "remove_empty";

    /**
     * Constructor for removeEmpty stage.
     */
    public RemoveEmptyStage() {
        // Default constructor
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        return input.stream().filter(series -> {
            SampleList samples = series.getSamples();
            // Remove if empty or if all values are NaN
            if (samples.isEmpty()) {
                return false;
            }
            // Check if all values are NaN
            for (Sample sample : samples) {
                if (!Double.isNaN(sample.getValue())) {
                    return true;
                }
            }
            return false;
        }).collect(Collectors.toList());
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // No parameters for removeEmpty stage
    }

    /**
     * Write to a stream for serialization.
     * @param out the stream output to write to
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No parameters for removeEmpty stage
    }

    /**
     * Create a RemoveEmptyStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new RemoveEmptyStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static RemoveEmptyStage readFrom(StreamInput in) throws IOException {
        return new RemoveEmptyStage();
    }

    /**
     * Create an RemoveEmptyStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return RemoveEmptyStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static RemoveEmptyStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }
        return new RemoveEmptyStage();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return true; // All RemoveEmptyStage instances are equal since they have no parameters
    }

    @Override
    public int hashCode() {
        return Objects.hash(NAME);
    }
}
