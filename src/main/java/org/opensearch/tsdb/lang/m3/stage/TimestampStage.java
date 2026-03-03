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
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pipeline stage that implements M3QL's timestamp function.
 *
 * <p>Returns the start time of each step in seconds (Unix time), aligned with the series step.
 * Each value is replaced with the step's start timestamp in seconds.
 *
 * <p>Usage: fetch a | timestamp
 */
@PipelineStageAnnotation(name = "timestamp")
public class TimestampStage implements UnaryPipelineStage {

    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "timestamp";

    /** Milliseconds per second for converting timestamp to seconds. */
    private static final double MS_PER_SECOND = 1000.0;

    /**
     * Constructor for timestamp stage.
     */
    public TimestampStage() {
        // Default constructor
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // No parameters for timestamp stage
    }

    /**
     * Replaces each sample's value with the start time of that step in seconds (Unix time).
     *
     * <p>Uses the series <strong>step grid</strong> ({@code minTimestamp}, {@code step}), not the
     * actual timestamp of each sample. For step index {@code j}, the emitted value is
     * {@code (minTimestamp + i * step) / 1000.0} in seconds. Output has one sample per input
     * sample; the timestamp of each output sample is the same step start (in ms), and the value
     * is that time in seconds.
     *
     */
    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return input;
        }
        List<TimeSeries> result = new ArrayList<>(input.size());
        for (TimeSeries series : input) {
            SampleList samples = series.getSamples();
            long minTimestamp = series.getMinTimestamp();
            long step = series.getStep();
            List<Sample> mappedSamples = new ArrayList<>(samples.size());
            for (int i = 0; i < samples.size(); i++) {
                long stepStartMs = minTimestamp + (long) i * step;
                double valueSeconds = stepStartMs / MS_PER_SECOND;
                mappedSamples.add(new FloatSample(stepStartMs, valueSeconds));
            }
            result.add(
                new TimeSeries(
                    mappedSamples,
                    series.getLabels(),
                    series.getMinTimestamp(),
                    series.getMaxTimestamp(),
                    series.getStep(),
                    series.getAlias()
                )
            );
        }
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No parameters to serialize
    }

    /**
     * Create a TimestampStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new TimestampStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static TimestampStage readFrom(StreamInput in) throws IOException {
        return new TimestampStage();
    }

    /**
     * Create a TimestampStage from arguments map.
     *
     * @param args Map of argument names to values (ignored; timestamp takes no arguments)
     * @return TimestampStage instance
     */
    public static TimestampStage fromArgs(Map<String, Object> args) {
        return new TimestampStage();
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
