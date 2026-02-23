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
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pipeline stage that implements M3QL's changed function.
 *
 * Takes one metric or a wildcard seriesList. Outputs 1 when the value changed
 * and outputs 0 when the value is null or the same. Each sample in the time
 * series is processed and outputs either 0 or 1 based on comparison with the
 * prior non-null and non-NaN value.
 *
 * This stage requires information from prior samples and does not support
 * concurrent segment search.
 *
 * Usage: fetch a | changed
 */
@PipelineStageAnnotation(name = "changed")
public class ChangedStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "changed";

    /**
     * Constructor for ChangedStage.
     */
    public ChangedStage() {
        // No arguments needed
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return input;
        }

        List<TimeSeries> result = new ArrayList<>(input.size());

        for (TimeSeries ts : input) {
            result.add(processSeries(ts));
        }

        return result;
    }

    /**
     * Process a single time series to detect changes.
     * Iterates through all expected timestamps in the range [minTimestamp, maxTimestamp]
     * with step increments. Missing samples are treated as null/NaN and emit 0.
     */
    private TimeSeries processSeries(TimeSeries series) {
        long minTimestamp = series.getMinTimestamp();
        long maxTimestamp = series.getMaxTimestamp();
        long stepSize = series.getStep();
        SampleList samples = series.getSamples();

        // Calculate total number of expected timestamps
        int numSteps = (int) ((maxTimestamp - minTimestamp) / stepSize) + 1;
        FloatSampleList.Builder resultBuilder = new FloatSampleList.Builder(numSteps);

        // Track last non-null value for comparison
        Double lastNonNullValue = null;

        // Pointer to current position in sparse samples list
        int sampleIdx = 0;

        // Iterate through all expected timestamps
        for (long timestamp = minTimestamp; timestamp <= maxTimestamp; timestamp += stepSize) {
            // Check if we have a sample at this timestamp
            Double currentValue = null;
            if (sampleIdx < samples.size()) {
                if (samples.getTimestamp(sampleIdx) == timestamp) {
                    double value = samples.getValue(sampleIdx);
                    currentValue = Double.isNaN(value) ? null : value;
                    sampleIdx++; // Move to next sample
                }
            }

            double outputValue;

            if (currentValue == null) {
                // Current value is null/NaN or missing: output 0
                outputValue = 0.0;
            } else {
                // Current value is not null/NaN: compare with prior value
                outputValue = (lastNonNullValue != null && !currentValue.equals(lastNonNullValue)) ? 1.0 : 0.0;
                lastNonNullValue = currentValue;
            }

            resultBuilder.add(timestamp, outputValue);
        }

        return new TimeSeries(
            resultBuilder.build(),
            series.getLabels(),
            series.getMinTimestamp(),
            series.getMaxTimestamp(),
            series.getStep(),
            series.getAlias()
        );
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
     * Create a ChangedStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new ChangedStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static ChangedStage readFrom(StreamInput in) throws IOException {
        return new ChangedStage();
    }

    /**
     * Create a ChangedStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return ChangedStage instance
     */
    public static ChangedStage fromArgs(Map<String, Object> args) {
        return new ChangedStage();
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
