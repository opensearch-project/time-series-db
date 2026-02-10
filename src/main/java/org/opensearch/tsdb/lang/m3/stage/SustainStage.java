/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.model.FloatSampleList;
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
 * Pipeline stage that filters individual samples based on sustained non-null value windows.
 *
 * This stage removes individual data points that do not have an uninterrupted prefix
 * of non-null values before and including that point for the specified duration. Samples
 * that don't meet the duration requirement are simply omitted from the output.
 *
 * The stage is designed to be used in conjunction with M3QL's value filtering to identify
 * periods where a time series maintains a condition (e.g., above a threshold) for a sustained
 * duration.

 *
 * Usage
 * -- Keep only samples where CPU usage is above 10 for a sustained 5 minutes
 * fetch name:cpu.usage | > 10 | sustain 5m

 * -- Keep only samples with at least 30 seconds of consecutive valid data before them
 * fetch name:metric.name | sustain 30s

 */
@PipelineStageAnnotation(name = "sustain")
public class SustainStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "sustain";

    /** The argument name for duration parameter. */
    public static final String DURATION_ARG = "duration";

    private final long duration;

    /**
     * Constructs a new SustainStage with the specified time duration.
     *
     * @param duration the minimum time duration
     * @throws IllegalArgumentException if duration is negative
     */
    public SustainStage(long duration) {
        if (duration < 0) {
            throw new IllegalArgumentException("Duration must be non-negative, got: " + duration);
        }
        this.duration = duration;
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

        for (TimeSeries series : input) {
            TimeSeries filtered = filterSamples(series);
            result.add(filtered);
        }

        return result;
    }

    /**
     * Filter samples in a time series, keeping only those that have a sustained prefix
     * of non-null values of the required duration leading up to and including that sample.
     *
     * <p>For each sample, this method checks if there's an uninterrupted sequence of
     * non-null, non-NaN values before and including that sample that meets the duration
     * requirement. Samples that don't meet this requirement are omitted from the output.</p>
     *
     * @param series the time series to filter
     * @return a new time series with filtered samples
     */
    private TimeSeries filterSamples(TimeSeries series) {
        SampleList samples = series.getSamples();

        if (samples == null || samples.isEmpty()) {
            return series;
        }

        // Special case: duration of 0 means no filtering
        if (duration == 0) {
            return series;
        }

        long step = series.getStep();
        if (step <= 0) {
            throw new IllegalStateException("Time series step must be positive, got: " + step);
        }

        // Calculate required number of samples (floor division)
        long requiredSamples = duration / step;

        FloatSampleList.Builder filteredSamples = new FloatSampleList.Builder();
        int consecutiveNonNull = 0;

        for (Sample sample : samples) {
            // Check if current sample is non-null and has a valid (non-NaN) value
            if (sample != null && !Double.isNaN(sample.getValue())) {
                consecutiveNonNull++;

                // Keep sample only if it has a sustained prefix
                if (consecutiveNonNull >= requiredSamples) {
                    filteredSamples.add(sample.getTimestamp(), sample.getValue());
                }
            } else {
                // Null or NaN breaks the prefix - reset counter
                consecutiveNonNull = 0;
            }
        }

        return new TimeSeries(
            filteredSamples.build(),
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

    /**
     * Get the duration
     *
     * @return the duration value
     */
    public long getDuration() {
        return duration;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(DURATION_ARG, TimeValue.timeValueMillis(duration).getStringRep());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(duration);
    }

    /**
     * Create a SustainStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new SustainStage instance with the deserialized duration
     * @throws IOException if an I/O error occurs
     */
    public static SustainStage readFrom(StreamInput in) throws IOException {
        long duration = in.readVLong();
        return new SustainStage(duration);
    }

    /**
     * Create a SustainStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return SustainStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static SustainStage fromArgs(Map<String, Object> args) {
        if (args == null || !args.containsKey(DURATION_ARG)) {
            throw new IllegalArgumentException("Sustain stage requires '" + DURATION_ARG + "' argument");
        }

        Object durationObj = args.get(DURATION_ARG);
        if (durationObj == null) {
            throw new IllegalArgumentException("Duration cannot be null");
        }

        long duration;
        if (durationObj instanceof String durationStr) {
            try {
                TimeValue timeValue = TimeValue.parseTimeValue(durationStr, null, "sustain");
                duration = timeValue.getMillis();
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid duration format: " + durationObj, e);
            }
        } else if (durationObj instanceof Number durationNum) {
            // Numeric value - unit determined by index configuration
            duration = durationNum.longValue();
        } else {
            throw new IllegalArgumentException("Duration must be a string or number, got: " + durationObj.getClass().getSimpleName());
        }

        return new SustainStage(duration);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(duration);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SustainStage that = (SustainStage) obj;
        return duration == that.duration;
    }
}
