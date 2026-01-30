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
 * Pipeline stage that scales time series values to represent "value per seconds".
 *
 * <p>The scaleToSeconds function adjusts the metric values in a seriesList to represent their
 * "value per seconds" based on a specified duration (in seconds). It takes the original value
 * as if it were emitted every N seconds (determined by the series step) and scales it to the
 * target seconds.</p>
 *
 * <p>Formula: scaledValue = originalValue * targetSeconds / stepSeconds</p>
 *
 * <h2>Example:</h2>
 * <p>If a series has a step of 10 seconds and value 10, and we scale to 2 seconds:</p>
 * <p>scaledValue = 10 * 2 / 10 = 2</p>
 *
 * <h2>Usage:</h2>
 * <pre>{@code
 * // Scale to 2 seconds
 * ScaleToSecondsStage scaleStage = new ScaleToSecondsStage(2);
 * List<TimeSeries> result = scaleStage.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Use Cases:</h2>
 * <ul>
 *   <li><strong>Normalize rates:</strong> Convert metrics to a standard per-second rate</li>
 *   <li><strong>Resolution adjustment:</strong> Adjust values for different time resolutions</li>
 *   <li><strong>Comparison:</strong> Make metrics with different steps comparable</li>
 * </ul>
 */
@PipelineStageAnnotation(name = ScaleToSecondsStage.NAME)
public class ScaleToSecondsStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "scale_to_seconds";

    private final double seconds;

    /**
     * Constructs a new ScaleToSecondsStage with the specified target seconds.
     *
     * @param seconds the target duration in seconds to scale values to
     * @throws IllegalArgumentException if seconds is less than or equal to 0
     */
    public ScaleToSecondsStage(double seconds) {
        if (seconds <= 0) {
            throw new IllegalArgumentException("Seconds must be positive, got: " + seconds);
        }
        this.seconds = seconds;
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
            // Calculate scale factor for this series based on its step
            // TODO: Get time unit from planner - step values are unit-less, we shouldn't assume milliseconds.
            // The planner should inform us of the actual time unit so we can correctly convert to seconds.
            long stepMs = series.getStep();
            double stepSeconds = stepMs / 1000.0;
            double scaleFactor = seconds / stepSeconds;

            // Process samples with the calculated scale factor
            SampleList originalSamples = series.getSamples();
            List<Sample> mappedSamples = new ArrayList<>(originalSamples.size());

            for (Sample sample : originalSamples) {
                double scaledValue = sample.getValue() * scaleFactor;
                mappedSamples.add(new FloatSample(sample.getTimestamp(), scaledValue));
            }

            // Create new time series with scaled samples, preserving all metadata
            TimeSeries mappedTimeSeries = new TimeSeries(
                mappedSamples,
                series.getLabels(),
                series.getMinTimestamp(),
                series.getMaxTimestamp(),
                series.getStep(),
                series.getAlias()
            );
            result.add(mappedTimeSeries);
        }

        return result;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return true;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("seconds", seconds);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(seconds);
    }

    /**
     * Create a ScaleToSecondsStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new ScaleToSecondsStage instance with the deserialized seconds
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static ScaleToSecondsStage readFrom(StreamInput in) throws IOException {
        double seconds = in.readDouble();
        return new ScaleToSecondsStage(seconds);
    }

    /**
     * Create a ScaleToSecondsStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return ScaleToSecondsStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static ScaleToSecondsStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }
        double seconds = ((Number) args.get("seconds")).doubleValue();
        return new ScaleToSecondsStage(seconds);
    }

    @Override
    public int hashCode() {
        return Double.hashCode(seconds);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ScaleToSecondsStage that = (ScaleToSecondsStage) obj;
        return Double.compare(seconds, that.seconds) == 0;
    }
}
