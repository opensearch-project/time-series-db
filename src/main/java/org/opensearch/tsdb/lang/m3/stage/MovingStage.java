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
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.lang.m3.stage.moving.AvgCircularBuffer;
import org.opensearch.tsdb.lang.m3.stage.moving.MaxCircularBuffer;
import org.opensearch.tsdb.lang.m3.stage.moving.MinCircularBuffer;
import org.opensearch.tsdb.lang.m3.stage.moving.RunningMedian;
import org.opensearch.tsdb.lang.m3.stage.moving.SumCircularBuffer;
import org.opensearch.tsdb.lang.m3.stage.moving.WindowTransformer;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Pipeline stage that implements M3QL's moving function for time-based intervals.
 * Uses circular buffers for efficient computation of moving aggregations.
 *
 * Supported functions: sum, avg, max, min, median
 * Default function: sum
 */
@PipelineStageAnnotation(name = "moving")
public class MovingStage implements UnaryPipelineStage {
    /** The name of this stage. */
    public static final String NAME = "moving";
    private final long interval;
    private final String function;

    /**
     * Creates a moving stage with the specified interval and aggregation function.
     *
     * @param interval the window size in the same time unit as sample timestamps
     * @param function the aggregation function (sum, avg, min, max, median)
     */
    public MovingStage(long interval, String function) {
        this.interval = interval;
        this.function = function.toLowerCase(Locale.ROOT);
    }

    /**
     * Creates a moving stage with the specified interval using sum as default function.
     *
     * @param interval the window size in the same time unit as sample timestamps
     */
    public MovingStage(long interval) {
        this(interval, "sum");
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input.isEmpty()) {
            return input;
        }

        List<TimeSeries> result = new ArrayList<>(input.size());

        for (TimeSeries ts : input) {
            List<Sample> samples = ts.getSamples();
            if (samples.isEmpty()) {
                result.add(ts);
                continue;
            }

            long stepSize = ts.getStep();

            // Calculate window size in data points
            int windowPoints = (int) (interval / stepSize);
            if (windowPoints == 0) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "windowSize should not be smaller than stepSize, windowSize=%d, stepSize=%d",
                        interval,
                        stepSize
                    )
                );
            }

            WindowTransformer windowTransformer = createTransformer(windowPoints);

            List<Sample> movingSamples = new ArrayList<>();
            int sampleIndex = 0;

            // Iterate through all timestamps in the time grid
            for (long timestamp = ts.getMinTimestamp(); timestamp <= ts.getMaxTimestamp(); timestamp += stepSize) {
                // Per M3 behavior, we evaluate the moving value at first, and then update the window with current data point

                // Step 1: Evaluate the aggregated value from the current window state
                // Only add sample if there are non-null values in the window
                if (windowTransformer.getNonNullCount() > 0) {
                    double aggregatedValue = windowTransformer.value();
                    movingSamples.add(new FloatSample(timestamp, aggregatedValue));
                }

                // Step 2: Update the window with the current data point
                // Check if current timestamp matches the next sample in the input series
                if (sampleIndex < samples.size() && samples.get(sampleIndex).getTimestamp() == timestamp) {
                    // Add actual value to window
                    windowTransformer.add(samples.get(sampleIndex).getValue());
                    sampleIndex++;
                } else {
                    // Add null placeholder to window for missing data point
                    windowTransformer.addNull();
                }
            }

            result.add(
                new TimeSeries(movingSamples, ts.getLabels(), ts.getMinTimestamp(), ts.getMaxTimestamp(), ts.getStep(), ts.getAlias())
            );
        }

        return result;
    }

    private WindowTransformer createTransformer(int windowPoints) {
        return switch (function) {
            case "sum" -> new SumCircularBuffer(windowPoints);
            case "avg" -> new AvgCircularBuffer(windowPoints);
            case "max" -> new MaxCircularBuffer(windowPoints);
            case "min" -> new MinCircularBuffer(windowPoints);
            case "median" -> new RunningMedian(windowPoints);
            default -> throw new IllegalArgumentException("Unsupported moving function: " + function);
        };
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("interval", interval);
        builder.field("function", function);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(interval);
        out.writeString(function);
    }

    /**
     * Deserializes a MovingStage from a stream.
     *
     * @param in the input stream
     * @return the deserialized MovingStage
     * @throws IOException if an I/O error occurs
     */
    public static MovingStage readFrom(StreamInput in) throws IOException {
        long interval = in.readLong();
        String function = in.readString();
        return new MovingStage(interval, function);
    }

    /**
     * Creates a MovingStage from a map of arguments.
     * Supports both interval (long) and time_interval (string like "1m") parameters.
     *
     * @param args the argument map
     * @return the created MovingStage
     * @throws IllegalArgumentException if required parameters are missing
     */
    public static MovingStage fromArgs(Map<String, Object> args) {
        String function = args.containsKey("function") ? (String) args.get("function") : "sum";

        if (args.containsKey("interval")) {
            long interval = ((Number) args.get("interval")).longValue();
            return new MovingStage(interval, function);
        } else if (args.containsKey("time_interval")) {
            // Support parsing time strings like "1m", "5h", etc. for convenience
            String timeIntervalStr = (String) args.get("time_interval");
            TimeValue timeInterval = TimeValue.parseTimeValue(timeIntervalStr, null, "moving time_interval");
            return new MovingStage(timeInterval.getMillis(), function);
        } else {
            throw new IllegalArgumentException("MovingStage requires 'interval' or 'time_interval' parameter");
        }
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MovingStage that = (MovingStage) obj;
        return interval == that.interval && Objects.equals(function, that.function);
    }

    @Override
    public int hashCode() {
        return Objects.hash(interval, function);
    }
}
