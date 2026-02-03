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
import java.util.Locale;
import java.util.Map;

/**
 * PerSecondRateStage converts a monotonic, increasing timeseries into per-second average rate.
 *
 * <p>It is meant for converting Prometheus counters (stored as M3 gauges) into native M3 delta counters.
 * Resets/decreases are treated as a start of new series, and missing datapoints are interpolated
 * up to lookback interval, after that the next datapoint is treated as a reset.</p>
 *
 * <p>The values are also smoothed by applying a moving average, of the same interval as the lookback,
 * as truncating Prometheus scrape timestamps into M3 buckets introduces significant jitter
 * and misleading graphs.</p>
 *
 * <h2>Parameters:</h2>
 * <ul>
 *   <li><strong>interval:</strong> Lookback interval in the same time unit as the data (required)</li>
 * </ul>
 *
 * <h2>Behavior:</h2>
 * <ul>
 *   <li>Calculates rate as (current - previous) / timeInSeconds</li>
 *   <li>On counter reset (decrease), treats current value as new start</li>
 *   <li>Interpolates missing values within lookback window</li>
 *   <li>Applies moving average for smoothing</li>
 *   <li>First windowSize values are null due to insufficient data</li>
 * </ul>
 */
@PipelineStageAnnotation(name = PerSecondRateStage.NAME)
public class PerSecondRateStage implements UnaryPipelineStage {

    /** The name identifier for this stage. */
    public static final String NAME = "per_second_rate";

    /** Lookback interval in the same time unit as the data. */
    private final long interval;

    /** How many time units in the data equal one second (e.g., 1000 for milliseconds). */
    private final long unitsPerSecond;

    /**
     * Constructor for PerSecondRateStage.
     *
     * @param interval lookback interval in the same time unit as the data
     * @param unitsPerSecond how many time units equal one second (e.g., 1000 for milliseconds)
     * @throws IllegalArgumentException if interval &lt;= 0 or unitsPerSecond &lt;= 0
     */
    public PerSecondRateStage(long interval, long unitsPerSecond) {
        if (interval <= 0) {
            throw new IllegalArgumentException("Interval must be positive, got: " + interval);
        }
        if (unitsPerSecond <= 0) {
            throw new IllegalArgumentException("Units per second must be positive, got: " + unitsPerSecond);
        }
        this.interval = interval;
        this.unitsPerSecond = unitsPerSecond;
    }

    /**
     * Constructor for deserialization.
     */
    public PerSecondRateStage(StreamInput in) throws IOException {
        this.interval = in.readLong();
        this.unitsPerSecond = in.readLong();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return false;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return input;
        }

        // Validate interval against series resolution
        for (TimeSeries series : input) {
            long resolution = series.getStep();
            if (interval < resolution || interval % resolution != 0) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "perSecondRate interval (%d) must be >= series resolution (%d) and evenly divisible. Series: %s",
                        interval,
                        resolution,
                        series.getLabels()
                    )
                );
            }
        }

        List<TimeSeries> results = new ArrayList<>(input.size());
        for (TimeSeries series : input) {
            results.add(processSeries(series));
        }
        return results;
    }

    /**
     * Process a single time series to calculate per-second rate.
     */
    private TimeSeries processSeries(TimeSeries series) {
        long minTimestamp = series.getMinTimestamp();
        long maxTimestamp = series.getMaxTimestamp();
        long stepSize = series.getStep();
        int numSteps = (int) ((maxTimestamp - minTimestamp) / stepSize) + 1;
        int windowSizeInSteps = (int) (interval / stepSize);

        // Scratch buffer for intermediate rate calculations (using Double to represent null as null)
        Double[] scratchBuf = new Double[numSteps];

        // First pass: calculate rates and interpolate
        Double prev = null;
        int lastValidStep = -1;
        int sampleIdx = 0; // Pointer to current position in sparse samples list
        SampleList samples = series.getSamples();

        int step = 0;
        for (long timestamp = minTimestamp; timestamp <= maxTimestamp; timestamp += stepSize, step++) {
            // Check if we have a sample at this timestamp
            Double cur = null;
            if (sampleIdx < samples.size()) {
                if (samples.getTimestamp(sampleIdx) == timestamp) {
                    cur = samples.getValue(sampleIdx);
                    sampleIdx++; // Move to next sample
                }
            }

            long diffDuration = (step - lastValidStep) * stepSize;

            // Skip if no previous value, or gap is too large (treat as reset)
            if (prev == null || diffDuration > interval) {
                lastValidStep = step;
                prev = cur;
                continue;
            }

            // Skip if current value is null
            if (cur == null || Double.isNaN(cur)) {
                continue;
            }

            // Calculate difference
            double diff = cur - prev;

            // Handle counter reset (decrease)
            if (diff < 0) {
                diff = cur; // Assume counter started at 0
            }

            // Calculate per-second rate (convert time units to seconds)
            double rate = diff * unitsPerSecond / diffDuration;

            // Backfill interpolated values
            for (int i = lastValidStep + 1; i <= step; i++) {
                scratchBuf[i] = rate;
            }

            prev = cur;
            lastValidStep = step;
        }

        // Second pass: apply moving average
        List<Sample> resultSamples = new ArrayList<>(numSteps);

        for (int i = windowSizeInSteps; i < numSteps; i++) {
            double sum = 0.0;
            int count = 0;

            // Calculate moving average over window [i - windowSizeInSteps, i) - does not include position i
            for (int j = i - windowSizeInSteps; j < i && j >= 0; j++) {
                if (scratchBuf[j] != null) {
                    sum += scratchBuf[j];
                    count++;
                }
            }

            // Only create sample if we have a complete window
            if (count >= windowSizeInSteps) {
                double avg = sum / count;
                // Calculate timestamp from step position (works even when input sample is missing)
                long timestamp = minTimestamp + (long) i * stepSize;
                resultSamples.add(new FloatSample(timestamp, avg));
            }
        }

        return new TimeSeries(
            resultSamples,
            series.getLabels(),
            series.getMinTimestamp(),
            series.getMaxTimestamp(),
            series.getStep(),
            series.getAlias()
        );
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("interval", interval);
        builder.field("unitsPerSecond", unitsPerSecond);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(interval);
        out.writeLong(unitsPerSecond);
    }

    /**
     * Read a PerSecondRateStage from the input stream.
     */
    public static PerSecondRateStage readFrom(StreamInput in) throws IOException {
        return new PerSecondRateStage(in);
    }

    /**
     * Create a PerSecondRateStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return PerSecondRateStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static PerSecondRateStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }

        Object intervalObj = args.get("interval");
        if (intervalObj == null) {
            throw new IllegalArgumentException("interval argument is required");
        }

        if (!(intervalObj instanceof Number intervalNum)) {
            throw new IllegalArgumentException("interval must be a number");
        }

        Object unitsPerSecondObj = args.get("unitsPerSecond");
        if (unitsPerSecondObj == null) {
            throw new IllegalArgumentException("unitsPerSecond argument is required");
        }

        if (!(unitsPerSecondObj instanceof Number unitsPerSecondNum)) {
            throw new IllegalArgumentException("unitsPerSecond must be a number");
        }

        long interval = intervalNum.longValue();
        long unitsPerSecond = unitsPerSecondNum.longValue();
        return new PerSecondRateStage(interval, unitsPerSecond);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PerSecondRateStage that = (PerSecondRateStage) obj;
        return interval == that.interval && unitsPerSecond == that.unitsPerSecond;
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(interval);
        result = 31 * result + Long.hashCode(unitsPerSecond);
        return result;
    }
}
