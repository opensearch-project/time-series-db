/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * UnaryPipelineStage that implements M3QL's keepLastValue function.
 *
 * At each datapoint, if the current value is null (missing) then set the value
 * of the datapoint to be the last non-null value.
 *
 * Behavior:
 * - If interval is undefined: uses latest non-null value within the request window
 * - If interval is defined: only looks for last non-null value within that interval
 * - Missing data points are identified by gaps in the expected timestamp sequence
 * - Uses TimeSeries metadata (minTimestamp, maxTimestamp, step) to determine expected points
 * - Window is inclusive of current point but exclusive at the beginning
 *
 * Usage:
 * - keepLastValue         (no interval limit)
 * - keepLastValue 1m      (only look back 1 minute)
 * - keepLastValue 5h      (only look back 5 hours)
 */
@PipelineStageAnnotation(name = KeepLastValueStage.NAME)
public class KeepLastValueStage implements UnaryPipelineStage {
    /** The name of this pipeline stage. */
    public static final String NAME = "keep_last_value";

    /**field name of look back window when constructing from args*/
    public static final String LOOK_BACK_WINDOW = "look_back_window";

    private final Long lookBackWindow;  // null means no interval limit

    /**
     * Constructor for keepLastValue without interval limit.
     */
    public KeepLastValueStage() {
        this.lookBackWindow = null;
    }

    /**
     * Constructor for keepLastValue with interval limit.
     * @param lookBackWindow maximum time window to look back for non-null values
     */
    public KeepLastValueStage(Long lookBackWindow) {
        this.lookBackWindow = lookBackWindow;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return input;
        }

        List<TimeSeries> result = new ArrayList<>();

        for (TimeSeries ts : input) {
            SampleList filledSamples = fillMissingValues(ts);

            // Create new time series with filled values, preserving original metadata
            result.add(
                new TimeSeries(filledSamples, ts.getLabels(), ts.getMinTimestamp(), ts.getMaxTimestamp(), ts.getStep(), ts.getAlias())
            );
        }

        return result;
    }

    private SampleList fillMissingValues(TimeSeries ts) {
        SampleList originalSamples = ts.getSamples();
        if (originalSamples.isEmpty()) {
            return originalSamples;
        }

        TimestampGeneratorIterator tsIt = new TimestampGeneratorIterator(ts.getMinTimestamp(), ts.getMaxTimestamp(), ts.getStep());
        FloatSampleList.Builder filledSamplesBuilder = new FloatSampleList.Builder(
            (int) ((ts.getMaxTimestamp() - ts.getMinTimestamp()) / ts.getStep()) + 1
        );
        long lookBackLimitMs = lookBackWindow != null ? lookBackWindow : Long.MAX_VALUE;
        Double lastSeenValue = null;
        long lastSeenTimestamp = Long.MIN_VALUE;
        int originalSampleIndex = 0;
        while (tsIt.hasNext()) {
            long timestamp = tsIt.next();
            boolean exists = false;
            double existingValue = 0.0; // only valid if exists == true
            if (originalSampleIndex < originalSamples.size() && originalSamples.getTimestamp(originalSampleIndex) == timestamp) {
                // there exists a sample for this timestamp already
                exists = true;
                existingValue = originalSamples.getValue(originalSampleIndex);
                originalSampleIndex++;
            }

            if (exists) {
                filledSamplesBuilder.add(timestamp, existingValue);
                lastSeenValue = existingValue;
                lastSeenTimestamp = timestamp;

            } else {
                // missing sample, try to fill
                if (lastSeenValue != null && (timestamp - lastSeenTimestamp) <= lookBackLimitMs) {
                    filledSamplesBuilder.add(timestamp, lastSeenValue);
                }
                // else skip this timestamp (remains null/missing)
            }
        }
        return filledSamplesBuilder.build();
    }

    /**
     * Iterator for generating timestamps on-demand without creating full array.
     */
    private static class TimestampGeneratorIterator {
        private long current;
        private final long max;
        private final long step;

        public TimestampGeneratorIterator(long min, long max, long step) {
            this.current = min;
            this.max = max;
            this.step = step;
        }

        public boolean hasNext() {
            return current <= max;
        }

        public long next() {
            long result = current;
            current += step;
            return result;
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        if (lookBackWindow != null) {
            builder.field(LOOK_BACK_WINDOW, lookBackWindow);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Write whether we have an interval
        out.writeOptionalLong(lookBackWindow);
    }

    /**
     * Create a KeepLastValueStage instance from the input stream for deserialization.
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     * @return a new KeepLastValueStage instance with the deserialized interval
     */
    public static KeepLastValueStage readFrom(StreamInput in) throws IOException {
        Long windowMs = in.readOptionalLong();
        return new KeepLastValueStage(windowMs);
    }

    /**
     * Create a KeepLastValueStage instance from XContent for deserialization.
     * @param parser the XContent parser to read from
     * @throws IOException if a parsing error occurs
     * @return a new KeepLastValueStage instance with the parsed interval
     */
    public static KeepLastValueStage fromXContent(XContentParser parser) throws IOException {
        Long lookBackWindow = null;
        String currentFieldName = null;
        XContentParser.Token token;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (LOOK_BACK_WINDOW.equals(currentFieldName)) {
                    lookBackWindow = parser.longValue();
                }
            }
        }

        return new KeepLastValueStage(lookBackWindow);
    }

    /**
     * Create a KeepLastValue from arguments map.
     *
     * @param args Map of argument names to values
     * @return KeepLastValue instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static KeepLastValueStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }
        Object lookBackWindowObj = args.get(LOOK_BACK_WINDOW); // default is no interval limit
        Long lookBackWindow = null;
        if (lookBackWindowObj != null) {
            if (lookBackWindowObj instanceof Number num) {
                lookBackWindow = num.longValue();
            } else {
                lookBackWindow = Long.parseLong(lookBackWindowObj.toString());
            }
        }
        return new KeepLastValueStage(lookBackWindow);
    }

    /**
     * Get the look back window if specified.
     * @return the look back window, or null if unlimited
     */
    public Long getLookBackWindow() {
        return lookBackWindow;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        KeepLastValueStage that = (KeepLastValueStage) obj;
        return Objects.equals(lookBackWindow, that.lookBackWindow);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lookBackWindow);
    }

    /**
     * Estimate temporary memory overhead for keep last value operations.
     * KeepLastValueStage can expand sparse series to dense series.
     *
     * @param input The input time series
     * @return Estimated temporary memory overhead in bytes
     */
    @Override
    public long estimateMemoryOverhead(List<TimeSeries> input) {
        if (input == null || input.isEmpty()) {
            return 0;
        }

        long totalOverhead = 0;
        for (TimeSeries ts : input) {
            // Estimate potential expansion: (maxTimestamp - minTimestamp) / step + 1
            long minTs = ts.getMinTimestamp();
            long maxTs = ts.getMaxTimestamp();
            long step = ts.getStep();

            if (step > 0 && maxTs >= minTs) {
                int potentialSamples = (int) ((maxTs - minTs) / step) + 1;
                int currentSamples = ts.getSamples().size();

                // Only track expansion (new samples beyond current)
                int expansion = Math.max(0, potentialSamples - currentSamples);
                totalOverhead += SampleList.ARRAYLIST_OVERHEAD + (expansion * TimeSeries.ESTIMATED_SAMPLE_SIZE);
            }
        }

        return totalOverhead;
    }
}
