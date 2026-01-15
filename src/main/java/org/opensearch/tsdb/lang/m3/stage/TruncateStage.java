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
 * Pipeline stage that truncates time series to only include samples within a specified time range.
 * The time range is [minTimestamp, maxTimestamp) where minTimestamp is inclusive and maxTimestamp is exclusive.
 * Uses binary search for efficient index lookup.
 */
@PipelineStageAnnotation(name = "truncate")
public class TruncateStage implements UnaryPipelineStage {
    /** The name of this stage. */
    public static final String NAME = "truncate";

    private final long truncateStart;
    private final long truncateEnd;

    /**
     * Creates a truncate stage with the specified time bounds.
     *
     * @param truncateStart the truncation start timestamp (inclusive)
     * @param truncateEnd the truncation end timestamp (exclusive)
     */
    public TruncateStage(long truncateStart, long truncateEnd) {
        if (truncateStart >= truncateEnd) {
            throw new IllegalArgumentException("truncateStart must be < truncateEnd");
        }
        this.truncateStart = truncateStart;
        this.truncateEnd = truncateEnd;
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
            SampleList samples = ts.getSamples();

            // Calculate the new time range for the truncated series
            // newMinTimestamp: smallest value of form (original_min + N * step) that is >= truncateStart
            // newMaxTimestamp: largest value of form (newMinTimestamp + M * step) that is < truncateEnd
            long originalMin = ts.getMinTimestamp();
            long step = ts.getStep();

            // Find the smallest aligned timestamp >= truncateStart
            long newMinTimestamp;
            if (truncateStart <= originalMin) {
                newMinTimestamp = originalMin;
            } else {
                // Calculate how many steps from originalMin to reach or exceed truncateStart
                long stepsNeeded = (truncateStart - originalMin + step - 1) / step;
                newMinTimestamp = originalMin + stepsNeeded * step;
            }

            // Find the largest aligned timestamp < truncateEnd
            long newMaxTimestamp = TimeSeries.calculateAlignedMaxTimestamp(newMinTimestamp, truncateEnd, step);

            if (samples.isEmpty()) {
                // Empty sample list: return empty time series with truncated time bounds
                result.add(
                    new TimeSeries(new ArrayList<>(), ts.getLabels(), newMinTimestamp, newMaxTimestamp, ts.getStep(), ts.getAlias())
                );
                continue;
            }

            // Find the start and end indices using binary search
            int startIndex = findStartIndex(samples, truncateStart);
            int endIndex = findEndIndex(samples, truncateEnd);

            if (startIndex > endIndex) {
                // No samples in range, return empty time series with truncated time bounds
                result.add(
                    new TimeSeries(new ArrayList<>(), ts.getLabels(), newMinTimestamp, newMaxTimestamp, ts.getStep(), ts.getAlias())
                );
            } else {
                // Extract sublist of samples in range
                SampleList truncatedSamples = samples.subList(startIndex, endIndex + 1);
                result.add(new TimeSeries(truncatedSamples, ts.getLabels(), newMinTimestamp, newMaxTimestamp, ts.getStep(), ts.getAlias()));
            }
        }

        return result;
    }

    /**
     * Finds the first index where timestamp >= truncateStart using binary search.
     *
     * @param samples the list of samples (assumed to be sorted by timestamp)
     * @param truncateStart the truncation start timestamp
     * @return the start index
     */
    private int findStartIndex(SampleList samples, long truncateStart) {
        int index = samples.search(truncateStart);

        if (index >= 0) {
            // Exact match found, return this index
            return index;
        } else {
            // Not found, index = -(insertion point) - 1
            // insertion point is where the element would be inserted
            return -(index + 1);
        }
    }

    /**
     * Finds the last index where timestamp < truncateEnd using binary search.
     * Since truncateEnd is exclusive, we exclude any sample with timestamp >= truncateEnd.
     *
     * @param samples the list of samples (assumed to be sorted by timestamp)
     * @param truncateEnd the truncation end timestamp (exclusive)
     * @return the end index
     */
    private int findEndIndex(SampleList samples, long truncateEnd) {
        int index = samples.search(truncateEnd);

        if (index >= 0) {
            // Exact match found, but truncateEnd is exclusive, so return index - 1
            return index - 1;
        } else {
            // Not found, index = -(insertion point) - 1
            // We want the last element before the insertion point
            return -(index + 1) - 1;
        }
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("truncate_start", truncateStart);
        builder.field("truncate_end", truncateEnd);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(truncateStart);
        out.writeLong(truncateEnd);
    }

    /**
     * Deserializes a TruncateStage from a stream.
     *
     * @param in the input stream
     * @return the deserialized TruncateStage
     * @throws IOException if an I/O error occurs
     */
    public static TruncateStage readFrom(StreamInput in) throws IOException {
        long truncateStart = in.readLong();
        long truncateEnd = in.readLong();
        return new TruncateStage(truncateStart, truncateEnd);
    }

    /**
     * Creates a TruncateStage from a map of arguments.
     *
     * @param args the argument map
     * @return the created TruncateStage
     * @throws IllegalArgumentException if required parameters are missing
     */
    public static TruncateStage fromArgs(Map<String, Object> args) {
        if (!args.containsKey("truncate_start") || !args.containsKey("truncate_end")) {
            throw new IllegalArgumentException("TruncateStage requires both 'truncate_start' and 'truncate_end' parameters");
        }
        long truncateStart = ((Number) args.get("truncate_start")).longValue();
        long truncateEnd = ((Number) args.get("truncate_end")).longValue();
        return new TruncateStage(truncateStart, truncateEnd);
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TruncateStage that = (TruncateStage) obj;
        return Objects.equals(truncateStart, that.truncateStart) && Objects.equals(truncateEnd, that.truncateEnd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(truncateStart, truncateEnd);
    }
}
