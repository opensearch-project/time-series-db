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
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Pipeline stage that truncates time series to only include samples within a specified time range.
 * Supports optional minimum and/or maximum timestamp bounds.
 * Uses binary search for efficient index lookup.
 */
@PipelineStageAnnotation(name = "truncate")
public class TruncateStage implements UnaryPipelineStage {
    /** The name of this stage. */
    public static final String NAME = "truncate";

    private final long minTimestamp;
    private final long maxTimestamp;

    /**
     * Creates a truncate stage with the specified time bounds.
     *
     * @param minTimestamp the minimum timestamp (inclusive)
     * @param maxTimestamp the maximum timestamp (inclusive)
     */
    public TruncateStage(long minTimestamp, long maxTimestamp) {
        if (minTimestamp > maxTimestamp) {
            throw new IllegalArgumentException("minTimestamp must be <= maxTimestamp");
        }
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
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

            // Find the start and end indices using binary search
            int startIndex = findStartIndex(samples, minTimestamp);
            int endIndex = findEndIndex(samples, maxTimestamp);

            if (startIndex > endIndex) {
                // No samples in range, return empty time series
                result.add(
                    new TimeSeries(
                        new ArrayList<>(),
                        ts.getLabels(),
                        ts.getMinTimestamp(),
                        ts.getMaxTimestamp(),
                        ts.getStep(),
                        ts.getAlias()
                    )
                );
            } else {
                // Extract sublist of samples in range
                List<Sample> truncatedSamples = new ArrayList<>(samples.subList(startIndex, endIndex + 1));
                result.add(
                    new TimeSeries(
                        truncatedSamples,
                        ts.getLabels(),
                        ts.getMinTimestamp(),
                        ts.getMaxTimestamp(),
                        ts.getStep(),
                        ts.getAlias()
                    )
                );
            }
        }

        return result;
    }

    /**
     * Finds the first index where timestamp >= minTimestamp using binary search.
     *
     * @param samples the list of samples (assumed to be sorted by timestamp)
     * @param minTimestamp the minimum timestamp
     * @return the start index
     */
    private int findStartIndex(List<Sample> samples, long minTimestamp) {
        int index = Collections.binarySearch(samples, new FloatSample(minTimestamp, 0.0), Comparator.comparingLong(Sample::getTimestamp));

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
     * Finds the last index where timestamp <= maxTimestamp using binary search.
     *
     * @param samples the list of samples (assumed to be sorted by timestamp)
     * @param maxTimestamp the maximum timestamp
     * @return the end index
     */
    private int findEndIndex(List<Sample> samples, long maxTimestamp) {
        int index = Collections.binarySearch(samples, new FloatSample(maxTimestamp, 0.0), Comparator.comparingLong(Sample::getTimestamp));

        if (index >= 0) {
            // Exact match found, return this index
            return index;
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
        builder.field("min_timestamp", minTimestamp);
        builder.field("max_timestamp", maxTimestamp);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
    }

    /**
     * Deserializes a TruncateStage from a stream.
     *
     * @param in the input stream
     * @return the deserialized TruncateStage
     * @throws IOException if an I/O error occurs
     */
    public static TruncateStage readFrom(StreamInput in) throws IOException {
        long minTimestamp = in.readLong();
        long maxTimestamp = in.readLong();
        return new TruncateStage(minTimestamp, maxTimestamp);
    }

    /**
     * Creates a TruncateStage from a map of arguments.
     *
     * @param args the argument map
     * @return the created TruncateStage
     * @throws IllegalArgumentException if required parameters are missing
     */
    public static TruncateStage fromArgs(Map<String, Object> args) {
        if (!args.containsKey("min_timestamp") || !args.containsKey("max_timestamp")) {
            throw new IllegalArgumentException("TruncateStage requires both 'min_timestamp' and 'max_timestamp' parameters");
        }
        long minTimestamp = ((Number) args.get("min_timestamp")).longValue();
        long maxTimestamp = ((Number) args.get("max_timestamp")).longValue();
        return new TruncateStage(minTimestamp, maxTimestamp);
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TruncateStage that = (TruncateStage) obj;
        return Objects.equals(minTimestamp, that.minTimestamp) && Objects.equals(maxTimestamp, that.maxTimestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(minTimestamp, maxTimestamp);
    }
}
