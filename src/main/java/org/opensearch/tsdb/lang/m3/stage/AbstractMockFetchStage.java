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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract base class for mock fetch stages that generate synthetic time series data.
 */
public abstract class AbstractMockFetchStage implements UnaryPipelineStage {

    protected final Map<String, String> tags;
    protected final long startTime;
    protected final long step;
    protected final long endTime;

    /**
     * Constructor for AbstractMockFetchStage.
     *
     * @param tags Map of tag key-value pairs for the series
     * @param startTime Start timestamp in milliseconds
     * @param step Step size in milliseconds
     */
    protected AbstractMockFetchStage(Map<String, String> tags, long startTime, long endTime, long step) {
        this.tags = tags != null ? new HashMap<>(tags) : new HashMap<>();
        this.startTime = startTime;
        this.endTime = endTime;
        this.step = step;

        // Add default tag if no tags provided
        if (this.tags.isEmpty()) {
            this.tags.put("name", getDefaultTagName());
        }
    }

    /**
     * Generate the list of values for this mock fetch stage.
     * This is the key strategy method that differentiates each mockFetch variant.
     *
     * @return List of values to be used for generating time series samples
     */
    protected abstract List<Double> generateValues();

    /**
     * Get the default tag name to use when no tags are provided.
     *
     * @return Default tag value (e.g., "mockFetch", "mockFetchLine")
     */
    protected abstract String getDefaultTagName();

    /**
     * Process method for PipelineStage interface.
     * MockFetch stages generate new time series data rather than transforming existing data.
     * The input parameter is ignored.
     *
     * @param input ignored (can be null or empty)
     * @return generated time series list with one series
     */
    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (step == 0) {
            throw new IllegalStateException(getName() + " stage requires setQueryContext() to be called before process()");
        }

        List<Double> values = generateValues();
        if (values == null || values.isEmpty()) {
            throw new IllegalStateException(getName() + " generated empty values");
        }

        FloatSampleList.Builder builder = new FloatSampleList.Builder(values.size());
        for (int i = 0; i < values.size(); i++) {
            long timestamp = startTime + (i * step);
            double value = values.get(i);
            // Skip missing samples
            if (!Double.isNaN(value)) {
                builder.add(timestamp, value);
            }
        }

        Labels labels = ByteLabels.fromMap(tags);
        SampleList samples = builder.build();

        // Calculate actual maxTimestamp from generated samples (exclusive endTime)
        long actualMaxTimestamp = startTime + ((values.size() - 1) * step);

        TimeSeries series = new TimeSeries(samples, labels, startTime, actualMaxTimestamp, step, null);

        return List.of(series);
    }

    /**
     * Write common fields (tags, startTime, endTime, step) to output stream.
     * Subclasses should call this after writing their specific fields.
     *
     * @param out the output stream
     * @throws IOException if an I/O error occurs
     */
    protected void writeCommonFields(StreamOutput out) throws IOException {
        out.writeMap(tags, StreamOutput::writeString, StreamOutput::writeString);
        out.writeVLong(startTime);
        out.writeVLong(endTime);
        out.writeVLong(step);
    }

    /**
     * Read common fields (tags, startTime, endTime, step) from input stream.
     * Subclasses should call this after reading their specific fields.
     *
     * @param in the input stream
     * @return array containing [tags, startTime, endTime, step]
     * @throws IOException if an I/O error occurs
     */
    protected static Object[] readCommonFields(StreamInput in) throws IOException {
        Map<String, String> tags = in.readMap(StreamInput::readString, StreamInput::readString);
        long startTime = in.readVLong();
        long endTime = in.readVLong();
        long step = in.readVLong();
        return new Object[] { tags, startTime, endTime, step };
    }

    /**
     * Write common fields (tags, startTime, endTime, step) to XContent.
     * Subclasses should call this after writing their specific fields.
     *
     * @param builder The XContentBuilder to write to
     * @throws IOException if serialization fails
     */
    protected void writeCommonFieldsToXContent(XContentBuilder builder) throws IOException {
        builder.field("tags", tags);
        builder.field("startTime", startTime);
        builder.field("endTime", endTime);
        builder.field("step", step);
    }

    /**
     * Parse tags from arguments map with default name fallback.
     *
     * @param args Map of argument names to values
     * @param defaultName Default name to use if no tags provided
     * @return Map of parsed tags
     */
    protected static Map<String, String> parseTagsFromArgs(Map<String, Object> args, String defaultName) {
        Map<String, String> tags = new HashMap<>();
        if (args.containsKey("tags") && args.get("tags") instanceof Map<?, ?> tagsMap) {
            for (Map.Entry<?, ?> entry : tagsMap.entrySet()) {
                tags.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            }
        }

        if (tags.isEmpty()) {
            tags.put("name", defaultName);
        }

        return tags;
    }

    /**
     * Parse startTime from arguments map.
     *
     * @param args Map of argument names to values
     * @return startTime value, or 0 if not provided
     */
    protected static long parseStartTimeFromArgs(Map<String, Object> args) {
        if (args.containsKey("startTime") && args.get("startTime") instanceof Number num) {
            return num.longValue();
        }
        return 0L;
    }

    /**
    * Parse endTime from arguments map.
    *
    * @param args Map of argument names to values
    * @return endTime value, or 0 if not provided
    */
    protected static long parseEndTimeFromArgs(Map<String, Object> args) {
        if (args.containsKey("endTime") && args.get("endTime") instanceof Number num) {
            return num.longValue();
        }
        return 0L;
    }

    /**
     * Parse step from arguments map.
     *
     * @param args Map of argument names to values
     * @return step value, or 0 if not provided
     */
    protected static long parseStepFromArgs(Map<String, Object> args) {
        if (args.containsKey("step") && args.get("step") instanceof Number num) {
            return num.longValue();
        }
        return 0L;
    }

    /**
     * Returns the tags for testing purposes.
     * @return map of tags
     */
    public Map<String, String> getTags() {
        return new HashMap<>(tags);
    }

    /**
     * Returns the startTime for testing purposes.
     * @return start time in milliseconds
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns the endTime for testing purposes.
     * @return end time in milliseconds
     */
    public long getEndTime() {
        return endTime;
    }

    /**
     * Returns the step for testing purposes.
     * @return step size in milliseconds
     */
    public long getStep() {
        return step;
    }

    @Override
    public boolean isCoordinatorOnly() {
        return true; // All MockFetch variants must run on coordinator since they don't fetch from shards
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AbstractMockFetchStage that = (AbstractMockFetchStage) obj;
        return startTime == that.startTime && endTime == that.endTime && step == that.step && Objects.equals(tags, that.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tags, startTime, endTime, step);
    }
}
