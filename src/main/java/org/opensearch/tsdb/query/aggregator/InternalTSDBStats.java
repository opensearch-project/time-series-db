/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Internal aggregation result for TSDB statistics.
 *
 * <p>This class represents the result of TSDB statistics aggregations, containing
 * statistics about label keys and values across time series data.</p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Total Time Series Count:</strong> Total number of unique time series</li>
 *   <li><strong>Tag Statistics:</strong> Per-tag cardinality and value distribution</li>
 *   <li><strong>Optional Value Stats:</strong> Detailed per-value counts when enabled</li>
 *   <li><strong>Serialization:</strong> Supports streaming serialization/deserialization
 *       for distributed processing</li>
 * </ul>
 */
public class InternalTSDBStats extends InternalAggregation {

    private final HeadStats headStats;
    private final Long numSeries;
    private final Map<String, LabelStats> labelStatsMap;

    /**
     * Statistics for the head (in-memory time series).
     */
    public static class HeadStats {
        private final long numSeries;
        private final long chunkCount;
        private final long minTime;
        private final long maxTime;

        public HeadStats(long numSeries, long chunkCount, long minTime, long maxTime) {
            this.numSeries = numSeries;
            this.chunkCount = chunkCount;
            this.minTime = minTime;
            this.maxTime = maxTime;
        }

        public HeadStats(StreamInput in) throws IOException {
            this.numSeries = in.readVLong();
            this.chunkCount = in.readVLong();
            this.minTime = in.readVLong();
            this.maxTime = in.readVLong();
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(numSeries);
            out.writeVLong(chunkCount);
            out.writeVLong(minTime);
            out.writeVLong(maxTime);
        }

        public long getNumSeries() {
            return numSeries;
        }

        public long getChunkCount() {
            return chunkCount;
        }

        public long getMinTime() {
            return minTime;
        }

        public long getMaxTime() {
            return maxTime;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            HeadStats headStats = (HeadStats) o;
            return numSeries == headStats.numSeries
                && chunkCount == headStats.chunkCount
                && minTime == headStats.minTime
                && maxTime == headStats.maxTime;
        }

        @Override
        public int hashCode() {
            return Objects.hash(numSeries, chunkCount, minTime, maxTime);
        }
    }

    /**
     * Statistics for a single label.
     */
    public static class LabelStats {
        private final Long numSeries;
        private final Map<String, Long> valuesStats;

        /**
         * Constructor for LabelStats.
         */
        public LabelStats(Long numSeries, Map<String, Long> valuesStats) {
            this.numSeries = numSeries;
            this.valuesStats = valuesStats;
        }

        public LabelStats(StreamInput in) throws IOException {
            boolean hasNumSeries = in.readBoolean();
            this.numSeries = hasNumSeries ? in.readVLong() : null;

            // Read valuesStats (counts)
            boolean hasValuesStats = in.readBoolean();
            if (hasValuesStats) {
                this.valuesStats = in.readMap(StreamInput::readString, StreamInput::readVLong);
            } else {
                this.valuesStats = null;
            }
        }

        public void writeTo(StreamOutput out) throws IOException {
            if (numSeries != null) {
                out.writeBoolean(true);
                out.writeVLong(numSeries);
            } else {
                out.writeBoolean(false);
            }

            // Write valuesStats (counts) if present
            if (valuesStats != null) {
                out.writeBoolean(true);
                out.writeMap(valuesStats, StreamOutput::writeString, StreamOutput::writeVLong);
            } else {
                out.writeBoolean(false);
            }
        }

        public Long getNumSeries() {
            return numSeries;
        }

        public List<String> getValues() {
            return valuesStats != null ? new ArrayList<>(valuesStats.keySet()) : List.of();
        }

        public Map<String, Long> getValuesStats() {
            return valuesStats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LabelStats labelStats = (LabelStats) o;
            return Objects.equals(numSeries, labelStats.numSeries) && Objects.equals(valuesStats, labelStats.valuesStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(numSeries, valuesStats);
        }
    }

    /**
     * Creates a new InternalTSDBStats aggregation result.
     *
     * @param name the name of the aggregation
     * @param headStats the head statistics (null if not requested)
     * @param numSeries the total number of unique time series (null if not requested)
     * @param labelStatsMap the label statistics map
     * @param metadata the aggregation metadata
     */
    public InternalTSDBStats(
        String name,
        HeadStats headStats,
        Long numSeries,
        Map<String, LabelStats> labelStatsMap,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.headStats = headStats;
        this.numSeries = numSeries;
        this.labelStatsMap = labelStatsMap;
    }

    /**
     * Reads an InternalTSDBStats from a stream for deserialization.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public InternalTSDBStats(StreamInput in) throws IOException {
        super(in);

        // Read headStats if present
        boolean hasHeadStats = in.readBoolean();
        this.headStats = hasHeadStats ? new HeadStats(in) : null;

        boolean hasNumSeries = in.readBoolean();
        this.numSeries = hasNumSeries ? in.readVLong() : null;

        int labelCount = in.readVInt();
        this.labelStatsMap = new HashMap<>(labelCount);
        for (int i = 0; i < labelCount; i++) {
            String labelName = in.readString();
            LabelStats stats = new LabelStats(in);
            this.labelStatsMap.put(labelName, stats);
        }
    }

    /**
     * Writes the InternalTSDBStats data to a stream for serialization.
     *
     * @param out the stream output to write to
     * @throws IOException if an I/O error occurs during writing
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // Write headStats if present
        if (headStats != null) {
            out.writeBoolean(true);
            headStats.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        if (numSeries != null) {
            out.writeBoolean(true);
            out.writeVLong(numSeries);
        } else {
            out.writeBoolean(false);
        }

        out.writeVInt(labelStatsMap.size());
        for (Map.Entry<String, LabelStats> entry : labelStatsMap.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    /**
     * Returns the writeable name used for stream serialization.
     *
     * @return the writeable name "tsdb_stats"
     */
    @Override
    public String getWriteableName() {
        return "tsdb_stats";
    }

    /**
     * Reduces multiple InternalTSDBStats aggregations into a single result.
     *
     * <p>This method will be implemented in the next PR to handle:</p>
     * <ul>
     * <li><b>Shard-level reduce (!isFinalReduce):</b> Deduplicates time series using fingerprints
     *     to handle overlaps between Head and ClosedChunkIndex within the same shard</li>
     * <li><b>Coordinator reduce (isFinalReduce):</b> Simply sums counts from different shards
     *     since each time series is guaranteed to exist on only one shard</li>
     * </ul>
     *
     * @param aggregations the list of aggregations to reduce
     * @param reduceContext the context for the reduce operation
     * @return the reduced aggregation result
     */
    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        // TODO: Implement reduce logic in next PR (PR #2)
        // For now, just return the first aggregation if available
        if (aggregations.isEmpty()) {
            return new InternalTSDBStats(name, null, null, new HashMap<>(), metadata);
        }
        return aggregations.get(0);
    }

    /**
     * Retrieves a property value based on the given path.
     *
     * @param path the property path to retrieve
     * @return the property value
     * @throws IllegalArgumentException if the property path is unknown
     */
    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            String property = path.get(0);
            if ("numSeries".equals(property)) {
                return numSeries;
            } else if ("labelStats".equals(property)) {
                return labelStatsMap;
            }
        }
        throw new IllegalArgumentException("Unknown property [" + path.get(0) + "] for TSDBStatsAggregation [" + name + "]");
    }

    /**
     * Returns the head statistics.
     *
     * @return the head statistics, or null if not requested
     */
    public HeadStats getHeadStats() {
        return headStats;
    }

    /**
     * Returns the total number of unique time series.
     *
     * @return the total time series count, or null if not requested
     */
    public Long getNumSeries() {
        return numSeries;
    }

    /**
     * Returns the label statistics map.
     *
     * @return the label statistics
     */
    public Map<String, LabelStats> getLabelStats() {
        return labelStatsMap;
    }

    /**
     * Serializes the TSDB statistics to XContent format (grouped format).
     *
     * @param builder the XContent builder to write to
     * @param params the serialization parameters
     * @return the XContent builder for method chaining
     * @throws IOException if an I/O error occurs during serialization
     */
    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        // Write headStats if present
        if (headStats != null) {
            builder.startObject("headStats");
            builder.field("numSeries", headStats.numSeries);
            builder.field("chunkCount", headStats.chunkCount);
            builder.field("minTime", headStats.minTime);
            builder.field("maxTime", headStats.maxTime);
            builder.endObject();
        }

        builder.startObject("labelStats");

        // Write numSeries at the start of labelStats
        if (numSeries != null) {
            builder.field("numSeries", numSeries);
        }

        for (Map.Entry<String, LabelStats> entry : labelStatsMap.entrySet()) {
            builder.startObject(entry.getKey());
            LabelStats stats = entry.getValue();
            if (stats.numSeries != null) {
                builder.field("numSeries", stats.numSeries);
            }
            builder.field("values", stats.getValues());
            if (stats.valuesStats != null) {
                builder.field("valuesStats", stats.valuesStats);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    /**
     * Indicates whether this aggregation must be reduced even when there's only
     * a single internal aggregation.
     *
     * @return false, as InternalTSDBStats does not require reduction for single aggregations
     */
    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalTSDBStats that = (InternalTSDBStats) o;
        return Objects.equals(headStats, that.headStats)
            && Objects.equals(numSeries, that.numSeries)
            && Objects.equals(getName(), that.getName())
            && Objects.equals(getMetadata(), that.getMetadata())
            && Objects.equals(labelStatsMap, that.labelStatsMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getMetadata(), headStats, numSeries, labelStatsMap);
    }
}
