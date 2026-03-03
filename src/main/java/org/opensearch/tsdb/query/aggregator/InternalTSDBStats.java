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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Internal aggregation result for TSDB statistics.
 *
 * <p>This class represents the result of TSDB statistics aggregations, containing
 * statistics about label keys and values across time series data.</p>
 *
 * <h2>Two-Phase Reduce Strategy:</h2>
 * <p>This class uses different data structures for different reduce phases:</p>
 * <ul>
 *   <li><strong>Shard-level phase:</strong> Uses {@link ShardLevelStats} with exact fingerprint sets
 *       to deduplicate time series between Head and ClosedChunkIndex within a shard</li>
 *   <li><strong>Coordinator-level phase:</strong> Uses {@link CoordinatorLevelStats} with final
 *       counts summed from different shards (no deduplication needed due to routing)</li>
 * </ul>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Total Time Series Count:</strong> Total number of unique time series</li>
 *   <li><strong>Tag Statistics:</strong> Per-tag cardinality and value distribution</li>
 *   <li><strong>Optional Value Stats:</strong> Detailed per-value counts when enabled</li>
 *   <li><strong>Exact Counting:</strong> Uses fingerprint sets for precise cardinality</li>
 *   <li><strong>Memory Efficient:</strong> Fingerprint sets discarded after shard-level reduce
 *       to minimize network bandwidth</li>
 * </ul>
 */
public class InternalTSDBStats extends InternalAggregation {

    private final HeadStats headStats;

    // Exactly one of these will be non-null to indicate which phase we're in
    private final ShardLevelStats shardStats;
    private final CoordinatorLevelStats coordinatorStats;

    /**
     * Statistics for the head (in-memory time series).
     *
     * @param numSeries the number of active time series in the head
     * @param chunkCount the total number of memory chunks currently held
     * @param minTime the minimum sample timestamp present in the head
     * @param maxTime the maximum sample timestamp present in the head
     */
    public record HeadStats(long numSeries, long chunkCount, long minTime, long maxTime) {

        /**
         * Deserializes a {@code HeadStats} instance from a stream.
         *
         * @param in the stream input to read from
         * @throws IOException if an I/O error occurs during reading
         */
        public HeadStats(StreamInput in) throws IOException {
            this(in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
        }

        /**
         * Serializes this {@code HeadStats} instance to a stream.
         *
         * @param out the stream output to write to
         * @throws IOException if an I/O error occurs during writing
         */
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(numSeries);
            out.writeVLong(chunkCount);
            out.writeVLong(minTime);
            out.writeVLong(maxTime);
        }
    }

    /**
     * Shard-level statistics containing exact fingerprint sets.
     *
     * <p>Used during shard-level aggregation and reduce to deduplicate time series
     * between Head and ClosedChunkIndex using exact fingerprints. Converted to {@link CoordinatorLevelStats}
     * after shard-level reduce to save network bandwidth.</p>
     */
    public record ShardLevelStats(Set<Long> seriesFingerprintSet, Map<String, Map<String, Set<Long>>> labelStats,
        boolean includeValueStats) {

        public ShardLevelStats(StreamInput in) throws IOException {
            this(readSeriesFingerprintSet(in), readLabelStatsMap(in), in.readBoolean());
        }

        private static Set<Long> readSeriesFingerprintSet(StreamInput in) throws IOException {
            boolean hasSet = in.readBoolean();
            if (!hasSet) {
                return null;
            }
            return in.readSet(StreamInput::readVLong);
        }

        private static Map<String, Map<String, Set<Long>>> readLabelStatsMap(StreamInput in) throws IOException {
            return in.readMap(
                StreamInput::readString,  // Key reader: label name
                input -> {  // Value reader: Map<String, Set<Long>>
                    boolean hasSets = input.readBoolean();
                    if (!hasSets) {
                        return null;
                    }
                    return input.readOrderedMap(
                        StreamInput::readString,  // Key reader: value name
                        valueInput -> {  // Value reader: Set<Long> or null
                            boolean hasValueSet = valueInput.readBoolean();
                            if (!hasValueSet) {
                                return null;
                            }
                            return valueInput.readSet(StreamInput::readVLong);
                        }
                    );
                }
            );
        }

        /**
         * Serializes ShardLevelStats to a stream.
         *
         * @param out the stream output to write to
         * @throws IOException if an I/O error occurs during writing
         */
        public void writeTo(StreamOutput out) throws IOException {
            // Write seriesFingerprintSet
            if (seriesFingerprintSet != null) {
                out.writeBoolean(true);
                out.writeCollection(seriesFingerprintSet, StreamOutput::writeVLong);
            } else {
                out.writeBoolean(false);
            }

            // Write labelStats using writeMap
            out.writeMap(
                labelStats,
                StreamOutput::writeString,  // Key writer: label name
                (output, valueFingerprintSets) -> {  // Value writer: Map<String, Set<Long>>
                    if (valueFingerprintSets != null) {
                        output.writeBoolean(true);
                        output.writeMap(
                            valueFingerprintSets,
                            StreamOutput::writeString,  // Key writer: value name
                            (valueOutput, fingerprintSet) -> {  // Value writer: Set<Long> or null
                                if (fingerprintSet != null) {
                                    valueOutput.writeBoolean(true);
                                    valueOutput.writeCollection(fingerprintSet, StreamOutput::writeVLong);
                                } else {
                                    valueOutput.writeBoolean(false);
                                }
                            }
                        );
                    } else {
                        output.writeBoolean(false);
                    }
                }
            );

            // Write global includeValueStats flag
            out.writeBoolean(includeValueStats);
        }
    }

    /**
     * Coordinator-level statistics containing final counts.
     *
     * <p>Used after shard-level reduce when aggregating results from multiple shards.
     * Contains pre-computed cardinality counts instead of fingerprint sets to minimize
     * network bandwidth.</p>
     */
    public record CoordinatorLevelStats(Long totalNumSeries, Map<String, LabelStats> labelStats) {

        public CoordinatorLevelStats(StreamInput in) throws IOException {
            this(readTotalNumSeries(in), readLabelStatsMap(in));
        }

        private static Long readTotalNumSeries(StreamInput in) throws IOException {
            boolean hasNumSeries = in.readBoolean();
            return hasNumSeries ? in.readVLong() : null;
        }

        private static Map<String, LabelStats> readLabelStatsMap(StreamInput in) throws IOException {
            return in.readMap(StreamInput::readString, LabelStats::new);
        }

        public void writeTo(StreamOutput out) throws IOException {
            if (totalNumSeries != null) {
                out.writeBoolean(true);
                out.writeVLong(totalNumSeries);
            } else {
                out.writeBoolean(false);
            }

            out.writeMap(labelStats, StreamOutput::writeString, (output, stats) -> stats.writeTo(output));
        }

        /**
         * Coordinator-level label statistics with final counts.
         *
         * <p>The valuesStats map is always non-null and contains all label values.
         * When includeValueStats=true, the Long counts are populated with actual cardinality.
         * When includeValueStats=false, the Long counts are 0 (sentinel for "not counted").</p>
         */
        public record LabelStats(Long numSeries, Map<String, Long> valuesStats) {

            // Compact constructor to ensure valuesStats is never null
            public LabelStats {
                if (valuesStats == null) {
                    throw new IllegalArgumentException("valuesStats cannot be null (use empty map instead)");
                }
            }

            public LabelStats(StreamInput in) throws IOException {
                this(
                    in.readBoolean() ? in.readVLong() : null,
                    in.readMap(StreamInput::readString, StreamInput::readVLong)  // 0 means "not counted"
                );
            }

            public void writeTo(StreamOutput out) throws IOException {
                if (numSeries != null) {
                    out.writeBoolean(true);
                    out.writeVLong(numSeries);
                } else {
                    out.writeBoolean(false);
                }

                // Write valuesStats map (0 means "not counted" when includeValueStats=false)
                out.writeMap(valuesStats, StreamOutput::writeString, StreamOutput::writeVLong);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CoordinatorLevelStats that = (CoordinatorLevelStats) o;
            return Objects.equals(totalNumSeries, that.totalNumSeries) && Objects.equals(labelStats, that.labelStats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(totalNumSeries, labelStats);
        }
    }

    /**
     * Factory method for creating shard-level stats (with fingerprint sets).
     *
     * @param name the name of the aggregation
     * @param shardStats the shard-level statistics with fingerprint sets
     * @param metadata the aggregation metadata
     * @return InternalTSDBStats instance for shard-level phase
     */
    public static InternalTSDBStats forShardLevel(String name, ShardLevelStats shardStats, Map<String, Object> metadata) {
        return new InternalTSDBStats(name, null, shardStats, null, metadata);
    }

    /**
     * Factory method for creating coordinator-level stats (with final counts).
     *
     * @param name the name of the aggregation
     * @param headStats the head statistics (null if not populated)
     * @param coordinatorStats the coordinator-level statistics with final counts
     * @param metadata the aggregation metadata
     * @return InternalTSDBStats instance for coordinator-level phase
     */
    public static InternalTSDBStats forCoordinatorLevel(
        String name,
        HeadStats headStats,
        CoordinatorLevelStats coordinatorStats,
        Map<String, Object> metadata
    ) {
        return new InternalTSDBStats(name, headStats, null, coordinatorStats, metadata);
    }

    /**
     * Private constructor - use factory methods instead.
     */
    private InternalTSDBStats(
        String name,
        HeadStats headStats,
        ShardLevelStats shardStats,
        CoordinatorLevelStats coordinatorStats,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.headStats = headStats;
        this.shardStats = shardStats;
        this.coordinatorStats = coordinatorStats;

        // Sanity check: exactly one of shardStats or coordinatorStats must be non-null
        if ((shardStats == null) == (coordinatorStats == null)) {
            throw new IllegalArgumentException("Exactly one of shardStats or coordinatorStats must be non-null");
        }
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

        // Read which mode we're in
        boolean isShardLevel = in.readBoolean();
        if (isShardLevel) {
            this.shardStats = new ShardLevelStats(in);
            this.coordinatorStats = null;
        } else {
            this.shardStats = null;
            this.coordinatorStats = new CoordinatorLevelStats(in);
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

        // Write which mode we're in and the corresponding stats
        if (shardStats != null) {
            out.writeBoolean(true); // isShardLevel = true
            shardStats.writeTo(out);
        } else {
            out.writeBoolean(false); // isShardLevel = false
            coordinatorStats.writeTo(out);
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
     * <p>This method uses different strategies based on the reduce phase:</p>
     * <ul>
     * <li><b>Shard-level reduce (!isFinalReduce):</b> Merges fingerprint sets to deduplicate
     *     time series between Head and ClosedChunkIndex within the same shard. Converts
     *     sets to exact counts and returns {@link CoordinatorLevelStats} to save network bandwidth.</li>
     * <li><b>Coordinator reduce (isFinalReduce):</b> Simply sums counts from different shards
     *     since each time series is guaranteed to exist on only one shard (by routing).</li>
     * </ul>
     *
     * @param aggregations the list of aggregations to reduce
     * @param reduceContext the context for the reduce operation
     * @return the reduced aggregation result
     */
    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        if (aggregations.isEmpty()) {
            // Return empty coordinator-level stats
            return forCoordinatorLevel(name, null, new CoordinatorLevelStats(null, new HashMap<>()), metadata);
        }

        if (!reduceContext.isFinalReduce()) {
            // ===== SHARD-LEVEL REDUCE: Merge fingerprint sets, convert to counts =====
            return reduceShardLevel(aggregations);
        } else {
            // ===== COORDINATOR REDUCE: Sum counts =====
            return reduceCoordinatorLevel(aggregations);
        }
    }

    /**
     * Shard-level reduce: Merges fingerprint sets and converts to coordinator-level counts.
     *
     * <p>This method merges fingerprint sets from Head and ClosedChunkIndex to deduplicate
     * time series within a shard, then converts the sets to exact counts and
     * returns {@link CoordinatorLevelStats} to minimize network bandwidth.</p>
     */
    private InternalTSDBStats reduceShardLevel(List<InternalAggregation> aggregations) {
        Set<Long> mergedFingerprints = new HashSet<>();
        Map<String, Map<String, Set<Long>>> mergedLabelStats = new HashMap<>();

        // Capture global includeValueStats flag from first shard (all shards have same value)
        boolean includeValueStats = false;
        if (!aggregations.isEmpty()) {
            InternalTSDBStats firstStats = (InternalTSDBStats) aggregations.get(0);
            if (firstStats.shardStats != null) {
                includeValueStats = firstStats.shardStats.includeValueStats();
            }
        }

        for (InternalAggregation agg : aggregations) {
            InternalTSDBStats stats = (InternalTSDBStats) agg;

            // All inputs must be shard-level stats
            if (stats.shardStats == null) {
                throw new IllegalStateException("Expected shard-level stats but got coordinator-level stats in shard reduce");
            }

            // Merge series fingerprints (automatic deduplication via Set)
            Set<Long> fingerprintSet = stats.shardStats.seriesFingerprintSet();
            if (fingerprintSet != null) {
                mergedFingerprints.addAll(fingerprintSet);
            }

            // Merge per-label fingerprint sets
            for (Map.Entry<String, Map<String, Set<Long>>> entry : stats.shardStats.labelStats().entrySet()) {
                String labelName = entry.getKey();
                Map<String, Set<Long>> valueFingerprintSets = entry.getValue();

                Map<String, Set<Long>> mergedValueSets = mergedLabelStats.computeIfAbsent(labelName, k -> new LinkedHashMap<>());

                // Merge value fingerprint sets
                if (valueFingerprintSets != null) {
                    for (Map.Entry<String, Set<Long>> ve : valueFingerprintSets.entrySet()) {
                        String valueKey = ve.getKey();
                        Set<Long> newSet = ve.getValue();

                        // Merge sets (automatic deduplication)
                        Set<Long> mergedSet = mergedValueSets.computeIfAbsent(valueKey, k -> new HashSet<>());
                        if (newSet != null) {
                            mergedSet.addAll(newSet);
                        }
                    }
                }
            }
        }

        // Convert fingerprint sets to exact counts
        Long totalSeries = mergedFingerprints.isEmpty() ? null : (long) mergedFingerprints.size();

        Map<String, CoordinatorLevelStats.LabelStats> finalLabelStats = new HashMap<>();
        for (Map.Entry<String, Map<String, Set<Long>>> entry : mergedLabelStats.entrySet()) {
            String labelName = entry.getKey();
            Map<String, Set<Long>> valueFingerprintSets = entry.getValue();

            // Calculate label cardinality and value counts
            // Note: valueCounts is ALWAYS populated (preserves insertion order via LinkedHashMap)
            // When includeValueStats=true: counts are actual cardinality
            // When includeValueStats=false: counts are 0 (sentinel for "not counted")
            Long labelCardinality = null;
            Map<String, Long> valueCounts = new LinkedHashMap<>();

            // Use global flag instead of checking sets (O(1) vs O(n) optimization)
            // The includeValueStats flag is the same for all shards (query-level parameter)
            if (includeValueStats) {
                // includeValueStats=true: compute exact counts
                Set<Long> allFingerprintsForLabel = new HashSet<>();

                for (Map.Entry<String, Set<Long>> ve : valueFingerprintSets.entrySet()) {
                    String value = ve.getKey();
                    Set<Long> fingerprintSetForValue = ve.getValue();

                    if (fingerprintSetForValue != null && !fingerprintSetForValue.isEmpty()) {
                        long valueCount = fingerprintSetForValue.size();
                        valueCounts.put(value, valueCount);

                        // Collect for label cardinality
                        allFingerprintsForLabel.addAll(fingerprintSetForValue);
                    } else {
                        valueCounts.put(value, 0L);  // Empty fingerprint set
                    }
                }

                labelCardinality = allFingerprintsForLabel.isEmpty() ? null : (long) allFingerprintsForLabel.size();
            } else {
                // includeValueStats=false: populate with 0 counts (sentinel for "not counted")
                for (String value : valueFingerprintSets.keySet()) {
                    valueCounts.put(value, 0L);
                }
            }

            finalLabelStats.put(labelName, new CoordinatorLevelStats.LabelStats(labelCardinality, valueCounts));
        }

        // Return coordinator-level stats (fingerprints converted to counts to save network bandwidth)
        CoordinatorLevelStats coordinatorStats = new CoordinatorLevelStats(totalSeries, finalLabelStats);
        return forCoordinatorLevel(name, null, coordinatorStats, metadata);
    }

    /**
     * Coordinator-level reduce: Sums counts from different shards.
     *
     * <p>This method sums pre-computed counts from different shards. No deduplication
     * is needed because each time series exists on only one shard (guaranteed by routing).</p>
     */
    private InternalTSDBStats reduceCoordinatorLevel(List<InternalAggregation> aggregations) {
        HeadStats mergedHeadStats = null; // TODO: Merge HeadStats in future when populated
        Long totalSeries = null;
        Map<String, LabelStatsBuilder> builders = new HashMap<>();

        for (InternalAggregation agg : aggregations) {
            InternalTSDBStats stats = (InternalTSDBStats) agg;

            // All inputs must be coordinator-level stats
            if (stats.coordinatorStats == null) {
                throw new IllegalStateException("Expected coordinator-level stats but got shard-level stats in coordinator reduce");
            }

            // Sum numSeries (already deduplicated at shard level)
            Long numSeries = stats.coordinatorStats.totalNumSeries();
            if (numSeries != null) {
                totalSeries = (totalSeries == null ? 0 : totalSeries) + numSeries;
            }

            // Merge label stats
            for (Map.Entry<String, CoordinatorLevelStats.LabelStats> entry : stats.coordinatorStats.labelStats().entrySet()) {
                String labelName = entry.getKey();
                CoordinatorLevelStats.LabelStats ls = entry.getValue();

                LabelStatsBuilder builder = builders.computeIfAbsent(labelName, k -> new LabelStatsBuilder());

                // Sum numSeries per label if present
                if (ls.numSeries() != null) {
                    builder.hasNumSeries = true;
                    builder.numSeries = (builder.numSeries == null ? 0 : builder.numSeries) + ls.numSeries();
                }

                // Sum value counts
                if (ls.valuesStats() != null) {
                    if (builder.valueCounts == null) {
                        builder.valueCounts = new LinkedHashMap<>();
                    }
                    for (Map.Entry<String, Long> ve : ls.valuesStats().entrySet()) {
                        builder.valueCounts.merge(ve.getKey(), ve.getValue(), Long::sum);
                    }
                }
            }
        }

        // Build final result
        Map<String, CoordinatorLevelStats.LabelStats> finalLabelStats = new HashMap<>();
        for (Map.Entry<String, LabelStatsBuilder> entry : builders.entrySet()) {
            LabelStatsBuilder builder = entry.getValue();
            finalLabelStats.put(
                entry.getKey(),
                new CoordinatorLevelStats.LabelStats(builder.hasNumSeries ? builder.numSeries : null, builder.valueCounts)
            );
        }

        CoordinatorLevelStats coordinatorStats = new CoordinatorLevelStats(totalSeries, finalLabelStats);
        return forCoordinatorLevel(name, mergedHeadStats, coordinatorStats, metadata);
    }

    /**
     * Helper class for building coordinator-level label stats during reduce.
     */
    private static class LabelStatsBuilder {
        boolean hasNumSeries = false;
        Long numSeries = null;
        Map<String, Long> valueCounts = null;
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
                return getNumSeries();
            } else if ("labelStats".equals(property)) {
                return getLabelStats();
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
     * <p>Only available for coordinator-level stats. Returns null for shard-level stats.</p>
     *
     * @return the total time series count, or null if not available
     */
    public Long getNumSeries() {
        return coordinatorStats != null ? coordinatorStats.totalNumSeries() : null;
    }

    /**
     * Returns the label statistics map.
     *
     * <p>Only available for coordinator-level stats. Returns empty map for shard-level stats.</p>
     *
     * @return the label statistics map
     */
    public Map<String, CoordinatorLevelStats.LabelStats> getLabelStats() {
        return coordinatorStats != null ? coordinatorStats.labelStats() : Map.of();
    }

    /**
     * Serializes the TSDB statistics to XContent format (grouped format).
     *
     * <p>Only coordinator-level stats are serialized to XContent for the final response.
     * Shard-level stats are not exposed externally.</p>
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
            builder.field("numSeries", headStats.numSeries());
            builder.field("chunkCount", headStats.chunkCount());
            builder.field("minTime", headStats.minTime());
            builder.field("maxTime", headStats.maxTime());
            builder.endObject();
        }

        builder.startObject("labelStats");

        // Only serialize coordinator-level stats (skip shard-level stats)
        if (coordinatorStats != null) {
            // Write numSeries at the start of labelStats
            Long totalNumSeries = coordinatorStats.totalNumSeries();
            if (totalNumSeries != null) {
                builder.field("numSeries", totalNumSeries);
            }

            for (Map.Entry<String, CoordinatorLevelStats.LabelStats> entry : coordinatorStats.labelStats().entrySet()) {
                builder.startObject(entry.getKey());
                CoordinatorLevelStats.LabelStats stats = entry.getValue();
                if (stats.numSeries() != null) {
                    builder.field("numSeries", stats.numSeries());
                }
                builder.field("valuesStats", stats.valuesStats());
                builder.endObject();
            }
        }

        builder.endObject();
        return builder;
    }

    /**
     * Indicates whether this aggregation must be reduced even when there's only
     * a single internal aggregation.
     *
     * <p>Returns true because this aggregation uses a two-phase strategy where
     * shard-level results (ShardLevelStats with fingerprint sets) must be converted
     * to coordinator-level results (CoordinatorLevelStats with final counts) via
     * the reduce phase, even when there's only one shard.</p>
     *
     * @return true, as InternalTSDBStats requires reduction to convert shard-level to coordinator-level stats
     */
    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalTSDBStats that = (InternalTSDBStats) o;
        return Objects.equals(headStats, that.headStats)
            && Objects.equals(getName(), that.getName())
            && Objects.equals(getMetadata(), that.getMetadata())
            && Objects.equals(shardStats, that.shardStats)
            && Objects.equals(coordinatorStats, that.coordinatorStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getMetadata(), headStats, shardStats, coordinatorStats);
    }
}
