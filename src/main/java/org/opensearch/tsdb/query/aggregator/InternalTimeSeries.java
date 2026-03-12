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
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.query.utils.SampleMerger;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.tsdb.query.utils.RamUsageConstants;
import org.opensearch.tsdb.query.breaker.ReduceCircuitBreakerConsumer;

/**
 * Internal aggregation result for time series pipeline aggregators.
 *
 * <p>This class represents the result of time series pipeline aggregations, supporting
 * both decoded samples and compressed chunks based on the encoding mode. It implements
 * the {@link TimeSeriesProvider} interface to provide access to the underlying time series data.</p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Time Series Storage:</strong> Maintains a list of time series with their
 *       associated samples, labels, and metadata</li>
 *   <li><strong>Reduce Stage Support:</strong> Supports optional reduce stages for
 *       final aggregation operations</li>
 *   <li><strong>Label-based Merging:</strong> Uses {@link SampleMerger} for
 *       intelligent merging of time series with matching labels</li>
 *   <li><strong>Serialization:</strong> Supports streaming serialization/deserialization
 *       for distributed processing and records serialized byte size for network metrics when possible</li>
 *   <li><strong>Encoding Modes:</strong> Supports two encoding modes for network transmission:
 *     <ul>
 *       <li><strong>NONE:</strong> Decoded samples sent over the wire (used when pipeline stages
 *           need to be applied on data nodes)</li>
 *       <li><strong>XOR:</strong> Compressed chunks sent over the wire (used when data nodes have
 *           no pipeline stages to process, minimizing network transfer and data node CPU usage)</li>
 *     </ul>
 *   </li>
 * </ul>
 *
 * <p>Data nodes choose the encoding based on whether pipeline stages need to be applied locally.
 * The coordinator decodes compressed data during the reduce phase when needed. When merging
 * segment results on a data node (e.g. CSS), payload can be kept compressed and only decoded
 * on the coordinator.</p>
 */
public class InternalTimeSeries extends InternalAggregation implements TimeSeriesProvider {

    /**
     * Wire format version constants and cluster-synced setting.
     *
     * <p>{@code serialFormatSetting} is the single source of truth for wire format versioning.
     * On write, the negated setting ({@code -serialFormatSetting}) is written as the first VInt.
     * On read, the sign of the first VInt determines the format:
     * <ul>
     *   <li>{@code firstVInt >= 0} → legacy format (value is the time series count)</li>
     *   <li>{@code firstVInt < 0} → versioned format ({@code -firstVInt} is the version; read encoding byte next)</li>
     * </ul>
     */
    public static final int LEGACY_SERIAL_VERSION = 0;
    public static final int CURRENT_SERIAL_VERSION = 1;

    public static volatile int serialFormatSetting = LEGACY_SERIAL_VERSION; // this will be synced with cluster setting

    /**
     * Encoding format for time series data transmission.
     */
    public enum Encoding {
        NONE((byte) 0),  // Decoded samples
        XOR((byte) 1);   // XOR-compressed chunks

        private final byte id;

        Encoding(byte id) {
            this.id = id;
        }

        public byte getId() {
            return id;
        }

        public static Encoding fromId(byte id) {
            for (Encoding encoding : values()) {
                if (encoding.id == id) {
                    return encoding;
                }
            }
            throw new IllegalArgumentException("Unknown encoding ID: " + id);
        }
    }

    /**
     * Sealed interface representing time series data in its wire format.
     * Each encoding type has its own implementation for encoding-specific logic.
     */
    private sealed interface WireData permits DecodedData, CompressedData {
        Encoding getEncoding();

        List<TimeSeries> decode();

        void writeTo(StreamOutput out) throws IOException;

        boolean dataEquals(WireData other);

        int dataHashCode();

        UnaryPipelineStage getReduceStage();
    }

    /**
     * Decoded time series data (NONE encoding).
     */
    private static final class DecodedData implements WireData {
        private final List<TimeSeries> timeSeriesList;
        private final UnaryPipelineStage reduceStage;

        DecodedData(List<TimeSeries> timeSeriesList, UnaryPipelineStage reduceStage) {
            this.timeSeriesList = timeSeriesList != null ? timeSeriesList : List.of();
            this.reduceStage = reduceStage;
        }

        @Override
        public Encoding getEncoding() {
            return Encoding.NONE;
        }

        @Override
        public List<TimeSeries> decode() {
            return timeSeriesList;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (serialFormatSetting != LEGACY_SERIAL_VERSION) {
                out.writeVInt(-serialFormatSetting);
                out.writeByte(Encoding.NONE.getId());
            }
            writeTimeSeriesAndReduceStage(out, timeSeriesList, reduceStage);
        }

        /**
         * Writes time series list and optional reduce stage — shared by both legacy and versioned formats.
         */
        private static void writeTimeSeriesAndReduceStage(StreamOutput out, List<TimeSeries> timeSeriesList, UnaryPipelineStage reduceStage)
            throws IOException {
            out.writeVInt(timeSeriesList.size());
            for (TimeSeries series : timeSeriesList) {
                out.writeInt(0);  // hash - placeholder for now
                SampleList samples = series.getSamples();
                out.writeVInt(samples.size());
                for (Sample sample : samples) {
                    sample.writeTo(out);
                }
                Map<String, String> labelsMap = series.getLabels() != null ? series.getLabels().toMapView() : Map.of();
                out.writeMap(labelsMap, StreamOutput::writeString, StreamOutput::writeString);
                out.writeOptionalString(series.getAlias());
                out.writeLong(series.getMinTimestamp());
                out.writeLong(series.getMaxTimestamp());
                out.writeLong(series.getStep());
            }
            if (reduceStage != null) {
                out.writeBoolean(true);
                out.writeString(reduceStage.getName());
                reduceStage.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        public UnaryPipelineStage getReduceStage() {
            return reduceStage;
        }

        @Override
        public boolean dataEquals(WireData other) {
            if (!(other instanceof DecodedData that)) return false;
            return timeSeriesListEquals(timeSeriesList, that.timeSeriesList)
                && Objects.equals(
                    reduceStage != null ? reduceStage.getName() : null,
                    that.reduceStage != null ? that.reduceStage.getName() : null
                );
        }

        @Override
        public int dataHashCode() {
            return Objects.hash(timeSeriesListHashCode(timeSeriesList), reduceStage != null ? reduceStage.getName() : null);
        }

        static DecodedData readFrom(StreamInput in) throws IOException {
            return readFromLegacy(in, in.readVInt());
        }

        /**
         * Read DecodedData from old format (pre-compressed mode) where no marker or encoding byte was written.
         *
         * @param in the stream input
         * @param timeSeriesCount the already-read time series count (was read during format detection)
         */
        static DecodedData readFromLegacy(StreamInput in, int timeSeriesCount) throws IOException {
            List<TimeSeries> timeSeriesList = new ArrayList<>(timeSeriesCount);
            for (int i = 0; i < timeSeriesCount; i++) {
                timeSeriesList.add(readTimeSeries(in));
            }
            boolean hasReduceStage = in.readBoolean();
            UnaryPipelineStage reduceStage = null;
            if (hasReduceStage) {
                String stageName = in.readString();
                reduceStage = (UnaryPipelineStage) PipelineStageFactory.readFrom(in, stageName);
            }
            return new DecodedData(timeSeriesList, reduceStage);
        }
    }

    /**
     * XOR-compressed time series data (XOR encoding).
     */
    private static final class CompressedData implements WireData {
        private final List<CompressedTimeSeries> compressedTimeSeries;

        CompressedData(List<CompressedTimeSeries> compressedTimeSeries) {
            this.compressedTimeSeries = compressedTimeSeries != null ? compressedTimeSeries : List.of();
        }

        @Override
        public Encoding getEncoding() {
            return Encoding.XOR;
        }

        @Override
        public List<TimeSeries> decode() {
            return decodeCompressedTimeSeries(compressedTimeSeries);
        }

        public List<CompressedTimeSeries> getCompressedTimeSeries() {
            return compressedTimeSeries;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            if (serialFormatSetting == LEGACY_SERIAL_VERSION) {
                throw new IllegalStateException(
                    "Cannot serialize compressed data with legacy wire format. "
                        + "Enable versioned serialization (serialFormatSetting != 0) before enabling compressed mode."
                );
            }
            out.writeVInt(-serialFormatSetting);
            out.writeByte(Encoding.XOR.getId());
            out.writeVInt(compressedTimeSeries.size());
            for (CompressedTimeSeries series : compressedTimeSeries) {
                series.writeTo(out);
            }
        }

        @Override
        public boolean dataEquals(WireData other) {
            if (!(other instanceof CompressedData that)) return false;
            return Objects.equals(compressedTimeSeries, that.compressedTimeSeries);
        }

        @Override
        public int dataHashCode() {
            return Objects.hash(compressedTimeSeries);
        }

        @Override
        public UnaryPipelineStage getReduceStage() {
            return null;
        }

        /**
         * Decodes a list of compressed time series into decoded time series.
         * Groups by labels, pools all chunks per group into a unified series, then decodes and aligns in one pass.
         */
        private static List<TimeSeries> decodeCompressedTimeSeries(List<CompressedTimeSeries> compressedList) {
            Map<Labels, List<CompressedTimeSeries>> seriesByLabels = new HashMap<>();
            for (CompressedTimeSeries compressedSeries : compressedList) {
                seriesByLabels.computeIfAbsent(compressedSeries.getLabels(), k -> new ArrayList<>()).add(compressedSeries);
            }
            List<TimeSeries> decodedTimeSeries = new ArrayList<>(seriesByLabels.size());
            for (Map.Entry<Labels, List<CompressedTimeSeries>> entry : seriesByLabels.entrySet()) {
                Labels labels = entry.getKey();
                List<CompressedTimeSeries> compressedGroup = entry.getValue();

                // All series in the group share the same query-derived time range and step
                // (set from the aggregator's minTimestamp/maxTimestamp in collectCompressed)
                CompressedTimeSeries first = compressedGroup.get(0);
                long minTimestamp = first.getMinTimestamp();
                long queryMaxTimestamp = first.getMaxTimestamp();
                long step = first.getStep();
                String alias = first.getAlias();

                // Pool all chunks from the group and decode+align in one call.
                // queryMaxTimestamp is the raw query end (exclusive), matching
                // the decompressed path's decodeSamples(minTimestamp, maxTimestamp).
                List<CompressedChunk> allChunks = new ArrayList<>();
                for (CompressedTimeSeries series : compressedGroup) {
                    allChunks.addAll(series.getChunks());
                }
                try {
                    SampleList samples = decodeAndAlign(allChunks, minTimestamp, queryMaxTimestamp, step);
                    // Skip series with no data in the query range, consistent with the
                    // decompressed path which returns early when allSamples.isEmpty().
                    if (!samples.isEmpty()) {
                        long alignedMax = TimeSeries.calculateAlignedMaxTimestamp(minTimestamp, queryMaxTimestamp, step);
                        decodedTimeSeries.add(new TimeSeries(samples, labels, minTimestamp, alignedMax, step, alias));
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed to decode compressed chunks for series: " + labels, e);
                }
            }
            return decodedTimeSeries;
        }

        /**
         * Decodes the given chunks within the query time range, aligning each chunk's samples to step
         * boundaries before merging. This matches the decompressed path where alignment happens
         * per-doc before merging by labels, ensuring consistent dedup behavior across both modes.
         *
         * <p>Argument order in merge: {@code merge(aligned, result)} places the accumulated result
         * in the val2 position so that {@code ANY_WINS} (which keeps val2) preserves the first
         * chunk's value for duplicate timestamps — matching the decompressed path's
         * {@code DedupIterator(FIRST)} behavior.</p>
         */
        private static SampleList decodeAndAlign(List<CompressedChunk> chunks, long queryMinTimestamp, long queryMaxTimestamp, long step)
            throws IOException {
            SampleList result = SampleList.fromList(List.of());
            for (CompressedChunk chunk : chunks) {
                SampleList decoded = chunk.decodeSamples(queryMinTimestamp, queryMaxTimestamp);
                SampleList aligned = SampleMerger.alignAndDeduplicate(decoded, queryMinTimestamp, step);
                result = MERGE_HELPER.merge(aligned, result, true);
            }
            return result;
        }

        static CompressedData readFrom(StreamInput in) throws IOException {
            int count = in.readVInt();
            List<CompressedTimeSeries> compressedTimeSeries = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                compressedTimeSeries.add(new CompressedTimeSeries(in));
            }
            return new CompressedData(compressedTimeSeries);
        }
    }

    private final WireData data;
    private static final SampleMerger MERGE_HELPER = new SampleMerger(SampleMerger.DeduplicatePolicy.ANY_WINS);

    /**
     * Creates a new InternalTimeSeries with decoded samples (encoding = NONE).
     *
     * @param name the name of the aggregation
     * @param timeSeriesList the list of decoded time series data
     * @param metadata the aggregation metadata
     */
    public InternalTimeSeries(String name, List<TimeSeries> timeSeriesList, Map<String, Object> metadata) {
        this(name, timeSeriesList, metadata, null);
    }

    /**
     * Creates a new InternalTimeSeries with decoded samples and optional reduce stage.
     *
     * @param name the name of the aggregation
     * @param timeSeriesList the list of decoded time series data
     * @param metadata the aggregation metadata
     * @param reduceStage the optional reduce stage for final aggregation operations
     */
    public InternalTimeSeries(String name, List<TimeSeries> timeSeriesList, Map<String, Object> metadata, UnaryPipelineStage reduceStage) {
        super(name, metadata);
        this.data = new DecodedData(timeSeriesList, reduceStage);
    }

    /** Private constructor for creating InternalTimeSeries with specific encoded data. */
    private InternalTimeSeries(String name, WireData data, Map<String, Object> metadata) {
        super(name, metadata);
        this.data = data;
    }

    /**
     * Creates a new InternalTimeSeries with compressed chunks (encoding = XOR).
     *
     * @param name the name of the aggregation
     * @param compressedTimeSeries the list of compressed time series data
     * @param metadata the aggregation metadata
     * @return a new InternalTimeSeries with XOR encoding
     */
    public static InternalTimeSeries compressed(
        String name,
        List<CompressedTimeSeries> compressedTimeSeries,
        Map<String, Object> metadata
    ) {
        return new InternalTimeSeries(name, new CompressedData(compressedTimeSeries), metadata);
    }

    /**
     * Reads an InternalTimeSeries from a stream for deserialization.
     * Handles backward compatibility with old builds using format marker detection.
     *
     * <p>Detection: if first VInt &gt;= 0, legacy format (value is timeSeriesCount);
     * if first VInt &lt; 0, versioned format ({@code -firstVInt} is the serial version,
     * read encoding byte next). This is self-describing so it works across coordinator
     * and data clusters without version negotiation.</p>
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during reading
     */
    public InternalTimeSeries(StreamInput in) throws IOException {
        super(in);
        int firstValue = in.readVInt();
        if (firstValue >= 0) {
            // Legacy format: firstValue is the timeSeriesCount
            this.data = DecodedData.readFromLegacy(in, firstValue);
        } else {
            // Versioned format: -firstValue is the serial version (currently only CURRENT_SERIAL_VERSION=1)
            int resolvedVersion = -firstValue;
            if (resolvedVersion != CURRENT_SERIAL_VERSION) {
                throw new IOException("Unknown wire format version: " + resolvedVersion);
            }
            Encoding encoding = Encoding.fromId(in.readByte());
            this.data = switch (encoding) {
                case NONE -> DecodedData.readFrom(in);
                case XOR -> CompressedData.readFrom(in);
            };
        }
    }

    /**
     * Serializes this aggregation and records serialized byte size for network metrics when the
     * stream supports {@link StreamOutput#position()}. Metrics must not affect serialization.
     */
    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        long startPos = getStreamPosition(out);
        data.writeTo(out);
        long endPos = getStreamPosition(out);
        if (startPos >= 0 && endPos >= 0 && endPos > startPos) {
            long serializedBytes = endPos - startPos;
            try {
                if (data.getEncoding() == Encoding.XOR) {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.compressedBytesTotal, serializedBytes);
                } else {
                    TSDBMetrics.recordHistogram(TSDBMetrics.AGGREGATION.decodedBytesTotal, serializedBytes);
                }
            } catch (Exception ignored) {
                // Metrics must not break serialization
            }
        }
    }

    /**
     * Returns the current write position when the stream supports it, else -1.
     * Used to measure serialized payload size; transport uses {@code BytesStreamOutput} which overrides {@link StreamOutput#position()}.
     */
    private static long getStreamPosition(StreamOutput out) {
        try {
            return out.position();
        } catch (UnsupportedOperationException | IOException e) {
            return -1;
        }
    }

    /**
     * Returns the writeable name used for stream serialization.
     *
     * @return the writeable name "time_series"
     */
    @Override
    public String getWriteableName() {
        return "time_series";
    }

    /**
     * Reduces multiple InternalTimeSeries aggregations into a single result.
     *
     * <p>Handles: (1) When a reduce stage is present, decodes XOR if needed and delegates to the stage;
     * (2) When merging segment results on a data node (partial reduce) and all aggs are XOR, merges
     * compressed payload without decoding; (3) Otherwise (final reduce or any NONE data), decodes and
     * merges time series by labels using {@link SampleMerger}.</p>
     *
     * <p>Circuit breaker tracking is performed to protect coordinator nodes (including
     * data cluster coordinators in CCS setups) from OOM conditions.</p>
     *
     * @param aggregations the list of aggregations to reduce
     * @param reduceContext the context for the reduce operation
     * @return the reduced aggregation result
     * @throws IllegalArgumentException if any aggregation is not an InternalTimeSeries
     */
    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        try (ReduceCircuitBreakerConsumer cbConsumer = ReduceCircuitBreakerConsumer.createConsumer(reduceContext)) {
            UnaryPipelineStage reduceStage = getReduceStage();
            // If we have a reduce stage, decode XOR if needed and delegate to the stage
            if (reduceStage != null) {
                // Track ArrayList allocation for providers list
                cbConsumer.accept(SampleList.ARRAYLIST_OVERHEAD);

                List<TimeSeriesProvider> timeSeriesProviders = new ArrayList<>(aggregations.size());
                for (InternalAggregation agg : aggregations) {
                    if (!(agg instanceof InternalTimeSeries its)) {
                        throw new IllegalArgumentException("Expected InternalTimeSeries but got: " + agg.getClass());
                    }
                    switch (its.getEncoding()) {
                        case XOR:
                            List<TimeSeries> decoded = its.getTimeSeries();
                            timeSeriesProviders.add(new InternalTimeSeries(its.name, decoded, its.metadata, null));
                            break;
                        case NONE:
                            timeSeriesProviders.add(its);
                            break;
                    }
                }

                // Use the stage's own reduce method with circuit breaker tracking
                return reduceStage.reduce(timeSeriesProviders, reduceContext.isFinalReduce(), cbConsumer);
            }

            // When merging segment results on a data node (CSS), keep payload compressed so we only decode on coordinator.
            if (!reduceContext.isFinalReduce()
                && aggregations.stream().allMatch(a -> a instanceof InternalTimeSeries its && its.getEncoding() == Encoding.XOR)) {
                return mergeCompressedWithoutDecoding(aggregations, cbConsumer);
            }

            // No reduce stage - collect all time series from all aggregations and merge by labels
            cbConsumer.accept(RamUsageConstants.HASHMAP_SHALLOW_SIZE);

            Map<Labels, TimeSeries> mergedSeriesByLabels = new HashMap<>();

            for (InternalAggregation agg : aggregations) {
                if (!(agg instanceof InternalTimeSeries its)) {
                    throw new IllegalArgumentException("Expected InternalTimeSeries but got: " + agg.getClass());
                }
                List<TimeSeries> timeSeriesList = its.getTimeSeries();

                for (TimeSeries series : timeSeriesList) {
                    Labels seriesLabels = series.getLabels();

                    TimeSeries existingSeries = mergedSeriesByLabels.get(seriesLabels);
                    if (existingSeries != null) {
                        SampleList mergedSamples = MERGE_HELPER.merge(
                            existingSeries.getSamples(),
                            series.getSamples(),
                            true // assumeSorted - samples should be sorted in reduce phase
                        );

                        cbConsumer.accept(mergedSamples.ramBytesUsed());

                        TimeSeries mergedSeries = new TimeSeries(
                            mergedSamples,
                            existingSeries.getLabels(),
                            existingSeries.getMinTimestamp(),
                            existingSeries.getMaxTimestamp(),
                            existingSeries.getStep(),
                            existingSeries.getAlias()
                        );
                        mergedSeriesByLabels.put(seriesLabels, mergedSeries);
                    } else {
                        cbConsumer.accept(RamUsageConstants.groupEntryBaseOverhead(seriesLabels) + series.ramBytesUsed());
                        mergedSeriesByLabels.put(seriesLabels, series);
                    }
                }
            }

            cbConsumer.accept(SampleList.ARRAYLIST_OVERHEAD);

            List<TimeSeries> combinedTimeSeries = new ArrayList<>(mergedSeriesByLabels.values());
            return new InternalTimeSeries(name, combinedTimeSeries, metadata, null);
        }
    }

    /**
     * Merges segment-level compressed results without decoding (data node / CSS merge).
     * Groups by labels, concatenates chunks per group, and keeps XOR encoding so decompression
     * only happens on the coordinator.
     */
    private InternalAggregation mergeCompressedWithoutDecoding(
        List<InternalAggregation> aggregations,
        ReduceCircuitBreakerConsumer cbConsumer
    ) {
        cbConsumer.accept(RamUsageConstants.HASHMAP_SHALLOW_SIZE);

        Map<Labels, List<CompressedTimeSeries>> byLabels = new HashMap<>();
        for (InternalAggregation agg : aggregations) {
            InternalTimeSeries its = (InternalTimeSeries) agg;
            if (!(its.data instanceof CompressedData compressed)) {
                continue;
            }
            List<CompressedTimeSeries> list = compressed.getCompressedTimeSeries();
            for (CompressedTimeSeries cts : list) {
                byLabels.computeIfAbsent(cts.getLabels(), k -> new ArrayList<>()).add(cts);
            }
        }
        List<CompressedTimeSeries> merged = new ArrayList<>(byLabels.size());
        for (Map.Entry<Labels, List<CompressedTimeSeries>> e : byLabels.entrySet()) {
            List<CompressedTimeSeries> group = e.getValue();
            if (group.size() == 1) {
                merged.add(group.get(0));
                continue;
            }
            // All series in the group share the same query-derived time range and step
            CompressedTimeSeries first = group.get(0);
            List<CompressedChunk> allChunks = new ArrayList<>();
            for (CompressedTimeSeries cts : group) {
                allChunks.addAll(cts.getChunks());
            }
            merged.add(
                new CompressedTimeSeries(
                    allChunks,
                    first.getLabels(),
                    first.getMinTimestamp(),
                    first.getMaxTimestamp(),
                    first.getStep(),
                    first.getAlias()
                )
            );
        }
        return InternalTimeSeries.compressed(name, merged, metadata);
    }

    /**
     * Returns the raw compressed time series list.
     * Only valid when encoding is XOR; throws if called on decoded data.
     */
    List<CompressedTimeSeries> getCompressedTimeSeries() {
        if (data instanceof CompressedData compressed) {
            return compressed.getCompressedTimeSeries();
        }
        throw new IllegalStateException(
            "getCompressedTimeSeries() called on non-compressed InternalTimeSeries (encoding=" + getEncoding() + ")"
        );
    }

    /**
     * Returns a property value for this aggregation.
     * Supports empty path (returns this) and "timeSeries" (returns decoded list).
     */
    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) return this;
        if (path.size() == 1 && "timeSeries".equals(path.get(0))) return getTimeSeries();
        throw new IllegalArgumentException("Unknown property [" + path.get(0) + "] for " + getClass().getSimpleName() + " [" + name + "]");
    }

    /**
     * Returns the list of time series contained in this aggregation result.
     * Decodes compressed data if encoding is XOR.
     *
     * @return the list of time series data
     */
    @Override
    public List<TimeSeries> getTimeSeries() {
        return data.decode();
    }

    /**
     * Gets the reduce stage associated with this aggregation result.
     *
     * @return the reduce stage, or null if there is no reduce stage
     */
    public UnaryPipelineStage getReduceStage() {
        return data.getReduceStage();
    }

    /**
     * Creates a new TimeSeriesProvider with the given time series data.
     * Always returns NONE encoding (decoded) after reduction.
     *
     * @param timeSeriesList the new time series data
     * @return a new InternalTimeSeries instance with the provided data
     */
    @Override
    public TimeSeriesProvider createReduced(List<TimeSeries> timeSeriesList) {
        return new InternalTimeSeries(name, timeSeriesList, metadata, getReduceStage());
    }

    /**
     * Renders the time series data as XContent (JSON).
     * Decodes compressed data if needed before rendering.
     */
    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("timeSeries");
        List<TimeSeries> timeSeriesList = getTimeSeries();
        for (TimeSeries series : timeSeriesList) {
            builder.startObject();
            builder.field("hash", 0);  // placeholder for now
            if (series.getAlias() != null) builder.field("alias", series.getAlias());
            builder.field("minTimestamp", series.getMinTimestamp());
            builder.field("maxTimestamp", series.getMaxTimestamp());
            builder.field("step", series.getStep());
            builder.startArray("samples");
            for (Sample sample : series.getSamples()) {
                builder.startObject();
                builder.field("timestamp", sample.getTimestamp());
                builder.field("value", sample.getValue());
                builder.endObject();
            }
            builder.endArray();
            if (series.getLabels() != null && !series.getLabels().isEmpty()) {
                builder.field("labels", series.getLabels().toMapView());
            }
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }

    /**
     * Indicates whether this aggregation must be reduced even when there is only one shard.
     */
    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    /**
     * Reads a TimeSeries object from a stream input during deserialization.
     *
     * <p>Deserializes: hash (placeholder), sample count and samples, labels, optional alias,
     * and time series metadata (min/max timestamp, step).</p>
     *
     * @param in the stream input to read from
     * @return the deserialized TimeSeries object
     * @throws IOException if an I/O error occurs during reading
     */
    private static TimeSeries readTimeSeries(StreamInput in) throws IOException {
        int hash = in.readInt();
        int sampleCount = in.readVInt();
        List<Sample> samples = new ArrayList<>(sampleCount);
        for (int i = 0; i < sampleCount; i++) {
            samples.add(Sample.readFrom(in));
        }
        Map<String, String> labelsMap = in.readMap(StreamInput::readString, StreamInput::readString);
        Labels labels = labelsMap.isEmpty() ? ByteLabels.emptyLabels() : ByteLabels.fromMap(labelsMap);
        String alias = in.readOptionalString();
        long minTimestamp = in.readLong();
        long maxTimestamp = in.readLong();
        long step = in.readLong();
        return new TimeSeries(samples, labels, minTimestamp, maxTimestamp, step, alias);
    }

    /**
     * Returns the encoding mode of this aggregation result.
     *
     * @return the encoding mode (NONE or XOR)
     */
    public Encoding getEncoding() {
        return data.getEncoding();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InternalTimeSeries that = (InternalTimeSeries) o;
        return Objects.equals(getName(), that.getName())
            && Objects.equals(getMetadata(), that.getMetadata())
            && data.getEncoding() == that.data.getEncoding()
            && data.dataEquals(that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName(), getMetadata(), data.getEncoding(), data.dataHashCode());
    }

    /** Compares two time series lists for equality. */
    private static boolean timeSeriesListEquals(List<TimeSeries> list1, List<TimeSeries> list2) {
        if (list1 == list2) return true;
        if (list1 == null || list2 == null) return false;
        if (list1.size() != list2.size()) return false;
        for (int i = 0; i < list1.size(); i++) {
            TimeSeries ts1 = list1.get(i);
            TimeSeries ts2 = list2.get(i);
            if (!Objects.equals(ts1.getAlias(), ts2.getAlias())) return false;
            if (ts1.getMinTimestamp() != ts2.getMinTimestamp()) return false;
            if (ts1.getMaxTimestamp() != ts2.getMaxTimestamp()) return false;
            if (ts1.getStep() != ts2.getStep()) return false;
            if (!ts1.getLabels().equals(ts2.getLabels())) return false;
            if (ts1.getSamples().size() != ts2.getSamples().size()) return false;
        }
        return true;
    }

    /** Computes hash code for a time series list. */
    private static int timeSeriesListHashCode(List<TimeSeries> list) {
        if (list == null) return 0;
        int result = 1;
        for (TimeSeries ts : list) {
            result = 31 * result + (ts == null
                ? 0
                : Objects.hash(ts.getAlias(), ts.getMinTimestamp(), ts.getMaxTimestamp(), ts.getStep(), ts.getSamples().size()));
        }
        return result;
    }
}
