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
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.utils.SampleMerger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Time series with compressed chunks for efficient network transport in compressed mode.
 * Used when data nodes have no pipeline stages to process.
 */
public class CompressedTimeSeries implements Writeable {
    private final List<CompressedChunk> chunks;
    private final Labels labels;
    private final String alias;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long step;
    private static final SampleMerger MERGE_HELPER = new SampleMerger(SampleMerger.DeduplicatePolicy.ANY_WINS);

    public CompressedTimeSeries(
        List<CompressedChunk> chunks,
        Labels labels,
        long minTimestamp,
        long maxTimestamp,
        long step,
        String alias
    ) {
        this.chunks = Objects.requireNonNull(chunks, "chunks cannot be null");
        this.labels = Objects.requireNonNull(labels, "labels cannot be null");
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
        this.alias = alias;
    }

    public CompressedTimeSeries(StreamInput in) throws IOException {
        int chunkCount = in.readVInt();
        this.chunks = new ArrayList<>(chunkCount);
        for (int i = 0; i < chunkCount; i++) {
            this.chunks.add(new CompressedChunk(in));
        }

        Map<String, String> labelsMap = in.readMap(StreamInput::readString, StreamInput::readString);
        this.labels = labelsMap.isEmpty() ? ByteLabels.emptyLabels() : ByteLabels.fromMap(labelsMap);
        this.alias = in.readOptionalString();
        this.minTimestamp = in.readLong();
        this.maxTimestamp = in.readLong();
        this.step = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(chunks.size());
        for (CompressedChunk chunk : chunks) {
            chunk.writeTo(out);
        }
        out.writeMap(labels.toMapView(), StreamOutput::writeString, StreamOutput::writeString);
        out.writeOptionalString(alias);
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
        out.writeLong(step);
    }

    public List<CompressedChunk> getChunks() {
        return chunks;
    }

    public Labels getLabels() {
        return labels;
    }

    public String getAlias() {
        return alias;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public long getStep() {
        return step;
    }

    public long getTotalCompressedSize() {
        return chunks.stream().mapToLong(CompressedChunk::getCompressedSize).sum();
    }

    public int getChunkCount() {
        return chunks.size();
    }

    public List<Sample> decodeAllSamples(long queryMinTimestamp, long queryMaxTimestamp) throws IOException {
        List<List<Sample>> allDecodedSamples = new ArrayList<>(chunks.size());
        for (CompressedChunk chunk : chunks) {
            if (chunk.overlapsTimeRange(queryMinTimestamp, queryMaxTimestamp)) {
                allDecodedSamples.add(chunk.decodeSamples(queryMinTimestamp, queryMaxTimestamp));
            }
        }

        if (allDecodedSamples.isEmpty()) {
            return List.of();
        }

        List<Sample> result = allDecodedSamples.get(0);
        for (int i = 1; i < allDecodedSamples.size(); i++) {
            SampleList merged = MERGE_HELPER.merge(SampleList.fromList(result), SampleList.fromList(allDecodedSamples.get(i)), true);
            result = merged.toList();
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompressedTimeSeries that = (CompressedTimeSeries) o;
        return minTimestamp == that.minTimestamp
            && maxTimestamp == that.maxTimestamp
            && step == that.step
            && Objects.equals(chunks, that.chunks)
            && Objects.equals(labels.toMapView(), that.labels.toMapView())
            && Objects.equals(alias, that.alias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(chunks, labels.toMapView(), alias, minTimestamp, maxTimestamp, step);
    }

    @Override
    public String toString() {
        return "CompressedTimeSeries{chunkCount="
            + chunks.size()
            + ", compressedSize="
            + getTotalCompressedSize()
            + " bytes, labels="
            + labels.toMapView()
            + ", alias='"
            + alias
            + '\''
            + ", minTimestamp="
            + minTimestamp
            + ", maxTimestamp="
            + maxTimestamp
            + ", step="
            + step
            + '}';
    }
}
