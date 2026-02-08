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
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.chunk.XORIterator;
import org.opensearch.tsdb.core.model.Sample;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Compressed chunk for efficient network transport in compressed mode.
 * Encapsulates raw encoded bytes with metadata for coordinator-side decoding.
 * Used when data nodes have no pipeline stages to process.
 */
public class CompressedChunk implements Writeable {
    private final byte[] chunkBytes;
    private final Encoding encoding;
    private final long minTimestamp;
    private final long maxTimestamp;

    public CompressedChunk(byte[] chunkBytes, long minTimestamp, long maxTimestamp) {
        this(chunkBytes, Encoding.XOR, minTimestamp, maxTimestamp);
    }

    public CompressedChunk(byte[] chunkBytes, Encoding encoding, long minTimestamp, long maxTimestamp) {
        this.chunkBytes = Objects.requireNonNull(chunkBytes, "chunkBytes cannot be null");
        this.encoding = Objects.requireNonNull(encoding, "encoding cannot be null");
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    public CompressedChunk(StreamInput in) throws IOException {
        this.chunkBytes = in.readByteArray();
        int encodingOrdinal = in.readVInt();
        this.encoding = Encoding.values()[encodingOrdinal];
        this.minTimestamp = in.readLong();
        this.maxTimestamp = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByteArray(chunkBytes);
        out.writeVInt(encoding.ordinal());
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
    }

    /** Returns internal array - do not modify. */
    public byte[] getChunkBytes() {
        return chunkBytes;
    }

    public Encoding getEncoding() {
        return encoding;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public int getCompressedSize() {
        return chunkBytes.length;
    }

    public ChunkIterator getChunkIterator() {
        return switch (encoding) {
            case XOR -> new XORIterator(chunkBytes);
            default -> throw new IllegalArgumentException("Unsupported encoding for CompressedChunk: " + encoding);
        };
    }

    public List<Sample> decodeSamples(long queryMinTimestamp, long queryMaxTimestamp) throws IOException {
        ChunkIterator iterator = getChunkIterator();
        ChunkIterator.DecodeResult decodeResult = iterator.decodeSamples(queryMinTimestamp, queryMaxTimestamp);
        if (iterator.error() != null) {
            throw new IOException("Error decoding samples from compressed chunk", iterator.error());
        }
        return decodeResult.samples();
    }

    public boolean overlapsTimeRange(long queryMinTimestamp, long queryMaxTimestamp) {
        return !(maxTimestamp < queryMinTimestamp || minTimestamp >= queryMaxTimestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CompressedChunk that = (CompressedChunk) o;
        return minTimestamp == that.minTimestamp
            && maxTimestamp == that.maxTimestamp
            && Arrays.equals(chunkBytes, that.chunkBytes)
            && encoding == that.encoding;
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(encoding, minTimestamp, maxTimestamp);
        result = 31 * result + Arrays.hashCode(chunkBytes);
        return result;
    }

    @Override
    public String toString() {
        return "CompressedChunk{encoding="
            + encoding
            + ", size="
            + chunkBytes.length
            + " bytes"
            + ", minTimestamp="
            + minTimestamp
            + ", maxTimestamp="
            + maxTimestamp
            + '}';
    }
}
