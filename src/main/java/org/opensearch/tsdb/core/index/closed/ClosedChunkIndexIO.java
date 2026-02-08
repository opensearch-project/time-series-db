/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.closed;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.tsdb.core.chunk.Chunk;
import org.opensearch.tsdb.core.chunk.Encoding;

import java.io.IOException;

/**
 * Utility functions for working with closed chunk index data. This class provides helpers for ser/deser of chunks.
 */
public class ClosedChunkIndexIO {
    /** Version 1 of chunk serialization format: [byte 1: version][byte 2: encoding][bytes 3-n: chunk data] */
    public static final int VERSION_1 = 1;

    private static final int VERSION_1_METADATA_SIZE = 2; // version + encoding

    private static final int DEFAULT_VERSION = VERSION_1; // default version to use for serialization

    /**
     * ClosedChunkIndexIO should not be instantiated
     */
    private ClosedChunkIndexIO() {
        // Utility class
    }

    /**
     * Serialize a Chunk into a BytesRef for storage, using the default serialization format.
     *
     * @param chunk the Chunk to serialize
     * @return a BytesRef containing the serialized Chunk
     */
    public static BytesRef serializeChunk(Chunk chunk) {
        return serializeChunk(chunk, DEFAULT_VERSION);
    }

    private static BytesRef serializeChunk(Chunk chunk, int version) {
        switch (version) {
            case VERSION_1:
                return serializeChunkV1(chunk);
            default:
                throw new IllegalArgumentException("Unsupported version: " + version);
        }
    }

    private static BytesRef serializeChunkV1(Chunk chunk) {
        byte[] serializedBytes = new byte[chunk.bytesSize() + VERSION_1_METADATA_SIZE];
        ByteArrayDataOutput out = new ByteArrayDataOutput(serializedBytes);
        try {
            out.writeByte((byte) VERSION_1);
            out.writeByte((byte) chunk.encoding().ordinal());
            out.writeBytes(chunk.bytes(), chunk.bytesSize());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new BytesRef(serializedBytes, 0, out.getPosition());
    }

    /**
     * Deserialize a BytesRef into a ClosedChunk.
     * @param serialized the BytesRef containing the serialized Chunk
     * @return the deserialized ClosedChunk
     */
    public static ClosedChunk getClosedChunkFromSerialized(BytesRef serialized) {
        ByteArrayDataInput in = new ByteArrayDataInput(serialized.bytes, serialized.offset, serialized.length);

        int version = in.readByte();
        switch (version) {
            case VERSION_1:
                return getClosedChunkFromSerializedV1(in, serialized.length - serialized.offset);
            default:
                throw new IllegalStateException("Unsupported chunk version: " + version);
        }
    }

    private static ClosedChunk getClosedChunkFromSerializedV1(ByteArrayDataInput in, int length) {
        Encoding encoding = Encoding.values()[in.readByte()];
        byte[] chunkBytes = new byte[length - in.getPosition()];
        in.readBytes(chunkBytes, 0, chunkBytes.length);
        return new ClosedChunk(chunkBytes, encoding);
    }
}
