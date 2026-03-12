/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.Encoding;

public class CompressedChunkTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        byte[] chunkBytes = new byte[] { 1, 2, 3, 4, 5 };
        Encoding encoding = Encoding.XOR;
        long minTs = 1000L;
        long maxTs = 2000L;

        CompressedChunk chunk = new CompressedChunk(chunkBytes, encoding, minTs, maxTs);

        assertSame(chunkBytes, chunk.getChunkBytes());
        assertEquals(encoding, chunk.getEncoding());
        assertEquals(minTs, chunk.getMinTimestamp());
        assertEquals(maxTs, chunk.getMaxTimestamp());
        assertEquals(5, chunk.getCompressedSize());
    }

    public void testConstructorWithDefaultEncoding() {
        byte[] chunkBytes = new byte[] { 1, 2, 3 };
        long minTs = 1000L;
        long maxTs = 2000L;

        CompressedChunk chunk = new CompressedChunk(chunkBytes, minTs, maxTs);

        assertEquals(Encoding.XOR, chunk.getEncoding());
        assertEquals(3, chunk.getCompressedSize());
    }

    public void testConstructorNullChunkBytes() {
        expectThrows(NullPointerException.class, () -> { new CompressedChunk(null, Encoding.XOR, 1000L, 2000L); });
    }

    public void testConstructorNullEncoding() {
        expectThrows(NullPointerException.class, () -> { new CompressedChunk(new byte[] { 1, 2, 3 }, null, 1000L, 2000L); });
    }

    public void testOverlapsTimeRange() {
        CompressedChunk chunk = new CompressedChunk(new byte[] { 1, 2, 3 }, Encoding.XOR, 1000L, 2000L);

        assertTrue(chunk.overlapsTimeRange(500L, 2500L));
        assertTrue(chunk.overlapsTimeRange(1200L, 1800L));
        assertTrue(chunk.overlapsTimeRange(500L, 1500L));
        assertTrue(chunk.overlapsTimeRange(1500L, 2500L));
        assertFalse(chunk.overlapsTimeRange(0L, 500L));
        assertFalse(chunk.overlapsTimeRange(2500L, 3000L));
        assertFalse(chunk.overlapsTimeRange(0L, 1000L));
        assertTrue(chunk.overlapsTimeRange(2000L, 3000L));
    }

    public void testSerialization() throws Exception {
        byte[] chunkBytes = new byte[] { 10, 20, 30, 40, 50 };
        CompressedChunk original = new CompressedChunk(chunkBytes, Encoding.XOR, 5000L, 10000L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        CompressedChunk deserialized = new CompressedChunk(in);

        assertArrayEquals(chunkBytes, deserialized.getChunkBytes());
        assertEquals(Encoding.XOR, deserialized.getEncoding());
        assertEquals(5000L, deserialized.getMinTimestamp());
        assertEquals(10000L, deserialized.getMaxTimestamp());
        assertEquals(5, deserialized.getCompressedSize());
    }

    public void testSerializationWithEmptyChunk() throws Exception {
        byte[] emptyBytes = new byte[0];
        CompressedChunk original = new CompressedChunk(emptyBytes, Encoding.XOR, 0L, 0L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        CompressedChunk deserialized = new CompressedChunk(in);

        assertEquals(0, deserialized.getChunkBytes().length);
        assertEquals(0, deserialized.getCompressedSize());
    }

    public void testEqualsAndHashCode() {
        byte[] bytes1 = new byte[] { 1, 2, 3 };
        byte[] bytes2 = new byte[] { 1, 2, 3 };
        byte[] bytes3 = new byte[] { 4, 5, 6 };

        CompressedChunk chunk1 = new CompressedChunk(bytes1, Encoding.XOR, 1000L, 2000L);
        CompressedChunk chunk2 = new CompressedChunk(bytes2, Encoding.XOR, 1000L, 2000L);
        CompressedChunk chunk3 = new CompressedChunk(bytes3, Encoding.XOR, 1000L, 2000L);
        CompressedChunk chunk4 = new CompressedChunk(bytes1, Encoding.XOR, 1500L, 2000L);

        assertEquals(chunk1, chunk2);
        assertEquals(chunk1.hashCode(), chunk2.hashCode());
        assertNotEquals(chunk1, chunk3);
        assertNotEquals(chunk1, chunk4);
        assertEquals(chunk1, chunk1);
        assertNotEquals(chunk1, null);
        assertNotEquals(chunk1, "not a chunk");
    }

    public void testToString() {
        CompressedChunk chunk = new CompressedChunk(new byte[] { 1, 2, 3, 4, 5 }, Encoding.XOR, 1000L, 2000L);
        String str = chunk.toString();

        assertTrue(str.contains("CompressedChunk"));
        assertTrue(str.contains("XOR"));
        assertTrue(str.contains("5 bytes"));
        assertTrue(str.contains("1000"));
        assertTrue(str.contains("2000"));
    }

    public void testDecodeSamplesWithCorruptedData() throws Exception {
        byte[] corruptedData = new byte[] { (byte) 0xFF, (byte) 0xFF, (byte) 0xFF };
        CompressedChunk chunk = new CompressedChunk(corruptedData, Encoding.XOR, 1000L, 2000L);

        expectThrows(Exception.class, () -> chunk.decodeSamples(1000L, 2000L));
    }
}
