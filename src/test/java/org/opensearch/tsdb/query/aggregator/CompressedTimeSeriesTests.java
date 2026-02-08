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
import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CompressedTimeSeriesTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        List<CompressedChunk> chunks = createTestChunks(3);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api", "region", "us-west"));
        long minTs = 1000L;
        long maxTs = 5000L;
        long step = 1000L;
        String alias = "test-alias";

        CompressedTimeSeries series = new CompressedTimeSeries(chunks, labels, minTs, maxTs, step, alias);

        assertEquals(chunks, series.getChunks());
        assertEquals(labels, series.getLabels());
        assertEquals(minTs, series.getMinTimestamp());
        assertEquals(maxTs, series.getMaxTimestamp());
        assertEquals(step, series.getStep());
        assertEquals(alias, series.getAlias());
        assertEquals(3, series.getChunkCount());
    }

    public void testConstructorNullChunks() {
        expectThrows(NullPointerException.class, () -> new CompressedTimeSeries(null, ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null));
    }

    public void testConstructorNullLabels() {
        expectThrows(NullPointerException.class, () -> new CompressedTimeSeries(createTestChunks(1), null, 1000L, 2000L, 1000L, null));
    }

    public void testGetTotalCompressedSize() {
        List<CompressedChunk> chunks = new ArrayList<>();
        chunks.add(new CompressedChunk(new byte[10], Encoding.XOR, 1000L, 2000L));
        chunks.add(new CompressedChunk(new byte[20], Encoding.XOR, 2000L, 3000L));
        chunks.add(new CompressedChunk(new byte[30], Encoding.XOR, 3000L, 4000L));

        CompressedTimeSeries series = new CompressedTimeSeries(chunks, ByteLabels.emptyLabels(), 1000L, 4000L, 1000L, null);

        assertEquals(60L, series.getTotalCompressedSize());
    }

    public void testGetTotalCompressedSizeEmpty() {
        CompressedTimeSeries series = new CompressedTimeSeries(new ArrayList<>(), ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null);

        assertEquals(0L, series.getTotalCompressedSize());
        assertEquals(0, series.getChunkCount());
    }

    public void testSerialization() throws Exception {
        List<CompressedChunk> chunks = createTestChunks(2);
        Labels labels = ByteLabels.fromMap(Map.of("env", "prod"));
        CompressedTimeSeries original = new CompressedTimeSeries(chunks, labels, 1000L, 5000L, 1000L, "my-series");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        CompressedTimeSeries deserialized = new CompressedTimeSeries(in);

        assertEquals(2, deserialized.getChunkCount());
        assertEquals(labels.toMapView(), deserialized.getLabels().toMapView());
        assertEquals(1000L, deserialized.getMinTimestamp());
        assertEquals(5000L, deserialized.getMaxTimestamp());
        assertEquals(1000L, deserialized.getStep());
        assertEquals("my-series", deserialized.getAlias());
    }

    public void testSerializationWithNullAlias() throws Exception {
        CompressedTimeSeries original = new CompressedTimeSeries(createTestChunks(1), ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        CompressedTimeSeries deserialized = new CompressedTimeSeries(in);

        assertNull(deserialized.getAlias());
    }

    public void testSerializationWithEmptyLabels() throws Exception {
        CompressedTimeSeries original = new CompressedTimeSeries(
            createTestChunks(1),
            ByteLabels.emptyLabels(),
            1000L,
            2000L,
            1000L,
            "test"
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        CompressedTimeSeries deserialized = new CompressedTimeSeries(in);

        assertTrue(deserialized.getLabels().isEmpty());
    }

    public void testEqualsAndHashCode() {
        List<CompressedChunk> chunks1 = createTestChunks(2);
        List<CompressedChunk> chunks2 = createTestChunks(2);
        Labels labels1 = ByteLabels.fromMap(Map.of("key", "value"));
        Labels labels2 = ByteLabels.fromMap(Map.of("key", "value"));

        CompressedTimeSeries series1 = new CompressedTimeSeries(chunks1, labels1, 1000L, 2000L, 1000L, "alias");
        CompressedTimeSeries series2 = new CompressedTimeSeries(chunks2, labels2, 1000L, 2000L, 1000L, "alias");
        CompressedTimeSeries series3 = new CompressedTimeSeries(chunks1, labels1, 1500L, 2000L, 1000L, "alias");

        assertEquals(series1, series2);
        assertEquals(series1.hashCode(), series2.hashCode());
        assertNotEquals(series1, series3);
        assertEquals(series1, series1);
        assertNotEquals(series1, null);
        assertNotEquals(series1, "not a series");
    }

    public void testToString() {
        CompressedTimeSeries series = new CompressedTimeSeries(
            createTestChunks(2),
            ByteLabels.fromMap(Map.of("service", "api")),
            1000L,
            2000L,
            1000L,
            "test-alias"
        );

        String str = series.toString();
        assertTrue(str.contains("CompressedTimeSeries"));
        assertTrue(str.contains("chunkCount=2"));
        assertTrue(str.contains("test-alias"));
        assertTrue(str.contains("1000"));
        assertTrue(str.contains("2000"));
    }

    public void testDecodeAllSamplesEmptyChunks() throws Exception {
        CompressedTimeSeries series = new CompressedTimeSeries(new ArrayList<>(), ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null);
        List<Sample> result = series.decodeAllSamples(1000L, 2000L);
        assertEquals(List.of(), result);
    }

    public void testDecodeAllSamplesSingleChunk() throws Exception {
        XORChunk xorChunk = new XORChunk();
        ChunkAppender appender = xorChunk.appender();
        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);
        appender.append(3000L, 30.0);

        CompressedChunk chunk = new CompressedChunk(xorChunk.bytes(), Encoding.XOR, 1000L, 3000L);
        CompressedTimeSeries series = new CompressedTimeSeries(
            List.of(chunk),
            ByteLabels.fromMap(Map.of("job", "test")),
            1000L,
            3000L,
            1000L,
            null
        );

        List<Sample> result = series.decodeAllSamples(1000L, 4000L);
        assertEquals(3, result.size());
        assertEquals(1000L, result.get(0).getTimestamp());
        assertEquals(10.0, result.get(0).getValue(), 0.001);
        assertEquals(3000L, result.get(2).getTimestamp());
        assertEquals(30.0, result.get(2).getValue(), 0.001);
    }

    public void testDecodeAllSamplesSkipsNonOverlappingChunks() throws Exception {
        XORChunk xorChunk = new XORChunk();
        xorChunk.appender().append(5000L, 50.0);
        xorChunk.appender().append(6000L, 60.0);

        CompressedChunk chunk = new CompressedChunk(xorChunk.bytes(), Encoding.XOR, 5000L, 6000L);
        CompressedTimeSeries series = new CompressedTimeSeries(List.of(chunk), ByteLabels.emptyLabels(), 5000L, 6000L, 1000L, null);

        // Query range does not overlap chunk
        List<Sample> result = series.decodeAllSamples(1000L, 2000L);
        assertEquals(List.of(), result);
    }

    private List<CompressedChunk> createTestChunks(int count) {
        List<CompressedChunk> chunks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            byte[] bytes = new byte[] { (byte) i, (byte) (i + 1), (byte) (i + 2) };
            chunks.add(new CompressedChunk(bytes, Encoding.XOR, i * 1000L, (i + 1) * 1000L));
        }
        return chunks;
    }
}
