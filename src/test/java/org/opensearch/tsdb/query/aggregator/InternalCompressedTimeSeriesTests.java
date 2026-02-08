/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.chunk.ChunkAppender;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.chunk.XORChunk;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Tests for InternalTimeSeries with XOR (compressed) encoding mode.
 */
public class InternalCompressedTimeSeriesTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        List<CompressedTimeSeries> series = createCompressedSeries(2);
        InternalTimeSeries aggregation = InternalTimeSeries.compressed("test", series, Map.of());

        assertEquals("test", aggregation.getName());
        assertEquals(InternalTimeSeries.Encoding.XOR, aggregation.getEncoding());
        assertEquals(2, aggregation.getTimeSeries().size());
    }

    public void testGetWriteableName() {
        InternalTimeSeries aggregation = InternalTimeSeries.compressed("test", List.of(), Map.of());
        assertEquals("time_series", aggregation.getWriteableName());
        assertEquals(InternalTimeSeries.Encoding.XOR, aggregation.getEncoding());
    }

    public void testSerialization() throws Exception {
        List<CompressedTimeSeries> series = createCompressedSeries(2);
        InternalTimeSeries original = InternalTimeSeries.compressed("test-agg", series, Map.of("key", "value"));

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTimeSeries deserialized = new InternalTimeSeries(in);

        assertEquals("test-agg", deserialized.getName());
        assertEquals(InternalTimeSeries.Encoding.XOR, deserialized.getEncoding());
        assertEquals(2, deserialized.getTimeSeries().size());
    }

    public void testGetTimeSeriesDecodesChunks() {
        XORChunk xorChunk = new XORChunk();
        ChunkAppender appender = xorChunk.appender();
        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);
        appender.append(3000L, 30.0);

        byte[] chunkBytes = xorChunk.bytes();
        CompressedChunk compressedChunk = new CompressedChunk(chunkBytes, Encoding.XOR, 1000L, 3000L);

        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        CompressedTimeSeries compressedSeries = new CompressedTimeSeries(
            List.of(compressedChunk),
            labels,
            1000L,
            3000L,
            1000L,
            "test-alias"
        );

        InternalTimeSeries aggregation = InternalTimeSeries.compressed("test", List.of(compressedSeries), Map.of());

        List<TimeSeries> decoded = aggregation.getTimeSeries();

        assertEquals(1, decoded.size());
        TimeSeries series = decoded.get(0);
        assertEquals(3, series.getSamples().size());
        assertEquals(labels.toMapView(), series.getLabels().toMapView());
        assertEquals("test-alias", series.getAlias());
    }

    public void testGetTimeSeriesWithEmptyChunks() {
        CompressedTimeSeries emptySeries = new CompressedTimeSeries(List.of(), ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null);

        InternalTimeSeries aggregation = InternalTimeSeries.compressed("test", List.of(emptySeries), Map.of());

        List<TimeSeries> decoded = aggregation.getTimeSeries();
        assertEquals(1, decoded.size());
        assertEquals(0, decoded.get(0).getSamples().size());
    }

    public void testGetTimeSeriesDecodesMultipleChunksInSingleSeries() {
        XORChunk chunk1 = new XORChunk();
        chunk1.appender().append(1000L, 10.0);
        chunk1.appender().append(2000L, 20.0);

        XORChunk chunk2 = new XORChunk();
        chunk2.appender().append(3000L, 30.0);
        chunk2.appender().append(4000L, 40.0);

        XORChunk chunk3 = new XORChunk();
        chunk3.appender().append(5000L, 50.0);
        chunk3.appender().append(6000L, 60.0);

        Labels labels = ByteLabels.fromMap(Map.of("service", "api", "region", "us-west"));

        CompressedTimeSeries compressedSeries = new CompressedTimeSeries(
            List.of(
                new CompressedChunk(chunk1.bytes(), Encoding.XOR, 1000L, 2000L),
                new CompressedChunk(chunk2.bytes(), Encoding.XOR, 3000L, 4000L),
                new CompressedChunk(chunk3.bytes(), Encoding.XOR, 5000L, 6000L)
            ),
            labels,
            1000L,
            6000L,
            1000L,
            "multi-chunk-series"
        );

        InternalTimeSeries aggregation = InternalTimeSeries.compressed("test", List.of(compressedSeries), Map.of());

        List<TimeSeries> decoded = aggregation.getTimeSeries();

        assertEquals(1, decoded.size());
        TimeSeries series = decoded.get(0);
        assertEquals(6, series.getSamples().size());
        assertEquals(labels.toMapView(), series.getLabels().toMapView());
        assertEquals("multi-chunk-series", series.getAlias());

        List<Sample> samples = series.getSamples().toList();
        assertEquals(1000L, samples.get(0).getTimestamp());
        assertEquals(10.0, samples.get(0).getValue(), 0.001);
        assertEquals(6000L, samples.get(5).getTimestamp());
        assertEquals(60.0, samples.get(5).getValue(), 0.001);
    }

    public void testReduceDecodesAndMerges() {
        XORChunk chunk1 = new XORChunk();
        chunk1.appender().append(1000L, 10.0);
        chunk1.appender().append(2000L, 20.0);

        XORChunk chunk2 = new XORChunk();
        chunk2.appender().append(3000L, 30.0);
        chunk2.appender().append(4000L, 40.0);

        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));

        CompressedTimeSeries series1 = new CompressedTimeSeries(
            List.of(new CompressedChunk(chunk1.bytes(), Encoding.XOR, 1000L, 2000L)),
            labels,
            1000L,
            4000L,
            1000L,
            null
        );

        CompressedTimeSeries series2 = new CompressedTimeSeries(
            List.of(new CompressedChunk(chunk2.bytes(), Encoding.XOR, 3000L, 4000L)),
            labels,
            1000L,
            4000L,
            1000L,
            null
        );

        InternalTimeSeries agg1 = InternalTimeSeries.compressed("test", List.of(series1), Map.of());
        InternalTimeSeries agg2 = InternalTimeSeries.compressed("test", List.of(series2), Map.of());

        List<InternalAggregation> toReduce = List.of(agg1, agg2);
        InternalAggregation reduced = agg1.reduce(toReduce, createReduceContext());

        assertTrue(reduced instanceof InternalTimeSeries);
        InternalTimeSeries result = (InternalTimeSeries) reduced;
        assertEquals(InternalTimeSeries.Encoding.NONE, result.getEncoding());
        assertEquals(1, result.getTimeSeries().size());

        TimeSeries merged = result.getTimeSeries().get(0);
        assertEquals(4, merged.getSamples().size());
        assertEquals(labels.toMapView(), merged.getLabels().toMapView());
    }

    /**
     * Partial reduce (e.g. data node merge) with all XOR aggs keeps payload compressed (mergeCompressedWithoutDecoding).
     */
    public void testReducePartialWithCompressedKeepsXOREncoding() {
        XORChunk chunk1 = new XORChunk();
        chunk1.appender().append(1000L, 10.0);
        XORChunk chunk2 = new XORChunk();
        chunk2.appender().append(2000L, 20.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        CompressedTimeSeries series1 = new CompressedTimeSeries(
            List.of(new CompressedChunk(chunk1.bytes(), Encoding.XOR, 1000L, 2000L)),
            labels,
            1000L,
            2000L,
            1000L,
            null
        );
        CompressedTimeSeries series2 = new CompressedTimeSeries(
            List.of(new CompressedChunk(chunk2.bytes(), Encoding.XOR, 2000L, 3000L)),
            labels,
            2000L,
            3000L,
            1000L,
            null
        );
        InternalTimeSeries agg1 = InternalTimeSeries.compressed("test", List.of(series1), Map.of());
        InternalTimeSeries agg2 = InternalTimeSeries.compressed("test", List.of(series2), Map.of());

        InternalAggregation reduced = agg1.reduce(List.of(agg1, agg2), createPartialReduceContext());

        assertTrue(reduced instanceof InternalTimeSeries);
        InternalTimeSeries result = (InternalTimeSeries) reduced;
        assertEquals(InternalTimeSeries.Encoding.XOR, result.getEncoding());
        assertEquals(1, result.getCompressedTimeSeries().size());
        assertEquals(2, result.getCompressedTimeSeries().get(0).getChunkCount());
    }

    public void testReduceWithDifferentLabels() {
        XORChunk chunk1 = new XORChunk();
        chunk1.appender().append(1000L, 10.0);

        XORChunk chunk2 = new XORChunk();
        chunk2.appender().append(2000L, 20.0);

        Labels labels1 = ByteLabels.fromMap(Map.of("service", "api"));
        Labels labels2 = ByteLabels.fromMap(Map.of("service", "web"));

        CompressedTimeSeries series1 = new CompressedTimeSeries(
            List.of(new CompressedChunk(chunk1.bytes(), Encoding.XOR, 1000L, 2000L)),
            labels1,
            1000L,
            2000L,
            1000L,
            null
        );

        CompressedTimeSeries series2 = new CompressedTimeSeries(
            List.of(new CompressedChunk(chunk2.bytes(), Encoding.XOR, 2000L, 3000L)),
            labels2,
            2000L,
            3000L,
            1000L,
            null
        );

        InternalTimeSeries agg1 = InternalTimeSeries.compressed("test", List.of(series1), Map.of());
        InternalTimeSeries agg2 = InternalTimeSeries.compressed("test", List.of(series2), Map.of());

        List<InternalAggregation> toReduce = List.of(agg1, agg2);
        InternalAggregation reduced = agg1.reduce(toReduce, createReduceContext());

        assertTrue(reduced instanceof InternalTimeSeries);
        InternalTimeSeries result = (InternalTimeSeries) reduced;
        assertEquals(InternalTimeSeries.Encoding.NONE, result.getEncoding());
        assertEquals(2, result.getTimeSeries().size());
    }

    public void testGetProperty() {
        InternalTimeSeries aggregation = InternalTimeSeries.compressed("test", List.of(), Map.of());

        assertEquals(aggregation, aggregation.getProperty(List.of()));

        Object timeSeries = aggregation.getProperty(List.of("timeSeries"));
        assertTrue(timeSeries instanceof List);

        expectThrows(IllegalArgumentException.class, () -> aggregation.getProperty(List.of("unknown")));
    }

    public void testCreateReduced() {
        InternalTimeSeries aggregation = InternalTimeSeries.compressed("test", List.of(), Map.of());

        TimeSeries series = new TimeSeries(List.of(), ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null);
        TimeSeriesProvider reduced = aggregation.createReduced(List.of(series));

        assertTrue(reduced instanceof InternalTimeSeries);
        assertEquals(1, reduced.getTimeSeries().size());
    }

    public void testMustReduceOnSingleInternalAgg() {
        InternalTimeSeries aggregation = InternalTimeSeries.compressed("test", List.of(), Map.of());
        assertFalse(aggregation.mustReduceOnSingleInternalAgg());
    }

    public void testDoXContentBody() throws Exception {
        XORChunk chunk = new XORChunk();
        chunk.appender().append(1000L, 10.0);

        CompressedTimeSeries series = new CompressedTimeSeries(
            List.of(new CompressedChunk(chunk.bytes(), Encoding.XOR, 1000L, 2000L)),
            ByteLabels.fromMap(Map.of("service", "api")),
            1000L,
            2000L,
            1000L,
            "test-alias"
        );

        InternalTimeSeries aggregation = InternalTimeSeries.compressed("test", List.of(series), Map.of());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        aggregation.doXContentBody(builder, InternalAggregation.EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue(json.contains("timeSeries"));
        assertTrue(json.contains("test-alias"));
        assertTrue(json.contains("service"));
        assertTrue(json.contains("api"));
    }

    public void testEqualsAndHashCode() {
        List<CompressedTimeSeries> series1 = createCompressedSeries(1);
        List<CompressedTimeSeries> series2 = createCompressedSeries(1);

        InternalTimeSeries agg1 = InternalTimeSeries.compressed("test", series1, Map.of());
        InternalTimeSeries agg2 = InternalTimeSeries.compressed("test", series2, Map.of());
        InternalTimeSeries agg3 = InternalTimeSeries.compressed("different", series1, Map.of());

        assertEquals(agg1, agg2);
        assertEquals(agg1.hashCode(), agg2.hashCode());
        assertNotEquals(agg1, agg3);
    }

    public void testInvalidEncodingId() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> InternalTimeSeries.Encoding.fromId((byte) 99)
        );
        assertTrue(exception.getMessage().contains("Unknown encoding ID"));
    }

    public void testReduceWithWrongAggregationType() {
        InternalTimeSeries agg1 = new InternalTimeSeries("test", List.of(), Map.of());

        InternalAggregation wrongType = new InternalAggregation("test", Map.of()) {
            @Override
            public String getWriteableName() {
                return "wrong-type";
            }

            @Override
            protected void doWriteTo(StreamOutput out) {}

            @Override
            public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
                return this;
            }

            @Override
            public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws java.io.IOException {
                return builder;
            }

            @Override
            public Object getProperty(List<String> path) {
                return this;
            }

            @Override
            public boolean mustReduceOnSingleInternalAgg() {
                return false;
            }
        };

        List<InternalAggregation> toReduce = List.of(agg1, wrongType);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> agg1.reduce(toReduce, createReduceContext())
        );
        assertTrue(exception.getMessage().contains("Expected InternalTimeSeries"));
    }

    public void testReduceWithNullTimeSeries() {
        InternalTimeSeries aggWithNull = new InternalTimeSeries("test", null, Map.of());
        InternalTimeSeries aggWithData = new InternalTimeSeries("test", List.of(createTimeSeries("service", "api", 1000L, 10.0)), Map.of());

        List<InternalAggregation> toReduce = List.of(aggWithNull, aggWithData);
        InternalAggregation reduced = aggWithNull.reduce(toReduce, createReduceContext());

        assertTrue(reduced instanceof InternalTimeSeries);
        InternalTimeSeries result = (InternalTimeSeries) reduced;
        assertEquals(1, result.getTimeSeries().size());
    }

    public void testGetTimeSeriesWithMultipleChunksAndDifferentTimeRanges() {
        XORChunk chunk1 = new XORChunk();
        chunk1.appender().append(1000L, 10.0);
        chunk1.appender().append(2000L, 20.0);

        XORChunk chunk2 = new XORChunk();
        chunk2.appender().append(5000L, 50.0);
        chunk2.appender().append(6000L, 60.0);

        XORChunk chunk3 = new XORChunk();
        chunk3.appender().append(9000L, 90.0);
        chunk3.appender().append(10000L, 100.0);

        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));

        CompressedTimeSeries series1 = new CompressedTimeSeries(
            List.of(new CompressedChunk(chunk1.bytes(), Encoding.XOR, 1000L, 2000L)),
            labels,
            1000L,
            2000L,
            1000L,
            null
        );

        CompressedTimeSeries series2 = new CompressedTimeSeries(
            List.of(new CompressedChunk(chunk2.bytes(), Encoding.XOR, 5000L, 6000L)),
            labels,
            5000L,
            6000L,
            1000L,
            null
        );

        CompressedTimeSeries series3 = new CompressedTimeSeries(
            List.of(new CompressedChunk(chunk3.bytes(), Encoding.XOR, 9000L, 10000L)),
            labels,
            9000L,
            10000L,
            1000L,
            null
        );

        InternalTimeSeries aggregation = InternalTimeSeries.compressed("test", List.of(series1, series2, series3), Map.of());

        List<TimeSeries> decoded = aggregation.getTimeSeries();

        assertEquals(1, decoded.size());
        TimeSeries merged = decoded.get(0);
        assertEquals(6, merged.getSamples().size());

        List<Long> timestamps = merged.getSamples().toList().stream().map(Sample::getTimestamp).sorted().toList();
        assertEquals(List.of(1000L, 2000L, 5000L, 6000L, 9000L, 10000L), timestamps);
        assertEquals(1000L, merged.getMinTimestamp());
        assertEquals(10000L, merged.getMaxTimestamp());
        assertEquals(labels.toMapView(), merged.getLabels().toMapView());
    }

    private TimeSeries createTimeSeries(String labelKey, String labelValue, long timestamp, double value) {
        Labels labels = ByteLabels.fromMap(Map.of(labelKey, labelValue));
        List<Sample> samples = List.of(new FloatSample(timestamp, (float) value));
        return new TimeSeries(samples, labels, timestamp, timestamp, 1000L, null);
    }

    private List<CompressedTimeSeries> createCompressedSeries(int count) {
        List<CompressedTimeSeries> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            XORChunk xorChunk = new XORChunk();
            ChunkAppender appender = xorChunk.appender();
            appender.append(i * 1000L, 10.0 * i);
            appender.append((i * 1000L) + 500, 20.0 * i);

            byte[] bytes = xorChunk.bytes();
            CompressedChunk chunk = new CompressedChunk(bytes, Encoding.XOR, i * 1000L, (i * 1000L) + 500);
            Labels labels = ByteLabels.fromMap(Map.of("idx", String.valueOf(i)));
            result.add(new CompressedTimeSeries(List.of(chunk), labels, i * 1000L, (i * 1000L) + 500, 500L, null));
        }
        return result;
    }

    private InternalAggregation.ReduceContext createReduceContext() {
        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(Map.of(), Collections.emptyList());
        return InternalAggregation.ReduceContext.forFinalReduction(null, null, (s) -> {}, emptyPipelineTree);
    }

    private InternalAggregation.ReduceContext createPartialReduceContext() {
        PipelineAggregator.PipelineTree emptyPipelineTree = new PipelineAggregator.PipelineTree(Map.of(), Collections.emptyList());
        return InternalAggregation.ReduceContext.forPartialReduction(null, null, () -> emptyPipelineTree);
    }
}
