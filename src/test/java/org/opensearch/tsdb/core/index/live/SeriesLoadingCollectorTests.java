/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.head.SeriesEventListener;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SeriesLoadingCollectorTests extends OpenSearchTestCase {

    /**
     * Test SeriesLoadingCollector with BINARY storage returns correct max reference
     */
    public void testMaxReferenceWithBinaryStorage() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

        // Add documents with references in non-ascending order
        addBinaryDocument(writer, ByteLabels.fromStrings("k1", "v1"), 100L);
        addBinaryDocument(writer, ByteLabels.fromStrings("k2", "v2"), 50L);
        addBinaryDocument(writer, ByteLabels.fromStrings("k3", "v3"), 200L);
        addBinaryDocument(writer, ByteLabels.fromStrings("k4", "v4"), 75L);

        writer.commit();
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        List<Long> collectedReferences = new ArrayList<>();
        SeriesLoader loader = series -> collectedReferences.add(series.getReference());

        SeriesLoadingCollectorManager manager = new SeriesLoadingCollectorManager(
            loader,
            LabelStorageType.BINARY,
            SeriesEventListener.NOOP
        );
        Long maxRef = searcher.search(new MatchAllDocsQuery(), manager);

        assertEquals("Max reference should be 200", 200L, maxRef.longValue());
        assertEquals("Should collect 4 series", 4, collectedReferences.size());

        reader.close();
        dir.close();
    }

    /**
     * Test SeriesLoadingCollector with SORTED_SET storage returns correct max reference
     */
    public void testMaxReferenceWithSortedSetStorage() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

        // Add documents with references in non-ascending order
        addSortedSetDocument(writer, ByteLabels.fromStrings("k1", "v1"), 100L);
        addSortedSetDocument(writer, ByteLabels.fromStrings("k2", "v2"), 300L);
        addSortedSetDocument(writer, ByteLabels.fromStrings("k3", "v3"), 150L);

        writer.commit();
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        List<Long> collectedReferences = new ArrayList<>();
        SeriesLoader loader = series -> collectedReferences.add(series.getReference());

        SeriesLoadingCollectorManager manager = new SeriesLoadingCollectorManager(
            loader,
            LabelStorageType.SORTED_SET,
            SeriesEventListener.NOOP
        );
        Long maxRef = searcher.search(new MatchAllDocsQuery(), manager);

        assertEquals("Max reference should be 300", 300L, maxRef.longValue());
        assertEquals("Should collect 3 series", 3, collectedReferences.size());

        reader.close();
        dir.close();
    }

    /**
     * Test SeriesLoadingCollector with empty index returns 0 for max reference
     */
    public void testMaxReferenceWithEmptyIndex() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        writer.commit();
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        List<Long> collectedReferences = new ArrayList<>();
        SeriesLoader loader = series -> collectedReferences.add(series.getReference());

        SeriesLoadingCollectorManager manager = new SeriesLoadingCollectorManager(
            loader,
            LabelStorageType.BINARY,
            SeriesEventListener.NOOP
        );
        Long maxRef = searcher.search(new MatchAllDocsQuery(), manager);

        assertEquals("Max reference should be 0 for empty index", 0L, maxRef.longValue());
        assertTrue("Should collect no series", collectedReferences.isEmpty());

        reader.close();
        dir.close();
    }

    /**
     * Test SeriesLoadingCollector correctly deserializes labels in BINARY format
     */
    public void testLabelsDeserializationBinary() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

        Labels expectedLabels = ByteLabels.fromStrings("__name__", "http_requests", "method", "GET", "status", "200");
        addBinaryDocument(writer, expectedLabels, 42L);

        writer.commit();
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        List<Labels> collectedLabels = new ArrayList<>();
        SeriesLoader loader = series -> collectedLabels.add(series.getLabels());

        SeriesLoadingCollectorManager manager = new SeriesLoadingCollectorManager(
            loader,
            LabelStorageType.BINARY,
            SeriesEventListener.NOOP
        );
        searcher.search(new MatchAllDocsQuery(), manager);

        assertEquals("Should collect 1 series", 1, collectedLabels.size());
        assertEquals("Labels should match", expectedLabels, collectedLabels.get(0));

        reader.close();
        dir.close();
    }

    /**
     * Test SeriesLoadingCollector correctly deserializes labels in SORTED_SET format
     */
    public void testLabelsDeserializationSortedSet() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

        Labels expectedLabels = ByteLabels.fromStrings("__name__", "cpu_usage", "host", "server1");
        addSortedSetDocument(writer, expectedLabels, 99L);

        writer.commit();
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        List<Labels> collectedLabels = new ArrayList<>();
        SeriesLoader loader = series -> collectedLabels.add(series.getLabels());

        SeriesLoadingCollectorManager manager = new SeriesLoadingCollectorManager(
            loader,
            LabelStorageType.SORTED_SET,
            SeriesEventListener.NOOP
        );
        searcher.search(new MatchAllDocsQuery(), manager);

        assertEquals("Should collect 1 series", 1, collectedLabels.size());
        assertEquals("Labels should match", expectedLabels, collectedLabels.get(0));

        reader.close();
        dir.close();
    }

    /**
     * Test SeriesLoadingCollector with SORTED_SET when advanceExact returns false
     */
    public void testSortedSetAdvanceExactReturnsFalse() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

        // Add a document with reference but no labels (edge case)
        Document doc = new Document();
        doc.add(new NumericDocValuesField(Constants.IndexSchema.REFERENCE, 123L));
        writer.addDocument(doc);

        writer.commit();
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        List<MemSeries> collectedSeries = new ArrayList<>();
        SeriesLoader loader = collectedSeries::add;

        SeriesLoadingCollectorManager manager = new SeriesLoadingCollectorManager(
            loader,
            LabelStorageType.SORTED_SET,
            SeriesEventListener.NOOP
        );
        Long maxRef = searcher.search(new MatchAllDocsQuery(), manager);

        assertEquals("Max reference should be 123", 123L, maxRef.longValue());
        assertEquals("Should collect 1 series", 1, collectedSeries.size());
        assertTrue("Series should have empty labels", collectedSeries.get(0).getLabels().isEmpty());

        reader.close();
        dir.close();
    }

    /**
     * Test that maxReference is correctly updated across multiple collectors
     */
    public void testMaxReferenceAcrossMultipleCollectors() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

        // Add multiple documents to potentially trigger multiple leaf readers
        for (int i = 0; i < 10; i++) {
            addBinaryDocument(writer, ByteLabels.fromStrings("k", "v" + i), i * 10L);
        }

        writer.commit();
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        SeriesLoader loader = series -> {};
        SeriesLoadingCollectorManager manager = new SeriesLoadingCollectorManager(
            loader,
            LabelStorageType.BINARY,
            SeriesEventListener.NOOP
        );
        Long maxRef = searcher.search(new MatchAllDocsQuery(), manager);

        assertEquals("Max reference should be 90", 90L, maxRef.longValue());

        reader.close();
        dir.close();
    }

    /**
     * Test that maxReference handles equal references correctly (tests else branch)
     */
    public void testMaxReferenceWithEqualReferences() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

        // Add documents with same reference (edge case)
        addBinaryDocument(writer, ByteLabels.fromStrings("k1", "v1"), 100L);
        addBinaryDocument(writer, ByteLabels.fromStrings("k2", "v2"), 100L);
        addBinaryDocument(writer, ByteLabels.fromStrings("k3", "v3"), 50L);

        writer.commit();
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        SeriesLoader loader = series -> {};
        SeriesLoadingCollectorManager manager = new SeriesLoadingCollectorManager(
            loader,
            LabelStorageType.BINARY,
            SeriesEventListener.NOOP
        );
        Long maxRef = searcher.search(new MatchAllDocsQuery(), manager);

        assertEquals("Max reference should be 100", 100L, maxRef.longValue());

        reader.close();
        dir.close();
    }

    /**
     * Test SeriesLoadingCollector with BINARY storage when advanceExact returns false
     */
    public void testBinaryAdvanceExactReturnsFalse() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

        // Add a document with reference but no labels in binary format
        Document doc = new Document();
        doc.add(new NumericDocValuesField(Constants.IndexSchema.REFERENCE, 456L));
        writer.addDocument(doc);

        writer.commit();
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        List<MemSeries> collectedSeries = new ArrayList<>();
        SeriesLoader loader = collectedSeries::add;

        SeriesLoadingCollectorManager manager = new SeriesLoadingCollectorManager(
            loader,
            LabelStorageType.BINARY,
            SeriesEventListener.NOOP
        );
        Long maxRef = searcher.search(new MatchAllDocsQuery(), manager);

        assertEquals("Max reference should be 456", 456L, maxRef.longValue());
        assertEquals("Should collect 1 series", 1, collectedSeries.size());
        assertTrue("Series should have empty labels", collectedSeries.get(0).getLabels().isEmpty());

        reader.close();
        dir.close();
    }

    /**
     * Test SeriesLoadingCollector with SORTED_SET storage and multiple label pairs
     */
    public void testSortedSetWithMultipleLabels() throws IOException {
        Directory dir = new ByteBuffersDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

        // Add document with many labels to test the for loop
        Labels multiLabels = ByteLabels.fromStrings(
            "__name__",
            "http_requests",
            "method",
            "GET",
            "status",
            "200",
            "host",
            "server1",
            "region",
            "us-west"
        );
        addSortedSetDocument(writer, multiLabels, 777L);

        writer.commit();
        writer.close();

        DirectoryReader reader = DirectoryReader.open(dir);
        IndexSearcher searcher = new IndexSearcher(reader);

        List<Labels> collectedLabels = new ArrayList<>();
        SeriesLoader loader = series -> collectedLabels.add(series.getLabels());

        SeriesLoadingCollectorManager manager = new SeriesLoadingCollectorManager(
            loader,
            LabelStorageType.SORTED_SET,
            SeriesEventListener.NOOP
        );
        searcher.search(new MatchAllDocsQuery(), manager);

        assertEquals("Should collect 1 series", 1, collectedLabels.size());
        assertEquals("Labels should match", multiLabels, collectedLabels.get(0));

        reader.close();
        dir.close();
    }

    // Helper method to add a document with BINARY label storage
    private void addBinaryDocument(IndexWriter writer, Labels labels, long reference) throws IOException {
        Document doc = new Document();
        doc.add(new NumericDocValuesField(Constants.IndexSchema.REFERENCE, reference));
        ByteLabels byteLabels = (ByteLabels) labels;
        BytesRef serializedLabels = new BytesRef(byteLabels.getRawBytes());
        doc.add(new BinaryDocValuesField(Constants.IndexSchema.LABELS, serializedLabels));
        writer.addDocument(doc);
    }

    // Helper method to add a document with SORTED_SET label storage
    private void addSortedSetDocument(IndexWriter writer, Labels labels, long reference) throws IOException {
        Document doc = new Document();
        doc.add(new NumericDocValuesField(Constants.IndexSchema.REFERENCE, reference));
        for (BytesRef labelRef : labels.toKeyValueBytesRefs()) {
            doc.add(new SortedSetDocValuesField(Constants.IndexSchema.LABELS, labelRef));
        }
        writer.addDocument(doc);
    }
}
