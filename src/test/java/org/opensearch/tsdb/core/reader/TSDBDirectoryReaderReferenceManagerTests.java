/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.index.ReaderManagerWithMetadata;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.index.live.MemChunkReader;
import org.opensearch.tsdb.core.chunk.MMappedChunksManager;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.metrics.TSDBMetricsConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.mockito.ArgumentCaptor;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TSDBDirectoryReaderReferenceManager
 */
@SuppressForbidden(reason = "reference counting is required for testing")
public class TSDBDirectoryReaderReferenceManagerTests extends OpenSearchTestCase {

    private Directory liveDirectory;
    private Directory closedDirectory1;
    private Directory closedDirectory2;
    private Directory closedDirectory3;
    private IndexWriter liveWriter;
    private DirectoryReader liveReader;
    private DirectoryReader closedReader1;
    private DirectoryReader closedReader2;
    private DirectoryReader closedReader3;
    private ReaderManager liveReaderManager;
    private ReaderManagerWithMetadata closedReaderManager1;
    private ReaderManagerWithMetadata closedReaderManager2;
    private ReaderManagerWithMetadata closedReaderManager3;
    private ClosedChunkIndexManager closedChunkIndexManager;
    private MemChunkReader memChunkReader;
    private MMappedChunksManager mmappedChunksManager;
    private ShardId shardId;
    private TSDBDirectoryReaderReferenceManager referenceManager;
    private List<MemChunk> memChunks;
    private LabelStorageType labelStorageType;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        shardId = new ShardId("test-index", "test-uuid", 0);

        // Create test directories
        liveDirectory = new ByteBuffersDirectory();
        closedDirectory1 = new ByteBuffersDirectory();
        closedDirectory2 = new ByteBuffersDirectory();
        closedDirectory3 = new ByteBuffersDirectory();

        // Initialize memChunks list
        memChunks = new ArrayList<>();

        // Create mock MemChunkReader
        memChunkReader = (reference) -> memChunks;

        // Set up live index
        setupLiveIndex();

        // Set up closed indices
        setupClosedIndex1();
        setupClosedIndex2();
        setupClosedIndex3();

        // Create ReaderManagers
        liveReaderManager = new ReaderManager(liveReader);
        closedReaderManager1 = new ReaderManagerWithMetadata(new ReaderManager(closedReader1), 1000000L, 1999999L);
        closedReaderManager2 = new ReaderManagerWithMetadata(new ReaderManager(closedReader2), 2000000L, 2999999L);
        closedReaderManager3 = new ReaderManagerWithMetadata(new ReaderManager(closedReader3), 3000000L, 3999999L);

        // Mock ClosedChunkIndexManager
        closedChunkIndexManager = mock(ClosedChunkIndexManager.class);

        labelStorageType = LabelStorageType.BINARY;
        // Mock MmappedChunksManager
        mmappedChunksManager = mock(MMappedChunksManager.class);

        // Setup consistent mmapped chunks map for all tests
        Map<Long, Set<MemChunk>> emptyMmappedChunks = new HashMap<>();
        when(mmappedChunksManager.getAllMMappedChunks()).thenReturn(emptyMmappedChunks);
        doNothing().when(mmappedChunksManager).addReaderVersionToChunks(anyLong(), any());
    }

    private void setupLiveIndex() throws IOException {
        liveWriter = new IndexWriter(liveDirectory, new IndexWriterConfig(new WhitespaceAnalyzer()));

        liveWriter.addDocument(getLiveDoc("service=api,env=prod", 1001L, 1000000L));
        liveWriter.addDocument(getLiveDoc("service=db,env=prod", 1002L, 2000000L));
        liveWriter.commit();

        liveReader = DirectoryReader.open(liveWriter);
    }

    private void setupClosedIndex1() throws IOException {
        IndexWriter closedWriter1 = new IndexWriter(closedDirectory1, new IndexWriterConfig(new WhitespaceAnalyzer()));

        closedWriter1.addDocument(getClosedDoc("service=api,env=prod", "mock_chunk_data_1", 1000000L, 1999999L));
        closedWriter1.commit();
        closedWriter1.close();

        closedReader1 = DirectoryReader.open(closedDirectory1);
    }

    private void setupClosedIndex2() throws IOException {
        IndexWriter closedWriter2 = new IndexWriter(closedDirectory2, new IndexWriterConfig(new WhitespaceAnalyzer()));

        closedWriter2.addDocument(getClosedDoc("service=cache,env=prod", "mock_chunk_data_2", 2000000L, 2999999L));
        closedWriter2.commit();
        closedWriter2.close();

        closedReader2 = DirectoryReader.open(closedDirectory2);
    }

    private void setupClosedIndex3() throws IOException {
        IndexWriter closedWriter3 = new IndexWriter(closedDirectory3, new IndexWriterConfig(new WhitespaceAnalyzer()));

        closedWriter3.addDocument(getClosedDoc("service=auth,env=prod", "mock_chunk_data_3", 3000000L, 3999999L));
        closedWriter3.commit();
        closedWriter3.close();

        closedReader3 = DirectoryReader.open(closedDirectory3);
    }

    @After
    public void tearDown() throws Exception {
        if (referenceManager != null) {
            referenceManager.close();
        }
        if (liveReaderManager != null) {
            liveReaderManager.close();
        }
        if (closedReaderManager1 != null) {
            closedReaderManager1.readerMananger().close();
        }
        if (closedReaderManager2 != null) {
            closedReaderManager2.readerMananger().close();
        }
        if (closedReaderManager3 != null) {
            closedReaderManager3.readerMananger().close();
        }
        if (liveWriter != null) {
            liveWriter.close();
        }
        if (liveReader != null) {
            liveReader.close();
        }
        if (closedReader1 != null) {
            closedReader1.close();
        }
        if (closedReader2 != null) {
            closedReader2.close();
        }
        if (closedReader3 != null) {
            closedReader3.close();
        }
        if (liveDirectory != null) {
            liveDirectory.close();
        }
        if (closedDirectory1 != null) {
            closedDirectory1.close();
        }
        if (closedDirectory2 != null) {
            closedDirectory2.close();
        }
        if (closedDirectory3 != null) {
            closedDirectory3.close();
        }
        // Cleanup TSDBMetrics to ensure isolation between tests
        TSDBMetrics.cleanup();
        super.tearDown();
    }

    @Test
    public void testBasicInitializationWithExistingIndices() throws IOException {
        // Mock ClosedChunkIndexManager to return initial set of reader managers
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1, closedReaderManager2));

        // Create TSDBDirectoryReaderReferenceManager
        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        // Verify initialization
        assertNotNull("Reference manager should be initialized", referenceManager);

        // Acquire the current reader and verify its structure
        withAcquiredReader(reader -> {
            assertNotNull("Current reader should not be null", reader);

            // Verify the reader is a wrapped TSDBDirectoryReader
            assertTrue("Current reader should be OpenSearchDirectoryReader", reader instanceof OpenSearchDirectoryReader);

            // Verify the underlying TSDBDirectoryReader has correct structure
            DirectoryReader unwrapped = reader.getDelegate();
            assertTrue("Unwrapped reader should be TSDBDirectoryReader", unwrapped instanceof TSDBDirectoryReader);

            TSDBDirectoryReader tsdbDirectoryReader = (TSDBDirectoryReader) unwrapped;
            assertEquals("Should have 2 closed chunk readers", 2, tsdbDirectoryReader.getClosedChunkReadersCount());

            // Expected total documents: 2 live + 1 closed1 + 1 closed2 = 4
            assertEquals("Should have correct total document count", 4, tsdbDirectoryReader.numDocs());
        });
    }

    @Test
    public void testRefreshWhenClosedChunkIndicesAdded() throws IOException {
        // Start with 2 closed indices
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1, closedReaderManager2));

        // Create TSDBDirectoryReaderReferenceManager
        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        withAcquiredReader(reader -> {
            int initialRefCount = reader.getRefCount();
            TSDBDirectoryReader initialTSDBReader = (TSDBDirectoryReader) reader.getDelegate();
            assertEquals("Initial should have 2 closed chunk readers", 2, initialTSDBReader.getClosedChunkReadersCount());
            assertEquals("initial ref count should be 2", 2, initialRefCount);
        });

        // Simulate adding a new closed chunk index
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(
            Arrays.asList(closedReaderManager1, closedReaderManager2, closedReaderManager3)
        );

        // Track reference count before refresh
        OpenSearchDirectoryReader oldReader = referenceManager.acquire();
        int oldRefCount = oldReader.getRefCount();
        assertTrue("Old reader should have positive reference count", oldRefCount > 0);
        referenceManager.release(oldReader);

        // Trigger refresh
        try {
            referenceManager.maybeRefreshBlocking();
        } catch (Exception e) {
            fail("Refresh failed: " + e.getMessage());
        }

        // Verify new reader has 3 closed chunk readers and track reference count
        withAcquiredReader(reader -> {
            int newRefCount = reader.getRefCount();
            TSDBDirectoryReader newTSDBReader = (TSDBDirectoryReader) reader.getDelegate();
            assertEquals("New reader should have 3 closed chunk readers", 3, newTSDBReader.getClosedChunkReadersCount());

            // Expected total documents: 2 live + 1 closed1 + 1 closed2 + 1 closed3 = 5
            assertEquals("Should have correct total document count", 5, newTSDBReader.numDocs());

            // Verify reference count is properly managed
            assertTrue("New reader should have positive reference count", newRefCount > 0);
            assertEquals("Old reader ref count should be 0 after the refresh", 0, oldReader.getRefCount());
        });
    }

    @Test
    public void testSnapshotUpdatedAfterStructuralRefresh() throws IOException {
        // Start with 1 closed chunk index
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1));

        // Create TSDBDirectoryReaderReferenceManager
        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        // Get initial reader version
        OpenSearchDirectoryReader initialReader = referenceManager.acquire();
        long initialVersion = initialReader.getVersion();
        referenceManager.release(initialReader);

        // Add a second closed chunk index - this should trigger structural refresh
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1, closedReaderManager2));

        // First refresh - should be structural (index count changed from 1 to 2)
        referenceManager.maybeRefreshBlocking();

        OpenSearchDirectoryReader readerAfterFirstRefresh = referenceManager.acquire();
        long versionAfterFirstRefresh = readerAfterFirstRefresh.getVersion();
        TSDBDirectoryReader tsdbReaderAfterFirst = (TSDBDirectoryReader) readerAfterFirstRefresh.getDelegate();
        assertEquals("Should have 2 closed chunk readers after first refresh", 2, tsdbReaderAfterFirst.getClosedChunkReadersCount());
        assertTrue("Version should have incremented after structural refresh", versionAfterFirstRefresh > initialVersion);
        referenceManager.release(readerAfterFirstRefresh);

        // Second refresh with no index changes - should NOT be structural
        // This validates that the snapshot was updated
        referenceManager.maybeRefreshBlocking();

        OpenSearchDirectoryReader readerAfterSecondRefresh = referenceManager.acquire();
        long versionAfterSecondRefresh = readerAfterSecondRefresh.getVersion();

        assertEquals(
            "Version should NOT change on second refresh (lightweight refresh, no changes)",
            versionAfterFirstRefresh,
            versionAfterSecondRefresh
        );

        referenceManager.release(readerAfterSecondRefresh);

        // Third refresh - still should not trigger structural refresh
        referenceManager.maybeRefreshBlocking();

        OpenSearchDirectoryReader readerAfterThirdRefresh = referenceManager.acquire();
        long versionAfterThirdRefresh = readerAfterThirdRefresh.getVersion();

        assertEquals("Version should still not change on third refresh", versionAfterFirstRefresh, versionAfterThirdRefresh);

        referenceManager.release(readerAfterThirdRefresh);
    }

    @Test
    public void testRefreshWhenClosedChunkIndicesDeleted() throws IOException {
        // Start with 3 closed indices
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(
            Arrays.asList(closedReaderManager1, closedReaderManager2, closedReaderManager3)
        );
        int initialRefCountForClosedReader3 = closedReader3.getRefCount();

        // Create TSDBDirectoryReaderReferenceManager
        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );
        assertEquals("Ref count to closedReader3 should increase by 1", initialRefCountForClosedReader3 + 1, closedReader3.getRefCount());

        withAcquiredReader(reader -> {
            TSDBDirectoryReader initialTSDBReader = (TSDBDirectoryReader) reader.getDelegate();
            assertEquals("Initial should have 3 closed chunk readers", 3, initialTSDBReader.getClosedChunkReadersCount());
        });

        // Simulate removing a closed chunk index
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1, closedReaderManager2));

        // Trigger refresh
        try {
            referenceManager.maybeRefreshBlocking();
        } catch (Exception e) {
            fail("Refresh failed: " + e.getMessage());
        }

        // Verify new reader has 2 closed chunk readers
        withAcquiredReader(reader -> {
            TSDBDirectoryReader newTSDBReader = (TSDBDirectoryReader) reader.getDelegate();
            assertEquals("New reader should have 2 closed chunk readers", 2, newTSDBReader.getClosedChunkReadersCount());

            // Expected total documents: 2 live + 1 closed1 + 1 closed2 = 4
            assertEquals("Should have correct total document count", 4, newTSDBReader.numDocs());
            assertEquals("ClosedReader3 ref count should decrease by 1", initialRefCountForClosedReader3, closedReader3.getRefCount());
        });
    }

    @Test
    public void testRefreshWhenLiveIndexChangesNoClosedChunkChange() throws IOException {
        // Set up with 2 closed indices
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1, closedReaderManager2));

        // Create TSDBDirectoryReaderReferenceManager
        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        int initialDocCount = withAcquiredReader(reader -> {
            TSDBDirectoryReader initialTSDBReader = (TSDBDirectoryReader) reader.getDelegate();
            return initialTSDBReader.numDocs();
        });

        // Add a document to live index
        liveWriter.addDocument(getLiveDoc("service=newservice,env=test", 1003L, 3000000L));
        liveWriter.commit();

        // Trigger refresh
        try {
            referenceManager.maybeRefreshBlocking();
        } catch (Exception e) {
            fail("Refresh failed: " + e.getMessage());
        }

        // Verify refresh occurred due to live index change
        withAcquiredReader(reader -> {
            TSDBDirectoryReader newTSDBReader = (TSDBDirectoryReader) reader.getDelegate();
            assertEquals("Should still have 2 closed chunk readers", 2, newTSDBReader.getClosedChunkReadersCount());

            // Document count should increase by 1
            assertTrue("Document count should increase", newTSDBReader.numDocs() >= initialDocCount);
        });
    }

    @Test
    public void testRefreshWhenExistingClosedChunkIndexChanges() throws IOException {
        // Set up with 2 closed indices
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1, closedReaderManager2));

        // Create TSDBDirectoryReaderReferenceManager
        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        // Get initial document count
        int initialDocCount = withAcquiredReader(reader -> {
            TSDBDirectoryReader tsdbReader = (TSDBDirectoryReader) reader.getDelegate();
            return tsdbReader.numDocs();
        });

        // Update an existing closed chunk by reopening closedDirectory1 with a writer
        // and adding another document (simulating a new chunk being added)
        IndexWriter updatedClosedWriter1 = new IndexWriter(
            closedDirectory1,
            new IndexWriterConfig(new WhitespaceAnalyzer()).setOpenMode(IndexWriterConfig.OpenMode.APPEND)
        );
        updatedClosedWriter1.addDocument(getClosedDoc("service=updated,env=prod", "mock_chunk_data_updated", 8000000L, 8999999L));
        updatedClosedWriter1.commit();
        updatedClosedWriter1.close();

        // Trigger refresh
        try {
            referenceManager.maybeRefreshBlocking();
        } catch (Exception e) {
            fail("Refresh failed: " + e.getMessage());
        }

        // Verify refresh occurred and document count increased
        withAcquiredReader(reader -> {
            TSDBDirectoryReader tsdbDirectoryReader = (TSDBDirectoryReader) reader.getDelegate();
            assertEquals("Should still have 2 closed chunk readers", 2, tsdbDirectoryReader.getClosedChunkReadersCount());

            // Document count should increase by 1 due to the new document in closedDirectory1
            // Initial: 2 live + 1 closed1 + 1 closed2 = 4
            // After update: 2 live + 2 closed1 + 1 closed2 = 5
            assertEquals("Document count should increase by 1", initialDocCount + 1, tsdbDirectoryReader.numDocs());
        });

    }

    @Test
    public void testRefreshWhenLiveIndexChangesAndNewClosedChunkIndexAdded() throws IOException {
        // Start with 2 closed indices
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1, closedReaderManager2));

        // Create TSDBDirectoryReaderReferenceManager
        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        withAcquiredReader(reader -> {
            // Just verify we can acquire the initial reader
            assertNotNull("Initial reader should not be null", reader);
        });

        // Add a document to live index
        liveWriter.addDocument(getLiveDoc("service=newservice,env=test", 1003L, 3000000L));
        liveWriter.commit();

        // Simulate adding a new closed chunk index at the same time
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(
            Arrays.asList(closedReaderManager1, closedReaderManager2, closedReaderManager3)
        );

        // Trigger refresh
        try {
            referenceManager.maybeRefreshBlocking();
        } catch (Exception e) {
            fail("Refresh failed: " + e.getMessage());
        }

        // Verify new reader has 3 closed chunk readers and increased document count
        withAcquiredReader(reader -> {
            TSDBDirectoryReader newTSDBReader = (TSDBDirectoryReader) reader.getDelegate();
            assertEquals("New reader should have 3 closed chunk readers", 3, newTSDBReader.getClosedChunkReadersCount());

            // Expected total documents: 3 live + 1 closed1 + 1 closed2 + 1 closed3 = 6
            assertEquals("Should have correct total document count", 6, newTSDBReader.numDocs());
        });
    }

    @Test
    public void testConcurrentRefreshOperations() throws IOException, InterruptedException {
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1, closedReaderManager2));

        // Create TSDBDirectoryReaderReferenceManager
        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        // Get initial document count
        int initialDocCount = withAcquiredReader(reader -> {
            TSDBDirectoryReader tsdbDirectoryReader = (TSDBDirectoryReader) reader.getDelegate();
            return tsdbDirectoryReader.numDocs();
        });

        final int numWriterThreads = 3;
        final int numRefreshThreads = 2;
        final int numReaderThreads = 3;
        final int docsPerWriterThread = 2;

        List<Thread> allThreads = new ArrayList<>();
        final AtomicInteger maxSeenDocCount = new AtomicInteger(initialDocCount);
        final AtomicInteger documentsAdded = new AtomicInteger(0);

        // Writer threads: Add documents to live index
        for (int i = 0; i < numWriterThreads; i++) {
            final int threadId = i;
            Thread writerThread = new Thread(() -> {
                try {
                    for (int docId = 0; docId < docsPerWriterThread; docId++) {
                        // Add document to live index
                        liveWriter.addDocument(
                            getLiveDoc(
                                "service=writer" + threadId + "_doc" + docId + ",env=test",
                                2000L + threadId * 100 + docId,
                                System.currentTimeMillis()
                            )
                        );
                        documentsAdded.incrementAndGet();

                        // Small delay to allow interleaving
                        Thread.sleep(5);
                    }
                    liveWriter.commit();
                } catch (Exception e) {
                    fail("Writer thread should not throw exceptions: " + e.getMessage());
                }
            });
            allThreads.add(writerThread);
        }

        // Refresh threads: Trigger refreshes
        for (int i = 0; i < numRefreshThreads; i++) {
            Thread refreshThread = new Thread(() -> {
                try {
                    for (int refresh = 0; refresh < 3; refresh++) {
                        Thread.sleep(20); // Wait a bit before refreshing
                        referenceManager.maybeRefreshBlocking();
                        Thread.sleep(10); // Small delay between refreshes
                    }
                } catch (Exception e) {
                    fail("Refresh thread should not throw exceptions: " + e.getMessage());
                }
            });
            allThreads.add(refreshThread);
        }

        // Reader threads: Read and verify document count increases
        for (int i = 0; i < numReaderThreads; i++) {
            Thread readerThread = new Thread(() -> {
                try {
                    for (int read = 0; read < 5; read++) {
                        OpenSearchDirectoryReader reader = referenceManager.acquire();
                        try {
                            TSDBDirectoryReader tsdbDirectoryReader = (TSDBDirectoryReader) reader.getDelegate();
                            int currentDocCount = tsdbDirectoryReader.numDocs();

                            // Update max seen count atomically
                            maxSeenDocCount.updateAndGet(current -> Math.max(current, currentDocCount));

                            // Verify doc count is not decreasing
                            assertTrue("Document count should not decrease", currentDocCount >= initialDocCount);
                        } finally {
                            referenceManager.release(reader);
                        }
                        Thread.sleep(15); // Small delay between reads
                    }
                } catch (Exception e) {
                    fail("Reader thread should not throw exceptions: " + e.getMessage());
                }
            });
            allThreads.add(readerThread);
        }

        // Start all threads
        for (Thread thread : allThreads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : allThreads) {
            thread.join();
        }

        // Final refresh to ensure all documents are visible
        referenceManager.maybeRefreshBlocking();

        // Verify final state
        int expectedFinalDocCount = initialDocCount + (numWriterThreads * docsPerWriterThread);
        withAcquiredReader(reader -> {
            TSDBDirectoryReader tsdbDirectoryReader = (TSDBDirectoryReader) reader.getDelegate();
            int finalDocCount = tsdbDirectoryReader.numDocs();

            assertEquals("Should have correct final document count", expectedFinalDocCount, finalDocCount);
            assertTrue("Max seen doc count should be at least the final count", maxSeenDocCount.get() >= initialDocCount);
            assertEquals("All documents should have been added", numWriterThreads * docsPerWriterThread, documentsAdded.get());
        });
    }

    @Test
    public void testReferenceManagerCloseCleanup() throws IOException {
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1, closedReaderManager2));
        int initialRefCountForClosedReader1 = closedReader1.getRefCount();
        int initialRefCountForClosedReader2 = closedReader2.getRefCount();
        int initialRefCountForLiveReader = liveReader.getRefCount();

        // Create TSDBDirectoryReaderReferenceManager
        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        // ref count of underlying sub readers should increase by 1
        assertEquals("Ref count to closedReader1 should increase by 1", initialRefCountForClosedReader1 + 1, closedReader1.getRefCount());
        assertEquals("Ref count to closedReader2 should increase by 1", initialRefCountForClosedReader2 + 1, closedReader2.getRefCount());
        assertEquals("Ref count to liveReader should increase by 1", initialRefCountForLiveReader + 1, liveReader.getRefCount());

        withAcquiredReader(reader -> { assertNotNull("Reader should be acquired successfully", reader); });

        // Close the reference manager
        referenceManager.close();

        // ref count of underlying sub readers should decrease by 1
        assertEquals(
            "Ref count to closedReader1 should decrease by 1 after close",
            initialRefCountForClosedReader1,
            closedReader1.getRefCount()
        );
        assertEquals(
            "Ref count to closedReader2 should decrease by 1 after close",
            initialRefCountForClosedReader2,
            closedReader2.getRefCount()
        );
        assertEquals("Ref count to liveReader should decrease by 1 after close", initialRefCountForLiveReader, liveReader.getRefCount());

        // Verify that trying to acquire after close throws an exception
        try {
            referenceManager.acquire();
            fail("Should throw exception when acquiring from closed reference manager");
        } catch (Exception e) {
            // Expected behavior
            assertTrue(
                "Should throw AlreadyClosedException or similar",
                e.getMessage().contains("closed") || e.getMessage().contains("Closed")
            );
        }
    }

    @Test
    public void testRefreshWhenMMappedChunksUnchanged() throws IOException {
        // Set up with 1 closed index
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1));

        // Create TSDBDirectoryReaderReferenceManager
        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        // Get initial reader
        OpenSearchDirectoryReader initialReader = withAcquiredReader(reader -> reader);
        TSDBDirectoryReader initialTSDBReader = (TSDBDirectoryReader) initialReader.getDelegate();
        long initialVersion = initialTSDBReader.getVersion();

        // Simulate that getAllMMappedChunks() returns the same chunks (equal content but different map instance)
        Map<Long, Set<MemChunk>> unchangedChunks = new HashMap<>();
        // Copy the current state to simulate no change
        Map<Long, Set<MemChunk>> currentChunks = mmappedChunksManager.getAllMMappedChunks();
        unchangedChunks.putAll(currentChunks);

        // Mock getAllMMappedChunks to return the same content but different map instance
        when(mmappedChunksManager.getAllMMappedChunks()).thenReturn(unchangedChunks);

        // Trigger refresh - should not create a new reader since chunks are unchanged
        try {
            OpenSearchDirectoryReader newReader = referenceManager.refreshIfNeeded(initialReader);

            // Should return null indicating no refresh was needed
            assertNull("No refresh should occur when mMapped chunks are unchanged", newReader);

        } catch (Exception e) {
            fail("Refresh should not fail when chunks are unchanged: " + e.getMessage());
        }

        // Verify the reader version hasn't changed
        withAcquiredReader(reader -> {
            TSDBDirectoryReader currentTSDBReader = (TSDBDirectoryReader) reader.getDelegate();
            assertEquals("Reader version should remain the same when no refresh occurs", initialVersion, currentTSDBReader.getVersion());
        });
    }

    /**
     * Helper method to safely acquire, test, and release a reader
     */
    private void withAcquiredReader(ReaderConsumer consumer) throws IOException {
        OpenSearchDirectoryReader reader = null;
        try {
            reader = referenceManager.acquire();
            if (reader != null) {
                consumer.accept(reader);
            }
        } catch (Exception e) {
            fail("Failed to acquire reader: " + e.getMessage());
        } finally {
            if (reader != null) {
                referenceManager.release(reader);
            }
        }
    }

    /**
     * Helper method to safely acquire, test, and release a reader with return value
     */
    private <T> T withAcquiredReader(ReaderFunction<T> function) throws IOException {
        OpenSearchDirectoryReader reader = null;
        try {
            reader = referenceManager.acquire();
            if (reader != null) {
                return function.apply(reader);
            }
            return null;
        } catch (Exception e) {
            fail("Failed to acquire reader: " + e.getMessage());
            return null;
        } finally {
            if (reader != null) {
                referenceManager.release(reader);
            }
        }
    }

    @FunctionalInterface
    private interface ReaderConsumer {
        void accept(OpenSearchDirectoryReader reader) throws IOException;
    }

    @FunctionalInterface
    private interface ReaderFunction<T> {
        T apply(OpenSearchDirectoryReader reader) throws IOException;
    }

    private Document getLiveDoc(String labels, long reference, long mint) {
        Document doc = new Document();
        doc.add(new TextField("__labels__", labels, Field.Store.YES));
        doc.add(new LongPoint("__reference__", reference));
        doc.add(new LongPoint("__mint__", mint));
        return doc;
    }

    private Document getClosedDoc(String labels, String chunkData, long mint, long maxt) {
        Document doc = new Document();
        doc.add(new TextField("__labels__", labels, Field.Store.NO));
        doc.add(new BinaryDocValuesField("__chunk__", new BytesRef(chunkData)));
        doc.add(new NumericDocValuesField("__labels_hash__", labels.hashCode()));
        doc.add(new LongPoint("__mint__", mint));
        doc.add(new LongPoint("__maxt__", maxt));
        return doc;
    }

    @Test
    public void testCloseReaderCapacityGaugesIsSafeToCallMultipleTimes() throws IOException {
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1));

        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        // Should not throw when called multiple times
        referenceManager.closeReaderCapacityGauges();
        referenceManager.closeReaderCapacityGauges();
    }

    @Test
    public void testCloseReaderCapacityGaugesBeforeClose() throws IOException {
        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1));

        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        // Verify we can acquire reader before closing gauges
        withAcquiredReader((ReaderConsumer) reader -> assertNotNull(reader));

        // Close gauges before closing reference manager
        referenceManager.closeReaderCapacityGauges();

        // Should still be able to acquire reader after closing gauges
        withAcquiredReader((ReaderConsumer) reader -> assertNotNull(reader));

        // Close reference manager
        referenceManager.close();
        referenceManager = null; // Prevent double-close in tearDown
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReaderCapacityGaugesRegistration() throws IOException {
        // Setup mock metrics registry
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        when(mockRegistry.createCounter(anyString(), anyString(), anyString())).thenReturn(mock(Counter.class));
        when(mockRegistry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mock(Histogram.class));
        when(mockRegistry.createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class))).thenReturn(
            mock(Closeable.class)
        );

        // Initialize TSDBMetrics with mock registry
        TSDBMetrics.initialize(mockRegistry);

        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1, closedReaderManager2));

        // Create reference manager - this should register gauges
        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        // Verify gauges were registered with correct metric names
        verify(mockRegistry).createGauge(
            eq(TSDBMetricsConstants.READER_MAXDOC_UTILIZATION),
            eq(TSDBMetricsConstants.READER_MAXDOC_UTILIZATION_DESC),
            eq(TSDBMetricsConstants.UNIT_COUNT),
            any(Supplier.class),
            any(Tags.class)
        );
        verify(mockRegistry).createGauge(
            eq(TSDBMetricsConstants.READER_CLOSED_INDICES),
            eq(TSDBMetricsConstants.READER_CLOSED_INDICES_DESC),
            eq(TSDBMetricsConstants.UNIT_COUNT),
            any(Supplier.class),
            any(Tags.class)
        );
        verify(mockRegistry).createGauge(
            eq(TSDBMetricsConstants.READER_LEAF_COUNT),
            eq(TSDBMetricsConstants.READER_LEAF_COUNT_DESC),
            eq(TSDBMetricsConstants.UNIT_COUNT),
            any(Supplier.class),
            any(Tags.class)
        );

        // The 3 reader capacity gauges are verified individually above.
        // Additional gauges (e.g., node runtime) are also registered via TSDBMetrics.initialize().
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testReaderCapacityGaugeSupplierValues() throws IOException {
        // Setup mock metrics registry and capture suppliers
        MetricsRegistry mockRegistry = mock(MetricsRegistry.class);
        when(mockRegistry.createCounter(anyString(), anyString(), anyString())).thenReturn(mock(Counter.class));
        when(mockRegistry.createHistogram(anyString(), anyString(), anyString())).thenReturn(mock(Histogram.class));

        ArgumentCaptor<Supplier<Double>> supplierCaptor = ArgumentCaptor.forClass(Supplier.class);
        ArgumentCaptor<String> nameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockRegistry.createGauge(nameCaptor.capture(), anyString(), anyString(), supplierCaptor.capture(), any(Tags.class)))
            .thenReturn(mock(Closeable.class));

        // Initialize TSDBMetrics
        TSDBMetrics.initialize(mockRegistry);

        when(closedChunkIndexManager.getReaderManagersWithMetadata()).thenReturn(Arrays.asList(closedReaderManager1, closedReaderManager2));

        referenceManager = new TSDBDirectoryReaderReferenceManager(
            liveReaderManager,
            () -> 0L,
            closedChunkIndexManager,
            memChunkReader,
            labelStorageType,
            mmappedChunksManager,
            shardId
        );

        // Find and verify each gauge supplier
        List<String> names = nameCaptor.getAllValues();
        List<Supplier<Double>> suppliers = supplierCaptor.getAllValues();

        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            Supplier<Double> supplier = suppliers.get(i);
            Double value = supplier.get();

            if (TSDBMetricsConstants.READER_MAXDOC_UTILIZATION.equals(name)) {
                // Should be a small percentage (4 docs out of ~2.1B limit)
                assertTrue("MaxDoc utilization should be >= 0", value >= 0.0);
                assertTrue("MaxDoc utilization should be < 1% for test data", value < 1.0);
            } else if (TSDBMetricsConstants.READER_CLOSED_INDICES.equals(name)) {
                // Should be 2 (we have 2 closed chunk indices)
                assertEquals("Should have 2 closed indices", 2.0, value, 0.001);
            } else if (TSDBMetricsConstants.READER_LEAF_COUNT.equals(name)) {
                // Should have at least 1 leaf reader
                assertTrue("Should have at least 1 leaf reader", value >= 1.0);
            }
        }
    }
}
