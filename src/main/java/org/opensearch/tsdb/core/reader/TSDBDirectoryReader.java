/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.reader;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.chunk.MMappedChunksManager;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexLeafReader;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndexLeafReader;
import org.opensearch.tsdb.core.index.live.MemChunkReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A custom DirectoryReader that directly extends {@link DirectoryReader} and provides
 * a unified view of both LiveSeriesIndex and multiple ClosedChunkIndex instances for tsdb query coordination.
 * Life cycle :
 * TSDBDirectoryReader is managed by {@link TSDBDirectoryReaderReferenceManager} created in TSDBEngine
 * TSDBDirectoryReader is instantiated when {@link TSDBDirectoryReaderReferenceManager} is created during TSDBEngine start up
 * TSDBDirectoryReader is refreshed when {@link TSDBDirectoryReaderReferenceManager#refreshIfNeeded(OpenSearchDirectoryReader)} is called
 * TSDBDirectoryReader is closed when
 * 1) During TSDBEngine refresh,  if new TSDBDirectoryReader is created, the old TSDBDirectoryReader will be decRef during {@link TSDBDirectoryReaderReferenceManager#decRef(OpenSearchDirectoryReader)} ()} and closed if no longer referenced
 * 2) During TSDBEngine shutdown, ReferenceManager is closed which closes the current TSDBDirectoryReader
 */
@SuppressForbidden(reason = "Reference managing is needed here")
public class TSDBDirectoryReader extends DirectoryReader {
    private final DirectoryReader liveSeriesIndexDirectoryReader;
    private final LongSupplier minLiveTimestampSupplier;
    private final List<DirectoryReaderWithMetadata> closedChunkIndexReadersWithMetadata;
    private final MemChunkReader memChunkReader;
    private final LabelStorageType labelStorageType;
    private final Map<Long, Set<MemChunk>> mMappedChunks;
    private final MMappedChunksManager mMappedChunksManager;
    private final long version;

    /**
     * Constructs a TSDBDirectoryReader that combines a live series index reader with metadata for pruning
     *
     * @param liveReader                          the DirectoryReaderWithMetadata for the live series index
     * @param minLiveTimestampSupplier            supplies the current minimum possible timestamp of an in-memory sample
     * @param closedChunkIndexReadersWithMetadata list of DirectoryReaderWithMetadata for closed chunk indices
     * @param memChunkReader                      reader to read MemChunk from reference, this is needed for LiveSeriesIndexLeafReader
     * @param labelStorageType                    the storage type configured for labels
     * @param version                             the version of this reader instance. Version will be increased by 1 for each refresh
     * @throws IOException if an I/O error occurs during construction
     */
    public TSDBDirectoryReader(
        DirectoryReader liveReader,
        LongSupplier minLiveTimestampSupplier,
        List<DirectoryReaderWithMetadata> closedChunkIndexReadersWithMetadata,
        MemChunkReader memChunkReader,
        Map<Long, Set<MemChunk>> mMappedChunks,
        MMappedChunksManager mMappedChunksManager,
        LabelStorageType labelStorageType,
        long version
    ) throws IOException {
        // We need to pass in closedChunkIndexReaders explicitly even though there is closedChunkIndexManager already.
        // This is because super constructor needs to be called first. And DirectoryReader constructor requires us to provide
        // List<LeafReader>
        // TODO : Should the leaves be sorted in any order?
        super(
            new CompositeDirectory(
                Stream.concat(
                    Stream.of(liveReader.directory()),
                    closedChunkIndexReadersWithMetadata.stream().map(rwm -> rwm.reader().directory())
                ).collect(Collectors.toList())
            ),
            buildCombinedLeaves(
                liveReader,
                minLiveTimestampSupplier.getAsLong(),
                closedChunkIndexReadersWithMetadata,
                memChunkReader,
                mMappedChunks,
                labelStorageType
            ),
            null
        );

        this.memChunkReader = memChunkReader;
        this.labelStorageType = labelStorageType;
        // Store the readers and metadata
        this.liveSeriesIndexDirectoryReader = liveReader;
        this.minLiveTimestampSupplier = minLiveTimestampSupplier;
        this.closedChunkIndexReadersWithMetadata = new ArrayList<>(closedChunkIndexReadersWithMetadata);

        // increment reference count of all sub readers by one
        this.liveSeriesIndexDirectoryReader.incRef();
        for (DirectoryReaderWithMetadata readerWithMetadata : this.closedChunkIndexReadersWithMetadata) {
            readerWithMetadata.reader().incRef();
        }
        this.mMappedChunks = mMappedChunks;
        this.mMappedChunksManager = mMappedChunksManager;
        this.version = version;
    }

    /**
     * Constructs a TSDBDirectoryReader that combines a live series index reader and closed chunk index readers with default  label storage type
     *
     * @param liveReader               the DirectoryReaderWithMetadata for the live series index
     * @param minLiveTimestampSupplier supplies the current minimum possible timestamp of an in-memory sample
     * @param closedChunkIndexReaders  list of DirectoryReaderWithMetadata for closed chunk indices
     * @param memChunkReader           reader to read MemChunk from reference, this is needed for LiveSeriesIndexLeafReader
     * @throws IOException if an I/O error occurs during construction
     */
    public TSDBDirectoryReader(
        DirectoryReader liveReader,
        LongSupplier minLiveTimestampSupplier,
        List<DirectoryReaderWithMetadata> closedChunkIndexReaders,
        MemChunkReader memChunkReader,
        Map<Long, Set<MemChunk>> mMappedChunks,
        MMappedChunksManager mMappedChunksManager,
        long version
    ) throws IOException {
        this(
            liveReader,
            minLiveTimestampSupplier,
            closedChunkIndexReaders,
            memChunkReader,
            mMappedChunks,
            mMappedChunksManager,
            LabelStorageType.BINARY,
            version
        );
    }

    /**
     * Constructs a TSDBDirectoryReader that combines a live series index reader and closed chunk index readers with default  version
     * @param liveReader               the DirectoryReaderWithMetadata for the live series index
     * @param minLiveTimestampSupplier supplies the current minimum possible timestamp of an in-memory sample
     * @param closedChunkIndexReaders  list of DirectoryReaderWithMetadata for closed chunk indices
     * @param memChunkReader           reader to read MemChunk from reference, this is needed for LiveSeriesIndexLeafReader
     * @throws IOException if an I/O error occurs during construction
     */
    public TSDBDirectoryReader(
        DirectoryReader liveReader,
        LongSupplier minLiveTimestampSupplier,
        List<DirectoryReaderWithMetadata> closedChunkIndexReaders,
        MemChunkReader memChunkReader,
        Map<Long, Set<MemChunk>> mMappedChunks,
        MMappedChunksManager mMappedChunksManager,
        LabelStorageType labelStorageType
    ) throws IOException {
        this(
            liveReader,
            minLiveTimestampSupplier,
            closedChunkIndexReaders,
            memChunkReader,
            mMappedChunks,
            mMappedChunksManager,
            labelStorageType,
            0L
        );
    }

    /**
     * Builds a combined array of LeafReaders from the live series index and closed chunk indices.
     */
    private static LeafReader[] buildCombinedLeaves(
        DirectoryReader liveDirectoryReader,
        long minLiveTimestamp,
        List<DirectoryReaderWithMetadata> closedChunkIndexReadersWithMetadata,
        MemChunkReader memChunkReader,
        Map<Long, Set<MemChunk>> mMappedChunks,
        LabelStorageType labelStorageType
    ) throws IOException {
        List<LeafReader> combined = new ArrayList<>();

        // Add live series leaf readers
        for (LeafReaderContext ctx : liveDirectoryReader.leaves()) {
            // TODO : pass in already mmaped chunks
            combined.add(new LiveSeriesIndexLeafReader(ctx.reader(), memChunkReader, labelStorageType, minLiveTimestamp, mMappedChunks));
        }

        // Add closed chunk leaf readers with metadata
        for (DirectoryReaderWithMetadata readerWithMetadata : closedChunkIndexReadersWithMetadata) {
            DirectoryReader reader = readerWithMetadata.reader();
            long minTimestamp = readerWithMetadata.minTimestamp();
            long maxTimestamp = readerWithMetadata.maxTimestamp();

            for (LeafReaderContext ctx : reader.leaves()) {
                combined.add(new ClosedChunkIndexLeafReader(ctx.reader(), labelStorageType, minTimestamp, maxTimestamp));
            }
        }

        return combined.toArray(new LeafReader[0]);
    }

    /**
     * Releases references to newly created readers after they've been passed to TSDBDirectoryReader constructor.
     *
     * <p>This method ensures proper reference counting by decRef'ing only NEW readers (not reused ones).
     * Since the TSDBDirectoryReader constructor incRef's all readers, this cleanup ensures that only
     * the TSDBDirectoryReader owns the new readers, preventing reference leaks.
     *
     * @param newLiveSeriesReader the new live series reader, or null if unchanged
     * @param newClosedChunkReaders list of closed chunk readers (mix of new and reused)
     * @param suppressExceptions if true, catches IOExceptions and adds them to parentException; if false, throws them
     * @param parentException optional parent exception to attach suppressed exceptions to (when suppressExceptions=true)
     * @throws IOException if suppressExceptions=false and decRef fails
     */
    private void cleanupNewReaders(
        DirectoryReader newLiveSeriesReader,
        List<DirectoryReader> newClosedChunkReaders,
        boolean suppressExceptions,
        Exception parentException
    ) throws IOException {
        if (newLiveSeriesReader != null) {
            if (suppressExceptions) {
                try {
                    newLiveSeriesReader.decRef();
                } catch (IOException decRefException) {
                    if (parentException != null) {
                        parentException.addSuppressed(decRefException);
                    }
                }
            } else {
                newLiveSeriesReader.decRef();
            }
        }

        for (int i = 0; i < newClosedChunkReaders.size(); i++) {
            DirectoryReader newReader = newClosedChunkReaders.get(i);
            DirectoryReader oldReader = this.closedChunkIndexReadersWithMetadata.get(i).reader();

            if (newReader != null && newReader != oldReader) {
                if (suppressExceptions) {
                    try {
                        newReader.decRef();
                    } catch (IOException decRefException) {
                        if (parentException != null) {
                            parentException.addSuppressed(decRefException);
                        }
                    }
                } else {
                    newReader.decRef();
                }
            }
        }
    }

    /**
     * Attempts to refresh the reader if any of the underlying readers have changed.
     * Only handles updates on existing DirectoryReaders.
     *
     * If new closed chunk indices are added or existing ones are removed,
     * it should be handled in TSDBDirectoryReaderReferenceManager.
     *
     * @return a new DirectoryReader if changes were detected, null otherwise
     * @throws IOException if an I/O error occurs during refresh
     */
    @Override
    protected DirectoryReader doOpenIfChanged() throws IOException {
        boolean anyReaderChanged = false;
        DirectoryReader newLiveSeriesReader = null;
        List<DirectoryReader> newClosedChunkReaders = new ArrayList<>();
        List<DirectoryReaderWithMetadata> newClosedChunkReadersWithMetadata = new ArrayList<>();

        try {
            // Check for changes in live series reader
            newLiveSeriesReader = DirectoryReader.openIfChanged(this.liveSeriesIndexDirectoryReader);
            if (newLiveSeriesReader != null) {
                anyReaderChanged = true;
            }

            // Check for changes in closed chunk readers, preserving metadata
            for (int i = 0; i < this.closedChunkIndexReadersWithMetadata.size(); i++) {
                DirectoryReaderWithMetadata oldReaderWithMetadata = this.closedChunkIndexReadersWithMetadata.get(i);
                DirectoryReader oldReader = oldReaderWithMetadata.reader();
                DirectoryReader newReader = DirectoryReader.openIfChanged(oldReader);

                if (newReader != null) {
                    // Reader changed - create new DirectoryReaderWithMetadata with same metadata
                    newClosedChunkReadersWithMetadata.add(
                        new DirectoryReaderWithMetadata(
                            newReader,
                            oldReaderWithMetadata.minTimestamp(),
                            oldReaderWithMetadata.maxTimestamp()
                        )
                    );
                    newClosedChunkReaders.add(newReader);
                    anyReaderChanged = true;
                } else {
                    // Reader unchanged - reuse existing DirectoryReaderWithMetadata
                    newClosedChunkReadersWithMetadata.add(oldReaderWithMetadata);
                    newClosedChunkReaders.add(oldReader);
                }
            }

            if (!anyReaderChanged) {
                return null;
            }

            TSDBDirectoryReader newTSDBReader = new TSDBDirectoryReader(
                newLiveSeriesReader != null ? newLiveSeriesReader : this.liveSeriesIndexDirectoryReader,
                minLiveTimestampSupplier,
                newClosedChunkReadersWithMetadata,
                memChunkReader,
                mMappedChunks,
                mMappedChunksManager,
                this.labelStorageType,
                version + 1
            );

            // Release local references - constructor has already incRef'd the readers
            cleanupNewReaders(newLiveSeriesReader, newClosedChunkReaders, false, null);

            return newTSDBReader;

        } catch (Exception e) {
            // Clean up new readers on failure
            cleanupNewReaders(newLiveSeriesReader, newClosedChunkReaders, true, e);
            throw e;
        }
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexCommit indexCommit) throws IOException {
        throw new UnsupportedOperationException("TSDBDirectoryReader does not support opening with IndexCommit");
    }

    @Override
    protected DirectoryReader doOpenIfChanged(IndexWriter indexWriter, boolean b) throws IOException {
        throw new UnsupportedOperationException("TSDBDirectoryReader does not support opening with IndexWriter");
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public boolean isCurrent() throws IOException {
        boolean isCurrent = this.liveSeriesIndexDirectoryReader.isCurrent();
        for (DirectoryReaderWithMetadata readerAndMetadata : this.closedChunkIndexReadersWithMetadata) {
            isCurrent = isCurrent && readerAndMetadata.reader().isCurrent();
        }
        return isCurrent;
    }

    @Override
    public IndexCommit getIndexCommit() throws IOException {
        throw new UnsupportedOperationException("Multi-index reader doesn't support IndexCommit");
    }

    @Override
    protected synchronized void doClose() throws IOException {
        // remove current reader version from mmaped chunk manager
        // if no reference to the mmapped chunks anymore , they will be dropped from live index
        mMappedChunksManager.removeMMappedChunksForReaderVersion(this.version, mMappedChunks);
        IOException firstException = null;
        try {
            this.liveSeriesIndexDirectoryReader.decRef();
        } catch (IOException e) {
            firstException = e;
        }
        for (final DirectoryReaderWithMetadata r : this.closedChunkIndexReadersWithMetadata) {
            try {
                r.reader().decRef();
            } catch (IOException e) {
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    /*
    * Return null to disable caching for multi-directory reader.
    * */
    @Override
    public CacheHelper getReaderCacheHelper() {
        // Multi-directory readers should return null to disable caching.
        // This is consistent with other OpenSearch multi-directory implementations
        // and ensures that IndicesService.canCache() returns false.
        return null;
    }

    /**
     * Returns the number of closed chunk index readers.
     * @return the number of closed chunk index readers
     */
    public int getClosedChunkReadersCount() {
        return this.closedChunkIndexReadersWithMetadata.size();
    }
}
