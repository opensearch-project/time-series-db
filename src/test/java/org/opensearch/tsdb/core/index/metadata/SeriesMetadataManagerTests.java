/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.metadata;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SeriesMetadataManagerTests extends OpenSearchTestCase {

    public void testCommitWithMetadata() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            SnapshotDeletionPolicy snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                SeriesMetadataManager manager = new SeriesMetadataManager(dir, () -> writer, snapshotDeletionPolicy);

                // Commit with metadata
                Map<Long, Long> metadata = new HashMap<>();
                metadata.put(1L, 100L);
                metadata.put(2L, 200L);
                metadata.put(3L, 300L);

                manager.commitWithMetadata(metadata);

                // Verify metadata file was created
                List<String> metadataFiles = SeriesMetadataIO.listMetadataFiles(dir);
                assertEquals(1, metadataFiles.size());
                assertTrue(metadataFiles.get(0).startsWith("series_metadata_"));
            }
        }
    }

    public void testApplyMetadata() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            SnapshotDeletionPolicy snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                SeriesMetadataManager manager = new SeriesMetadataManager(dir, () -> writer, snapshotDeletionPolicy);

                // Write metadata
                Map<Long, Long> originalMetadata = new HashMap<>();
                originalMetadata.put(10L, 1000L);
                originalMetadata.put(20L, 2000L);

                manager.commitWithMetadata(originalMetadata);

                // Read back using applyMetadata
                Map<Long, Long> readMetadata = new HashMap<>();
                manager.applyMetadata(readMetadata::put);

                assertEquals(originalMetadata, readMetadata);
            }
        }
    }

    public void testSnapshot() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            SnapshotDeletionPolicy snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                SeriesMetadataManager manager = new SeriesMetadataManager(dir, () -> writer, snapshotDeletionPolicy);

                // Commit with metadata
                Map<Long, Long> metadata = Map.of(1L, 100L);
                manager.commitWithMetadata(metadata);

                // Take snapshot
                IndexCommit snapshot = manager.snapshot();

                // Verify snapshot is wrapped
                assertTrue(snapshot instanceof MetadataAwareIndexCommit);

                MetadataAwareIndexCommit wrappedSnapshot = (MetadataAwareIndexCommit) snapshot;
                assertNotNull(wrappedSnapshot.getMetadataFilename());
                assertTrue(wrappedSnapshot.getMetadataFilename().startsWith("series_metadata_"));

                // Verify metadata file is in getFileNames
                Collection<String> files = snapshot.getFileNames();
                boolean hasMetadataFile = false;
                for (String file : files) {
                    if (file.startsWith("series_metadata_")) {
                        hasMetadataFile = true;
                        break;
                    }
                }
                assertTrue("Metadata file should be in getFileNames", hasMetadataFile);

                // Release snapshot
                manager.release(snapshot);
            }
        }
    }

    public void testSnapshotProtectsMetadataFiles() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            SnapshotDeletionPolicy snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                SeriesMetadataManager manager = new SeriesMetadataManager(dir, () -> writer, snapshotDeletionPolicy);

                // Commit 1
                manager.commitWithMetadata(Map.of(1L, 100L));
                List<String> filesAfterCommit1 = SeriesMetadataIO.listMetadataFiles(dir);
                assertEquals(1, filesAfterCommit1.size());
                String file1 = filesAfterCommit1.get(0);

                // Take snapshot of commit 1
                IndexCommit snapshot1 = manager.snapshot();

                // Commit 2 - should cleanup old files BUT snapshot1's file should be protected
                manager.commitWithMetadata(Map.of(2L, 200L));
                List<String> filesAfterCommit2 = SeriesMetadataIO.listMetadataFiles(dir);
                assertEquals(2, filesAfterCommit2.size());
                assertTrue("File from commit 1 should still exist", filesAfterCommit2.contains(file1));

                // Commit 3 - file from commit 1 should still be protected
                manager.commitWithMetadata(Map.of(3L, 300L));
                List<String> filesAfterCommit3 = SeriesMetadataIO.listMetadataFiles(dir);
                assertEquals(2, filesAfterCommit3.size()); // snapshot1 file + current file
                assertTrue("File from commit 1 should still be protected", filesAfterCommit3.contains(file1));

                // Release snapshot - now file from commit 1 can be cleaned up
                manager.release(snapshot1);

                // Commit 4 - should cleanup old files including commit 1
                manager.commitWithMetadata(Map.of(4L, 400L));
                List<String> filesAfterCommit4 = SeriesMetadataIO.listMetadataFiles(dir);
                assertEquals(1, filesAfterCommit4.size()); // Only current file
                assertFalse("File from commit 1 should be cleaned up", filesAfterCommit4.contains(file1));
            }
        }
    }

    public void testMultipleSnapshots() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            SnapshotDeletionPolicy snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                SeriesMetadataManager manager = new SeriesMetadataManager(dir, () -> writer, snapshotDeletionPolicy);

                // Commit 1 and snapshot
                manager.commitWithMetadata(Map.of(1L, 100L));
                IndexCommit snapshot1 = manager.snapshot();
                String file1 = ((MetadataAwareIndexCommit) snapshot1).getMetadataFilename();

                // Commit 2 and snapshot
                manager.commitWithMetadata(Map.of(2L, 200L));
                IndexCommit snapshot2 = manager.snapshot();
                String file2 = ((MetadataAwareIndexCommit) snapshot2).getMetadataFilename();

                // Commit 3
                manager.commitWithMetadata(Map.of(3L, 300L));

                // Both snapshot files should be protected
                List<String> files = SeriesMetadataIO.listMetadataFiles(dir);
                assertTrue("File 1 should be protected", files.contains(file1));
                assertTrue("File 2 should be protected", files.contains(file2));

                // Release snapshot 1
                manager.release(snapshot1);

                // Commit 4 - file1 can be cleaned up now
                manager.commitWithMetadata(Map.of(4L, 400L));
                files = SeriesMetadataIO.listMetadataFiles(dir);
                assertFalse("File 1 should be cleaned up", files.contains(file1));
                assertTrue("File 2 should still be protected", files.contains(file2));

                // Release snapshot 2
                manager.release(snapshot2);
            }
        }
    }

    public void testEmptyMetadata() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            SnapshotDeletionPolicy snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                SeriesMetadataManager manager = new SeriesMetadataManager(dir, () -> writer, snapshotDeletionPolicy);

                // Commit with empty metadata
                manager.commitWithMetadata(new HashMap<>());

                // Read back
                Map<Long, Long> readMetadata = new HashMap<>();
                manager.applyMetadata(readMetadata::put);

                assertTrue("Metadata should be empty", readMetadata.isEmpty());
            }
        }
    }

    public void testLargeMetadata() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            SnapshotDeletionPolicy snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                SeriesMetadataManager manager = new SeriesMetadataManager(dir, () -> writer, snapshotDeletionPolicy);

                // Write large metadata (10K series)
                Map<Long, Long> metadata = new HashMap<>();
                for (long i = 0; i < 10000; i++) {
                    metadata.put(i, i * 1000);
                }

                manager.commitWithMetadata(metadata);

                // Read back and verify
                Map<Long, Long> readMetadata = new HashMap<>();
                manager.applyMetadata(readMetadata::put);

                assertEquals(10000, readMetadata.size());
                assertEquals(Long.valueOf(0), readMetadata.get(0L));
                assertEquals(Long.valueOf(5000 * 1000), readMetadata.get(5000L));
                assertEquals(Long.valueOf(9999 * 1000), readMetadata.get(9999L));
            }
        }
    }

    public void testApplyMetadataOnEmptyIndex() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            SnapshotDeletionPolicy snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                SeriesMetadataManager manager = new SeriesMetadataManager(dir, () -> writer, snapshotDeletionPolicy);

                // Try to read metadata from empty index (no commits yet)
                Map<Long, Long> readMetadata = new HashMap<>();
                manager.applyMetadata(readMetadata::put);

                // Should not fail, just return empty
                assertTrue("Should handle empty index gracefully", readMetadata.isEmpty());
            }
        }
    }

    /**
     * Test that cleanupOldMetadataFiles protects the file referenced in commit data,
     * even if there's a generation mismatch between segments_N and the metadata file.
     */
    public void testCleanupProtectsCommitDataFileWithGenerationMismatch() throws IOException {
        Path tempDir = createTempDir();
        try (Directory dir = new MMapDirectory(tempDir)) {
            IndexWriterConfig iwc = new IndexWriterConfig(new WhitespaceAnalyzer());
            IndexDeletionPolicy baseDeletionPolicy = new KeepOnlyLastCommitDeletionPolicy();
            SnapshotDeletionPolicy snapshotDeletionPolicy = new SnapshotDeletionPolicy(baseDeletionPolicy);
            iwc.setIndexDeletionPolicy(snapshotDeletionPolicy);

            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                SeriesMetadataManager manager = new SeriesMetadataManager(dir, () -> writer, snapshotDeletionPolicy);

                // Commit 1 - creates series_metadata_1
                manager.commitWithMetadata(Map.of(1L, 100L));

                // Manually create a metadata file with generation 6 (simulating the mismatch)
                String mismatchedFile = SeriesMetadataIO.writeMetadata(dir, 6L, Map.of(99L, 9900L));

                // Create orphan files that should be cleaned up
                String orphan1 = SeriesMetadataIO.writeMetadata(dir, 10L, Map.of(88L, 8800L));
                String orphan2 = SeriesMetadataIO.writeMetadata(dir, 11L, Map.of(77L, 7700L));

                // Update commit data to point to the mismatched file
                // This simulates a commit where segments_N points to series_metadata_6 (mismatched generation)
                Map<String, String> commitData = new HashMap<>();
                commitData.put("live_series_metadata_file", mismatchedFile);
                writer.setLiveCommitData(commitData.entrySet(), false);
                writer.commit();

                // Verify all files exist before cleanup
                List<String> allFiles = SeriesMetadataIO.listMetadataFiles(dir);
                assertTrue("Mismatched file should exist", allFiles.contains(mismatchedFile));
                assertTrue("Orphan1 should exist", allFiles.contains(orphan1));
                assertTrue("Orphan2 should exist", allFiles.contains(orphan2));

                manager.cleanupOldMetadataFiles();

                // Verify cleanup behavior
                List<String> filesAfterCleanup = SeriesMetadataIO.listMetadataFiles(dir);
                assertTrue("Metadata file from commit should be protected", filesAfterCleanup.contains(mismatchedFile));
                assertFalse("Orphan1 should be cleaned up", filesAfterCleanup.contains(orphan1));
                assertFalse("Orphan2 should be cleaned up", filesAfterCleanup.contains(orphan2));
            }
        }
    }
}
