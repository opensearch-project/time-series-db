/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.retention;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.utils.Constants;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TimeBasedRetentionTests extends OpenSearchTestCase {

    private ClosedChunkIndexManager closedChunkIndexManager;
    private Path metricsDirectory;
    private TimeBasedRetention retention;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        closedChunkIndexManager = mock(ClosedChunkIndexManager.class);
        metricsDirectory = createTempDir();
        retention = new TimeBasedRetention(Constants.Time.DEFAULT_BLOCK_DURATION, 0);
    }

    public void testDropWithEmptyResults() {
        when(closedChunkIndexManager.getClosedChunkIndexes(any(), any())).thenReturn(List.of());
        var result = retention.drop(closedChunkIndexManager, Instant.ofEpochMilli(0), Instant.now());
        assertTrue(result.success().isEmpty());
    }

    public void testConstructorWithInvalidDuration() {
        try {
            retention = new TimeBasedRetention(0, 0);
        } catch (IllegalArgumentException e) {
            return;
        }

        fail("IllegalArgumentException should have been thrown");
    }

    public void testRunInterval() {
        var ccm = mock(ClosedChunkIndexManager.class);
        var retention = new TimeBasedRetention(Constants.Time.DEFAULT_BLOCK_DURATION, Duration.ofMinutes(10).toMillis());

        // First call should set the last run time.
        var result = retention.run(ccm);
        assertTrue(result.success().isEmpty());
        assertTrue(result.failure().isEmpty());

        // Second call should be short-circuited.
        result = retention.run(ccm);
        assertTrue(result.success().isEmpty());
        assertTrue(result.failure().isEmpty());

        verify(ccm, times(2)).getClosedChunkIndexes(any(), any());
    }

    public void testDropWithSingleIndexSuccess() throws Exception {
        // Create two indexes: one old (to be removed) and one recent (to serve as reference for TTL)
        Path oldIndexPath = metricsDirectory.resolve("block_old");
        Path recentIndexPath = metricsDirectory.resolve("block_recent");

        ClosedChunkIndex.Metadata oldMetadata = new ClosedChunkIndex.Metadata("block_old", 0L, Constants.Time.DEFAULT_BLOCK_DURATION);
        ClosedChunkIndex.Metadata recentMetadata = new ClosedChunkIndex.Metadata(
            "block_recent",
            Constants.Time.DEFAULT_BLOCK_DURATION * 2,
            Constants.Time.DEFAULT_BLOCK_DURATION * 3
        );

        ClosedChunkIndex oldIndex = new ClosedChunkIndex(oldIndexPath, oldMetadata);
        ClosedChunkIndex recentIndex = new ClosedChunkIndex(recentIndexPath, recentMetadata);

        when(closedChunkIndexManager.getClosedChunkIndexes(any(), any())).thenReturn(Arrays.asList(oldIndex, recentIndex));
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            try {
                var readerManager = oldIndex.getDirectoryReaderManager().acquire();
                countDownLatch.countDown();
                Thread.sleep(3000);
                oldIndex.getDirectoryReaderManager().release(readerManager);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        t.start();
        countDownLatch.await();
        var result = retention.drop(closedChunkIndexManager, Instant.ofEpochMilli(0), Instant.now());
        assertEquals(1, result.success().size());
        assertEquals(oldIndex, result.success().getFirst());
        verify(closedChunkIndexManager).remove(eq(oldIndex));
        assertFalse(Files.exists(oldIndexPath));
        t.join();
        // Clean up the recent index
        recentIndex.close();
    }

    public void testDropWithMultipleIndexesSuccess() throws Exception {
        // Create 4 indexes: 3 old (to be removed) and 1 recent (to serve as reference for TTL)
        Path indexPath1 = metricsDirectory.resolve("block_100");
        Path indexPath2 = metricsDirectory.resolve("block_200");
        Path indexPath3 = metricsDirectory.resolve("block_300");
        Path indexPath4 = metricsDirectory.resolve("block_400");

        // With TTL=DEFAULT_BLOCK_DURATION , first 3 should be removed
        ClosedChunkIndex.Metadata metadata1 = new ClosedChunkIndex.Metadata("block_100", 0L, Constants.Time.DEFAULT_BLOCK_DURATION);
        ClosedChunkIndex.Metadata metadata2 = new ClosedChunkIndex.Metadata(
            "block_200",
            Constants.Time.DEFAULT_BLOCK_DURATION,
            Constants.Time.DEFAULT_BLOCK_DURATION * 2
        );
        ClosedChunkIndex.Metadata metadata3 = new ClosedChunkIndex.Metadata(
            "block_300",
            Constants.Time.DEFAULT_BLOCK_DURATION * 2,
            Constants.Time.DEFAULT_BLOCK_DURATION * 3
        );
        ClosedChunkIndex.Metadata metadata4 = new ClosedChunkIndex.Metadata(
            "block_400",
            Constants.Time.DEFAULT_BLOCK_DURATION * 4,
            Constants.Time.DEFAULT_BLOCK_DURATION * 5
        );

        ClosedChunkIndex realIndex1 = new ClosedChunkIndex(indexPath1, metadata1);
        ClosedChunkIndex realIndex2 = new ClosedChunkIndex(indexPath2, metadata2);
        ClosedChunkIndex realIndex3 = new ClosedChunkIndex(indexPath3, metadata3);
        ClosedChunkIndex realIndex4 = new ClosedChunkIndex(indexPath4, metadata4);

        when(closedChunkIndexManager.getClosedChunkIndexes(any(), any())).thenReturn(
            Arrays.asList(realIndex1, realIndex2, realIndex3, realIndex4)
        );

        var result = retention.drop(closedChunkIndexManager, Instant.ofEpochMilli(0), Instant.now());

        assertEquals(3, result.success().size());
        verify(closedChunkIndexManager).remove(realIndex1);
        verify(closedChunkIndexManager).remove(realIndex2);
        verify(closedChunkIndexManager).remove(realIndex3);
        assertFalse(Files.exists(indexPath1));
        assertFalse(Files.exists(indexPath2));
        assertFalse(Files.exists(indexPath3));

        // Clean up the recent index
        realIndex4.close();
    }

    public void testDropContinuesAfterFirstFailure() throws Exception {
        // Create 3 indexes: 2 old (to be removed) and 1 recent (for TTL reference)
        Path indexPath1 = metricsDirectory.resolve("block_100");
        Path indexPath2 = metricsDirectory.resolve("block_200");
        Path indexPath3 = metricsDirectory.resolve("block_300");

        // Old indexes: 0-1000ms, 1000-2000ms, Recent: 3000-4000ms
        // With TTL=1000ms, first 2 should be attempted for removal (4000-2000 = 2000 >= 1000)
        ClosedChunkIndex.Metadata metadata1 = new ClosedChunkIndex.Metadata("block_100", 0L, Constants.Time.DEFAULT_BLOCK_DURATION);
        ClosedChunkIndex.Metadata metadata2 = new ClosedChunkIndex.Metadata(
            "block_200",
            Constants.Time.DEFAULT_BLOCK_DURATION,
            Constants.Time.DEFAULT_BLOCK_DURATION * 2
        );
        ClosedChunkIndex.Metadata metadata3 = new ClosedChunkIndex.Metadata(
            "block_300",
            Constants.Time.DEFAULT_BLOCK_DURATION * 2,
            Constants.Time.DEFAULT_BLOCK_DURATION * 3
        );

        ClosedChunkIndex realIndex1 = new ClosedChunkIndex(indexPath1, metadata1);
        ClosedChunkIndex realIndex2 = new ClosedChunkIndex(indexPath2, metadata2);
        ClosedChunkIndex realIndex3 = new ClosedChunkIndex(indexPath3, metadata3);

        when(closedChunkIndexManager.getClosedChunkIndexes(any(), any())).thenReturn(Arrays.asList(realIndex1, realIndex2, realIndex3));

        // First index removal throws exception
        doThrow(new RuntimeException("Removal failed")).when(closedChunkIndexManager).remove(eq(realIndex1));

        var result = retention.drop(closedChunkIndexManager, Instant.ofEpochMilli(0), Instant.now());

        assertEquals(1, result.success().size());
        verify(closedChunkIndexManager).remove(realIndex1);
        assertTrue(Files.exists(indexPath1));
        assertFalse(Files.exists(indexPath2));
        // Last index will be retained.
        assertTrue(Files.exists(indexPath3));

        // Clean up
        realIndex1.close();
        realIndex2.close();
        realIndex3.close();
    }
}
