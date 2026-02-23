/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndex;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SeriesStoreTests extends OpenSearchTestCase {

    public void testDropEmptySeriesBlocksConcurrentCreateUntilRemovalCompletes() throws Exception {
        SeriesMap seriesMap = new SeriesMap();
        LiveSeriesIndex liveSeriesIndex = Mockito.mock(LiveSeriesIndex.class);
        SeriesStore seriesStore = new SeriesStore(seriesMap, liveSeriesIndex);

        AtomicInteger addSeriesCalls = new AtomicInteger(0);
        AtomicBoolean removalUnblocked = new AtomicBoolean(false);
        Mockito.doAnswer(invocation -> {
            int call = addSeriesCalls.incrementAndGet();
            if (call > 1) {
                assertTrue("Recreate addSeries should wait for removal", removalUnblocked.get());
            }
            return null;
        }).when(liveSeriesIndex).addSeries(Mockito.any(), Mockito.anyLong(), Mockito.anyLong());

        CountDownLatch removeStarted = new CountDownLatch(1);
        CountDownLatch allowRemove = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            removeStarted.countDown();
            assertTrue("removeSeries should be unblocked", allowRemove.await(5, TimeUnit.SECONDS));
            removalUnblocked.set(true);
            return null;
        }).when(liveSeriesIndex).removeSeries(Mockito.anyList());

        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long ref = labels.stableHash();
        SeriesStore.SeriesOpResult created = seriesStore.getOrCreateSeries(ref, labels, 0L);
        assertTrue("Series should be created", created.created());
        assertNotNull("Series should exist in map", seriesStore.getByReference(ref));

        AtomicReference<Exception> deleteFailure = new AtomicReference<>();
        AtomicReference<Exception> createFailure = new AtomicReference<>();
        AtomicInteger droppedCount = new AtomicInteger(0);

        Thread deletionThread = new Thread(() -> {
            try {
                droppedCount.set(seriesStore.dropEmptySeries(Long.MAX_VALUE));
            } catch (Exception e) {
                deleteFailure.set(e);
            }
        });
        deletionThread.start();

        assertTrue("removeSeries should be called", removeStarted.await(5, TimeUnit.SECONDS));

        CountDownLatch createStarted = new CountDownLatch(1);
        CountDownLatch createDone = new CountDownLatch(1);
        Thread createThread = new Thread(() -> {
            createStarted.countDown();
            try {
                seriesStore.getOrCreateSeries(ref, labels, 0L);
            } catch (Exception e) {
                createFailure.set(e);
            } finally {
                createDone.countDown();
            }
        });
        createThread.start();

        assertTrue("create thread should start", createStarted.await(5, TimeUnit.SECONDS));
        assertBusy(() -> {
            Thread.State state = createThread.getState();
            assertTrue(
                "create thread should be blocked while removeSeries is in progress",
                state == Thread.State.WAITING || state == Thread.State.BLOCKED || state == Thread.State.TIMED_WAITING
            );
            assertEquals("create should not complete while removeSeries is blocked", 1L, createDone.getCount());
        }, 5, TimeUnit.SECONDS);
        assertNull("Series should not be visible while delete holds the ref lock", seriesStore.getByReference(ref));

        allowRemove.countDown();
        createThread.join(5000);
        deletionThread.join(5000);

        assertNull("delete thread should not throw", deleteFailure.get());
        assertNull("create thread should not throw", createFailure.get());
        assertEquals("One series should be dropped", 1, droppedCount.get());

        MemSeries finalSeries = seriesStore.getByReference(ref);
        assertNotNull("Series should be recreated after deletion completes", finalSeries);
        assertFalse("Series should not be marked deleted", finalSeries.isDeleted());
        assertEquals("addSeries should be called twice", 2, addSeriesCalls.get());
    }

    public void testFailedSeriesCleanupDoesNotDeleteRecreatedSeries() {
        SeriesMap seriesMap = new SeriesMap();
        LiveSeriesIndex liveSeriesIndex = Mockito.mock(LiveSeriesIndex.class);
        SeriesStore seriesStore = new SeriesStore(seriesMap, liveSeriesIndex);

        Labels labels = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        long ref = labels.stableHash();

        SeriesStore.SeriesOpResult first = seriesStore.getOrCreateSeries(ref, labels, 0L);
        MemSeries original = first.series();
        assertTrue("First create should succeed", first.created());

        // Simulate a failure being observed before the original series is cleaned up.
        original.markFailed();

        SeriesStore.SeriesOpResult recreated = seriesStore.getOrCreateSeries(ref, labels, 0L);
        MemSeries replacement = recreated.series();
        assertNotSame("Recreation should return a new instance", original, replacement);
        assertSame("Replacement should be in the map", replacement, seriesStore.getByReference(ref));

        // Late cleanup of the failed instance must not delete the replacement.
        seriesStore.markSeriesAsFailed(original);

        assertSame("Replacement should survive cleanup of old instance", replacement, seriesStore.getByReference(ref));
    }

    public void testStripedLockRequiresPowerOfTwo() {
        SeriesMap seriesMap = new SeriesMap();
        LiveSeriesIndex liveSeriesIndex = Mockito.mock(LiveSeriesIndex.class);

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new SeriesStore(seriesMap, () -> liveSeriesIndex, 3)
        );
        assertEquals("stripes must be a power of two, got: 3", exception.getMessage());
    }
}
