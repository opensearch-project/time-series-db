/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.index.engine.TSDBTragicException;
import org.opensearch.tsdb.core.index.live.LiveSeriesIndex;
import org.opensearch.tsdb.core.index.live.SeriesUpdater;
import org.opensearch.tsdb.core.model.Labels;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * SeriesStore coordinates lifecycle changes across SeriesMap and LiveSeriesIndex
 * using per-reference striped locking.
 */
public final class SeriesStore {
    private static final int DEFAULT_STRIPES = 1 << 14; // 16k, copied from prom

    private final SeriesMap seriesMap;
    private final Supplier<LiveSeriesIndex> liveSeriesIndexSupplier;
    private final SeriesEventListener eventListener;
    private final StripedRefLock refLocks;

    public SeriesStore(SeriesMap seriesMap, LiveSeriesIndex liveSeriesIndex) {
        this(seriesMap, () -> liveSeriesIndex, SeriesEventListener.NOOP, DEFAULT_STRIPES);
    }

    public SeriesStore(SeriesMap seriesMap, LiveSeriesIndex liveSeriesIndex, SeriesEventListener eventListener) {
        this(seriesMap, () -> liveSeriesIndex, eventListener, DEFAULT_STRIPES);
    }

    public SeriesStore(SeriesMap seriesMap, Supplier<LiveSeriesIndex> liveSeriesIndexSupplier) {
        this(seriesMap, liveSeriesIndexSupplier, SeriesEventListener.NOOP, DEFAULT_STRIPES);
    }

    public SeriesStore(SeriesMap seriesMap, Supplier<LiveSeriesIndex> liveSeriesIndexSupplier, SeriesEventListener eventListener) {
        this(seriesMap, liveSeriesIndexSupplier, eventListener, DEFAULT_STRIPES);
    }

    SeriesStore(SeriesMap seriesMap, Supplier<LiveSeriesIndex> liveSeriesIndexSupplier, int stripes) {
        this(seriesMap, liveSeriesIndexSupplier, SeriesEventListener.NOOP, stripes);
    }

    SeriesStore(SeriesMap seriesMap, Supplier<LiveSeriesIndex> liveSeriesIndexSupplier, SeriesEventListener eventListener, int stripes) {
        this.seriesMap = seriesMap;
        this.liveSeriesIndexSupplier = liveSeriesIndexSupplier;
        this.eventListener = eventListener == null ? SeriesEventListener.NOOP : eventListener;
        this.refLocks = new StripedRefLock(stripes);
    }

    public MemSeries getByReference(long ref) {
        return seriesMap.getByReference(ref);
    }

    public List<MemSeries> snapshot() {
        return seriesMap.getSeriesMap();
    }

    /**
     * Loads series from the LiveSeriesIndex into memory.
     *
     * This is intended for startup/recovery only and must not run concurrently with ingestion or
     * lifecycle operations (create/delete/upgrade). It bypasses per-ref locking and writes directly
     * to the SeriesMap.
     */
    public void loadSeriesFromIndex() {
        liveSeriesIndexSupplier.get().loadSeriesFromIndex(series -> {
            seriesMap.add(series);
            series.markPersisted();
        }, eventListener);
    }

    public void updateSeriesFromCommitData(SeriesUpdater seriesUpdater) {
        liveSeriesIndexSupplier.get().updateSeriesFromCommitData(seriesUpdater);
    }

    public SeriesOpResult getOrCreateSeries(long ref, Labels labels, long minTimestampForDoc) {
        boolean hasLabels = labels != null && !labels.isEmpty();
        LiveSeriesIndex liveSeriesIndex = liveSeriesIndexSupplier.get();
        Lock lock = refLocks.lockFor(ref);
        lock.lock();
        try {
            MemSeries existing = seriesMap.getByReference(ref);
            if (existing != null && existing.isFailed()) {
                markSeriesAsFailedUnderLock(existing);
                existing = null;
            }

            if (existing != null) {
                if (existing.isStub() && hasLabels) {
                    return upgradeStubSeriesWithLabels(existing, ref, labels, minTimestampForDoc);
                }
                return new SeriesOpResult(existing, false, false, false);
            }

            MemSeries newSeries = hasLabels ? new MemSeries(ref, labels, eventListener) : new MemSeries(ref, null, true, eventListener);
            try {
                if (hasLabels) {
                    liveSeriesIndex.addSeries(labels, ref, minTimestampForDoc);
                }

                MemSeries actual = seriesMap.putIfAbsent(newSeries);
                boolean created = actual == newSeries;
                boolean stubCreated = false;
                if (created && !hasLabels) {
                    seriesMap.incrementStubSeriesCount();
                    stubCreated = true;
                }
                return new SeriesOpResult(actual, created, stubCreated, false);
            } catch (Exception e) {
                if (seriesMap.getByReference(ref) == newSeries) {
                    markSeriesAsFailedUnderLock(newSeries);
                } else if (hasLabels) {
                    try {
                        liveSeriesIndex.removeSeries(List.of(ref));
                    } catch (Exception ignored) {
                        // Best-effort cleanup to avoid dangling entries in LSI.
                    }
                }
                throw e;
            }
        } finally {
            lock.unlock();
        }
    }

    public FailedSeriesResult markSeriesAsFailed(MemSeries series) {
        Lock lock = refLocks.lockFor(series.getReference());
        lock.lock();
        try {
            return markSeriesAsFailedUnderLock(series);
        } finally {
            lock.unlock();
        }
    }

    public void cleanupDeletedSeries(MemSeries series) {
        if (!series.isDeleted()) {
            return;
        }

        long ref = series.getReference();
        LiveSeriesIndex liveSeriesIndex = liveSeriesIndexSupplier.get();
        Lock lock = refLocks.lockFor(ref);
        lock.lock();
        try {
            // Check if this series is still in the map - another thread may have already cleaned it up
            if (seriesMap.getByReference(ref) != series) {
                return;
            }

            try {
                liveSeriesIndex.removeSeries(List.of(ref));
                seriesMap.delete(series);
            } catch (Exception e) {
                throw new TSDBTragicException("Failed to remove deleted series from live series index: ref=" + ref, e);
            }
        } finally {
            lock.unlock();
        }
    }

    public int dropEmptySeries(long minSeqNoToKeep) {
        LiveSeriesIndex liveSeriesIndex = liveSeriesIndexSupplier.get();
        int dropped = 0;
        for (MemSeries series : seriesMap.getSeriesMap()) {
            long ref = series.getReference();
            Lock lock = refLocks.lockFor(ref);
            lock.lock();
            try {
                if (seriesMap.getByReference(ref) != series) {
                    continue;
                }

                series.lock();
                try {
                    if (series.getMaxSeqNo() >= minSeqNoToKeep) {
                        continue;
                    }

                    // Atomically try to mark series as deleted (only succeeds if refCount == 0)
                    if (!series.tryMarkDeleted()) {
                        continue;
                    }

                    seriesMap.delete(series);
                } finally {
                    series.unlock();
                }

                liveSeriesIndex.removeSeries(List.of(ref));
                dropped++;
            } catch (IOException e) {
                throw new TSDBTragicException("Failed to remove empty series from live series index: ref=" + ref, e);
            } finally {
                lock.unlock();
            }
        }
        return dropped;
    }

    public boolean deleteStubSeries(MemSeries series) {
        long ref = series.getReference();
        Lock lock = refLocks.lockFor(ref);
        lock.lock();
        try {
            if (seriesMap.getByReference(ref) != series) {
                return false;
            }
            if (!series.isStub()) {
                return false;
            }
            seriesMap.delete(series);
            seriesMap.decrementStubSeriesCount();
            return true;
        } finally {
            lock.unlock();
        }
    }

    private SeriesOpResult upgradeStubSeriesWithLabels(MemSeries series, long ref, Labels labels, long minTimestampForDoc) {
        try {
            series.lock();
            try {
                series.upgradeWithLabels(labels);
                seriesMap.decrementStubSeriesCount();
            } finally {
                series.unlock();
            }

            liveSeriesIndexSupplier.get().addSeries(labels, ref, minTimestampForDoc);
            return new SeriesOpResult(series, true, false, true);
        } catch (Exception e) {
            markSeriesAsFailedUnderLock(series);
            throw e;
        }
    }

    private FailedSeriesResult markSeriesAsFailedUnderLock(MemSeries series) {
        long ref = series.getReference();
        if (seriesMap.getByReference(ref) != series) {
            // A newer series instance already replaced this ref; don't delete it.
            series.markFailed();
            series.markPersisted();
            return new FailedSeriesResult(false, null);
        }

        LiveSeriesIndex liveSeriesIndex = liveSeriesIndexSupplier.get();
        Exception removeFailure = null;
        try {
            liveSeriesIndex.removeSeries(List.of(ref));
        } catch (Exception ignored) {
            // Suppress; unused series will be cleaned up from the head eventually.
            removeFailure = ignored;
        }

        boolean wasStub = series.isStub();
        if (wasStub) {
            seriesMap.decrementStubSeriesCount();
        }

        seriesMap.delete(series);
        series.markFailed();
        series.markPersisted();
        return new FailedSeriesResult(wasStub, removeFailure);
    }

    public record FailedSeriesResult(boolean wasStub, Exception liveSeriesIndexRemoveFailure) {
    }

    public record SeriesOpResult(MemSeries series, boolean created, boolean stubCreated, boolean stubUpgraded) {
    }

    private static final class StripedRefLock {
        private final ReentrantLock[] locks;

        private StripedRefLock(int stripes) {
            if (Integer.bitCount(stripes) != 1) {
                throw new IllegalArgumentException("stripes must be a power of two, got: " + stripes);
            }
            locks = new ReentrantLock[stripes];
            for (int i = 0; i < stripes; i++) {
                locks[i] = new ReentrantLock();
            }
        }

        private Lock lockFor(long ref) {
            int h = Long.hashCode(ref);
            int idx = h & (locks.length - 1);
            return locks[idx];
        }
    }
}
