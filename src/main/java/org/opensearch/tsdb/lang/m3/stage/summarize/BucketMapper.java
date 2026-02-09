/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage.summarize;

import org.apache.lucene.util.RamUsageEstimator;

/**
 * Maps timestamps to bucket start timestamps for summarization.
 *
 * <p>This class encapsulates the bucket alignment logic for a single time series.
 * Buckets are aligned to a reference time, and all timestamps are mapped to their
 * corresponding bucket start based on the interval.</p>
 *
 * <p>The reference time determines the alignment of buckets. All bucket boundaries
 * are calculated as offsets from the reference time using the specified interval.</p>
 */
public class BucketMapper {
    /** Shallow size of a BucketMapper instance in bytes. */
    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BucketMapper.class);

    private final long interval;
    private final long referenceTime;

    /**
     * Create a bucket mapper with the specified reference time alignment.
     * Buckets align to interval boundaries from the specified reference time.
     *
     * @param interval bucket interval in the same time unit as timestamps
     * @param referenceTime reference time for alignment
     */
    public BucketMapper(long interval, long referenceTime) {
        if (interval <= 0) {
            throw new IllegalArgumentException("Interval must be positive, got: " + interval);
        }
        this.interval = interval;
        this.referenceTime = referenceTime;
    }

    /**
     * Map a timestamp to its bucket start timestamp.
     *
     * @param timestamp the timestamp to map
     * @return the bucket start timestamp
     */
    public long mapToBucket(long timestamp) {
        // Calculate offset from reference time, then round down to interval
        long offset = timestamp - referenceTime;
        long bucketOffset = (offset / interval) * interval;
        return referenceTime + bucketOffset;
    }

    /**
     * Calculate the bucket start for a given time range.
     *
     * @param minTimestamp minimum timestamp in the range
     * @return bucket start timestamp
     */
    public long calculateBucketStart(long minTimestamp) {
        return mapToBucket(minTimestamp);
    }

    /**
     * Calculate the bucket end (exclusive) for a given time range.
     *
     * @param maxTimestamp maximum timestamp in the range
     * @return bucket end timestamp (exclusive)
     */
    public long calculateBucketEnd(long maxTimestamp) {
        long lastBucketStart = mapToBucket(maxTimestamp);
        return lastBucketStart + interval;
    }

    public long getInterval() {
        return interval;
    }

    public long getReferenceTime() {
        return referenceTime;
    }

    /**
     * Static utility method to calculate the bucket start for a given timestamp.
     * This is useful for determining the adjusted start time before creating a BucketMapper instance.
     *
     * @param timestamp the timestamp to map to a bucket start
     * @param interval the bucket interval
     * @param referenceTime the reference time for bucket alignment
     * @return the bucket start timestamp
     */
    public static long calculateBucketStart(long timestamp, long interval, long referenceTime) {
        if (interval <= 0) {
            throw new IllegalArgumentException("Interval must be positive, got: " + interval);
        }
        long offset = timestamp - referenceTime;
        long bucketOffset = (offset / interval) * interval;
        return referenceTime + bucketOffset;
    }
}
