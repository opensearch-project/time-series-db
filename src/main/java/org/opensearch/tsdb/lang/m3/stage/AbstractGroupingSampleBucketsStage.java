/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract class that provides some common method implementations of {@link AbstractGroupingSampleStage} when the bucket
 * is an implementation of sample
 */
public abstract class AbstractGroupingSampleBucketsStage extends AbstractGroupingSampleStage<
    AbstractGroupingSampleBucketsStage.SamplesBuckets> {

    public static final long BUCKETS_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(SamplesBuckets.class);

    public AbstractGroupingSampleBucketsStage(String groupByLabel) {
        super(groupByLabel);
    }

    public AbstractGroupingSampleBucketsStage(List<String> groupByLabels) {
        super(groupByLabels);
    }

    public AbstractGroupingSampleBucketsStage() {
        super();
    }

    @Override
    protected SamplesBuckets initializeBuckets(long minTimestamp, long maxTimestamp, long step) {
        return new SamplesBuckets(minTimestamp, maxTimestamp, step);
    }

    @Override
    protected SampleList bucketsToSampleList(SamplesBuckets buckets) {
        ArrayList<Sample> samples = new ArrayList<>(buckets.nonNullCount);
        for (Sample sample : buckets.buckets) {
            if (sample != null) {
                samples.add(sample);
            }
        }
        return SampleList.fromList(samples);
    }

    @Override
    protected long estimateStateSize() {
        return BUCKETS_SHALLOW_SIZE;
    }

    /**
     * A simple class that holds the actual bucket array as well as metas (min/max timestamp, step, etc.)
     */
    protected static final class SamplesBuckets {
        public final long minTimestamp;
        public final long maxTimestamp;
        public final long step;
        public final Sample[] buckets;
        public int nonNullCount; // default to 0

        private SamplesBuckets(long minTimestamp, long maxTimestamp, long step) {
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
            this.step = step;
            this.buckets = new Sample[Math.toIntExact((maxTimestamp - minTimestamp) / step + 1)];
            Arrays.fill(buckets, null);
        }

    }
}
