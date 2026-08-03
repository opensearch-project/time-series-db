/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.SampleList;

import java.util.Arrays;
import java.util.List;

/**
 * Abstract class that provides some common method implementations of {@link AbstractGroupingSampleStage} when the bucket
 * is a double number
 */
public abstract class AbstractGroupingDoubleBucketsStage extends AbstractGroupingSampleStage<
    AbstractGroupingDoubleBucketsStage.DoubleBuckets> {

    public static final long BUCKETS_SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(DoubleBuckets.class);

    public AbstractGroupingDoubleBucketsStage(String groupByLabel) {
        super(groupByLabel);
    }

    public AbstractGroupingDoubleBucketsStage(List<String> groupByLabels) {
        super(groupByLabels);
    }

    public AbstractGroupingDoubleBucketsStage() {
        super();
    }

    @Override
    protected AbstractGroupingDoubleBucketsStage.DoubleBuckets initializeBuckets(long minTimestamp, long maxTimestamp, long step) {
        return new AbstractGroupingDoubleBucketsStage.DoubleBuckets(minTimestamp, maxTimestamp, step);
    }

    @Override
    protected SampleList bucketsToSampleList(AbstractGroupingDoubleBucketsStage.DoubleBuckets buckets) {
        FloatSampleList.Builder builder = new FloatSampleList.Builder(buckets.nonNullCount);
        for (int i = 0; i < buckets.buckets.length; i++) {
            if (!Double.isNaN(buckets.buckets[i])) {
                long ts = buckets.minTimestamp + buckets.step * i;
                builder.add(ts, buckets.buckets[i]);
            }
        }
        return builder.build();
    }

    @Override
    protected long estimateStateSize() {
        return BUCKETS_SHALLOW_SIZE;
    }

    /**
     * A simple class that holds the actual bucket array as well as metas (min/max timestamp, step, etc.)
     */
    protected static final class DoubleBuckets {
        public final long minTimestamp;
        public final long maxTimestamp;
        public final long step;
        public final double[] buckets;

        public int nonNullCount; // default to 0

        protected DoubleBuckets(long minTimestamp, long maxTimestamp, long step) {
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
            this.step = step;
            this.buckets = new double[Math.toIntExact((maxTimestamp - minTimestamp) / step + 1)];
            Arrays.fill(this.buckets, Double.NaN);
        }
    }
}
