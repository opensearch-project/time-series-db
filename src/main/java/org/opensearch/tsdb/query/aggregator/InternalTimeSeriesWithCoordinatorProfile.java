/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Extension of InternalTimeSeries that includes comprehensive coordinator-level profiling information.
 *
 * <p>This class wraps time series results along with detailed profiling metrics collected
 * during coordinator-level aggregation processing, including stage timings, data volumes,
 * and performance metrics.</p>
 */
public class InternalTimeSeriesWithCoordinatorProfile extends InternalTimeSeries {
    private final CoordinatorProfileInfo coordinatorProfileInfo;

    private final long serializeTimeNanos;
    private final long deserializeTimeNanos;

    public InternalTimeSeriesWithCoordinatorProfile(
        String name,
        List<TimeSeries> timeSeries,
        Map<String, Object> metadata,
        UnaryPipelineStage reduceStage,
        CoordinatorProfileInfo coordinatorProfileInfo
    ) {
        super(name, timeSeries, metadata, reduceStage, AggregationExecStats.EMPTY, AggregationDataSource.EMPTY);
        this.coordinatorProfileInfo = coordinatorProfileInfo;
        this.serializeTimeNanos = 0;
        this.deserializeTimeNanos = 0;
    }

    public InternalTimeSeriesWithCoordinatorProfile(
        String name,
        List<TimeSeries> timeSeries,
        Map<String, Object> metadata,
        UnaryPipelineStage reduceStage,
        AggregationExecStats execStats,
        AggregationDataSource dataSource,
        CoordinatorProfileInfo coordinatorProfileInfo
    ) {
        super(name, timeSeries, metadata, reduceStage, execStats, dataSource);
        this.coordinatorProfileInfo = coordinatorProfileInfo;
        this.serializeTimeNanos = 0;
        this.deserializeTimeNanos = 0;
    }

    public InternalTimeSeriesWithCoordinatorProfile(StreamInput in) throws IOException {
        this(in, System.nanoTime());
    }

    private InternalTimeSeriesWithCoordinatorProfile(StreamInput in, long deserStart) throws IOException {
        super(in);
        this.serializeTimeNanos = in.readLong();
        this.deserializeTimeNanos = System.nanoTime() - deserStart;
        boolean hasProfileInfo = in.readBoolean();
        this.coordinatorProfileInfo = hasProfileInfo ? new CoordinatorProfileInfo(in) : null;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        long serStart = System.nanoTime();
        super.doWriteTo(out);
        long serTimeNanos = System.nanoTime() - serStart;
        out.writeLong(serTimeNanos);
        out.writeBoolean(coordinatorProfileInfo != null);
        if (coordinatorProfileInfo != null) {
            coordinatorProfileInfo.writeTo(out);
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        super.doXContentBody(builder, params);
        if (coordinatorProfileInfo != null) {
            builder.startObject("coordinator_profile");
            coordinatorProfileInfo.toXContent(builder, params);
            builder.endObject();
        }
        return builder;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        if (!reduceContext.isFinalReduce()) {
            return super.reduce(aggregations, reduceContext);
        }

        long reduceStart = System.nanoTime();
        long totalSerNanos = 0;
        long totalDeserNanos = 0;

        for (InternalAggregation agg : aggregations) {
            if (agg instanceof InternalTimeSeriesWithCoordinatorProfile p) {
                totalSerNanos += p.getSerializeTimeNanos();
                totalDeserNanos += p.getDeserializeTimeNanos();
            }
        }

        InternalAggregation result = super.reduce(aggregations, reduceContext);
        long internalReduceTimeNanos = System.nanoTime() - reduceStart;

        List<TimeSeries> resultTs = ((InternalTimeSeries) result).getTimeSeries();
        UnaryPipelineStage resultRs = ((InternalTimeSeries) result).getReduceStage();
        AggregationExecStats resultExecStats = ((InternalTimeSeries) result).getExecStats();
        AggregationDataSource resultDataSource = ((InternalTimeSeries) result).getDataSource();
        CoordinatorProfileInfo info = new CoordinatorProfileInfo("", 0L, 0L, internalReduceTimeNanos, totalSerNanos, totalDeserNanos);
        return new InternalTimeSeriesWithCoordinatorProfile(name, resultTs, metadata, resultRs, resultExecStats, resultDataSource, info);
    }

    @Override
    public String getWriteableName() {
        return "time_series_with_coordinator_profile";
    }

    public CoordinatorProfileInfo getCoordinatorProfileInfo() {
        return coordinatorProfileInfo;
    }

    public long getSerializeTimeNanos() {
        return serializeTimeNanos;
    }

    public long getDeserializeTimeNanos() {
        return deserializeTimeNanos;
    }
}
