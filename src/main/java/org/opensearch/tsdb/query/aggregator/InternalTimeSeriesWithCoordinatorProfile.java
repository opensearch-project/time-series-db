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

    public InternalTimeSeriesWithCoordinatorProfile(
        String name,
        List<TimeSeries> timeSeries,
        Map<String, Object> metadata,
        UnaryPipelineStage reduceStage,
        CoordinatorProfileInfo coordinatorProfileInfo
    ) {
        super(name, timeSeries, metadata, reduceStage);
        this.coordinatorProfileInfo = coordinatorProfileInfo;
    }

    public InternalTimeSeriesWithCoordinatorProfile(StreamInput in) throws IOException {
        super(in);
        boolean hasProfileInfo = in.readBoolean();
        this.coordinatorProfileInfo = hasProfileInfo ? new CoordinatorProfileInfo(in) : null;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
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

    public CoordinatorProfileInfo getCoordinatorProfileInfo() {
        return coordinatorProfileInfo;
    }

    @Override
    public String getWriteableName() {
        return "time_series_with_coordinator_profile";
    }
}
