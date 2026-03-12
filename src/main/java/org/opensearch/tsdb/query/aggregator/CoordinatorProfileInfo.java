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
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Holds profiling information for coordinator-level query execution.
 * Includes stage-by-stage timing breakdown and total coordinator reduce time.
 */
public class CoordinatorProfileInfo implements Writeable, ToXContentFragment {
    private final String stages;
    private final long reduceTimeNanos;
    private final long totalTimeNanos;

    public CoordinatorProfileInfo(String stages, long reduceTimeNanos, long totalTimeNanos) {
        this.stages = stages;
        this.reduceTimeNanos = reduceTimeNanos;
        this.totalTimeNanos = totalTimeNanos;
    }

    public CoordinatorProfileInfo(StreamInput in) throws IOException {
        this.stages = in.readString();
        this.reduceTimeNanos = in.readVLong();
        this.totalTimeNanos = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(stages);
        out.writeVLong(reduceTimeNanos);
        out.writeVLong(totalTimeNanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("stages", stages);
        builder.field("reduce_time_ns", reduceTimeNanos);
        builder.field("total_profiled_stages_time_ns", totalTimeNanos);
        return builder;
    }

    public String getStages() {
        return stages;
    }

    public long getReduceTimeNanos() {
        return reduceTimeNanos;
    }

    public long getTotalTimeNanos() {
        return totalTimeNanos;
    }
}
