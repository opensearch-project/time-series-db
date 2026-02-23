/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An internal stage which should NOT be used in user query, it will be added by need
 * when we're parsing the query to OpenSearch DSL
 */
@PipelineStageAnnotation(name = CopyStage.NAME)
public class CopyStage implements UnaryPipelineStage {

    public static final String NAME = "_copy"; // use underscore to indicate internal stage

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        List<TimeSeries> result = new ArrayList<>(input.size());
        for (TimeSeries timeSeries : input) {
            result.add(timeSeries.deepcopy());
        }
        return result;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        // no params
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // no params
    }

    @Override
    public boolean isCoordinatorOnly() {
        return true;
    }

    /**
     * Estimate memory overhead for deep copy operations.
     * CopyStage creates a full deep copy of all time series including labels, samples, and alias.
     *
     * <p>Delegates to {@link TimeSeries#ramBytesUsed()} for per-series estimation, ensuring
     * the calculation stays accurate as underlying implementations change.</p>
     *
     * @param input The input time series
     * @return Estimated memory overhead in bytes
     */
    @Override
    public long estimateMemoryOverhead(List<TimeSeries> input) {
        return UnaryPipelineStage.estimateDeepCopyOverhead(input);
    }

    /**
     * Create an CopyStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new CopyStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static CopyStage readFrom(StreamInput in) throws IOException {
        return new CopyStage();
    }

    /**
     * Create an CopyStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return CopyStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static CopyStage fromArgs(Map<String, Object> args) {
        return new CopyStage();
    }
}
