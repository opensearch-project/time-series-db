/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.tsdb.query.stage.PipelineStage;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Profiling-enabled coordinator aggregation builder.
 *
 * <p>This builder creates {@link TimeSeriesCoordinatorAggregator} instances with profiling enabled,
 * which collects performance metrics for coordinator-level pipeline stages and returns results
 * wrapped in {@link InternalTimeSeriesWithCoordinatorProfile}.</p>
 *
 * <p>This builder uses a separate {@link #NAME} to maintain backward compatibility during rolling
 * upgrades. Old nodes will only deserialize the standard coordinator builder, while new nodes can
 * deserialize both.</p>
 *
 * @see TimeSeriesCoordinatorAggregationBuilder
 * @see InternalTimeSeriesWithCoordinatorProfile
 */
public class ProfilingTimeSeriesCoordinatorAggregationBuilder extends TimeSeriesCoordinatorAggregationBuilder {
    public static final String NAME = "coordinator_pipeline_with_profile";

    public ProfilingTimeSeriesCoordinatorAggregationBuilder(
        String name,
        List<PipelineStage> stages,
        LinkedHashMap<String, TimeSeriesCoordinatorAggregator.MacroDefinition> macroDefinitions,
        Map<String, String> references,
        String inputReference
    ) {
        super(name, stages, macroDefinitions, references, inputReference);
    }

    public ProfilingTimeSeriesCoordinatorAggregationBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metadata) {
        Map<String, Object> metadataWithProfile = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
        metadataWithProfile.put("_enable_coordinator_profiling", true);

        return new TimeSeriesCoordinatorAggregator(
            name,
            bucketsPaths,
            getStages(),
            getMacroDefinitions(),
            getReferences(),
            getInputReference(),
            metadataWithProfile
        );
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public static ProfilingTimeSeriesCoordinatorAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        TimeSeriesCoordinatorAggregationBuilder base = TimeSeriesCoordinatorAggregationBuilder.parse(aggregationName, parser);

        return new ProfilingTimeSeriesCoordinatorAggregationBuilder(
            base.getName(),
            base.getStages(),
            base.getMacroDefinitions(),
            base.getReferences(),
            base.getInputReference()
        );
    }
}
