/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.tsdb.query.aggregator.InternalTimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregationBuilder;
import org.opensearch.tsdb.query.aggregator.TimeSeriesUnfoldAggregationBuilder;

import java.util.List;

/**
 * Plugin for time-series database (TSDB) engine
 */
public class TSDBPlugin extends Plugin implements SearchPlugin {

    /**
     * Default constructor
     */
    public TSDBPlugin() {}

    @Override
    public List<AggregationSpec> getAggregations() {
        return List.of(
            // Register TimeSeriesUnfoldAggregation
            new AggregationSpec(
                TimeSeriesUnfoldAggregationBuilder.NAME,
                TimeSeriesUnfoldAggregationBuilder::new,
                TimeSeriesUnfoldAggregationBuilder::parse
            ).addResultReader(InternalTimeSeries::new).setAggregatorRegistrar(TimeSeriesUnfoldAggregationBuilder::registerAggregators)
        );
    }

    @Override
    public List<PipelineAggregationSpec> getPipelineAggregations() {
        return List.of(
            // Register TimeSeriesCoordinatorAggregation
            new PipelineAggregationSpec(
                TimeSeriesCoordinatorAggregationBuilder.NAME,
                TimeSeriesCoordinatorAggregationBuilder::new,
                TimeSeriesCoordinatorAggregationBuilder::parse
            ).addResultReader(InternalTimeSeries::new)
        );
    }
}
