/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for pipeline stages that perform data point-level transformations.
 * Provides common functionality for mapping operations that transform individual samples
 * within time series without aggregating across multiple series.
 *
 * <p>This class follows the template method pattern, handling the iteration through
 * time series and samples while delegating the actual transformation logic to
 * concrete implementations via the {@link #mapSample(long, double, UpdateConsumer)} method.</p>
 *
 * <h2>Key Characteristics:</h2>
 * <ul>
 *   <li><strong>Sample-level transformation:</strong> Operates on individual data points</li>
 *   <li><strong>Independent processing:</strong> Each time series is processed independently</li>
 *   <li><strong>Preserves structure:</strong> Maintains the same number of time series</li>
 *   <li><strong>Metadata handling:</strong> Delegates metadata updates to concrete implementations</li>
 * </ul>
 *
 * <h2>Performance Considerations:</h2>
 * <p>Mapper stages are typically optimized for concurrent segment search since
 * each time series can be processed independently. The default implementation
 * of {@link #supportConcurrentSegmentSearch()} returns true.</p>
 *
 * @since 0.0.1
 */
public abstract class AbstractMapperStage implements UnaryPipelineStage {

    /**
     * Default constructor for mapper stages.
     */
    protected AbstractMapperStage() {
        // Default constructor
    }

    /**
     * Process a list of time series by applying the mapping function to each sample.
     * This method implements the template method pattern, iterating through all
     * time series and samples while delegating the actual transformation to
     * {@link #mapSample(long, double, UpdateConsumer)}.
     *
     * @param input The input time series to process
     * @return The processed time series with mapped samples
     * @throws NullPointerException if input is null
     */
    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }

        if (input.isEmpty()) {
            return input;
        }

        List<TimeSeries> result = new ArrayList<>(input.size());

        for (TimeSeries series : input) {
            SampleList originalSamples = series.getSamples();
            SampleList.UpdatableIterator updatableIterator = originalSamples.updatableIterator();
            if (updatableIterator != null) {
                while (updatableIterator.hasNext()) {
                    Sample sample = updatableIterator.next();
                    mapSample(sample.getTimestamp(), sample.getValue(), updatableIterator::update);
                }
                // Create the new time series with mapped samples and updated metadata
                TimeSeries mappedTimeSeries = createMappedTimeSeries(originalSamples, series);
                result.add(mappedTimeSeries);
            } else {
                // The original sample list doesn't support in-place update (e.g. constant list)
                FloatSampleList.Builder newListBuilder = new FloatSampleList.Builder(originalSamples.size());
                for (Sample sample : originalSamples) {
                    mapSample(sample.getTimestamp(), sample.getValue(), newListBuilder::add);
                }
                // Create the new time series with mapped samples and updated metadata
                TimeSeries mappedTimeSeries = createMappedTimeSeries(newListBuilder.build(), series);
                result.add(mappedTimeSeries);
            }
        }

        return result;
    }

    /**
     * Map a single sample to a new sample. This is the core transformation method
     * that concrete implementations must provide.
     * <br>
     * The new sample should be directly consumed by provided updateConsumer, and each method call
     * <b>must</b> call {@link UpdateConsumer#update(long, double)} once
     */
    protected abstract void mapSample(long timestamp, double value, UpdateConsumer updateConsumer);

    /**
     * Create a new time series with the mapped samples and appropriate metadata.
     * The default implementation preserves all metadata from the original series.
     * Concrete implementations should override this method if they need to update
     * metadata (e.g., min/max timestamps, labels, etc.).
     *
     * @param mappedSamples The list of mapped samples
     * @param originalSeries The original time series
     * @return A new time series with the mapped samples and updated metadata
     */
    protected TimeSeries createMappedTimeSeries(SampleList mappedSamples, TimeSeries originalSeries) {
        // Default implementation preserves all metadata
        return new TimeSeries(
            mappedSamples,
            originalSeries.getLabels(),
            originalSeries.getMinTimestamp(),
            originalSeries.getMaxTimestamp(),
            originalSeries.getStep(),
            originalSeries.getAlias()
        );
    }

    /**
     * Get the name of this pipeline stage.
     *
     * @return The stage name
     */
    @Override
    public abstract String getName();

    /**
     * Mapper stages support concurrent segment search by default since each
     * time series can be processed independently.
     *
     * @return true to indicate support for concurrent segment search
     */
    @Override
    public boolean supportConcurrentSegmentSearch() {
        return true;
    }

    /**
     * Mapper stages are not global aggregations by default.
     *
     * @return false to indicate this is not a global aggregation
     */
    @Override
    public boolean isGlobalAggregation() {
        return false;
    }

    /**
     * Mapper stages can be executed in the UnfoldAggregator by default.
     *
     * @return false to indicate this stage can be executed in the UnfoldAggregator
     */
    @Override
    public boolean isCoordinatorOnly() {
        return false;
    }

    /**
     * Estimate memory overhead for mapper stage operations.
     * Mapper stages allocate new TimeSeries objects with new samples (same cardinality as input).
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

    @Override
    public int hashCode() {
        return getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return true;
    }

    public interface UpdateConsumer {
        void update(long timestamp, double value);
    }
}
