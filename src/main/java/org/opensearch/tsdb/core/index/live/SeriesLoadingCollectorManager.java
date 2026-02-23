/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.search.CollectorManager;
import org.opensearch.tsdb.core.head.SeriesEventListener;
import org.opensearch.tsdb.core.mapping.LabelStorageType;

import java.util.Collection;

/**
 * Manager for creating and coordinating series loading collectors.
 *
 * This collector manager creates SeriesLoadingCollector instances for loading
 * complete series data from the live series index during query execution,
 * handling parallel collection across multiple index segments.
 */
public class SeriesLoadingCollectorManager implements CollectorManager<SeriesLoadingCollector, Long> {
    private final SeriesLoader seriesLoader;
    private final LabelStorageType labelStorageType;
    private final SeriesEventListener eventListener;

    /**
     * Constructor for SeriesLoadingCollectorManager
     *
     * @param seriesLoader     SeriesLoader to load series with
     * @param labelStorageType the label storage type configuration
     * @param eventListener    event listener for chunk lifecycle events
     */
    public SeriesLoadingCollectorManager(SeriesLoader seriesLoader, LabelStorageType labelStorageType, SeriesEventListener eventListener) {
        this.seriesLoader = seriesLoader;
        this.labelStorageType = labelStorageType;
        this.eventListener = eventListener;
    }

    @Override
    public SeriesLoadingCollector newCollector() {
        return new SeriesLoadingCollector(seriesLoader, labelStorageType, eventListener);
    }

    @Override
    public Long reduce(Collection<SeriesLoadingCollector> collectors) {
        return collectors.stream().mapToLong(SeriesLoadingCollector::getMaxReference).max().orElse(0L);
    }
}
