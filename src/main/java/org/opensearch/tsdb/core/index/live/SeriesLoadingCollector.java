/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.index.live;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.tsdb.core.head.MemSeries;
import org.opensearch.tsdb.core.head.SeriesEventListener;
import org.opensearch.tsdb.core.mapping.LabelStorageType;
import org.opensearch.tsdb.core.mapping.Constants;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.LabelsStorage;

import java.io.IOException;

/**
 * Lucene collector for loading complete series data.
 *
 * This collector loads full series information from the live series index,
 * including all series data and metadata needed for query processing and
 * result construction.
 */
public class SeriesLoadingCollector implements Collector {

    private final SeriesLoader seriesLoader;
    private final LabelStorageType labelStorageType;
    private final SeriesEventListener eventListener;
    private NumericDocValues referenceValues;
    private LabelsStorage labelsStorage;
    private long maxReference = 0L;

    /**
     * Constructs a SeriesLoadingCollector with the given head instance.
     * @param seriesLoader SeriesLoadingCallback, used to load series
     * @param labelStorageType the label storage type configuration
     * @param eventListener event listener for chunk lifecycle events
     */
    public SeriesLoadingCollector(SeriesLoader seriesLoader, LabelStorageType labelStorageType, SeriesEventListener eventListener) {
        this.seriesLoader = seriesLoader;
        this.labelStorageType = labelStorageType;
        this.eventListener = eventListener;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext leafReaderContext) throws IOException {
        referenceValues = leafReaderContext.reader().getNumericDocValues(Constants.IndexSchema.REFERENCE);

        // Load labels storage based on configured storage type
        labelsStorage = labelStorageType.getLabelsStorage(leafReaderContext.reader());

        return new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {
                // no scoring needed
            }

            @Override
            public void collect(int doc) throws IOException {
                // Read labels from storage
                Labels labels = labelsStorage.readLabels(doc);

                referenceValues.advanceExact(doc);
                long reference = referenceValues.longValue();

                seriesLoader.load(new MemSeries(reference, labels, eventListener));
                if (maxReference < reference) {
                    maxReference = reference;
                }
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    /**
     * Get the max reference seen
     * @return the max reference
     */
    public long getMaxReference() {
        return maxReference;
    }
}
