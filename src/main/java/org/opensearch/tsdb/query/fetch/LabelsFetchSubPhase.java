/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.document.DocumentField;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.reader.TSDBDocValues;
import org.opensearch.tsdb.core.reader.TSDBLeafReader;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Fetch sub-phase that retrieves TSDB labels from DocValues and adds them to search hits.
 * This enables querying labels via the _search API using the ext parameter.
 */
public class LabelsFetchSubPhase implements FetchSubPhase {

    public static final String NAME = "tsdb_labels";

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
        if (fetchContext.getSearchExt(NAME) == null) {
            return null;
        }
        return new LabelsFetchSubPhaseProcessor();
    }

    private static class LabelsFetchSubPhaseProcessor implements FetchSubPhaseProcessor {
        private TSDBLeafReader tsdbLeafReader;
        private TSDBDocValues tsdbDocValues;

        @Override
        public void setNextReader(LeafReaderContext readerContext) throws IOException {
            this.tsdbLeafReader = TSDBLeafReader.unwrapLeafReader(readerContext.reader());
            if (tsdbLeafReader == null) {
                throw new IOException("Expected TSDBLeafReader but found: " + readerContext.reader().getClass().getName());
            }
            this.tsdbDocValues = tsdbLeafReader.getTSDBDocValues();
        }

        @Override
        public void process(HitContext hitContext) throws IOException {
            Labels labels = tsdbLeafReader.labelsForDoc(hitContext.docId(), tsdbDocValues);

            if (labels == null || labels.isEmpty()) {
                return;
            }

            // TODO: Add circuit breaker tracking here once FetchContext exposes SearchContext or CircuitBreakerService.
            // Currently, FetchContext does not provide access to the circuit breaker, so we can only log warnings.
            // See:
            // https://github.com/opensearch-project/OpenSearch/blob/main/server/src/main/java/org/opensearch/search/fetch/FetchContext.java
            Map<String, String> labelsMap = labels.toMapView();
            hitContext.hit().setDocumentField(NAME, new DocumentField(NAME, Collections.singletonList(labelsMap)));
        }
    }
}
