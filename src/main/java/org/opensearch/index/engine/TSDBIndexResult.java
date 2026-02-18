/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.engine;

/**
 * TSDB-specific IndexResult that carries whether a new time series was created during indexing.
 *
 * <p>When a sample is appended to an existing series, the data is immediately queryable via
 * the in-memory MemChunks without waiting for a LiveSeriesIndex refresh. When a new series is
 * created, it must be discovered through a refresh before queries can find it. This flag allows
 * downstream consumers (e.g., ingestion lag metrics) to differentiate the two cases.</p>
 */
public class TSDBIndexResult extends Engine.IndexResult {

    private final boolean newSeriesCreated;

    public TSDBIndexResult(long version, long term, long seqNo, boolean newSeriesCreated) {
        super(version, term, seqNo, true);
        this.newSeriesCreated = newSeriesCreated;
    }

    public boolean isNewSeriesCreated() {
        return newSeriesCreated;
    }
}
