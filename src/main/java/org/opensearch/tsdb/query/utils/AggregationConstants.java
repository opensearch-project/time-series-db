/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

/**
 * Constants used across time series aggregation functionality.
 */
public final class AggregationConstants {

    /**
     * Separator used in aggregation bucket paths to denote nested aggregations.
     * For example, "parent_agg&gt;child_agg&gt;grandchild_agg" represents a path
     * from parent to child to grandchild aggregation.
     */
    public static final String BUCKETS_PATH_SEPARATOR = ">";

    private AggregationConstants() {
        // Utility class, prevent instantiation
    }
}
