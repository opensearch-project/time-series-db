/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

/**
 * Constants shared between {@code RestTSDBStatsAction} and {@code TSDBStatsResponseListener}.
 */
public final class TSDBStatsConstants {

    private TSDBStatsConstants() {}

    // User defined aggregation name
    public static final String AGGREGATION_NAME = "tsdb_stats_summary";

    // Include option values
    public static final String INCLUDE_ALL = "all";
    public static final String INCLUDE_HEAD_STATS = "headStats";
    public static final String INCLUDE_LABEL_VALUES = "labelValues";
    public static final String INCLUDE_VALUE_STATS = "valueStats";

    // Format values
    public static final String FORMAT_GROUPED = "grouped";
    public static final String FORMAT_FLAT = "flat";

    // Dedup mode values
    public static final String DEDUP_MODE_INDEXED = "indexed";
    public static final String DEDUP_MODE_RECOMPUTED = "recomputed";
}
