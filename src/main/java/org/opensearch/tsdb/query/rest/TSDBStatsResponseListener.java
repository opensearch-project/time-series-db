/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.tsdb.query.aggregator.InternalTSDBStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Response listener for TSDB stats queries.
 *
 * <p>This listener handles formatting the response in either 'grouped' or 'flat' format
 * based on the request parameters.</p>
 */
public class TSDBStatsResponseListener implements ActionListener<SearchResponse> {

    private final RestChannel channel;
    private final List<String> includeOptions;
    private final String format;

    /**
     * Creates a new TSDBStatsResponseListener.
     *
     * @param channel the REST channel to send the response to
     * @param includeOptions list of stats to include (headStats, labelStats, valueStats)
     * @param format the response format (grouped or flat)
     */
    public TSDBStatsResponseListener(RestChannel channel, List<String> includeOptions, String format) {
        this.channel = channel;
        this.includeOptions = includeOptions;
        this.format = format;
    }

    @Override
    public void onResponse(SearchResponse searchResponse) {
        try {
            XContentBuilder builder = channel.newBuilder();
            builder.startObject();

            // Check if aggregations are null
            if (searchResponse.getAggregations() == null) {
                builder.field("error", "No aggregations in response");
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder));
                return;
            }

            // Extract the tsdb_stats aggregation
            Aggregation agg = searchResponse.getAggregations().get("tsdb_stats");
            if (agg instanceof InternalTSDBStats) {
                InternalTSDBStats tsdbStats = (InternalTSDBStats) agg;

                // Format response based on format parameter
                if ("flat".equals(format)) {
                    formatFlatResponse(tsdbStats, builder);
                } else {
                    // Default to grouped
                    formatGroupedResponse(tsdbStats, builder);
                }
            } else {
                builder.field("error", "Unexpected aggregation type");
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder));
                return;
            }

            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        } catch (Exception e) {
            onFailure(e);
        }
    }

    /**
     * Formats the response in 'grouped' format.
     *
     * Grouped format organizes statistics by label name with nested values and stats.
     * Example:
     * {
     *   "headStats": { "numSeries": 100, ... },
     *   "totalTimeSeries": 1000,
     *   "labelStats": {
     *     "cluster": {
     *       "totalTimeSeries": 100,
     *       "values": ["prod", "staging"],
     *       "valuesStats": { "prod": 80, "staging": 20 }
     *     }
     *   }
     * }
     *
     * @param stats the TSDB stats aggregation result
     * @param builder the XContent builder
     * @throws IOException if an I/O error occurs
     */
    private void formatGroupedResponse(InternalTSDBStats stats, XContentBuilder builder) throws IOException {
        boolean includeValueStats = includeOptions.isEmpty() || includeOptions.contains("valueStats");
        boolean includeHeadStats = includeOptions.isEmpty() || includeOptions.contains("headStats");
        boolean includeLabelStats = includeOptions.isEmpty() || includeOptions.contains("labelStats");

        // Write headStats if included
        if (includeHeadStats && stats.getHeadStats() != null) {
            InternalTSDBStats.HeadStats headStats = stats.getHeadStats();
            builder.startObject("headStats");
            builder.field("numSeries", headStats.numSeries());
            builder.field("chunkCount", headStats.chunkCount());
            builder.field("minTime", headStats.minTime());
            builder.field("maxTime", headStats.maxTime());
            builder.endObject();
        }

        // Write labelStats if included
        if (includeLabelStats) {
            builder.startObject("labelStats");

            // Write numSeries at the start of labelStats
            if (stats.getNumSeries() != null) {
                builder.field("numSeries", stats.getNumSeries());
            }

            for (Map.Entry<String, InternalTSDBStats.LabelStats> entry : stats.getLabelStats().entrySet()) {
                builder.startObject(entry.getKey());
                InternalTSDBStats.LabelStats labelStats = entry.getValue();
                if (labelStats.getNumSeries() != null) {
                    builder.field("numSeries", labelStats.getNumSeries());
                }
                builder.field("values", labelStats.getValues());
                // Only include valuesStats if valueStats is in includeOptions
                if (includeValueStats && labelStats.getValuesStats() != null) {
                    builder.field("valuesStats", labelStats.getValuesStats());
                }
                builder.endObject();
            }
            builder.endObject();
        }
    }

    /**
     * Formats the response in 'flat' format.
     *
     * Flat format converts grouped data into flat arrays for easier consumption.
     * Example:
     * {
     *   "headStats": { "numSeries": 100, ... },
     *   "seriesCountByMetricName": [
     *     { "name": "http_requests_total", "value": 50 }
     *   ],
     *   "labelValueCountByLabelName": [
     *     { "name": "__name__", "value": 10 }
     *   ],
     *   "memoryInBytesByLabelName": [
     *     { "name": "__name__", "value": 1024 }
     *   ],
     *   "seriesCountByLabelValuePair": [
     *     { "name": "cluster=prod", "value": 80 }
     *   ]
     * }
     *
     * @param stats the TSDB stats aggregation result
     * @param builder the XContent builder
     * @throws IOException if an I/O error occurs
     */
    private void formatFlatResponse(InternalTSDBStats stats, XContentBuilder builder) throws IOException {
        boolean includeValueStats = includeOptions.isEmpty() || includeOptions.contains("valueStats");
        boolean includeHeadStats = includeOptions.isEmpty() || includeOptions.contains("headStats");
        boolean includeLabelStats = includeOptions.isEmpty() || includeOptions.contains("labelStats");

        // Write headStats if included
        if (includeHeadStats && stats.getHeadStats() != null) {
            InternalTSDBStats.HeadStats headStats = stats.getHeadStats();
            builder.startObject("headStats");
            builder.field("numSeries", headStats.numSeries());
            builder.field("chunkCount", headStats.chunkCount());
            builder.field("minTime", headStats.minTime());
            builder.field("maxTime", headStats.maxTime());
            builder.endObject();
        }

        // Only process labelStats if included
        if (includeLabelStats) {
            Map<String, InternalTSDBStats.LabelStats> labelStatsMap = stats.getLabelStats();

            // seriesCountByMetricName - series count for each name value
            List<NameValuePair> seriesCountByMetricName = new ArrayList<>();
            InternalTSDBStats.LabelStats nameLabelStats = labelStatsMap.get("name");
            if (nameLabelStats != null && nameLabelStats.getValuesStats() != null) {
                for (Map.Entry<String, Long> entry : nameLabelStats.getValuesStats().entrySet()) {
                    seriesCountByMetricName.add(new NameValuePair(entry.getKey(), entry.getValue()));
                }
                // Sort by count descending
                seriesCountByMetricName.sort(Comparator.comparingLong(NameValuePair::value).reversed());
            }
            writeNameValueArray(builder, "seriesCountByMetricName", seriesCountByMetricName);

            // labelValueCountByLabelName - count of distinct values for each label
            List<NameValuePair> labelValueCounts = new ArrayList<>();
            for (Map.Entry<String, InternalTSDBStats.LabelStats> entry : labelStatsMap.entrySet()) {
                String labelName = entry.getKey();
                InternalTSDBStats.LabelStats labelStat = entry.getValue();
                long valueCount = labelStat.getValues() != null ? labelStat.getValues().size() : 0;
                labelValueCounts.add(new NameValuePair(labelName, valueCount));
            }
            // Sort by count descending
            labelValueCounts.sort(Comparator.comparingLong(NameValuePair::value).reversed());
            writeNameValueArray(builder, "labelValueCountByLabelName", labelValueCounts);

            // memoryInBytesByLabelName - estimated memory usage by label name
            // Follows Prometheus approach: (len(name) + header + len(value) + header) * numSeries
            // See: https://github.com/prometheus/prometheus/blob/main/model/labels/labels_slicelabels.go#L520
            List<NameValuePair> memoryByLabel = new ArrayList<>();
            for (Map.Entry<String, InternalTSDBStats.LabelStats> entry : labelStatsMap.entrySet()) {
                String labelName = entry.getKey();
                InternalTSDBStats.LabelStats labelStat = entry.getValue();
                long memoryBytes = 0;

                // String header overhead in Java: ~24 bytes (object header + hashcode + length)
                final long STRING_HEADER_BYTES = 24;

                if (labelStat.getValuesStats() != null) {
                    // Calculate memory for each label name/value pair weighted by series count
                    for (Map.Entry<String, Long> valueEntry : labelStat.getValuesStats().entrySet()) {
                        String value = valueEntry.getKey();
                        long numSeries = valueEntry.getValue();

                        // Memory for label name: UTF-16 chars (2 bytes each) + header
                        long nameBytes = (labelName.length() * 2L) + STRING_HEADER_BYTES;
                        // Memory for label value: UTF-16 chars (2 bytes each) + header
                        long valueBytes = (value.length() * 2L) + STRING_HEADER_BYTES;
                        // Total memory = (name + value) * number of series with this label value
                        memoryBytes += (nameBytes + valueBytes) * numSeries;
                    }
                } else if (labelStat.getValues() != null) {
                    // Fallback when valuesStats is null but values list exists
                    // Assume 1 series per value for estimation
                    for (String value : labelStat.getValues()) {
                        long nameBytes = (labelName.length() * 2L) + STRING_HEADER_BYTES;
                        long valueBytes = (value.length() * 2L) + STRING_HEADER_BYTES;
                        memoryBytes += nameBytes + valueBytes;
                    }
                }
                memoryByLabel.add(new NameValuePair(labelName, memoryBytes));
            }
            // Sort by memory descending
            memoryByLabel.sort(Comparator.comparingLong(NameValuePair::value).reversed());
            writeNameValueArray(builder, "memoryInBytesByLabelName", memoryByLabel);

            // seriesCountByLabelValuePair - only include if valueStats is in includeOptions
            if (includeValueStats) {
                List<NameValuePair> seriesCountByPair = new ArrayList<>();
                for (Map.Entry<String, InternalTSDBStats.LabelStats> entry : labelStatsMap.entrySet()) {
                    String labelName = entry.getKey();
                    InternalTSDBStats.LabelStats labelStat = entry.getValue();
                    if (labelStat.getValuesStats() != null) {
                        for (Map.Entry<String, Long> valueEntry : labelStat.getValuesStats().entrySet()) {
                            String pairName = labelName + "=" + valueEntry.getKey();
                            seriesCountByPair.add(new NameValuePair(pairName, valueEntry.getValue()));
                        }
                    }
                }
                // Sort by count descending
                seriesCountByPair.sort(Comparator.comparingLong(NameValuePair::value).reversed());
                writeNameValueArray(builder, "seriesCountByLabelValuePair", seriesCountByPair);
            }
        }
    }

    /**
     * Writes an array of name-value pairs to the XContent builder.
     *
     * @param builder the XContent builder
     * @param fieldName the field name for the array
     * @param pairs the list of name-value pairs
     * @throws IOException if an I/O error occurs
     */
    private void writeNameValueArray(XContentBuilder builder, String fieldName, List<NameValuePair> pairs) throws IOException {
        builder.startArray(fieldName);
        for (NameValuePair pair : pairs) {
            builder.startObject();
            builder.field("name", pair.name());
            builder.field("value", pair.value());
            builder.endObject();
        }
        builder.endArray();
    }

    /**
     * Simple record to hold name-value pairs.
     */
    private record NameValuePair(String name, long value) {
    }

    @Override
    public void onFailure(Exception e) {
        try {
            XContentBuilder builder = channel.newErrorBuilder();
            builder.startObject();
            builder.field("error", e.getMessage());
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder));
        } catch (IOException ioException) {
            channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, ioException.getMessage()));
        }
    }
}
