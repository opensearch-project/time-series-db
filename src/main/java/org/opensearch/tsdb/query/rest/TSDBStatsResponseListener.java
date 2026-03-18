/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.tsdb.query.aggregator.InternalTSDBStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.query.utils.TSDBStatsConstants.AGGREGATION_NAME;
import static org.opensearch.tsdb.query.utils.TSDBStatsConstants.FORMAT_FLAT;
import static org.opensearch.tsdb.query.utils.TSDBStatsConstants.INCLUDE_ALL;
import static org.opensearch.tsdb.query.utils.TSDBStatsConstants.INCLUDE_HEAD_STATS;
import static org.opensearch.tsdb.query.utils.TSDBStatsConstants.INCLUDE_LABEL_VALUES;
import static org.opensearch.tsdb.query.utils.TSDBStatsConstants.INCLUDE_VALUE_STATS;

/**
 * Response listener for TSDB stats queries.
 *
 * <p>This listener handles formatting the response in either 'grouped' or 'flat' format
 * based on the request parameters.</p>
 */
public class TSDBStatsResponseListener extends RestToXContentListener<SearchResponse> {

    // Response field names
    private static final String FIELD_ERROR = "error";
    private static final String FIELD_HEAD_STATS = "headStats";
    private static final String FIELD_NUM_SERIES = "numSeries";
    private static final String FIELD_NUM_CHUNKS = "numChunks";

    private static final String FIELD_MIN_TIME = "minTime";
    private static final String FIELD_MAX_TIME = "maxTime";
    private static final String FIELD_LABEL_STATS = "labelStats";
    private static final String FIELD_VALUES = "values";
    private static final String FIELD_VALUES_STATS = "valuesStats";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_VALUE = "value";
    private static final String FIELD_SERIES_COUNT_BY_METRIC_NAME = "seriesCountByMetricName";
    private static final String FIELD_LABEL_VALUE_COUNT_BY_LABEL_NAME = "labelValueCountByLabelName";
    private static final String FIELD_MEMORY_IN_BYTES_BY_LABEL_NAME = "memoryInBytesByLabelName";
    private static final String FIELD_SERIES_COUNT_BY_LABEL_VALUE_PAIR = "seriesCountByLabelValuePair";

    // Label name for metric name
    private static final String LABEL_NAME = "name";

    // String header overhead in Java: ~24 bytes (object header + hashcode + length)
    private static final long STRING_HEADER_BYTES = 24;

    private final List<String> includeOptions;
    private final String format;

    /**
     * Creates a new TSDBStatsResponseListener.
     *
     * @param channel the REST channel to send the response to
     * @param includeOptions list of stats to include (headStats, labelValues, valueStats)
     * @param format the response format (grouped or flat)
     */
    public TSDBStatsResponseListener(RestChannel channel, List<String> includeOptions, String format) {
        super(channel);
        this.includeOptions = includeOptions;
        this.format = format;
    }

    /**
     * Builds the REST response from a search response by transforming it into the requested format.
     *
     * @param searchResponse the search response to transform
     * @param builder the XContent builder to use for constructing the response
     * @return a REST response with the transformed data
     * @throws Exception if an error occurs during transformation
     */
    @Override
    public RestResponse buildResponse(SearchResponse searchResponse, XContentBuilder builder) throws Exception {
        try {
            return buildStatsResponse(searchResponse, builder);
        } catch (Exception e) {
            // Discard the partially-written builder and create a fresh one for the error response
            XContentBuilder errorBuilder = channel.newErrorBuilder();
            return buildErrorResponse(errorBuilder, e);
        }
    }

    /**
     * Builds the stats response from a search response.
     */
    private RestResponse buildStatsResponse(SearchResponse searchResponse, XContentBuilder builder) throws IOException {
        builder.startObject();

        if (searchResponse.getAggregations() == null) {
            builder.field(FIELD_ERROR, "No aggregations in response");
            builder.endObject();
            return new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
        }

        Aggregation agg = searchResponse.getAggregations().get(AGGREGATION_NAME);
        if (!(agg instanceof InternalTSDBStats)) {
            builder.field(FIELD_ERROR, "Unexpected aggregation type");
            builder.endObject();
            return new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
        }

        InternalTSDBStats tsdbStats = (InternalTSDBStats) agg;

        if (FORMAT_FLAT.equals(format)) {
            formatFlatResponse(tsdbStats, builder);
        } else {
            formatGroupedResponse(tsdbStats, builder);
        }

        builder.endObject();
        return new BytesRestResponse(RestStatus.OK, builder);
    }

    /**
     * Builds an error response when processing fails.
     *
     * @param builder the XContent builder to use for the error response
     * @param error the exception that caused the error
     * @return a REST response with error details
     * @throws IOException if an I/O error occurs during writing
     */
    private RestResponse buildErrorResponse(XContentBuilder builder, Exception error) throws IOException {
        builder.startObject();
        builder.field(FIELD_ERROR, error.getMessage());
        builder.endObject();
        return new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder);
    }

    /**
     * Formats the response in 'grouped' format.
     *
     * @param stats the TSDB stats aggregation result
     * @param builder the XContent builder
     * @throws IOException if an I/O error occurs
     */
    private void formatGroupedResponse(InternalTSDBStats stats, XContentBuilder builder) throws IOException {
        boolean includeValueStats = includeOptions.contains(INCLUDE_ALL) || includeOptions.contains(INCLUDE_VALUE_STATS);
        boolean includeHeadStats = includeOptions.contains(INCLUDE_ALL) || includeOptions.contains(INCLUDE_HEAD_STATS);
        boolean includeLabelStats = includeOptions.contains(INCLUDE_ALL) || includeOptions.contains(INCLUDE_LABEL_VALUES);

        if (includeHeadStats && stats.getHeadStats() != null) {
            writeHeadStats(stats.getHeadStats(), builder);
        }

        if (includeLabelStats) {
            builder.startObject(FIELD_LABEL_STATS);

            if (stats.getNumSeries() != null) {
                builder.field(FIELD_NUM_SERIES, stats.getNumSeries());
            }

            for (Map.Entry<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> entry : stats.getLabelStats().entrySet()) {
                builder.startObject(entry.getKey());
                InternalTSDBStats.CoordinatorLevelStats.LabelStats labelStats = entry.getValue();
                if (labelStats.numSeries() != null) {
                    builder.field(FIELD_NUM_SERIES, labelStats.numSeries());
                }
                builder.field(FIELD_VALUES, labelStats.valuesStats().keySet());
                if (includeValueStats) {
                    builder.field(FIELD_VALUES_STATS, labelStats.valuesStats());
                }
                builder.endObject();
            }
            builder.endObject();
        }
    }

    /**
     * Formats the response in 'flat' format.
     *
     * @param stats the TSDB stats aggregation result
     * @param builder the XContent builder
     * @throws IOException if an I/O error occurs
     */
    private void formatFlatResponse(InternalTSDBStats stats, XContentBuilder builder) throws IOException {
        boolean includeValueStats = includeOptions.contains(INCLUDE_ALL) || includeOptions.contains(INCLUDE_VALUE_STATS);
        boolean includeHeadStats = includeOptions.contains(INCLUDE_ALL) || includeOptions.contains(INCLUDE_HEAD_STATS);
        boolean includeLabelValues = includeOptions.contains(INCLUDE_ALL) || includeOptions.contains(INCLUDE_LABEL_VALUES);

        if (includeHeadStats && stats.getHeadStats() != null) {
            writeHeadStats(stats.getHeadStats(), builder);
        }

        if (!includeLabelValues) {
            return;
        }

        Map<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> labelStatsMap = stats.getLabelStats();

        // seriesCountByMetricName - series count for each name value
        List<NameValuePair> seriesCountByMetricName = new ArrayList<>();
        InternalTSDBStats.CoordinatorLevelStats.LabelStats nameLabelStats = labelStatsMap.get(LABEL_NAME);
        if (nameLabelStats != null) {
            for (Map.Entry<String, Long> entry : nameLabelStats.valuesStats().entrySet()) {
                seriesCountByMetricName.add(new NameValuePair(entry.getKey(), entry.getValue()));
            }
            seriesCountByMetricName.sort(Comparator.comparingLong(NameValuePair::value).reversed());
        }
        writeNameValueArray(builder, FIELD_SERIES_COUNT_BY_METRIC_NAME, seriesCountByMetricName);

        // labelValueCountByLabelName - count of distinct values for each label
        List<NameValuePair> labelValueCounts = new ArrayList<>();
        for (Map.Entry<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> entry : labelStatsMap.entrySet()) {
            long valueCount = entry.getValue().valuesStats().size();
            labelValueCounts.add(new NameValuePair(entry.getKey(), valueCount));
        }
        labelValueCounts.sort(Comparator.comparingLong(NameValuePair::value).reversed());
        writeNameValueArray(builder, FIELD_LABEL_VALUE_COUNT_BY_LABEL_NAME, labelValueCounts);

        // memoryInBytesByLabelName - estimated memory usage by label name
        // Follows Prometheus approach: (len(name) + header + len(value) + header) * numSeries
        List<NameValuePair> memoryByLabel = new ArrayList<>();
        for (Map.Entry<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> entry : labelStatsMap.entrySet()) {
            String labelName = entry.getKey();
            InternalTSDBStats.CoordinatorLevelStats.LabelStats labelStat = entry.getValue();
            long memoryBytes = 0;

            for (Map.Entry<String, Long> valueEntry : labelStat.valuesStats().entrySet()) {
                String value = valueEntry.getKey();
                long numSeries = valueEntry.getValue();

                long nameBytes = (labelName.length() * 2L) + STRING_HEADER_BYTES;
                long valueBytes = (value.length() * 2L) + STRING_HEADER_BYTES;
                // When numSeries is 0 (includeValueStats=false), assume 1 series per value for estimation
                memoryBytes += (nameBytes + valueBytes) * Math.max(numSeries, 1);
            }
            memoryByLabel.add(new NameValuePair(labelName, memoryBytes));
        }
        memoryByLabel.sort(Comparator.comparingLong(NameValuePair::value).reversed());
        writeNameValueArray(builder, FIELD_MEMORY_IN_BYTES_BY_LABEL_NAME, memoryByLabel);

        // seriesCountByLabelValuePair - only include if valueStats is in includeOptions
        if (includeValueStats) {
            List<NameValuePair> seriesCountByPair = new ArrayList<>();
            for (Map.Entry<String, InternalTSDBStats.CoordinatorLevelStats.LabelStats> entry : labelStatsMap.entrySet()) {
                String labelName = entry.getKey();
                for (Map.Entry<String, Long> valueEntry : entry.getValue().valuesStats().entrySet()) {
                    String pairName = labelName + "=" + valueEntry.getKey();
                    seriesCountByPair.add(new NameValuePair(pairName, valueEntry.getValue()));
                }
            }
            seriesCountByPair.sort(Comparator.comparingLong(NameValuePair::value).reversed());
            writeNameValueArray(builder, FIELD_SERIES_COUNT_BY_LABEL_VALUE_PAIR, seriesCountByPair);
        }
    }

    /**
     * Writes headStats to the XContent builder.
     */
    private void writeHeadStats(InternalTSDBStats.HeadStats headStats, XContentBuilder builder) throws IOException {
        builder.startObject(FIELD_HEAD_STATS);
        builder.field(FIELD_NUM_SERIES, headStats.numSeries());
        builder.field(FIELD_NUM_CHUNKS, headStats.numChunks());
        builder.field(FIELD_MIN_TIME, headStats.minTime());
        builder.field(FIELD_MAX_TIME, headStats.maxTime());
        builder.endObject();
    }

    /**
     * Writes an array of name-value pairs to the XContent builder.
     */
    private void writeNameValueArray(XContentBuilder builder, String fieldName, List<NameValuePair> pairs) throws IOException {
        builder.startArray(fieldName);
        for (NameValuePair pair : pairs) {
            builder.startObject();
            builder.field(FIELD_NAME, pair.name());
            builder.field(FIELD_VALUE, pair.value());
            builder.endObject();
        }
        builder.endArray();
    }

    /**
     * Simple record to hold name-value pairs.
     */
    private record NameValuePair(String name, long value) {
    }
}
