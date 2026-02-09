/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.lang.m3.common.SortByType;
import org.opensearch.tsdb.lang.m3.common.SortOrderType;
import org.opensearch.tsdb.lang.m3.stage.util.SortComparatorUtil;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Pipeline stage that implements M3QL's sort function.
 *
 * These functions are used to sort time series by avg, current, max, min, name, stddev, or sum.
 * sort also takes an optional second argument, asc or desc respectively, which specifies
 * whether the data should be sorted in ascending or descending order.
 * If no direction is specified, the default is descending order.
 *
 * The sorting process:
 * 1. For each time series, calculate the sorting key based on the selected function
 *    - Numeric sorts (avg, current, max, min, sum, stddev): use calculated values
 *    - String sorts (name): use the alias of the time series
 * 2. Sort the time series based on the sorting key, keeping each time series unchanged
 *
 * Usage: fetch a | sort avg desc
 *
 * Note: This is a global aggregation stage that operates on all time series at once.
 */
@PipelineStageAnnotation(name = "sort")
public class SortStage implements UnaryPipelineStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "sort";
    /** The argument name for sortBy parameter. */
    public static final String SORT_BY_ARG = "sortBy";
    /** The argument name for sortOrder parameter. */
    public static final String SORT_ORDER_ARG = "sortOrder";

    private final SortByType sortBy;
    private final SortOrderType sortOrder;

    /**
     * Constructs a new SortStage with the specified sort criteria and order.
     *
     * @param sortBy the criteria to sort by (avg, current, max, min, sum, stddev)
     * @param sortOrder the order to sort in (asc, desc)
     */
    public SortStage(SortByType sortBy, SortOrderType sortOrder) {
        this.sortBy = sortBy;
        this.sortOrder = sortOrder;
    }

    /**
     * Constructs a new SortStage with the specified sort criteria and default descending order.
     *
     * @param sortBy the criteria to sort by (avg, current, max, min, sum, stddev)
     */
    public SortStage(SortByType sortBy) {
        this(sortBy, SortOrderType.DESC); // Default to descending order
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        if (input.isEmpty()) {
            return input;
        }

        // Create a copy to avoid modifying the original list
        List<TimeSeries> result = new ArrayList<>(input);

        // Sort time series based on the calculated sorting key
        // Each time series remains unchanged, only the order changes
        Comparator<TimeSeries> comparator = SortComparatorUtil.createComparator(sortBy);

        if (sortOrder == SortOrderType.ASC) {
            result.sort(comparator);
        } else {
            result.sort(comparator.reversed());
        }

        return result;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Get the sort criteria.
     * @return the sort criteria
     */
    public SortByType getSortBy() {
        return sortBy;
    }

    /**
     * Get the sort order.
     * @return the sort order
     */
    public SortOrderType getSortOrder() {
        return sortOrder;
    }

    @Override
    public boolean isGlobalAggregation() {
        return true;
    }

    @Override
    public boolean isCoordinatorOnly() {
        return true;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(SORT_BY_ARG, sortBy.getValue());
        builder.field(SORT_ORDER_ARG, sortOrder.getValue());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sortBy.getValue());
        out.writeString(sortOrder.getValue());
    }

    /**
     * Create a SortStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new SortStage instance with the deserialized parameters
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static SortStage readFrom(StreamInput in) throws IOException {
        String sortByName = in.readString();
        String sortOrderName = in.readString();

        SortByType sortBy = SortByType.fromString(sortByName);
        SortOrderType sortOrder = SortOrderType.fromString(sortOrderName);

        return new SortStage(sortBy, sortOrder);
    }

    /**
     * Create a SortStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return SortStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static SortStage fromArgs(Map<String, Object> args) {
        if (args == null || !args.containsKey(SORT_BY_ARG)) {
            throw new IllegalArgumentException("Sort stage requires '" + SORT_BY_ARG + "' argument");
        }

        Object sortByObj = args.get(SORT_BY_ARG);
        if (sortByObj == null) {
            throw new IllegalArgumentException("SortBy cannot be null");
        }

        SortByType sortBy;
        if (sortByObj instanceof String sortByStr) {
            sortBy = SortByType.fromString(sortByStr);
        } else {
            throw new IllegalArgumentException(
                "Invalid type for '" + SORT_BY_ARG + "' argument. Expected String, but got " + sortByObj.getClass().getSimpleName()
            );
        }

        SortOrderType sortOrder = SortOrderType.DESC; // Default
        if (args.containsKey(SORT_ORDER_ARG)) {
            Object sortOrderObj = args.get(SORT_ORDER_ARG);
            if (sortOrderObj != null) {
                if (sortOrderObj instanceof String sortOrderStr) {
                    sortOrder = SortOrderType.fromString(sortOrderStr);
                } else {
                    throw new IllegalArgumentException(
                        "Invalid type for '"
                            + SORT_ORDER_ARG
                            + "' argument. Expected String, but got "
                            + sortOrderObj.getClass().getSimpleName()
                    );
                }
            }
        }

        return new SortStage(sortBy, sortOrder);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SortStage other = (SortStage) obj;
        return sortBy == other.sortBy && sortOrder == other.sortOrder;
    }

    @Override
    public int hashCode() {
        int result = sortBy != null ? sortBy.hashCode() : 0;
        result = 31 * result + (sortOrder != null ? sortOrder.hashCode() : 0);
        return result;
    }
}
