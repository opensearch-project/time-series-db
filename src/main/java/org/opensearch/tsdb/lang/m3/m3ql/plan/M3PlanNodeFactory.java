/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan;

import org.opensearch.tsdb.lang.m3.common.AggregationType;
import org.opensearch.tsdb.lang.m3.common.Constants;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AbsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AggregationPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasByTagsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FallbackSeriesConstantPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ValueFilterPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FetchPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.HistogramPercentilePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.IsNonNullPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.KeepLastValuePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.MovingPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PerSecondPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.RemoveEmptyPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScalePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScaleToSecondsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SortPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SummarizePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TimeshiftPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TransformNullPlanNode;

/**
 * Factory class for creating M3QL plan nodes based on function nodes.
 */
public class M3PlanNodeFactory {

    /**
     * Private constructor to prevent instantiation.
     */
    private M3PlanNodeFactory() {}

    /**
     * Creates an M3PlanNode based on the provided FunctionNode.
     *
     * @param functionNode the function node to convert
     * @return the corresponding M3PlanNode
     * @throws IllegalArgumentException if the function name is unknown
     */
    public static M3PlanNode create(FunctionNode functionNode) {
        switch (functionNode.getFunctionName()) {
            case Constants.Functions.ABS:
            case Constants.Functions.ABSOLUTE:
                return AbsPlanNode.of(functionNode);
            case Constants.Functions.ALIAS:
                return AliasPlanNode.of(functionNode);
            case Constants.Functions.ALIAS_BY_TAGS:
                return AliasByTagsPlanNode.of(functionNode);
            case Constants.Functions.FALLBACK_SERIES:
                return FallbackSeriesConstantPlanNode.of(functionNode);
            case Constants.Functions.FETCH:
                return FetchPlanNode.of(functionNode);
            case Constants.Functions.HISTOGRAM_PERCENTILE:
                return HistogramPercentilePlanNode.of(functionNode);
            case Constants.Functions.IS_NON_NULL:
                return IsNonNullPlanNode.of(functionNode);
            case Constants.Functions.KEEP_LAST_VALUE:
                return KeepLastValuePlanNode.of(functionNode);
            case Constants.Functions.MOVING:
            case Constants.Functions.MOVING_AVERAGE:
            case Constants.Functions.MOVING_MAX:
            case Constants.Functions.MOVING_MEDIAN:
            case Constants.Functions.MOVING_MIN:
            case Constants.Functions.MOVING_SUM:
                return MovingPlanNode.of(functionNode);
            case Constants.Functions.PER_SECOND:
                return PerSecondPlanNode.of(functionNode);
            case Constants.Functions.REMOVE_EMPTY:
                return RemoveEmptyPlanNode.of(functionNode);
            case Constants.Functions.SORT:
            case Constants.Functions.SORT_SERIES:
                return SortPlanNode.of(functionNode);
            case Constants.Functions.SUMMARIZE:
                return SummarizePlanNode.of(functionNode);
            case Constants.Functions.SCALE:
                return ScalePlanNode.of(functionNode);
            case Constants.Functions.SCALE_TO_SECONDS:
                return ScaleToSecondsPlanNode.of(functionNode);
            case Constants.Functions.TIMESHIFT:
                return TimeshiftPlanNode.of(functionNode);
            case Constants.Functions.TRANSFORM_NULL:
                return TransformNullPlanNode.of(functionNode);
            case Constants.Functions.ValueFilter.EQ:
            case Constants.Functions.ValueFilter.EQUALS:
            case Constants.Functions.ValueFilter.GE:
            case Constants.Functions.ValueFilter.GREATER_EQUAL:
            case Constants.Functions.ValueFilter.REMOVE_BELOW_VALUE:
            case Constants.Functions.ValueFilter.LE:
            case Constants.Functions.ValueFilter.LESS_EQUAL:
            case Constants.Functions.ValueFilter.REMOVE_ABOVE_VALUE:
            case Constants.Functions.ValueFilter.NE:
            case Constants.Functions.ValueFilter.NOT_EQUALS:
            case Constants.Functions.ValueFilter.GT:
            case Constants.Functions.ValueFilter.GREATER_THAN:
            case Constants.Functions.ValueFilter.LESS_THAN:
            case Constants.Functions.ValueFilter.LT:
                return ValueFilterPlanNode.of(functionNode);
            default:
                try {
                    return AggregationPlanNode.of(functionNode, AggregationType.fromString(functionNode.getFunctionName()));
                } catch (IllegalArgumentException ignored) {
                    throw new IllegalArgumentException("Unknown function: " + functionNode.getFunctionName());
                }
        }
    }
}
