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
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ExcludeByTagPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.DerivativePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.DivideScalarPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FallbackSeriesConstantPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TagSubPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ValueFilterPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FetchPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.HeadPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.HistogramPercentilePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.IntegralPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.IsNonNullPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.KeepLastValuePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.LogarithmPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.MovingPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.OffsetPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PerSecondPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PerSecondRatePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PercentileOfSeriesPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.RemoveEmptyPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.RoundPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScalePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScaleToSecondsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ShowTagsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SqrtPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SortPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SustainPlanNode;
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
     * @throws UnsupportedOperationException if the function is a known M3QL function that is not yet implemented
     * @throws IllegalArgumentException if the function name is unknown or invalid
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
            case Constants.Functions.DERIVATIVE:
                return DerivativePlanNode.of(functionNode);
            case Constants.Functions.EXCLUDE_BY_TAG:
                return ExcludeByTagPlanNode.of(functionNode);
            case Constants.Functions.TAG_SUB:
                return TagSubPlanNode.of(functionNode);
            case Constants.Functions.SHOW_TAGS:
                return ShowTagsPlanNode.of(functionNode);
            case Constants.Functions.FALLBACK_SERIES:
                return FallbackSeriesConstantPlanNode.of(functionNode);
            case Constants.Functions.FETCH:
                return FetchPlanNode.of(functionNode);
            case Constants.Functions.HEAD:
                return HeadPlanNode.of(functionNode);
            case Constants.Functions.HISTOGRAM_PERCENTILE:
                return HistogramPercentilePlanNode.of(functionNode);
            case Constants.Functions.INTEGRAL:
                return IntegralPlanNode.of(functionNode);
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
            case Constants.Functions.PERCENTILE_OF_SERIES:
            case Constants.Functions.MEDIAN:
            case Constants.Functions.MEDIAN_OF_SERIES:
                return PercentileOfSeriesPlanNode.of(functionNode);
            case Constants.Functions.PER_SECOND:
                return PerSecondPlanNode.of(functionNode);
            case Constants.Functions.PER_SECOND_RATE:
                return PerSecondRatePlanNode.of(functionNode);
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
            case Constants.Functions.DIVIDE_SCALAR:
                return DivideScalarPlanNode.of(functionNode);
            case Constants.Functions.SUSTAIN:
                return SustainPlanNode.of(functionNode);
            case Constants.Functions.TIMESHIFT:
                return TimeshiftPlanNode.of(functionNode);
            case Constants.Functions.TRANSFORM_NULL:
                return TransformNullPlanNode.of(functionNode);
            case Constants.Functions.LOGARITHM:
            case Constants.Functions.LOG:
                return LogarithmPlanNode.of(functionNode);
            case Constants.Functions.SQRT:
            case Constants.Functions.SQUARE_ROOT:
                return SqrtPlanNode.of(functionNode);
            case Constants.Functions.OFFSET:
                return OffsetPlanNode.of(functionNode);
            case Constants.Functions.ROUND:
                return RoundPlanNode.of(functionNode);
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
                    String functionName = functionNode.getFunctionName();
                    // Check if this is a known M3QL function that's not yet implemented
                    if (Constants.Functions.KNOWN_UNIMPLEMENTED_FUNCTIONS.contains(functionName)) {
                        throw new UnsupportedOperationException("Function '" + functionName + "' is not implemented");
                    }
                    // For unknown/invalid function names, throw regular IllegalArgumentException
                    throw new IllegalArgumentException("Unknown function: " + functionName);
                }
        }
    }
}
