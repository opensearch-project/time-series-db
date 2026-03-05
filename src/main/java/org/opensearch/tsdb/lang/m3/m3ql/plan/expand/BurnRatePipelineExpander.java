/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.expand;

import org.opensearch.tsdb.lang.m3.common.AggregationType;
import org.opensearch.tsdb.lang.m3.common.Constants;
import org.opensearch.tsdb.lang.m3.common.M3Duration;
import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.common.WindowAggregationType;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AggregationPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AsPercentPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.MovingPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScalePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TransformNullPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.UnionPlanNode;

import java.util.List;
import java.util.function.Function;

/**
 * Expands burnRate / asBurnRate / multiBurnRate / asMultiBurnRate into plan chains.
 * <p>
 * burnRate(total) interval slo → moving | asPercent | scale(1/(100-slo)) | transformNull(0)
 * multiBurnRate(total) interval1 interval2 slo → min(union(burnRate(interval1), burnRate(interval2)))
 */
public final class BurnRatePipelineExpander implements PipelineExpander {

    @Override
    public M3PlanNode expand(
        List<M3ASTNode> pipelineChildren,
        int lhsEndIndex,
        FunctionNode functionNode,
        Function<M3ASTNode, M3PlanNode> planner,
        Function<List<M3ASTNode>, M3PlanNode> plannerSlice
    ) {
        String fn = functionNode.getFunctionName();
        return switch (fn) {
            case Constants.Functions.Binary.BURN_RATE, Constants.Functions.Binary.AS_BURN_RATE -> expandBurnRate(
                pipelineChildren,
                lhsEndIndex,
                functionNode,
                planner,
                plannerSlice
            );

            case Constants.Functions.Binary.MULTI_BURN_RATE, Constants.Functions.Binary.AS_MULTI_BURN_RATE -> expandMultiBurnRate(
                pipelineChildren,
                lhsEndIndex,
                functionNode,
                plannerSlice
            );

            default -> throw new IllegalArgumentException("Unsupported pipeline expander function: " + fn);
        };
    }

    private M3PlanNode expandBurnRate(
        List<M3ASTNode> pipelineChildren,
        int lhsEndIndex,
        FunctionNode functionNode,
        Function<M3ASTNode, M3PlanNode> planner,
        Function<List<M3ASTNode>, M3PlanNode> plannerSlice
    ) {
        validateChildCount(functionNode, 3);
        String interval = extractIntervalParameter(functionNode, 1);
        double slo = extractSloParameter(functionNode, 2);

        M3PlanNode lhs = plannerSlice.apply(pipelineChildren.subList(0, lhsEndIndex));
        M3PlanNode rhs = planner.apply(functionNode.getChildren().getFirst());
        return buildBurnRateChain(lhs, rhs, interval, slo);
    }

    private M3PlanNode expandMultiBurnRate(
        List<M3ASTNode> pipelineChildren,
        int lhsEndIndex,
        FunctionNode functionNode,
        Function<List<M3ASTNode>, M3PlanNode> plannerSlice
    ) {
        validateChildCount(functionNode, 4);
        String interval1 = extractIntervalParameter(functionNode, 1);
        String interval2 = extractIntervalParameter(functionNode, 2);
        double slo = extractSloParameter(functionNode, 3);

        M3ASTNode rhsAst = functionNode.getChildren().getFirst();
        M3PlanNode rhs1 = plannerSlice.apply(List.of(rhsAst));
        M3PlanNode rhs2 = plannerSlice.apply(List.of(rhsAst));

        List<M3ASTNode> lhsSlice = pipelineChildren.subList(0, lhsEndIndex);
        M3PlanNode lhs1 = plannerSlice.apply(lhsSlice);
        M3PlanNode lhs2 = plannerSlice.apply(lhsSlice);

        M3PlanNode burnRate1 = buildBurnRateChain(lhs1, rhs1, interval1, slo);
        M3PlanNode burnRate2 = buildBurnRateChain(lhs2, rhs2, interval2, slo);

        UnionPlanNode union = new UnionPlanNode(M3PlannerContext.generateId());
        union.addChild(burnRate1);
        union.addChild(burnRate2);

        AggregationPlanNode min = new AggregationPlanNode(M3PlannerContext.generateId(), AggregationType.MIN, List.of());
        min.addChild(union);
        return min;
    }

    private M3PlanNode buildBurnRateChain(M3PlanNode lhs, M3PlanNode rhs, String interval, double slo) {
        double scaleFactor = 1.0 / (100.0 - slo);

        MovingPlanNode movingLeft = new MovingPlanNode(M3PlannerContext.generateId(), interval, WindowAggregationType.SUM);
        movingLeft.addChild(lhs);

        MovingPlanNode movingRight = new MovingPlanNode(M3PlannerContext.generateId(), interval, WindowAggregationType.SUM);
        movingRight.addChild(rhs);

        AsPercentPlanNode asPercent = new AsPercentPlanNode(M3PlannerContext.generateId(), List.of());
        asPercent.addChild(movingLeft);
        asPercent.addChild(movingRight);

        ScalePlanNode scale = new ScalePlanNode(M3PlannerContext.generateId(), scaleFactor);
        scale.addChild(asPercent);

        TransformNullPlanNode transformNull = new TransformNullPlanNode(M3PlannerContext.generateId(), 0.0);
        transformNull.addChild(scale);

        return transformNull;
    }

    private void validateChildCount(FunctionNode functionNode, int expectedCount) {
        int actual = functionNode.getChildren().size();
        if (actual != expectedCount) {
            throw new IllegalArgumentException(
                functionNode.getFunctionName() + " expects exactly " + expectedCount + " arguments, got " + actual
            );
        }
    }

    private String extractIntervalParameter(FunctionNode functionNode, int index) {
        M3ASTNode child = functionNode.getChildren().get(index);
        if (!(child instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Argument 'interval' of " + functionNode.getFunctionName() + " must be a value");
        }
        String interval = Utils.stripDoubleQuotes(valueNode.getValue());
        M3Duration.valueOf(interval);
        return interval;
    }

    private double extractSloParameter(FunctionNode functionNode, int index) {
        M3ASTNode child = functionNode.getChildren().get(index);
        if (!(child instanceof ValueNode valueNode)) {
            throw new IllegalArgumentException("Argument 'slo' of " + functionNode.getFunctionName() + " must be a value");
        }
        String sloStr = Utils.stripDoubleQuotes(valueNode.getValue());
        double slo;
        try {
            slo = Double.parseDouble(sloStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("SLO must be a numeric value, got: " + sloStr, e);
        }
        Utils.validateSlo(slo);
        return slo;
    }

}
