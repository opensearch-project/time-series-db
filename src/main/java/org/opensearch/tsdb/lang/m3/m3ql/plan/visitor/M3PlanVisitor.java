/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.visitor;

import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AbsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AggregationPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasByTagsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AliasPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.BinaryPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.DerivativePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.DivideScalarPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ExcludeByTagPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FallbackSeriesConstantPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.RoundPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ValueFilterPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FetchPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.HeadPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.HistogramPercentilePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.IntegralPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.KeepLastValuePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.LogarithmPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.MovingPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.OffsetPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PerSecondPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PerSecondRatePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.PercentileOfSeriesPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.IsNonNullPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.RemoveEmptyPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScalePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScaleToSecondsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ShowTagsPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SortPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SqrtPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SustainPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.SummarizePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TagSubPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TimeshiftPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TransformNullPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.UnionPlanNode;

/**
 * Visitor for M3QL plan nodes.
 * @param <T> The return type of the visitor methods
 */
public abstract class M3PlanVisitor<T> {

    /**
     * Protected constructor to allow extension.
     */
    protected M3PlanVisitor() {
        // Allow extension
    }

    /**
     * Default process method for a M3PlanNode.
     * @param planNode the plan node to process
     * @return the result of processing the plan node
     */
    public abstract T process(M3PlanNode planNode);

    /**
     * Visit method for AbsPlanNode.
     * @param planNode the AbsPlanNode to visit
     * @return the result of processing the AbsPlanNode
     */
    public T visit(AbsPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for AggregationPlanNode.
     * @param planNode the AggregationPlanNode to visit
     * @return the result of processing the AggregationPlanNode
     */
    public T visit(AggregationPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for AliasPlanNode.
     * @param planNode the AliasPlanNode to visit
     * @return the result of processing the AliasPlanNode
     */
    public T visit(AliasPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for AliasByTagsPlanNode.
     * @param planNode the AliasByTagsPlanNode to visit
     * @return the result of processing the AliasByTagsPlanNode
     */
    public T visit(AliasByTagsPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for BinaryPlanNode.
     * @param planNode the BinaryPlanNode to visit
     * @return the result of processing the BinaryPlanNode
     */
    public T visit(BinaryPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for ExcludeByTagPlanNode.
     * @param planNode the ExcludeByTagPlanNode to visit
     * @return the result of processing the ExcludeByTagPlanNode
     */
    public T visit(ExcludeByTagPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for TagSubPlanNode.
     * @param planNode the TagSubPlanNode to visit
     * @return the result of processing the TagSubPlanNode
     */
    public T visit(TagSubPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for FallbackSeriesConstantPlanNode.
     * @param planNode the FallbackSeriesConstantPlanNode to visit
     * @return the result of processing the FallbackSeriesConstantPlanNode
     */
    public T visit(FallbackSeriesConstantPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for FetchPlanNode.
     * @param planNode the FetchPlanNode to visit
     * @return the result of processing the FetchPlanNode
     */
    public T visit(FetchPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for DerivativePlanNode.
     * @param planNode the DerivativePlanNode to visit
     * @return the result of processing the DerivativePlanNode
     */
    public T visit(DerivativePlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for DivideScalarPlanNode.
     * @param planNode the DivideScalarPlanNode to visit
     * @return the result of processing the DivideScalarPlanNode
     */
    public T visit(DivideScalarPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for HeadPlanNode.
     * @param planNode the HeadPlanNode to visit
     * @return the result of processing the HeadPlanNode
     */
    public T visit(HeadPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for HistogramPercentilePlanNode.
     * @param planNode the HistogramPercentilePlanNode to visit
     * @return the result of processing the HistogramPercentilePlanNode
     */
    public T visit(HistogramPercentilePlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for IntegralPlanNode.
     * @param planNode the IntegralPlanNode to visit
     * @return the result of processing the IntegralPlanNode
     */
    public T visit(IntegralPlanNode planNode) {
        return process(planNode);
    }

    /** Visit method for KeepLastValuePlanNode.
     * @param planNode the KeepLastValuePlanNode to visit
     * @return the result of processing the KeepLastValuePlanNode
     */
    public T visit(KeepLastValuePlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for LogarithmPlanNode.
     * @param planNode the LogarithmPlanNode to visit
     * @return the result of processing the LogarithmPlanNode
     */
    public T visit(LogarithmPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for MovingPlanNode.
     * @param planNode the MovingPlanNode to visit
     * @return the result of processing the MovingPlanNode
     */
    public T visit(MovingPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for OffsetPlanNode.
     * @param planNode the OffsetPlanNode to visit
     * @return the result of processing the OffsetPlanNode
     */
    public T visit(OffsetPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for PerSecondPlanNode.
     * @param planNode the PerSecondPlanNode to visit
     * @return the result of processing the PerSecondPlanNode
     */
    public T visit(PerSecondPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for PerSecondRatePlanNode.
     * @param planNode the PerSecondRatePlanNode to visit
     * @return the result of processing the PerSecondRatePlanNode
     */
    public T visit(PerSecondRatePlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for PercentileOfSeriesPlanNode.
     * @param planNode the PercentileOfSeriesPlanNode to visit
     * @return the result of processing the PercentileOfSeriesPlanNode
     */
    public T visit(PercentileOfSeriesPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for IsNonNullPlanNode.
     * @param planNode the IsNonNullPlanNode to visit
     * @return the result of processing the IsNonNullPlanNode
     */
    public T visit(IsNonNullPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for RemoveEmptyPlanNode.
     * @param planNode the RemoveEmptyPlanNode to visit
     * @return the result of processing the RemoveEmptyPlanNode
     */
    public T visit(RemoveEmptyPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for RoundPlanNode.
     * @param planNode the RoundPlanNode to visit
     * @return the result of processing the RoundPlanNode
     */
    public T visit(RoundPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for ScalePlanNode.
     * @param planNode the ScalePlanNode to visit
     * @return the result of processing the ScalePlanNode
     */
    public T visit(ScalePlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for ScaleToSecondsPlanNode.
     * @param planNode the ScaleToSecondsPlanNode to visit
     * @return the result of processing the ScaleToSecondsPlanNode
     */
    public T visit(ScaleToSecondsPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for ShowTagsPlanNode.
     * @param planNode the ShowTagsPlanNode to visit
     * @return the result of processing the ShowTagsPlanNode
     */
    public T visit(ShowTagsPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for SortPlanNode.
     * @param planNode the SortPlanNode to visit
     * @return the result of processing the SortPlanNode
     */
    public T visit(SortPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for SqrtPlanNode.
     * @param planNode the SqrtPlanNode to visit
     * @return the result of processing the SqrtPlanNode
     */
    public T visit(SqrtPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for SummarizePlanNode.
     * @param planNode the SummarizePlanNode to visit
     * @return the result of processing the SummarizePlanNode
     */
    public T visit(SummarizePlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for SustainPlanNode.
     * @param planNode the SustainPlanNode to visit
     * @return the result of processing the SustainPlanNode
     */
    public T visit(SustainPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for TimeshiftPlanNode.
     * @param planNode the TimeshiftPlanNode to visit
     * @return the result of processing the TimeshiftPlanNode
     */
    public T visit(TimeshiftPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for TransformNullPlanNode.
     * @param planNode the TransformNullPlanNode to visit
     * @return the result of processing the TransformNullPlanNode
     */
    public T visit(TransformNullPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for UnionPlanNode.
     * @param planNode the UnionPlanNode to visit
     * @return the result of processing the UnionPlanNode
     */
    public T visit(UnionPlanNode planNode) {
        return process(planNode);
    }

    /**
     * Visit method for ValueFilterPlanNode.
     * @param planNode the ValueFilterPlanNode to visit
     * @return the result of processing the ValueFilterPlanNode
     */
    public T visit(ValueFilterPlanNode planNode) {
        return process(planNode);
    }
}
