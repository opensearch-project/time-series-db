/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.SortByType;
import org.opensearch.tsdb.lang.m3.common.SortOrderType;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;

/**
 * TopKPlanNode represents a plan node that handles topK operations in M3QL.
 *
 * The topK function returns the first k series in a sorted series list,
 * with optional sort criteria and order parameters.
 *
 * Function Spec:
 * - topK [k, (avg, current, min, max, stddev, sum), (asc/desc)]
 * - Defaults: k=10, sortBy=current, sortOrder=desc
 * - Usage: topK 10 or topK 5 avg asc
 *
 * This stage uses pushdown optimization to reduce network traffic.
 */
public class TopKPlanNode extends M3PlanNode {
    private final int k;
    private final SortByType sortBy;
    private final SortOrderType sortOrder;

    /**
     * Constructor for TopKPlanNode.
     *
     * @param id        The node ID
     * @param k         The number of top series to return
     * @param sortBy    The sorting function (avg, current, max, min, name, stddev, sum)
     * @param sortOrder The sorting order (asc, desc)
     */
    public TopKPlanNode(int id, int k, SortByType sortBy, SortOrderType sortOrder) {
        super(id);
        if (k <= 0) {
            throw new IllegalArgumentException("K must be positive, got: " + k);
        }
        if (sortBy == null) {
            throw new IllegalArgumentException("SortBy cannot be null");
        }
        if (sortOrder == null) {
            throw new IllegalArgumentException("SortOrder cannot be null");
        }
        this.k = k;
        this.sortBy = sortBy;
        this.sortOrder = sortOrder;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "TOPK(%d, %s, %s)", k, sortBy.getValue(), sortOrder.getValue());
    }

    /**
     * Returns the k parameter.
     *
     * @return The number of top series to return
     */
    public int getK() {
        return k;
    }

    /**
     * Returns the sorting function.
     *
     * @return The sorting function (avg, current, max, min, name, stddev, sum)
     */
    public SortByType getSortBy() {
        return sortBy;
    }

    /**
     * Returns the sorting order.
     *
     * @return The sorting order (asc, desc)
     */
    public SortOrderType getSortOrder() {
        return sortOrder;
    }

    /**
     * Creates a TopKPlanNode from a FunctionNode.
     * Expected format:
     * - topK() -> k=10, sortBy=current, sortOrder=desc
     * - topK(5) -> k=5, sortBy=current, sortOrder=desc
     * - topK(5, avg) -> k=5, sortBy=avg, sortOrder=desc
     * - topK(5, avg, asc) -> k=5, sortBy=avg, sortOrder=asc
     *
     * @param functionNode The function node to parse
     * @return TopKPlanNode instance
     * @throws IllegalArgumentException if the function arguments are invalid
     */
    public static TopKPlanNode of(FunctionNode functionNode) {
        int k = 10; // Default
        SortByType sortBy = SortByType.CURRENT; // Default
        SortOrderType sortOrder = SortOrderType.DESC; // Default

        int childCount = functionNode.getChildren().size();

        if (childCount > 3) {
            throw new IllegalArgumentException("topK function accepts at most 3 arguments: k, sortBy, and sortOrder");
        }

        // First argument (optional): k
        if (childCount >= 1) {
            M3ASTNode firstChild = functionNode.getChildren().get(0);
            if (!(firstChild instanceof ValueNode valueNode)) {
                throw new IllegalArgumentException("topK k argument must be a numeric value");
            }

            try {
                k = Integer.parseInt(valueNode.getValue());
                if (k <= 0) {
                    throw new IllegalArgumentException("topK k must be positive, got: " + k);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("topK k must be a valid integer, got: " + valueNode.getValue(), e);
            }
        }

        // Second argument (optional): sortBy
        if (childCount >= 2) {
            M3ASTNode secondChild = functionNode.getChildren().get(1);
            if (!(secondChild instanceof ValueNode sortByValue)) {
                throw new IllegalArgumentException("topK sortBy argument must be a value (avg, current, max, min, name, stddev, sum)");
            }
            sortBy = SortByType.fromString(sortByValue.getValue());
        }

        // Third argument (optional): sortOrder
        if (childCount >= 3) {
            M3ASTNode thirdChild = functionNode.getChildren().get(2);
            if (!(thirdChild instanceof ValueNode sortOrderValue)) {
                throw new IllegalArgumentException("topK sortOrder argument must be a value (asc, desc)");
            }
            sortOrder = SortOrderType.fromString(sortOrderValue.getValue());
        }

        return new TopKPlanNode(M3PlannerContext.generateId(), k, sortBy, sortOrder);
    }
}
