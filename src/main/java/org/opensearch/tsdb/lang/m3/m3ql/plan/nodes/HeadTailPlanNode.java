/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.HeadTailMode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;

/**
 * HeadTailPlanNode represents a plan node that handles head and tail operations in M3QL.
 *
 * The head function returns the first n series from the series list.
 * The tail function returns the last n series from the series list.
 * Both take an optional limit argument (defaults to 10).
 *
 * This is a coordinator-only stage that operates on all time series at once.
 */
public class HeadTailPlanNode extends M3PlanNode {
    private final int limit;
    private final HeadTailMode mode;

    /**
     * Constructor for HeadTailPlanNode.
     *
     * @param id    The node ID
     * @param limit The number of series to return (defaults to 10 if not specified)
     * @param mode  The operation mode (HEAD or TAIL)
     */
    public HeadTailPlanNode(int id, int limit, HeadTailMode mode) {
        super(id);
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, got: " + limit);
        }
        if (mode == null) {
            throw new IllegalArgumentException("Mode cannot be null");
        }
        this.limit = limit;
        this.mode = mode;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "%s(%d)", mode.name(), limit);
    }

    /**
     * Returns the limit.
     *
     * @return The limit
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Returns the mode.
     *
     * @return The operation mode
     */
    public HeadTailMode getMode() {
        return mode;
    }

    /**
     * Creates a HeadTailPlanNode from a FunctionNode for head function.
     * Expected format:
     * - head() -> defaults to 10
     * - head(5) -> returns first 5 series
     *
     * @param functionNode The function node to parse
     * @return HeadTailPlanNode instance with HEAD mode
     * @throws IllegalArgumentException if the function arguments are invalid
     */
    public static HeadTailPlanNode ofHead(FunctionNode functionNode) {
        int limit = parseLimit(functionNode, "head");
        return new HeadTailPlanNode(M3PlannerContext.generateId(), limit, HeadTailMode.HEAD);
    }

    /**
     * Creates a HeadTailPlanNode from a FunctionNode for tail function.
     * Expected format:
     * - tail() -> defaults to 10
     * - tail(5) -> returns last 5 series
     *
     * @param functionNode The function node to parse
     * @return HeadTailPlanNode instance with TAIL mode
     * @throws IllegalArgumentException if the function arguments are invalid
     */
    public static HeadTailPlanNode ofTail(FunctionNode functionNode) {
        int limit = parseLimit(functionNode, "tail");
        return new HeadTailPlanNode(M3PlannerContext.generateId(), limit, HeadTailMode.TAIL);
    }

    /**
     * Parse limit from function node arguments.
     *
     * @param functionNode The function node
     * @param functionName The function name (for error messages)
     * @return The parsed limit
     */
    private static int parseLimit(FunctionNode functionNode, String functionName) {
        int limit = 10; // Default

        if (!functionNode.getChildren().isEmpty()) {
            if (functionNode.getChildren().size() > 1) {
                throw new IllegalArgumentException(functionName + " function accepts at most 1 argument: limit");
            }

            M3ASTNode firstChild = functionNode.getChildren().getFirst();
            if (!(firstChild instanceof ValueNode valueNode)) {
                throw new IllegalArgumentException(functionName + " limit argument must be a numeric value");
            }

            try {
                limit = Integer.parseInt(valueNode.getValue());
                if (limit <= 0) {
                    throw new IllegalArgumentException(functionName + " limit must be positive, got: " + limit);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(functionName + " limit must be a valid integer, got: " + valueNode.getValue(), e);
            }
        }

        return limit;
    }
}
