/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan;

import org.opensearch.tsdb.lang.m3.common.Constants;
import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.GroupNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.RootNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.expand.BurnRatePipelineExpander;
import org.opensearch.tsdb.lang.m3.m3ql.plan.expand.PipelineExpander;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AsPercentPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.BinaryPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.DiffPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.DividePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FallbackSeriesBinaryPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.IntersectPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.UnionPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.common.Booleans.parseBooleanStrict;

/**
 * M3ASTConverter is responsible for converting the M3QL AST into a plan.
 */
public class M3ASTConverter {

    private static final Set<String> PIPELINE_EXPAND_FUNCTIONS = Set.of(
        Constants.Functions.Binary.BURN_RATE,
        Constants.Functions.Binary.AS_BURN_RATE,
        Constants.Functions.Binary.MULTI_BURN_RATE,
        Constants.Functions.Binary.AS_MULTI_BURN_RATE
    );

    private static final Set<String> FUNCTIONS_WITH_PIPELINE_ARG = Set.of(
        Constants.Functions.Binary.AS_PERCENT,
        Constants.Functions.Binary.RATIO,
        Constants.Functions.Binary.DIFF,
        Constants.Functions.Binary.SUBTRACT,
        Constants.Functions.Binary.DIVIDE,
        Constants.Functions.Binary.DIVIDE_SERIES,
        Constants.Functions.Binary.INTERSECT
    );

    /**
     * Constructor for M3ASTConverter.
     *
     * @param context the planner context containing state and configuration
     */
    public M3ASTConverter(M3PlannerContext context) {
        if (context == null) {
            throw new IllegalArgumentException("Must create planner context for AST conversion");
        }
    }

    /**
     * Builds the M3QL plan from the given AST root node.
     *
     * @param astRoot the root node of the M3QL AST
     * @return the root plan node of the M3QL plan
     */
    public M3PlanNode buildPlan(RootNode astRoot) {
        if (astRoot == null) {
            throw new IllegalStateException("AST root cannot be null");
        }

        if (astRoot.getChildren().size() != 1) {
            throw new IllegalStateException("AST root must have exactly one child");
        }

        if (!(astRoot.getChildren().getFirst() instanceof PipelineNode pipelineNode)) {
            throw new IllegalStateException("AST root child must be of type PipelineNode");
        }

        return M3PlanFinalizer.finalize(handlePipelineOrGroupNode(pipelineNode));
    }

    private M3PlanNode handlePipelineOrGroupNode(M3ASTNode node) {
        if (!isPipelineOrGroup(node)) {
            throw new IllegalArgumentException("node must be of type PipelineNode or GroupNode");
        }
        boolean outsideBoundaryMarker = node instanceof GroupNode;

        M3PlanNode resultPlanNode = null;
        M3PlanNode danglingPlanNode = null;

        for (int childIndex = 0; childIndex < node.getChildren().size(); childIndex++) {
            M3ASTNode childNode = node.getChildren().get(childIndex);

            if (isFetchFunction(childNode)) {
                M3PlanNode newChain = handleFetchFunction((FunctionNode) childNode);
                if (resultPlanNode == null) {
                    resultPlanNode = newChain;
                } else {
                    resultPlanNode = finalizePlanNode(resultPlanNode, danglingPlanNode);
                    danglingPlanNode = null;
                    resultPlanNode = mergeChainsAtBoundaryMarker(resultPlanNode, newChain);
                }
            } else if (childNode instanceof PipelineNode currPipelineNode) {
                assert danglingPlanNode == null : "danglingPlanNode should be null when starting a new pipeline";
                M3PlanNode newChain = handlePipelineOrGroupNode(currPipelineNode);
                if (resultPlanNode == null) {
                    resultPlanNode = newChain;
                } else {
                    resultPlanNode = mergeChainsAtBoundaryMarker(resultPlanNode, newChain);
                }
            } else if (isFallbackSeriesWithPipelineArg(childNode)) {
                // Handle fallbackSeries with pipeline argument as binary operation
                assert resultPlanNode != null : "resultPlanNode should not be null when handling fallbackSeries with pipeline arg";
                resultPlanNode = finalizePlanNode(resultPlanNode, danglingPlanNode);
                danglingPlanNode = null;
                resultPlanNode = handleFallbackSeriesWithPipelineArg(resultPlanNode, (FunctionNode) childNode);
            } else if (isPipelineExpandFunction(childNode)) {
                assert resultPlanNode != null : "resultPlanNode should not be null when handling pipeline expand function";
                resultPlanNode = finalizePlanNode(resultPlanNode, danglingPlanNode);
                danglingPlanNode = null;
                resultPlanNode = handleExpandedPipeline(node.getChildren(), childIndex, (FunctionNode) childNode);
            } else if (isFunctionNodeWithPipelineArg(childNode)) {
                assert resultPlanNode != null : "resultPlanNode should not be null when handling function with pipeline arg";
                resultPlanNode = finalizePlanNode(resultPlanNode, danglingPlanNode);
                danglingPlanNode = null;
                resultPlanNode = handleFunctionWithPipelineArg(resultPlanNode, (FunctionNode) childNode);
            } else if (childNode instanceof GroupNode groupNode) {
                M3PlanNode newChain = handlePipelineOrGroupNode(groupNode);
                if (resultPlanNode == null) {
                    resultPlanNode = newChain;
                } else {
                    resultPlanNode = finalizePlanNode(resultPlanNode, danglingPlanNode);
                    danglingPlanNode = null;
                    resultPlanNode = mergeChainsAtBoundaryMarker(resultPlanNode, newChain);
                }
            } else {
                // Just a regular function, chain it off the current dangling node or result node
                danglingPlanNode = handleRegularFunction(resultPlanNode, danglingPlanNode, childNode);
            }
        }

        // Finalize the sub-plan, removing leftover boundary markers
        if (outsideBoundaryMarker) {
            M3PlanNode subPlan = M3PlanFinalizer.finalize(finalizePlanNode(resultPlanNode, danglingPlanNode));
            ChainBoundaryMarker boundaryMarker = new ChainBoundaryMarker();
            boundaryMarker.addChild(subPlan);
            return boundaryMarker;
        }
        return finalizePlanNode(resultPlanNode, danglingPlanNode);
    }

    /**
     * Converts a list of AST children into a plan by wrapping them in a synthetic PipelineNode.
     */
    private M3PlanNode handlePipelineChildren(List<M3ASTNode> children) {
        PipelineNode syntheticPipeline = new PipelineNode();
        syntheticPipeline.getChildren().addAll(children);
        return handlePipelineOrGroupNode(syntheticPipeline);
    }

    // Finalizes the plan node by returning the dangling node if it exists, otherwise the result node.
    private M3PlanNode finalizePlanNode(M3PlanNode resultPlanNode, M3PlanNode danglingPlanNode) {
        if (danglingPlanNode != null) {
            return danglingPlanNode;
        }
        return Objects.requireNonNull(resultPlanNode, "Found null plan node during query planning.");
    }

    // True if M3ASTNode is a function node corresponding to a fetch, e.g. FUNCTION(fetch) or FUNCTION(mockFetch)
    private boolean isFetchFunction(M3ASTNode node) {
        if (!(node instanceof FunctionNode functionNode)) {
            return false;
        }
        String functionName = functionNode.getFunctionName();
        return Constants.Functions.FETCH.equals(functionName)
            || Constants.Functions.MOCK_FETCH.equals(functionName)
            || Constants.Functions.MOCK_FETCH_LINE.equals(functionName);
    }

    // Handles a fetch function node by creating a FetchPlanNode and wrapping it in a ChainBoundaryMarker.
    private M3PlanNode handleFetchFunction(FunctionNode fetchNode) {
        M3PlanNode planNode = M3PlanNodeFactory.create(fetchNode);
        ChainBoundaryMarker boundaryMarker = new ChainBoundaryMarker();
        boundaryMarker.addChild(planNode);
        return boundaryMarker;
    }

    // True if M3ASTNode is a PipelineNode or GroupNode
    private boolean isPipelineOrGroup(M3ASTNode node) {
        return node instanceof PipelineNode || node instanceof GroupNode;
    }

    // Merges two plan nodes using a UnionPlanNode if needed, handling boundary markers appropriately.
    private M3PlanNode mergeChainsAtBoundaryMarker(M3PlanNode resultPlanNode, M3PlanNode newChain) {
        assert resultPlanNode != null : "resultPlanNode should not be null when merging chains";

        M3PlanNode aboveBoundaryMarker = null;
        M3PlanNode belowBoundaryMarker = null;
        if (newChain instanceof ChainBoundaryMarker) {
            belowBoundaryMarker = newChain.getChildren().getFirst();
        } else {
            aboveBoundaryMarker = newChain;
            while (!newChain.getChildren().isEmpty() && !(newChain.getChildren().getFirst() instanceof ChainBoundaryMarker)) {
                newChain = newChain.getChildren().getFirst();
            }
            if (!newChain.getChildren().isEmpty()) {
                belowBoundaryMarker = newChain.getChildren().getFirst().getChildren().getFirst();
                newChain.getChildren().clear(); // drop the boundary marker
            }
        }
        assert belowBoundaryMarker != null : "chain must have boundary marker";

        // Merge the existing resultPlanNode with the belowBoundaryMarker part
        M3PlanNode mergedBelow = mergeWithExisting(resultPlanNode, belowBoundaryMarker);

        // Attach the union to the bottom of the aboveBoundaryMarker chain
        M3PlanNode aboveVistor = aboveBoundaryMarker;
        if (aboveVistor != null) {
            while (!aboveVistor.getChildren().isEmpty()) {
                aboveVistor = aboveVistor.getChildren().getFirst();
            }
            aboveVistor.addChild(mergedBelow);
        }
        return aboveBoundaryMarker != null ? aboveBoundaryMarker : mergedBelow;
    }

    private UnionPlanNode mergeWithExisting(M3PlanNode resultPlanNode, M3PlanNode newChain) {
        if (resultPlanNode instanceof UnionPlanNode unionPlanNode) {
            unionPlanNode.addChild(newChain);
            return unionPlanNode;
        } else {
            return UnionPlanNode.of(resultPlanNode, newChain);
        }
    }

    // True if the node is a fallbackSeries function with a pipeline/group argument
    private boolean isFallbackSeriesWithPipelineArg(M3ASTNode node) {
        if (!(node instanceof FunctionNode functionNode)) {
            return false;
        }

        if (!Constants.Functions.FALLBACK_SERIES.equals(functionNode.getFunctionName())) {
            return false;
        }

        // Check if it has an argument and if that argument is a pipeline or group
        if (functionNode.getChildren().isEmpty()) {
            return false; // No argument - will fail validation later
        }

        M3ASTNode firstArg = functionNode.getChildren().getFirst();
        return isPipelineOrGroup(firstArg); // Returns true if it's a pipeline/group
    }

    // Handles a fallbackSeries function with a pipeline argument by creating a BinaryPlanNode
    private M3PlanNode handleFallbackSeriesWithPipelineArg(M3PlanNode lhs, FunctionNode functionNode) {
        if (functionNode.getChildren().isEmpty()) {
            throw new IllegalArgumentException("fallbackSeries requires one argument");
        }

        M3ASTNode child = functionNode.getChildren().getFirst();
        if (!isPipelineOrGroup(child)) {
            throw new IllegalStateException(
                "Expected pipeline/group argument for fallbackSeries, got: " + child.getClass().getSimpleName()
            );
        }

        M3PlanNode rhs = handlePipelineOrGroupNode(child);
        BinaryPlanNode binaryPlanNode = new FallbackSeriesBinaryPlanNode(M3PlannerContext.generateId());
        binaryPlanNode.addChild(lhs);
        binaryPlanNode.addChild(rhs);
        return binaryPlanNode;
    }

    private boolean isPipelineExpandFunction(M3ASTNode node) {
        return node instanceof FunctionNode fn && PIPELINE_EXPAND_FUNCTIONS.contains(fn.getFunctionName());
    }

    private M3PlanNode handleExpandedPipeline(List<M3ASTNode> children, int lhsEndIndex, FunctionNode functionNode) {
        PipelineExpander expander = switch (functionNode.getFunctionName()) {
            case Constants.Functions.Binary.BURN_RATE, Constants.Functions.Binary.AS_BURN_RATE, Constants.Functions.Binary.MULTI_BURN_RATE,
                Constants.Functions.Binary.AS_MULTI_BURN_RATE -> new BurnRatePipelineExpander();
            default -> throw new IllegalArgumentException("No pipeline expander for: " + functionNode.getFunctionName());
        };
        return expander.expand(children, lhsEndIndex, functionNode, this::handlePipelineOrGroupNode, this::handlePipelineChildren);
    }

    // True if M3ASTNode is a function node that takes a pipeline as an argument
    private boolean isFunctionNodeWithPipelineArg(M3ASTNode node) {
        return node instanceof FunctionNode functionNode && functionNodeHasPipelineArg(functionNode);
    }

    // True if the function name is in the set of functions that take a pipeline argument
    private boolean functionNodeHasPipelineArg(FunctionNode node) {
        return FUNCTIONS_WITH_PIPELINE_ARG.contains(node.getFunctionName());
    }

    // Handles a function node by creating a BinaryPlanNode with the lhs, and creates rhs from the function node children
    private M3PlanNode handleFunctionWithPipelineArg(M3PlanNode lhs, FunctionNode functionNode) {
        M3ASTNode child = functionNode.getChildren().getFirst();
        if (!isPipelineOrGroup(child)) {
            throw new IllegalArgumentException(
                functionNode.getFunctionName() + " argument must be a pipeline or group, got: " + child.getClass().getSimpleName()
            );
        }
        M3PlanNode rhs = handlePipelineOrGroupNode(child);

        BinaryPlanNode binaryPlanNode = createBinaryPlanNode(functionNode);

        binaryPlanNode.addChild(lhs);
        binaryPlanNode.addChild(rhs);
        return binaryPlanNode;
    }

    private BinaryPlanNode createBinaryPlanNode(FunctionNode functionNode) {
        String functionName = functionNode.getFunctionName();
        return switch (functionName) {
            case Constants.Functions.Binary.AS_PERCENT, Constants.Functions.Binary.RATIO -> {
                List<String> tags = extractGroupByTags(functionNode, 1);
                yield new AsPercentPlanNode(M3PlannerContext.generateId(), tags);
            }

            case Constants.Functions.Binary.DIFF, Constants.Functions.Binary.SUBTRACT -> {
                boolean keepNans = extractKeepNansParameter(functionNode);
                List<String> tags = extractGroupByTags(functionNode, 2);
                yield new DiffPlanNode(M3PlannerContext.generateId(), keepNans, tags);
            }

            case Constants.Functions.Binary.DIVIDE, Constants.Functions.Binary.DIVIDE_SERIES -> {
                List<String> tags = extractGroupByTags(functionNode, 1);
                yield new DividePlanNode(M3PlannerContext.generateId(), tags);
            }

            case Constants.Functions.Binary.INTERSECT -> {
                List<String> tags = extractGroupByTags(functionNode, 1);
                yield new IntersectPlanNode(M3PlannerContext.generateId(), tags);
            }

            default -> throw new IllegalArgumentException("Binary function " + functionName + " is not supported.");
        };
    }

    /**
     * Extracts the group-by tags from function arguments.
     * @param functionNode
     * @param startIndex starting index for children node
     */
    private List<String> extractGroupByTags(FunctionNode functionNode, int startIndex) {
        return functionNode.getChildren()
            .stream()
            .skip(startIndex)
            .filter(astNode -> astNode instanceof ValueNode)
            .map(astNode -> Utils.stripDoubleQuotes(((ValueNode) astNode).getValue()))
            .collect(Collectors.toList());
    }

    /**
     * Extracts the keepNans boolean parameter from diff/subtract function (argument index 1).
     */
    private boolean extractKeepNansParameter(FunctionNode functionNode) {
        if (functionNode.getChildren().size() <= 1) {
            return false;
        }

        M3ASTNode booleanValue = functionNode.getChildren().get(1);
        if (booleanValue instanceof ValueNode node) {
            try {
                return parseBooleanStrict(node.getValue(), false);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                    "function " + functionNode.getFunctionName() + " expects argument 1 of type bool, received '" + node.getValue() + "'"
                );
            }
        }
        return false;
    }

    // Handles a regular function node by creating the corresponding plan node and chaining appropriately
    private M3PlanNode handleRegularFunction(M3PlanNode resultPlanNode, M3PlanNode danglingPlanNode, M3ASTNode currentNode) {
        if (!(currentNode instanceof FunctionNode functionNode)) {
            throw new IllegalStateException("Expecting regular function of type FunctionNode.");
        }

        M3PlanNode planNode = M3PlanNodeFactory.create(functionNode);

        if (danglingPlanNode == null) {
            planNode.addChild(resultPlanNode);
        } else {
            planNode.addChild(danglingPlanNode);
        }
        return planNode;
    }

    /**
     * Marker node to indicate the boundary between fetch and non-fetch operations in ungrouped chains. This node should not appear
     * in the finalized plan and is both created and cleaned up during AST to plan conversion. A boundary marker only has a single child.
     */
    private static class ChainBoundaryMarker extends M3PlanNode {
        private static final int MARKER_ID = -1;

        public ChainBoundaryMarker() {
            super(MARKER_ID); // no id needed since this node is temporary
        }

        @Override
        public <T> T accept(M3PlanVisitor<T> visitor) {
            return visitor.process(this);
        }

        @Override
        public String getExplainName() {
            throw new UnsupportedOperationException("Boundary marker should not appear in explain plans");
        }
    }

    /**
     * Finalizes the plan by removing any leftover ChainBoundaryMarker nodes.
     */
    private static class M3PlanFinalizer extends M3PlanVisitor<M3PlanNode> {

        // Finalizes the plan by removing any leftover ChainBoundaryMarker nodes.
        private static M3PlanNode finalize(M3PlanNode planNode) {
            M3PlanFinalizer finalizer = new M3PlanFinalizer();
            return planNode.accept(finalizer);
        }

        @Override
        public M3PlanNode process(M3PlanNode planNode) {
            List<M3PlanNode> children = planNode.getChildren();
            children.replaceAll(m3PlanNode -> m3PlanNode.accept(this));

            if (planNode instanceof ChainBoundaryMarker) {
                if (children.size() != 1) {
                    throw new IllegalStateException(
                        "ChainBoundaryMarker must have exactly one child. This error would signal an error during query plan generation."
                    );
                }
                return children.getFirst();
            }

            return planNode;
        }
    }
}
