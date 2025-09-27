/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.GroupNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;

import java.util.List;

/**
 * AST transformation that normalizes GROUP nodes:
 * 1. If the group has no direct fetch or pipeline children, the group is flattened (children become parent's children)
 * 2. If the group has a fetch, but it's not the first child, drop all children before the first fetch as they are no-ops
 * <pre>
 * Case 1:
 * GROUP
 *   FUNCTION(moving)
 *   FUNCTION(sum)
 *
 * Becomes flattened (children move to parent):
 * FUNCTION(moving)
 * FUNCTION(sum)
 *
 * Case 2:
 * GROUP
 *   FUNCTION(scale)
 *   FUNCTION(fetch)
 *   FUNCTION(sum)
 *
 * Becomes minimized (children before fetch dropped):
 * GROUP
 *   FUNCTION(fetch)
 *   FUNCTION(sum)
 * </pre>
 */
public class GroupNormalizationTransformation implements ASTTransformation {
    private static final String FETCH_FUNCTION = "fetch";

    /**
     * Constructor for GroupNormalizationTransformation.
     */
    public GroupNormalizationTransformation() {}

    @Override
    public boolean canTransform(M3ASTNode node) {
        return node instanceof GroupNode groupNode && findFirstFetchIndex(groupNode) != 0 && !containsPipeline(groupNode);
    }

    @Override
    public List<M3ASTNode> transform(M3ASTNode node) {
        if (!(node instanceof GroupNode groupNode)) {
            throw new IllegalArgumentException("Expected GroupNode");
        }

        int firstFetchIndex = findFirstFetchIndex(groupNode);

        if (firstFetchIndex < 0) {
            // No fetch found - flatten the group (return its children)
            return groupNode.getChildren();
        }

        assert firstFetchIndex > 0 : "canTransform guarantees fetch index is greater than 0";
        // Fetch found but not first - drop children before fetch
        GroupNode newGroup = new GroupNode();
        List<M3ASTNode> childrenFromFetch = groupNode.getChildren().subList(firstFetchIndex, groupNode.getChildren().size());
        for (M3ASTNode child : childrenFromFetch) {
            newGroup.addChildNode(child);
        }
        return List.of(newGroup);
    }

    /**
     * Find the index of the first direct child that is a fetch function.
     *
     * @return index of first fetch, or -1 if no fetch found
     */
    private int findFirstFetchIndex(GroupNode groupNode) {
        List<M3ASTNode> children = groupNode.getChildren();
        for (int i = 0; i < children.size(); i++) {
            M3ASTNode child = children.get(i);
            if (child instanceof FunctionNode functionNode && FETCH_FUNCTION.equals(functionNode.getFunctionName())) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Check if the group contains any pipeline nodes.
     */
    private boolean containsPipeline(GroupNode groupNode) {
        for (M3ASTNode child : groupNode.getChildren()) {
            if (child instanceof PipelineNode) {
                return true;
            }
        }
        return false;
    }
}
