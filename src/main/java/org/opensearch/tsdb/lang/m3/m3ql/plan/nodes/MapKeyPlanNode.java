/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.Utils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.visitor.M3PlanVisitor;

import java.util.Locale;

/**
 * MapKeyPlanNode represents a node in the M3QL plan that renames tag keys in time series.
 * Takes a seriesList and two arguments: the old key name to replace and the new key name.
 * Will replace the tag with key matching the argument key with the replacement key in all the input series.
 */
public class MapKeyPlanNode extends M3PlanNode {
    private final String oldKey;
    private final String newKey;

    /**
     * Constructor for MapKeyPlanNode
     * @param id unique identifier for this node
     * @param oldKey the existing tag key to rename
     * @param newKey the new tag key name
     */
    public MapKeyPlanNode(int id, String oldKey, String newKey) {
        super(id);
        this.oldKey = oldKey;
        this.newKey = newKey;
    }

    /**
     * Get the old tag key to rename.
     * @return the old tag key
     */
    public String getOldKey() {
        return oldKey;
    }

    /**
     * Get the new tag key name.
     * @return the new tag key
     */
    public String getNewKey() {
        return newKey;
    }

    @Override
    public <T> T accept(M3PlanVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "MAP_KEY(old=%s,new=%s)", oldKey, newKey);
    }

    /**
     * Factory method to create a MapKeyPlanNode from a FunctionNode.
     * Expects the function node to have exactly 2 children that are ValueNodes
     * representing the old key and new key.
     *
     * @param functionNode the function node to convert
     * @return an instance of MapKeyPlanNode
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static MapKeyPlanNode of(FunctionNode functionNode) {
        int argCount = functionNode.getChildren().size();
        if (argCount != 2) {
            throw new IllegalArgumentException("MapKey function requires exactly 2 arguments: newKey and oldKey");
        }

        // Parse new key (first argument)
        if (!(functionNode.getChildren().get(0) instanceof ValueNode newKeyNode)) {
            throw new IllegalArgumentException("First argument must be a value representing new key name");
        }
        String newKey = Utils.stripDoubleQuotes(newKeyNode.getValue());
        if (newKey.trim().isEmpty()) {
            throw new IllegalArgumentException("New key cannot be empty");
        }

        // Parse old key (second argument)
        if (!(functionNode.getChildren().get(1) instanceof ValueNode oldKeyNode)) {
            throw new IllegalArgumentException("Second argument must be a value representing old key name");
        }
        String oldKey = Utils.stripDoubleQuotes(oldKeyNode.getValue());
        if (oldKey.trim().isEmpty()) {
            throw new IllegalArgumentException("Old key cannot be empty");
        }

        return new MapKeyPlanNode(M3PlannerContext.generateId(), oldKey, newKey);
    }
}
