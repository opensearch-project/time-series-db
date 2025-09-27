/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.nodes;

import org.opensearch.tsdb.lang.m3.m3ql.parser.M3ASTVisitor;

/**
 * Tags occur in fetch expressions, for example fetch dc:{dca*,phx*} name:queries will have two tag specifier nodes, one for "dc" and one for "name".
 */
public class TagKeyNode extends M3ASTNode {

    /**
     * Constructor for TagKeyNode.
     */
    public TagKeyNode() {}

    private String keyName = null;
    private boolean inverted = false;

    /**
     * Set the key name for this tag.
     * @param key the key name
     */
    public void setKeyName(String key) {
        keyName = key;
    }

    /**
     * Get the key name for this tag.
     * @return the key name
     */
    public String getKeyName() {
        return keyName;
    }

    /**
     * Set whether this tag key is inverted (i.e., negated).
     * @param inverted true if inverted, false otherwise
     */
    public void setInverted(boolean inverted) {
        this.inverted = inverted;
    }

    /**
     * Check if this tag key is inverted (i.e., negated).
     * @return true if inverted, false otherwise
     */
    public boolean isInverted() {
        return inverted;
    }

    @Override
    public <T> T accept(M3ASTVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExplainName() {
        return "TAG_KEY(%s)".formatted(keyName);
    }
}
