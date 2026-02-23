/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

/**
 * Enumeration for head and tail stage operation modes.
 */
public enum HeadTailMode {
    HEAD("head"),
    TAIL("tail");

    private final String displayName;

    HeadTailMode(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public static HeadTailMode fromString(String name) {
        return "tail".equals(name) ? TAIL : HEAD;
    }
}
