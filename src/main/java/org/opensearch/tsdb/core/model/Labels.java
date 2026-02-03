/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.apache.lucene.util.BytesRef;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Labels is a set of name/value pairs.
 */
public interface Labels {

    /**
     * Convert to key:value string format
     * @return string representation
     */
    String toKeyValueString();

    /**
     * Convert to array of key:value byte references
     * @return array of BytesRef objects containing key:value pairs
     */
    BytesRef[] toKeyValueBytesRefs();

    /**
     * Get a read-only map view of the labels
     * @return map view
     */
    Map<String, String> toMapView();

    /**
     * Check if labels are empty
     * @return true if empty
     */
    boolean isEmpty();

    /**
     * Get stable hash of labels
     * @return stable hash value
     */
    long stableHash();

    /**
     * Get the value for a label name
     * @param name label name
     * @return label value or empty string if not found
     */
    String get(String name);

    /**
     * Check if a label exists
     * @param name label name
     * @return true if label exists
     */
    boolean has(String name);

    /**
     * Get set of labels in index format (key:value pairs using current delimiter - in this example ':')
     * @return set of label strings formatted for indexing
     */
    Set<String> toIndexSet();

    /**
     * Create a new Labels instance with the specified label added or updated.
     * If the label already exists, its value will be updated. If it doesn't exist, it will be added.
     * The resulting Labels instance maintains sorted order.
     *
     * @param name  the label name
     * @param value the label value
     * @return a new Labels instance with the label added/updated
     * @throws IllegalArgumentException if name is null or empty
     */
    Labels withLabel(String name, String value);

    /**
     * Create a new Labels instance with multiple labels added or updated in a single operation.
     * This is more efficient than calling withLabel multiple times as it performs only one allocation
     * and copy operation instead of N operations for N labels.
     * If a label already exists, its value will be updated. If it doesn't exist, it will be added.
     * The resulting Labels instance maintains sorted order.
     *
     * @param newLabels map of label names to values to add or update
     * @return a new Labels instance with all labels added/updated
     * @throws IllegalArgumentException if any label name is null or empty
     */
    Labels withLabels(Map<String, String> newLabels);

    /**
     * Deep copy the contents of the labels
     * @return a newly copied label
     */
    Labels deepCopy();

    /**
     * Estimate the memory footprint of this Labels instance in bytes.
     *
     * <p>The estimate includes:
     * <ul>
     *   <li>Object overhead for the Labels implementation</li>
     *   <li>Internal data structures (byte arrays, strings, etc.)</li>
     *   <li>Label names and values</li>
     * </ul>
     *
     * @return estimated size in bytes
     */
    long estimateBytes();

    /**
     * Extract sorted names (label names) from this Labels instance.
     * Returns a list of all label names sorted by name.
     *
     * @return a list of sorted label names
     */
    List<String> extractSortedNames();

    /**
     * Find common names between this Labels instance and a sorted list of label names.
     * Returns names that are present in both this Labels instance and the sorted list.
     *
     * @param sortedNames a sorted list of label names to compare with
     * @return a list of common names, sorted by name
     */
    List<String> findCommonNamesWithSortedList(List<String> sortedNames);

    /**
     * Find the common label names across a list of Labels instances.
     * Returns label names that are present in all Labels instances in the list.
     *
     * @param labelsList the list of Labels instances to find common label names from
     * @return a list of common label names present in all Labels instances, sorted by name.
     *         Returns empty list if the input list is null, empty, or contains null/empty Labels instances.
     */
    static List<String> findCommonLabelNames(List<Labels> labelsList) {
        if (labelsList == null || labelsList.isEmpty()) {
            return new ArrayList<>();
        }

        // If any Labels is null or empty, return empty list
        for (Labels labels : labelsList) {
            if (labels == null || labels.isEmpty()) {
                return new ArrayList<>();
            }
        }

        // If only one label, return all its names
        if (labelsList.size() == 1) {
            return labelsList.get(0).extractSortedNames();
        }

        // Start with names from the first Labels instance
        List<String> commonNames = labelsList.get(0).extractSortedNames();

        // Intersect with remaining Labels instances
        for (int i = 1; i < labelsList.size() && !commonNames.isEmpty(); i++) {
            commonNames = labelsList.get(i).findCommonNamesWithSortedList(commonNames);
        }

        return commonNames;
    }
}
