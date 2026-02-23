/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

import java.util.Locale;

/**
 * Utility class for semantic version comparison and detection.
 * Provides flexible version normalization and comparison logic similar to Go's semver.Canonical.
 * Supports versions in the format MAJOR.MINOR.PATCH with optional prerelease identifiers
 * (e.g., "1.10.0-alpha", "1.0.0-alpha.1").
 */
public class SemanticVersionComparator {

    /**
     * Private constructor to prevent instantiation.
     */
    private SemanticVersionComparator() {
        // Prevent instantiation
    }

    /**
     * Determines if a string represents a semantic version using flexible detection.
     * Attempts normalization — if it succeeds, the value is a valid semantic version.
     *
     * @param value the string to check
     * @return true if the value can be normalized to a semantic version
     */
    public static boolean isSemanticVersion(String value) {
        if (value == null || value.trim().isEmpty()) {
            return false;
        }

        try {
            normalizeVersion(value);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Normalizes a version string using flexible rules similar to Go's semver.Canonical.
     * Prerelease identifiers (after a hyphen) are preserved as-is.
     * Examples:
     * - "1" → "v1.0.0"
     * - "2.0" → "v2.0.0"
     * - "30.500" → "v30.500.0"
     * - "30.500.100" → "v30.500.100"
     * - "v1.2.3" → "v1.2.3" (already normalized)
     * - "1.10.0-alpha" → "v1.10.0-alpha"
     * - "1.0.0-alpha.1" → "v1.0.0-alpha.1"
     *
     * @param version the version string to normalize
     * @return the normalized version string with "v" prefix
     * @throws IllegalArgumentException if version cannot be normalized
     */
    public static String normalizeVersion(String version) {
        if (version == null || version.trim().isEmpty()) {
            throw new IllegalArgumentException("Version cannot be null or empty");
        }

        String trimmed = version.trim();

        // Remove existing "v" prefix if present
        if (trimmed.startsWith("v")) {
            trimmed = trimmed.substring(1);
        }

        // Separate prerelease: find the first hyphen preceded by a digit.
        // Everything after that hyphen is treated as the prerelease identifier.
        // e.g., "1.2.3-alpha" → base "1.2.3", prerelease "alpha"
        // "1.2.3-1.2.3-abc" → base "1.2.3", prerelease "1.2.3-abc"
        String prerelease = null;
        for (int i = 1; i < trimmed.length(); i++) {
            if (trimmed.charAt(i) == '-' && Character.isDigit(trimmed.charAt(i - 1))) {
                prerelease = trimmed.substring(i + 1);
                trimmed = trimmed.substring(0, i);
                if (prerelease.isEmpty()) {
                    throw new IllegalArgumentException("Prerelease identifier cannot be empty in: " + version);
                }
                break;
            }
        }

        // Split by dots and validate each component
        String[] parts = trimmed.split("\\.");

        if (parts.length == 0 || parts.length > 3) {
            throw new IllegalArgumentException("Invalid version format: " + version);
        }

        // Validate and extract version components
        int major = parseVersionNumber(parts[0], "major", version);
        int minor = parts.length > 1 ? parseVersionNumber(parts[1], "minor", version) : 0;
        int patch = parts.length > 2 ? parseVersionNumber(parts[2], "patch", version) : 0;

        String normalized = String.format(Locale.ROOT, "v%d.%d.%d", major, minor, patch);
        if (prerelease != null) {
            normalized += "-" + prerelease;
        }
        return normalized;
    }

    /**
     * Parses a version number component and validates it.
     */
    private static int parseVersionNumber(String component, String componentName, String originalVersion) {
        if (component == null || component.trim().isEmpty()) {
            throw new IllegalArgumentException("Invalid " + componentName + " version component in: " + originalVersion);
        }

        try {
            String trimmed = component.trim();
            // Allow leading zeros for version components (unlike strict semver)
            int value = Integer.parseInt(trimmed);
            if (value < 0) {
                throw new IllegalArgumentException("Negative " + componentName + " version component in: " + originalVersion);
            }
            return value;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid " + componentName + " version component '" + component + "' in: " + originalVersion,
                e
            );
        }
    }

    /**
     * Compares two version strings using semantic version rules.
     * Both versions must be valid semantic versions.
     * <p>
     * Prerelease comparison follows the semver spec:
     * <ul>
     *   <li>A version without prerelease has higher precedence than one with prerelease
     *       (e.g., 1.0.0 &gt; 1.0.0-alpha)</li>
     *   <li>Prerelease identifiers are compared dot-separated, left to right</li>
     *   <li>Numeric identifiers are compared as integers; string identifiers are compared lexicographically</li>
     *   <li>Numeric identifiers have lower precedence than string identifiers</li>
     *   <li>A shorter set of identifiers has lower precedence if all preceding identifiers are equal</li>
     * </ul>
     *
     * @param version1 the first version to compare
     * @param version2 the second version to compare
     * @return negative if version1 &lt; version2, zero if equal, positive if version1 &gt; version2
     * @throws IllegalArgumentException if either version is not a valid semantic version
     */
    public static int compareSemanticVersions(String version1, String version2) {
        String normalized1 = normalizeVersion(version1);
        String normalized2 = normalizeVersion(version2);

        // Split off prerelease
        String base1 = normalized1;
        String pre1 = null;
        String base2 = normalized2;
        String pre2 = null;

        int h1 = normalized1.indexOf('-');
        if (h1 >= 0) {
            base1 = normalized1.substring(0, h1);
            pre1 = normalized1.substring(h1 + 1);
        }
        int h2 = normalized2.indexOf('-');
        if (h2 >= 0) {
            base2 = normalized2.substring(0, h2);
            pre2 = normalized2.substring(h2 + 1);
        }

        // Compare base version components (skip the 'v' prefix)
        int[] v1 = parseBaseComponents(base1.substring(1));
        int[] v2 = parseBaseComponents(base2.substring(1));

        for (int i = 0; i < 3; i++) {
            int cmp = Integer.compare(v1[i], v2[i]);
            if (cmp != 0) {
                return cmp;
            }
        }

        // Base versions are equal — compare prerelease
        if (pre1 == null && pre2 == null) {
            return 0;
        } else if (pre1 == null) {
            return 1; // release > prerelease
        } else if (pre2 == null) {
            return -1; // prerelease < release
        } else {
            return comparePrereleaseIdentifiers(pre1, pre2);
        }
    }

    /**
     * Parses "MAJOR.MINOR.PATCH" (without 'v' prefix) into int[3].
     */
    private static int[] parseBaseComponents(String base) {
        String[] parts = base.split("\\.");
        return new int[] { Integer.parseInt(parts[0]), Integer.parseInt(parts[1]), Integer.parseInt(parts[2]) };
    }

    /**
     * Compares two prerelease strings per the semver spec.
     * Identifiers are split by dots and compared left to right.
     * Numeric identifiers are compared as integers; string identifiers are compared lexicographically.
     * Numeric identifiers have lower precedence than string identifiers.
     * A shorter set of identifiers has lower precedence if all preceding identifiers are equal.
     */
    private static int comparePrereleaseIdentifiers(String pre1, String pre2) {
        String[] ids1 = pre1.split("\\.");
        String[] ids2 = pre2.split("\\.");

        int len = Math.min(ids1.length, ids2.length);
        for (int i = 0; i < len; i++) {
            int cmp = compareSingleIdentifier(ids1[i], ids2[i]);
            if (cmp != 0) {
                return cmp;
            }
        }

        // All compared identifiers are equal — shorter set has lower precedence
        return Integer.compare(ids1.length, ids2.length);
    }

    /**
     * Compares two individual prerelease identifiers.
     * If both are numeric, compare as integers. If both are strings, compare lexicographically.
     * Numeric identifiers have lower precedence than string identifiers.
     */
    private static int compareSingleIdentifier(String id1, String id2) {
        boolean isNum1 = isNumeric(id1);
        boolean isNum2 = isNumeric(id2);

        if (isNum1 && isNum2) {
            return Integer.compare(Integer.parseInt(id1), Integer.parseInt(id2));
        } else if (isNum1) {
            return -1; // numeric < string
        } else if (isNum2) {
            return 1; // string > numeric
        } else {
            return id1.compareTo(id2);
        }
    }

    private static boolean isNumeric(String s) {
        try {
            Integer.parseInt(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
