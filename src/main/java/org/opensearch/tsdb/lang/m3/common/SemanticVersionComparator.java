/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for semantic version comparison and detection.
 * Provides flexible version normalization and comparison logic similar to Go's semver.Canonical.
 */
public class SemanticVersionComparator {

    /**
     * Private constructor to prevent instantiation.
     */
    private SemanticVersionComparator() {
        // Prevent instantiation
    }

    /**
     * Semantic version regex pattern for validation after normalization.
     * Matches: vMAJOR.MINOR.PATCH with optional prerelease and build metadata
     */
    private static final Pattern SEMVER_PATTERN = Pattern.compile(
        "^v(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$"
    );

    /**
     * Pattern to extract version components from normalized version string.
     */
    private static final Pattern VERSION_EXTRACT_PATTERN = Pattern.compile("^v(\\d+)\\.(\\d+)\\.(\\d+)(?:-(.*?))?(?:\\+(.*?))?$");

    /**
     * Determines if a string represents a semantic version using flexible detection.
     * Applies normalization first, then validates against semantic version pattern.
     *
     * @param value the string to check
     * @return true if the value is a semantic version after normalization
     */
    public static boolean isSemanticVersion(String value) {
        if (value == null || value.trim().isEmpty()) {
            return false;
        }

        try {
            String normalizedVersion = normalizeVersion(value);
            return SEMVER_PATTERN.matcher(normalizedVersion).matches();
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Normalizes a version string using flexible rules similar to Go's semver.Canonical.
     * Examples:
     * - "1" → "v1.0.0"
     * - "2.0" → "v2.0.0"
     * - "30.500" → "v30.500.0"
     * - "30.500.100" → "v30.500.100"
     * - "v1.2.3" → "v1.2.3" (already normalized)
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

        // Split by dots and validate each component
        String[] parts = trimmed.split("\\.");

        if (parts.length == 0 || parts.length > 3) {
            throw new IllegalArgumentException("Invalid version format: " + version);
        }

        // Validate and extract version components
        int major = parseVersionNumber(parts[0], "major", version);
        int minor = parts.length > 1 ? parseVersionNumber(parts[1], "minor", version) : 0;
        int patch = parts.length > 2 ? parseVersionNumber(parts[2], "patch", version) : 0;

        return String.format(Locale.ROOT, "v%d.%d.%d", major, minor, patch);
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
     *
     * @param version1 the first version to compare
     * @param version2 the second version to compare
     * @return negative if version1 &lt; version2, zero if equal, positive if version1 &gt; version2
     * @throws IllegalArgumentException if either version is not a valid semantic version
     */
    public static int compareSemanticVersions(String version1, String version2) {
        if (!isSemanticVersion(version1)) {
            throw new IllegalArgumentException("Invalid semantic version: " + version1);
        }
        if (!isSemanticVersion(version2)) {
            throw new IllegalArgumentException("Invalid semantic version: " + version2);
        }

        String normalized1 = normalizeVersion(version1);
        String normalized2 = normalizeVersion(version2);

        VersionComponents v1 = extractVersionComponents(normalized1);
        VersionComponents v2 = extractVersionComponents(normalized2);

        // Compare major version
        int majorCompare = Integer.compare(v1.major, v2.major);
        if (majorCompare != 0) {
            return majorCompare;
        }

        // Compare minor version
        int minorCompare = Integer.compare(v1.minor, v2.minor);
        if (minorCompare != 0) {
            return minorCompare;
        }

        // Compare patch version
        int patchCompare = Integer.compare(v1.patch, v2.patch);
        if (patchCompare != 0) {
            return patchCompare;
        }

        // Compare prerelease (if both have prerelease, compare lexicographically)
        // Version without prerelease is greater than version with prerelease
        if (v1.prerelease == null && v2.prerelease == null) {
            return 0;
        } else if (v1.prerelease == null) {
            return 1; // version1 > version2 (no prerelease > prerelease)
        } else if (v2.prerelease == null) {
            return -1; // version1 < version2 (prerelease < no prerelease)
        } else {
            return v1.prerelease.compareTo(v2.prerelease);
        }
    }

    /**
     * Extracts version components from a normalized version string.
     */
    private static VersionComponents extractVersionComponents(String normalizedVersion) {
        Matcher matcher = VERSION_EXTRACT_PATTERN.matcher(normalizedVersion);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Failed to parse normalized version: " + normalizedVersion);
        }

        int major = Integer.parseInt(matcher.group(1));
        int minor = Integer.parseInt(matcher.group(2));
        int patch = Integer.parseInt(matcher.group(3));
        String prerelease = matcher.group(4); // can be null

        return new VersionComponents(major, minor, patch, prerelease);
    }

    /**
     * Applies a comparison operator to two version strings.
     * Automatically detects whether to use semantic or lexicographic comparison.
     *
     * @param seriesValue the value from the time series tag
     * @param operator the comparison operator
     * @param comparisonValue the value to compare against (determines comparison mode)
     * @return true if the comparison condition is satisfied
     */
    public static boolean applyComparison(String seriesValue, TagComparisonOperator operator, String comparisonValue) {
        if (seriesValue == null || comparisonValue == null) {
            return false;
        }

        // Decision: Use semantic comparison if comparison value is a semantic version
        boolean useSemanticComparison = isSemanticVersion(comparisonValue);

        if (useSemanticComparison) {
            // Series value must also be a valid semantic version for semantic comparison
            if (!isSemanticVersion(seriesValue)) {
                return false; // Filter out invalid versions
            }

            int result = compareSemanticVersions(seriesValue, comparisonValue);
            return operator.apply(result);
        } else {
            // Use lexicographic string comparison
            int result = seriesValue.compareTo(comparisonValue);
            return operator.apply(result);
        }
    }

    /**
     * Internal class to hold version components.
     */
    private static class VersionComponents {
        final int major;
        final int minor;
        final int patch;
        final String prerelease;

        VersionComponents(int major, int minor, int patch, String prerelease) {
            this.major = major;
            this.minor = minor;
            this.patch = patch;
            this.prerelease = prerelease;
        }
    }
}
