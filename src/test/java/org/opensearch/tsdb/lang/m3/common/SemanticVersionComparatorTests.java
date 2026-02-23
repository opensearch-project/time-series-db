/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

import org.opensearch.test.OpenSearchTestCase;

public class SemanticVersionComparatorTests extends OpenSearchTestCase {

    public void testVersionNormalizationSimple() {
        assertEquals("v1.0.0", SemanticVersionComparator.normalizeVersion("1"));
        assertEquals("v2.0.0", SemanticVersionComparator.normalizeVersion("2.0"));
        assertEquals("v1.2.3", SemanticVersionComparator.normalizeVersion("1.2.3"));
        assertEquals("v1.2.3", SemanticVersionComparator.normalizeVersion("v1.2.3"));
    }

    public void testVersionNormalizationWithIncompleteVersions() {
        assertEquals("v30.500.0", SemanticVersionComparator.normalizeVersion("30.500"));
        assertEquals("v29.5.0", SemanticVersionComparator.normalizeVersion("29.5"));
        assertEquals("v30.500.100", SemanticVersionComparator.normalizeVersion("30.500.100"));
        assertEquals("v30.600.0", SemanticVersionComparator.normalizeVersion("30.600"));
    }

    public void testVersionNormalizationEdgeCases() {
        assertEquals("v0.0.0", SemanticVersionComparator.normalizeVersion("0"));
        assertEquals("v0.1.0", SemanticVersionComparator.normalizeVersion("0.1"));
        assertEquals("v0.0.1", SemanticVersionComparator.normalizeVersion("0.0.1"));
    }

    public void testVersionNormalizationWithPrerelease() {
        assertEquals("v1.10.0-alpha", SemanticVersionComparator.normalizeVersion("1.10.0-alpha"));
        assertEquals("v1.0.0-alpha.1", SemanticVersionComparator.normalizeVersion("1.0.0-alpha.1"));
        assertEquals("v1.2.3-beta", SemanticVersionComparator.normalizeVersion("v1.2.3-beta"));
        assertEquals("v2.0.0-rc.1", SemanticVersionComparator.normalizeVersion("2.0.0-rc.1"));
        assertEquals("v1.0.0-0.3.7", SemanticVersionComparator.normalizeVersion("1.0.0-0.3.7"));
    }

    public void testVersionNormalizationWithMultipleHyphens() {
        // Multiple hyphens allowed — everything after first hyphen (preceded by digit) is prerelease
        assertEquals("v1.2.3-1.2.3-abc", SemanticVersionComparator.normalizeVersion("1.2.3-1.2.3-abc"));
        assertEquals("v1.0.0-alpha-1", SemanticVersionComparator.normalizeVersion("1.0.0-alpha-1"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("1.2.3-1.2.3-abc"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("1.0.0-alpha-1"));
    }

    public void testVersionNormalizationInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion(null));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion(""));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("   "));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("abc"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("1.2.3.4"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("1.-1"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("1.0.0-"));
    }

    public void testSemanticVersionDetection() {
        // Valid semantic versions
        assertTrue(SemanticVersionComparator.isSemanticVersion("1"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("1.0"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("1.2.3"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("v1.2.3"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("30.500.100"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("29.5"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("0.1.10"));

        // Valid semantic versions with prerelease
        assertTrue(SemanticVersionComparator.isSemanticVersion("1.0.0-alpha"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("1.10.0-alpha.1"));
        assertTrue(SemanticVersionComparator.isSemanticVersion("v2.0.0-rc.1"));

        // Invalid semantic versions
        assertFalse(SemanticVersionComparator.isSemanticVersion(null));
        assertFalse(SemanticVersionComparator.isSemanticVersion(""));
        assertFalse(SemanticVersionComparator.isSemanticVersion("abc"));
        assertFalse(SemanticVersionComparator.isSemanticVersion("1.2.3.4"));
        assertFalse(SemanticVersionComparator.isSemanticVersion("not-a-version"));
    }

    public void testSemanticVersionComparison() {
        // Major version differences
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.0", "2.0.0") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("2.0.0", "1.0.0") > 0);

        // Minor version differences
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.1.0", "1.2.0") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.2.0", "1.1.0") > 0);

        // Patch version differences
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.1", "1.0.2") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.2", "1.0.1") > 0);

        // Equal versions
        assertEquals(0, SemanticVersionComparator.compareSemanticVersions("1.2.3", "1.2.3"));
        assertEquals(0, SemanticVersionComparator.compareSemanticVersions("1", "1.0.0"));
        assertEquals(0, SemanticVersionComparator.compareSemanticVersions("1.2", "1.2.0"));
    }

    public void testSemanticVersionComparisonFromExamples() {
        // From the documentation examples
        assertTrue(SemanticVersionComparator.compareSemanticVersions("29.5", "30.500.100") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("30.5", "30.500.100") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("30.500.1", "30.500.100") < 0);
        assertEquals(0, SemanticVersionComparator.compareSemanticVersions("30.500.100", "30.500.100"));
        assertTrue(SemanticVersionComparator.compareSemanticVersions("30.600", "30.500.100") > 0);
    }

    public void testSemanticVersionComparisonInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.compareSemanticVersions("1.2.3", "invalid"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.compareSemanticVersions("invalid", "1.2.3"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.compareSemanticVersions(null, "1.2.3"));
    }

    public void testSemanticVersionComparisonAllOperatorsViaCompare() {
        // Test compareSemanticVersions result can be used with all comparison operators
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.0", "2.0.0") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("2.0.0", "1.0.0") > 0);
        assertEquals(0, SemanticVersionComparator.compareSemanticVersions("1.0.0", "1.0.0"));
    }

    // ========== Prerelease Comparison Tests ==========

    public void testPrereleaseSameBaseDifferentPrerelease() {
        // 1.10.0-alpha < 1.10.0-beta (alphabetically)
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.10.0-alpha", "1.10.0-beta") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.10.0-beta", "1.10.0-alpha") > 0);
    }

    public void testPrereleaseVsRelease() {
        // 1.10.0-alpha < 1.10.0 (prerelease is always less than release)
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.10.0-alpha", "1.10.0") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.10.0", "1.10.0-alpha") > 0);
    }

    public void testPrereleaseNumericIdentifiers() {
        // 1.10.0-alpha.1 < 1.10.0-alpha.2 (numeric identifiers compared as numbers)
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.10.0-alpha.1", "1.10.0-alpha.2") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.10.0-alpha.2", "1.10.0-alpha.1") > 0);

        // Numeric comparison, not string: 1.0.0-2 < 1.0.0-10 (as numbers, not strings)
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.0-2", "1.0.0-10") < 0);
    }

    public void testPrereleaseEqualIdentifiers() {
        assertEquals(0, SemanticVersionComparator.compareSemanticVersions("1.0.0-alpha", "1.0.0-alpha"));
        assertEquals(0, SemanticVersionComparator.compareSemanticVersions("1.0.0-alpha.1", "1.0.0-alpha.1"));
    }

    public void testPrereleaseShorterSetLowerPrecedence() {
        // 1.0.0-alpha < 1.0.0-alpha.1 (shorter set has lower precedence)
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.0-alpha", "1.0.0-alpha.1") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.0-alpha.1", "1.0.0-alpha") > 0);
    }

    public void testPrereleaseNumericVsString() {
        // Numeric identifiers have lower precedence than string identifiers
        // 1.0.0-1 < 1.0.0-alpha (numeric < string)
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.0-1", "1.0.0-alpha") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.0-alpha", "1.0.0-1") > 0);
    }

    public void testPrereleaseDifferentBaseVersions() {
        // When base versions differ, prerelease is irrelevant
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.0-alpha", "2.0.0-alpha") < 0);
        assertTrue(SemanticVersionComparator.compareSemanticVersions("1.0.0-alpha", "1.1.0") < 0);
    }
}
