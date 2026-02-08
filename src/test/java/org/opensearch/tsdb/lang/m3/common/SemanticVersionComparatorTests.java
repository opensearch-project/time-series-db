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

    public void testVersionNormalizationInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion(null));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion(""));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("   "));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("abc"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("1.2.3.4"));
        assertThrows(IllegalArgumentException.class, () -> SemanticVersionComparator.normalizeVersion("1.-1"));
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

    public void testApplyComparisonSemanticMode() {
        // When comparison value is semantic version, use semantic comparison
        String comparisonValue = "1.2.0";  // This is semantic version
        assertTrue(SemanticVersionComparator.isSemanticVersion(comparisonValue));

        // Valid series values with semantic comparison
        assertTrue(SemanticVersionComparator.applyComparison("1.1.0", TagComparisonOperator.LT, comparisonValue));
        assertFalse(SemanticVersionComparator.applyComparison("1.3.0", TagComparisonOperator.LT, comparisonValue));
        assertTrue(SemanticVersionComparator.applyComparison("1.2.0", TagComparisonOperator.EQ, comparisonValue));
        assertFalse(SemanticVersionComparator.applyComparison("1.2.1", TagComparisonOperator.EQ, comparisonValue));

        // Invalid series values should be filtered out (return false)
        assertFalse(SemanticVersionComparator.applyComparison("invalid", TagComparisonOperator.LT, comparisonValue));
        assertFalse(SemanticVersionComparator.applyComparison("abc", TagComparisonOperator.EQ, comparisonValue));
    }

    public void testApplyComparisonLexicographicMode() {
        // When comparison value is not semantic version, use lexicographic comparison
        String comparisonValue = "denver";  // This is not a semantic version
        assertFalse(SemanticVersionComparator.isSemanticVersion(comparisonValue));

        // Lexicographic string comparison
        assertTrue(SemanticVersionComparator.applyComparison("atlanta", TagComparisonOperator.LT, comparisonValue));
        assertTrue(SemanticVersionComparator.applyComparison("boston", TagComparisonOperator.LT, comparisonValue));
        assertTrue(SemanticVersionComparator.applyComparison("chicago", TagComparisonOperator.LT, comparisonValue));
        assertFalse(SemanticVersionComparator.applyComparison("denver", TagComparisonOperator.LT, comparisonValue));
        assertFalse(SemanticVersionComparator.applyComparison("florida", TagComparisonOperator.LT, comparisonValue));

        assertTrue(SemanticVersionComparator.applyComparison("denver", TagComparisonOperator.EQ, comparisonValue));
        assertFalse(SemanticVersionComparator.applyComparison("atlanta", TagComparisonOperator.EQ, comparisonValue));
    }

    public void testApplyComparisonNullInputs() {
        assertFalse(SemanticVersionComparator.applyComparison(null, TagComparisonOperator.EQ, "1.0.0"));
        assertFalse(SemanticVersionComparator.applyComparison("1.0.0", TagComparisonOperator.EQ, null));
        assertFalse(SemanticVersionComparator.applyComparison(null, TagComparisonOperator.EQ, null));
    }

    public void testAllComparisonOperators() {
        String v1 = "1.0.0";
        String v2 = "2.0.0";

        assertTrue(SemanticVersionComparator.applyComparison(v1, TagComparisonOperator.LT, v2));
        assertTrue(SemanticVersionComparator.applyComparison(v1, TagComparisonOperator.LE, v2));
        assertTrue(SemanticVersionComparator.applyComparison(v1, TagComparisonOperator.LE, v1));
        assertFalse(SemanticVersionComparator.applyComparison(v1, TagComparisonOperator.GT, v2));
        assertFalse(SemanticVersionComparator.applyComparison(v1, TagComparisonOperator.GE, v2));
        assertTrue(SemanticVersionComparator.applyComparison(v1, TagComparisonOperator.GE, v1));
        assertTrue(SemanticVersionComparator.applyComparison(v1, TagComparisonOperator.EQ, v1));
        assertFalse(SemanticVersionComparator.applyComparison(v1, TagComparisonOperator.EQ, v2));
        assertTrue(SemanticVersionComparator.applyComparison(v1, TagComparisonOperator.NE, v2));
        assertFalse(SemanticVersionComparator.applyComparison(v1, TagComparisonOperator.NE, v1));
    }
}
