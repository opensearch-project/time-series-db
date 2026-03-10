/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.common;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link Utils}.
 */
public class UtilsTests extends OpenSearchTestCase {

    /**
     * Valid SLO in (0, 100) does not throw.
     */
    public void testValidateSlo_acceptsValidSlo() {
        Utils.validateSlo(0.1);
        Utils.validateSlo(1.0);
        Utils.validateSlo(50.0);
        Utils.validateSlo(99.0);
        Utils.validateSlo(99.9);
    }

    /**
     * SLO zero is rejected.
     */
    public void testValidateSlo_rejectsZero() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Utils.validateSlo(0.0));
        assertEquals("SLO must be between 0 and 100 (exclusive), got: 0.0", e.getMessage());
    }

    /**
     * SLO below zero is rejected.
     */
    public void testValidateSlo_rejectsNegative() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Utils.validateSlo(-1.0));
        assertEquals("SLO must be between 0 and 100 (exclusive), got: -1.0", e.getMessage());
    }

    /**
     * SLO NaN is rejected.
     */
    public void testValidateSlo_rejectsNaN() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Utils.validateSlo(Double.NaN));
        assertEquals("SLO must be a finite number, got: NaN", e.getMessage());
    }

    /**
     * SLO positive infinity is rejected.
     */
    public void testValidateSlo_rejectsPositiveInfinity() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> Utils.validateSlo(Double.POSITIVE_INFINITY));
        assertEquals("SLO must be a finite number, got: Infinity", e.getMessage());
    }
}
