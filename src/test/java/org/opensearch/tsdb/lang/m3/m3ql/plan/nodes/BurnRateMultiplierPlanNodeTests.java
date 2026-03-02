/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.common.Constants;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;

/**
 * Unit tests for BurnRateMultiplierPlanNode with burn-rate factor).
 */
public class BurnRateMultiplierPlanNodeTests extends OpenSearchTestCase {

    private static FunctionNode burnRateMultiplierNode(String sloValue) {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName(Constants.Functions.BURN_RATE_MULTIPLIER);
        fn.addChildNode(new ValueNode(sloValue));
        return fn;
    }

    /**
     * burnRateMultiplier with valid SLO returns ScalePlanNode with factor 100/(100-slo).
     */
    public void testOf_validSlo_returnsScaleNodeWithFactor() throws Exception {
        try (M3PlannerContext context = M3PlannerContext.create()) {
            ScalePlanNode node = BurnRateMultiplierPlanNode.of(burnRateMultiplierNode("99.9"));
            assertEquals(1000.0, node.getScaleFactor(), 1e-6);
            assertTrue(node.getExplainName().contains("1000"));
        }
    }

    /**
     * burnRateMultiplier with SLO 90 returns ScalePlanNode with factor 10.
     */
    public void testOf_slo90_returnsScaleNodeWithFactor10() throws Exception {
        try (M3PlannerContext context = M3PlannerContext.create()) {
            ScalePlanNode node = BurnRateMultiplierPlanNode.of(burnRateMultiplierNode("90"));
            assertEquals(10.0, node.getScaleFactor(), 1e-6);
        }
    }

    /**
     * burnRateMultiplier with no arguments throws.
     */
    public void testOf_noArgument_throws() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName(Constants.Functions.BURN_RATE_MULTIPLIER);
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> BurnRateMultiplierPlanNode.of(fn));
        assertEquals("burnRateMultiplier expects exactly one argument", e.getMessage());
    }

    /**
     * burnRateMultiplier with non-ValueNode argument throws.
     */
    public void testOf_nonValueNodeArgument_throws() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName(Constants.Functions.BURN_RATE_MULTIPLIER);
        fn.addChildNode(new FunctionNode());
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> BurnRateMultiplierPlanNode.of(fn));
        assertEquals("Argument to burnRateMultiplier should be a value node", e.getMessage());
    }

    /**
     * burnRateMultiplier with non-numeric SLO throws.
     */
    public void testOf_nonNumericSlo_throws() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> BurnRateMultiplierPlanNode.of(burnRateMultiplierNode("abc"))
        );
        assertEquals("SLO must be a numeric value, got: abc", e.getMessage());
    }

    /**
     * burnRateMultiplier with SLO 100 throws.
     */
    public void testOf_slo100_throws() {
        IllegalArgumentException e = assertThrows(
            IllegalArgumentException.class,
            () -> BurnRateMultiplierPlanNode.of(burnRateMultiplierNode("100"))
        );
        assertEquals("SLO must be between 0 and 100 (exclusive), got: 100.0", e.getMessage());
    }
}
