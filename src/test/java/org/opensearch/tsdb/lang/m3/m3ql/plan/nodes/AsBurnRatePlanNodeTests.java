/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

public class AsBurnRatePlanNodeTests extends BinaryPlanNodeTests {

    protected BinaryPlanNode getBinaryPlanNode() {
        return new AsBurnRatePlanNode(1, "1d", 99.9);
    }

    public void testAsBurnRatePlanNode() {
        verifyPlanNodeName("AS_BURN_RATE(interval=1d,slo=99.9)");
        verifyVisitorAccept();
    }

    public void testGetters() {
        AsBurnRatePlanNode node = new AsBurnRatePlanNode(1, "1d", 99.9);
        assertEquals("1d", node.getInterval());
        assertEquals(99.9, node.getSlo(), 0.0001);
    }

    public void testIntervalMillis() {
        assertEquals(86400000L, new AsBurnRatePlanNode(1, "1d", 99.9).getIntervalMillis());
        assertEquals(86400000L, new AsBurnRatePlanNode(1, "24h", 99.9).getIntervalMillis());
        assertEquals(1800000L, new AsBurnRatePlanNode(1, "30m", 99.9).getIntervalMillis());
    }

    public void testNegativeIntervalIsRejected() {
        expectThrows(IllegalArgumentException.class, () -> new AsBurnRatePlanNode(1, "-1d", 99.9));
    }

    public void testZeroIntervalIsRejected() {
        expectThrows(IllegalArgumentException.class, () -> new AsBurnRatePlanNode(1, "0m", 99.9));
    }
}
