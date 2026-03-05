/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.expand;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.ValueNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.M3PlannerContext;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AggregationPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.AsPercentPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.FetchPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.M3PlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.MovingPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.ScalePlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.TransformNullPlanNode;
import org.opensearch.tsdb.lang.m3.m3ql.plan.nodes.UnionPlanNode;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class BurnRatePipelineExpanderTests extends OpenSearchTestCase {

    private M3PlannerContext context;
    private final BurnRatePipelineExpander expander = new BurnRatePipelineExpander();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        context = M3PlannerContext.create();
    }

    @Override
    public void tearDown() throws Exception {
        if (context != null) {
            context.close();
        }
        super.tearDown();
    }

    // --- expand burnRate ---

    public void testExpandBurnRateProducesCorrectChain() {
        FunctionNode fn = burnRateFunction("burnRate", "1h", "99.0");

        M3PlanNode lhsPlan = stubPlanNode();
        M3PlanNode rhsPlan = stubPlanNode();

        Function<M3ASTNode, M3PlanNode> planner = node -> rhsPlan;
        Function<List<M3ASTNode>, M3PlanNode> plannerSlice = nodes -> lhsPlan;

        List<M3ASTNode> outerChildren = List.of(new ValueNode("placeholder"));

        M3PlanNode result = expander.expand(outerChildren, 1, fn, planner, plannerSlice);

        // Chain: TransformNull -> Scale -> AsPercent -> [MovingLeft, MovingRight]
        assertInstanceOf(TransformNullPlanNode.class, result);
        assertEquals(1, result.getChildren().size());

        M3PlanNode scale = result.getChildren().getFirst();
        assertInstanceOf(ScalePlanNode.class, scale);
        assertEquals(1.0 / (100.0 - 99.0), ((ScalePlanNode) scale).getScaleFactor(), 0.001);

        M3PlanNode asPercent = scale.getChildren().getFirst();
        assertInstanceOf(AsPercentPlanNode.class, asPercent);
        assertEquals(2, asPercent.getChildren().size());

        M3PlanNode movingLeft = asPercent.getChildren().get(0);
        M3PlanNode movingRight = asPercent.getChildren().get(1);
        assertInstanceOf(MovingPlanNode.class, movingLeft);
        assertInstanceOf(MovingPlanNode.class, movingRight);
    }

    public void testExpandAsBurnRateProducesCorrectChain() {
        FunctionNode fn = burnRateFunction("asBurnRate", "5m", "95.0");

        M3PlanNode lhsPlan = stubPlanNode();
        M3PlanNode rhsPlan = stubPlanNode();

        M3PlanNode result = expander.expand(List.of(new ValueNode("placeholder")), 1, fn, node -> rhsPlan, nodes -> lhsPlan);

        assertInstanceOf(TransformNullPlanNode.class, result);
        ScalePlanNode scale = (ScalePlanNode) result.getChildren().getFirst();
        assertEquals(1.0 / (100.0 - 95.0), scale.getScaleFactor(), 0.001);
    }

    // --- expand multiBurnRate ---

    public void testExpandMultiBurnRateProducesMinOfUnion() {
        FunctionNode fn = multiBurnRateFunction("multiBurnRate", "24h", "2h", "99.9");

        M3PlanNode stubPlan = stubPlanNode();
        Function<List<M3ASTNode>, M3PlanNode> plannerSlice = nodes -> stubPlanNode();

        List<M3ASTNode> outerChildren = List.of(new ValueNode("placeholder"));

        M3PlanNode result = expander.expand(outerChildren, 1, fn, node -> stubPlan, plannerSlice);

        // min(union(burnRate1, burnRate2))
        assertInstanceOf(AggregationPlanNode.class, result);
        AggregationPlanNode min = (AggregationPlanNode) result;
        assertEquals(1, min.getChildren().size());

        M3PlanNode union = min.getChildren().getFirst();
        assertInstanceOf(UnionPlanNode.class, union);
        assertEquals(2, union.getChildren().size());

        // Each child of union is a burn rate chain (TransformNull at root)
        assertInstanceOf(TransformNullPlanNode.class, union.getChildren().get(0));
        assertInstanceOf(TransformNullPlanNode.class, union.getChildren().get(1));
    }

    public void testExpandAsMultiBurnRateProducesMinOfUnion() {
        FunctionNode fn = multiBurnRateFunction("asMultiBurnRate", "1h", "5m", "99.0");

        M3PlanNode result = expander.expand(List.of(new ValueNode("placeholder")), 1, fn, node -> stubPlanNode(), nodes -> stubPlanNode());

        assertInstanceOf(AggregationPlanNode.class, result);
    }

    // --- validation errors ---

    public void testBurnRateWrongArgCountThrows() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("burnRate");
        fn.addChildNode(new PipelineNode());
        fn.addChildNode(new ValueNode("1h"));
        // missing slo argument

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> expander.expand(List.of(new ValueNode("x")), 1, fn, node -> stubPlanNode(), nodes -> stubPlanNode())
        );
        assertTrue(e.getMessage().contains("expects exactly 3 arguments"));
    }

    public void testMultiBurnRateWrongArgCountThrows() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("multiBurnRate");
        fn.addChildNode(new PipelineNode());
        fn.addChildNode(new ValueNode("1h"));
        fn.addChildNode(new ValueNode("99.0"));
        // missing interval2

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> expander.expand(List.of(new ValueNode("x")), 1, fn, node -> stubPlanNode(), nodes -> stubPlanNode())
        );
        assertTrue(e.getMessage().contains("expects exactly 4 arguments"));
    }

    public void testBurnRateInvalidIntervalThrows() {
        FunctionNode fn = burnRateFunction("burnRate", "notADuration", "99.0");

        expectThrows(
            IllegalArgumentException.class,
            () -> expander.expand(List.of(new ValueNode("x")), 1, fn, node -> stubPlanNode(), nodes -> stubPlanNode())
        );
    }

    public void testBurnRateInvalidSloThrows() {
        FunctionNode fn = burnRateFunction("burnRate", "1h", "not_a_number");

        expectThrows(
            IllegalArgumentException.class,
            () -> expander.expand(List.of(new ValueNode("x")), 1, fn, node -> stubPlanNode(), nodes -> stubPlanNode())
        );
    }

    public void testBurnRateSloOutOfRangeThrows() {
        FunctionNode fn = burnRateFunction("burnRate", "1h", "100.0");

        expectThrows(
            IllegalArgumentException.class,
            () -> expander.expand(List.of(new ValueNode("x")), 1, fn, node -> stubPlanNode(), nodes -> stubPlanNode())
        );
    }

    public void testBurnRateNonValueIntervalThrows() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("burnRate");
        fn.addChildNode(new PipelineNode());
        fn.addChildNode(new FunctionNode()); // not a ValueNode
        fn.addChildNode(new ValueNode("99.0"));

        expectThrows(
            IllegalArgumentException.class,
            () -> expander.expand(List.of(new ValueNode("x")), 1, fn, node -> stubPlanNode(), nodes -> stubPlanNode())
        );
    }

    public void testBurnRateNonValueSloThrows() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("burnRate");
        fn.addChildNode(new PipelineNode());
        fn.addChildNode(new ValueNode("1h"));
        fn.addChildNode(new FunctionNode()); // not a ValueNode

        expectThrows(
            IllegalArgumentException.class,
            () -> expander.expand(List.of(new ValueNode("x")), 1, fn, node -> stubPlanNode(), nodes -> stubPlanNode())
        );
    }

    public void testExpandUnsupportedFunctionThrows() {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName("unsupported");
        fn.addChildNode(new PipelineNode());
        fn.addChildNode(new ValueNode("1h"));
        fn.addChildNode(new ValueNode("99.0"));

        expectThrows(
            IllegalArgumentException.class,
            () -> expander.expand(List.of(new ValueNode("x")), 1, fn, node -> stubPlanNode(), nodes -> stubPlanNode())
        );
    }

    // --- helpers ---

    private static FunctionNode burnRateFunction(String name, String interval, String slo) {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName(name);
        fn.addChildNode(new PipelineNode()); // rhs pipeline (child 0)
        fn.addChildNode(new ValueNode(interval));
        fn.addChildNode(new ValueNode(slo));
        return fn;
    }

    private static FunctionNode multiBurnRateFunction(String name, String interval1, String interval2, String slo) {
        FunctionNode fn = new FunctionNode();
        fn.setFunctionName(name);
        fn.addChildNode(new PipelineNode()); // rhs pipeline (child 0)
        fn.addChildNode(new ValueNode(interval1));
        fn.addChildNode(new ValueNode(interval2));
        fn.addChildNode(new ValueNode(slo));
        return fn;
    }

    private M3PlanNode stubPlanNode() {
        return new FetchPlanNode(M3PlannerContext.generateId(), Map.of(), Map.of());
    }

    private static void assertInstanceOf(Class<?> expected, Object actual) {
        assertTrue("Expected " + expected.getSimpleName() + " but got " + actual.getClass().getSimpleName(), expected.isInstance(actual));
    }
}
