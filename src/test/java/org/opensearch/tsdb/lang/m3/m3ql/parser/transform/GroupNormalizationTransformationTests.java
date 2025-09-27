/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser.transform;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.FunctionNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.GroupNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.M3ASTNode;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.PipelineNode;

import java.util.List;

public class GroupNormalizationTransformationTests extends OpenSearchTestCase {

    public void testSimpleGroupWithoutFetchIsFlattened() {
        GroupNormalizationTransformation transformation = new GroupNormalizationTransformation();

        // Create group with just functions: (moving | sum)
        GroupNode group = new GroupNode();
        group.addChildNode(createFunction("moving"));
        group.addChildNode(createFunction("sum"));

        assertTrue("Should transform groups without fetch", transformation.canTransform(group));

        List<M3ASTNode> result = transformation.transform(group);
        assertEquals("Should return the children", 2, result.size());
        assertEquals("moving", ((FunctionNode) result.get(0)).getFunctionName());
        assertEquals("sum", ((FunctionNode) result.get(1)).getFunctionName());
    }

    public void testGroupWithFetchIsPreserved() {
        GroupNormalizationTransformation transformation = new GroupNormalizationTransformation();

        // Create group with fetch: (fetch | sum)
        GroupNode group = new GroupNode();
        group.addChildNode(createFunction("fetch"));
        group.addChildNode(createFunction("sum"));

        assertFalse("Unable to transform groups with fetch as the first function", transformation.canTransform(group));
    }

    public void testGroupWithFetchNotFirstDropsPrecedingChildren() {
        GroupNormalizationTransformation transformation = new GroupNormalizationTransformation();

        // Create group with fetch not first: (moving | fetch | sum)
        GroupNode group = new GroupNode();
        group.addChildNode(createFunction("moving"));
        group.addChildNode(createFunction("fetch"));
        group.addChildNode(createFunction("sum"));

        List<M3ASTNode> result = transformation.transform(group);
        assertEquals("Should return modified group", 1, result.size());

        GroupNode modifiedGroup = (GroupNode) result.getFirst();
        assertEquals("Should have 2 children", 2, modifiedGroup.getChildren().size());
        assertEquals("fetch", ((FunctionNode) modifiedGroup.getChildren().get(0)).getFunctionName());
        assertEquals("sum", ((FunctionNode) modifiedGroup.getChildren().get(1)).getFunctionName());
    }

    public void testNestedGroups() {
        GroupNormalizationTransformation transformation = new GroupNormalizationTransformation();

        // Create nested structure: fetch | ((moving | sum) | transformNull)
        // This represents: fetch name:abc | ((moving 1h avg | sum) | transformNull)
        PipelineNode root = new PipelineNode();
        root.addChildNode(createFunction("fetch"));

        // Create outer group: ((moving | sum) | transformNull)
        GroupNode outerGroup = new GroupNode();

        // Create inner group: (moving | sum)
        GroupNode innerGroup = new GroupNode();
        innerGroup.addChildNode(createFunction("moving"));
        innerGroup.addChildNode(createFunction("sum"));

        outerGroup.addChildNode(innerGroup);
        outerGroup.addChildNode(createFunction("transformNull"));

        root.addChildNode(outerGroup);

        // Apply transformation using ASTTransformer to test nested handling
        ASTTransformer transformer = new ASTTransformer(List.of(transformation));
        transformer.transform(root);

        // Verify the structure after transformation
        // Should be: fetch | moving | sum | transformNull (all groups flattened)
        assertEquals("Root should have 4 children", 4, root.getChildren().size());
        assertEquals("fetch", ((FunctionNode) root.getChildren().get(0)).getFunctionName());
        assertEquals("moving", ((FunctionNode) root.getChildren().get(1)).getFunctionName());
        assertEquals("sum", ((FunctionNode) root.getChildren().get(2)).getFunctionName());
        assertEquals("transformNull", ((FunctionNode) root.getChildren().get(3)).getFunctionName());
    }

    public void testTripleNestedGroups() {
        GroupNormalizationTransformation transformation = new GroupNormalizationTransformation();

        // Create triple nested structure: (((moving | sum)))
        GroupNode outerGroup = new GroupNode();
        GroupNode middleGroup = new GroupNode();
        GroupNode innerGroup = new GroupNode();

        innerGroup.addChildNode(createFunction("moving"));
        innerGroup.addChildNode(createFunction("sum"));
        middleGroup.addChildNode(innerGroup);
        outerGroup.addChildNode(middleGroup);

        // Apply transformation multiple times to test deep nesting
        ASTTransformer transformer = new ASTTransformer(List.of(transformation));

        // Create a container to hold the group
        PipelineNode container = new PipelineNode();
        container.addChildNode(outerGroup);

        transformer.transform(container);

        // After transformation, should be flattened to just the functions
        assertEquals("Container should have 2 children", 2, container.getChildren().size());
        assertEquals("moving", ((FunctionNode) container.getChildren().get(0)).getFunctionName());
        assertEquals("sum", ((FunctionNode) container.getChildren().get(1)).getFunctionName());
    }

    public void testMixedNestedGroupsWithFetch() {
        GroupNormalizationTransformation transformation = new GroupNormalizationTransformation();

        // Create structure: ((fetch | sum) | transformNull)
        // Inner group has fetch (should be preserved), outer group should be flattened
        GroupNode outerGroup = new GroupNode();
        GroupNode innerGroupWithFetch = new GroupNode();

        innerGroupWithFetch.addChildNode(createFunction("fetch"));
        innerGroupWithFetch.addChildNode(createFunction("sum"));

        outerGroup.addChildNode(innerGroupWithFetch);
        outerGroup.addChildNode(createFunction("transformNull"));

        PipelineNode container = new PipelineNode();
        container.addChildNode(outerGroup);

        ASTTransformer transformer = new ASTTransformer(List.of(transformation));
        transformer.transform(container);

        // After transformation: innerGroupWithFetch | transformNull
        assertEquals("Container should have 2 children", 2, container.getChildren().size());
        assertTrue("First child should be a group", container.getChildren().get(0) instanceof GroupNode);
        assertEquals("transformNull", ((FunctionNode) container.getChildren().get(1)).getFunctionName());

        // The preserved group should still have fetch and sum
        GroupNode preservedGroup = (GroupNode) container.getChildren().get(0);
        assertEquals("Preserved group should have 2 children", 2, preservedGroup.getChildren().size());
        assertEquals("fetch", ((FunctionNode) preservedGroup.getChildren().get(0)).getFunctionName());
        assertEquals("sum", ((FunctionNode) preservedGroup.getChildren().get(1)).getFunctionName());
    }

    private FunctionNode createFunction(String name) {
        FunctionNode function = new FunctionNode();
        function.setFunctionName(name);
        return function;
    }
}
