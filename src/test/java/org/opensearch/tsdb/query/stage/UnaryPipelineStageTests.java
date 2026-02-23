/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class UnaryPipelineStageTests extends OpenSearchTestCase {

    public void testUnaryPipelineStageInheritance() {
        // Verify UnaryPipelineStage extends PipelineStage
        assertTrue("UnaryPipelineStage should extend PipelineStage", PipelineStage.class.isAssignableFrom(UnaryPipelineStage.class));
    }

    public void testInterfaceMethodSignatures() throws Exception {
        // Verify the interface has expected method signatures
        Class<UnaryPipelineStage> interfaceClass = UnaryPipelineStage.class;

        // Should inherit process method from PipelineStage
        assertTrue(
            "Should have process method from parent",
            Arrays.stream(interfaceClass.getMethods())
                .anyMatch(
                    m -> "process".equals(m.getName())
                        && m.getParameterCount() == 1
                        && List.class.isAssignableFrom(m.getParameterTypes()[0])
                )
        );

        // Should inherit getName method from PipelineStage
        assertTrue(
            "Should have getName method from parent",
            Arrays.stream(interfaceClass.getMethods())
                .anyMatch(m -> "getName".equals(m.getName()) && m.getParameterCount() == 0 && String.class.equals(m.getReturnType()))
        );

        // Should inherit supportConcurrentSegmentSearch method from PipelineStage
        assertTrue(
            "Should have supportConcurrentSegmentSearch method from parent",
            Arrays.stream(interfaceClass.getMethods())
                .anyMatch(
                    m -> "supportConcurrentSegmentSearch".equals(m.getName())
                        && m.getParameterCount() == 0
                        && boolean.class.equals(m.getReturnType())
                )
        );
    }

    public void testDefaultMethodInheritance() {
        // Create a minimal test implementation to verify default method behavior
        TestUnaryPipelineStage testStage = new TestUnaryPipelineStage();

        // Test default implementations
        assertFalse("Default isCoordinatorOnly should return false", testStage.isCoordinatorOnly());
        assertFalse("Default supportConcurrentSegmentSearch should return false", testStage.supportConcurrentSegmentSearch());
    }

    // Minimal test implementation to test default methods
    private static class TestUnaryPipelineStage implements UnaryPipelineStage {
        @Override
        public List<TimeSeries> process(List<TimeSeries> input) {
            return input; // Pass-through implementation
        }

        @Override
        public String getName() {
            return "test";
        }

        @Override
        public void toXContent(XContentBuilder builder, ToXContent.Params params) {
            // Empty implementation for testing
        }

        @Override
        public void writeTo(StreamOutput out) {
            // Empty implementation for testing
        }

    }

    public void testReduceMethodThrowsException() {
        // Test that reduce method throws exception for unary stages
        TestUnaryPipelineStage stage = new TestUnaryPipelineStage();

        List<TimeSeriesProvider> aggregations = Arrays.asList(createMockProvider("provider1"), createMockProvider("provider2"));

        // Test the reduce method - should throw UnsupportedOperationException for unary stages
        UnsupportedOperationException exception = assertThrows(
            UnsupportedOperationException.class,
            () -> stage.reduce(aggregations, false, null)
        );

        assertTrue("Exception message should contain class name", exception.getMessage().contains("TestUnaryPipelineStage"));
        assertTrue("Exception message should mention reduce function", exception.getMessage().contains("reduce function"));
    }

    public void testIsGlobalAggregationDefault() {
        // Test default isGlobalAggregation behavior
        TestUnaryPipelineStage stage = new TestUnaryPipelineStage();
        assertFalse("Default isGlobalAggregation should be false", stage.isGlobalAggregation());

        // Test through interface
        UnaryPipelineStage unaryStage = stage;
        assertFalse("Interface isGlobalAggregation should be false", unaryStage.isGlobalAggregation());
    }

    public void testIsCoordinatorOnlyDefault() {
        // Test default isCoordinatorOnly behavior
        TestUnaryPipelineStage stage = new TestUnaryPipelineStage();
        assertFalse("Default isCoordinatorOnly should be false", stage.isCoordinatorOnly());

        // Test through interface
        UnaryPipelineStage unaryStage = stage;
        assertFalse("Interface isCoordinatorOnly should be false", unaryStage.isCoordinatorOnly());
    }

    // Helper methods for creating mock providers
    private TimeSeriesProvider createMockProvider(String name) {
        return new TimeSeriesProvider() {
            @Override
            public List<TimeSeries> getTimeSeries() {
                return Collections.emptyList();
            }

            @Override
            public TimeSeriesProvider createReduced(List<TimeSeries> reducedTimeSeries) {
                return this;
            }
        };
    }

}
