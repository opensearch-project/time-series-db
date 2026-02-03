/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for TimeSeriesUnfoldAggregatorFactory.
 */
public class TimeSeriesUnfoldAggregatorFactoryTests extends OpenSearchTestCase {

    public void testSupportsConcurrentSegmentSearchWithSupportingStages() {
        // Arrange - ScaleStage supports concurrent segment search
        List<UnaryPipelineStage> stages = List.of(new ScaleStage(2.0));

        // Create factory with minimal parameters (nulls are acceptable for this test)
        TimeSeriesUnfoldAggregatorFactory factory = createFactory("test_css", stages, 1000L, 2000L, 100L);

        // Act & Assert
        assertTrue("Factory with ScaleStage should support CSS", factory.supportsConcurrentSegmentSearch());
    }

    public void testSupportsConcurrentSegmentSearchWithEmptyStages() {
        // Arrange - Empty stages should support CSS
        List<UnaryPipelineStage> emptyStages = List.of();

        TimeSeriesUnfoldAggregatorFactory factory = createFactory("test_empty", emptyStages, 1000L, 2000L, 100L);

        // Act & Assert
        assertTrue("Factory with empty stages should support CSS", factory.supportsConcurrentSegmentSearch());
    }

    public void testSupportsConcurrentSegmentSearchWithNullStages() {
        // Arrange - Null stages should support CSS
        TimeSeriesUnfoldAggregatorFactory factory = createFactory("test_null", null, 1000L, 2000L, 100L);

        // Act & Assert
        assertTrue("Factory with null stages should support CSS", factory.supportsConcurrentSegmentSearch());
    }

    public void testSupportsConcurrentSegmentSearchWithMixedStages() {
        // Arrange - Mix of stages where all support CSS
        List<UnaryPipelineStage> stages = List.of(
            new ScaleStage(2.0),  // Supports CSS
            new ScaleStage(0.5)   // Supports CSS
        );

        TimeSeriesUnfoldAggregatorFactory factory = createFactory("test_mixed", stages, 1000L, 2000L, 100L);

        // Act & Assert
        assertTrue("Factory with all CSS-supporting stages should support CSS", factory.supportsConcurrentSegmentSearch());
    }

    public void testFactoryConfiguration() {
        // Arrange
        List<UnaryPipelineStage> stages = List.of(new ScaleStage(3.0));
        long minTimestamp = 5000L;
        long maxTimestamp = 10000L;
        long step = 250L;

        // Act
        TimeSeriesUnfoldAggregatorFactory factory = createFactory("config_test", stages, minTimestamp, maxTimestamp, step);

        // Assert - We can't directly access private fields, but we can verify the factory was created successfully
        assertNotNull("Factory should be created successfully", factory);
        assertEquals("config_test", factory.name());
    }

    public void testFactoryWithLargeTimeRange() {
        // Arrange - Test with large timestamp values
        List<UnaryPipelineStage> stages = List.of(new ScaleStage(1.0));
        long minTimestamp = Long.MIN_VALUE;
        long maxTimestamp = Long.MAX_VALUE;
        long step = Long.MAX_VALUE;

        // Act
        TimeSeriesUnfoldAggregatorFactory factory = createFactory("large_range", stages, minTimestamp, maxTimestamp, step);

        // Assert
        assertNotNull("Factory should handle large timestamp values", factory);
        assertTrue("Should support CSS with simple stages", factory.supportsConcurrentSegmentSearch());
    }

    public void testFactoryWithZeroStep() {
        // Arrange - Test edge case with zero step
        List<UnaryPipelineStage> stages = List.of(new ScaleStage(1.0));

        // Act
        TimeSeriesUnfoldAggregatorFactory factory = createFactory("zero_step", stages, 1000L, 2000L, 0L);

        // Assert
        assertNotNull("Factory should handle zero step", factory);
    }

    /**
     * Tests that factory properly initializes circuit breaker warning threshold with null QueryShardContext.
     * This verifies the null-safe initialization logic that uses default value when context is not available.
     */
    public void testCircuitBreakerWarnThresholdWithNullContext() {
        // Arrange - Create factory with null QueryShardContext (typical in tests)
        List<UnaryPipelineStage> stages = List.of(new ScaleStage(1.0));

        // Act - Should not throw NPE
        TimeSeriesUnfoldAggregatorFactory factory = createFactory("null_context", stages, 1000L, 2000L, 100L);

        // Assert - Factory created successfully without NPE
        assertNotNull("Factory should be created successfully with null context", factory);
    }

    /**
     * Tests that factory can be created with all edge case parameters.
     */
    public void testFactoryWithEdgeCaseParameters() {
        // Arrange - Null stages, large timestamps, zero step
        // Act
        TimeSeriesUnfoldAggregatorFactory factory = createFactory(
            "edge_case",
            null,  // Null stages
            Long.MIN_VALUE,  // Min timestamp
            Long.MAX_VALUE,  // Max timestamp
            0L  // Zero step
        );

        // Assert
        assertNotNull("Factory should handle all edge cases", factory);
        assertTrue("Should support CSS with null stages", factory.supportsConcurrentSegmentSearch());
    }

    /**
     * Helper method to create TimeSeriesUnfoldAggregatorFactory with minimal parameters.
     * Uses null for complex OpenSearch infrastructure components that aren't needed for basic tests.
     */
    private TimeSeriesUnfoldAggregatorFactory createFactory(
        String name,
        List<UnaryPipelineStage> stages,
        long minTimestamp,
        long maxTimestamp,
        long step
    ) {
        try {
            // Create an empty AggregatorFactories.Builder to satisfy the constructor
            AggregatorFactories.Builder subFactoriesBuilder = new AggregatorFactories.Builder();

            return new TimeSeriesUnfoldAggregatorFactory(
                name,
                null, // QueryShardContext - not needed for basic tests
                null, // AggregatorFactory parent - not needed
                subFactoriesBuilder, // AggregatorFactories.Builder - required by parent constructor
                Map.of(), // metadata
                stages,
                minTimestamp,
                maxTimestamp,
                step
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to create factory for test", e);
        }
    }
}
