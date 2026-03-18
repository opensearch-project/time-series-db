/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.util.BigArrays;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.query.utils.TSDBStatsConstants;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TSDBStatsAggregatorFactory.
 *
 * <p>Test coverage includes:
 * <ul>
 *   <li>Factory configuration with various parameters</li>
 *   <li>Concurrent segment search support</li>
 *   <li>Aggregator creation with different configurations</li>
 * </ul>
 */
public class TSDBStatsAggregatorFactoryTests extends OpenSearchTestCase {

    public void testSupportsConcurrentSegmentSearch() {
        // Arrange - Test with both true and false for includeValueStats
        TSDBStatsAggregatorFactory factory1 = createFactory("test_css_true", 1000L, 2000L, true);
        TSDBStatsAggregatorFactory factory2 = createFactory("test_css_false", 1000L, 2000L, false);

        // Act & Assert - CSS support should be unconditional
        assertTrue("TSDBStatsAggregatorFactory should support CSS", factory1.supportsConcurrentSegmentSearch());
        assertTrue(
            "TSDBStatsAggregatorFactory should support CSS regardless of includeValueStats",
            factory2.supportsConcurrentSegmentSearch()
        );
    }

    public void testFactoryConfiguration() {
        // Arrange - Test with various timestamp ranges and boolean flags
        long minTimestamp = 5000L;
        long maxTimestamp = 10000L;

        // Act
        TSDBStatsAggregatorFactory factory = createFactory("config_test", minTimestamp, maxTimestamp, true);

        // Assert - Verify factory was created successfully with correct name
        assertNotNull("Factory should be created successfully", factory);
        assertEquals("config_test", factory.name());
    }

    public void testCreateInternal() throws Exception {
        // Arrange
        long minTimestamp = 1000L;
        long maxTimestamp = 5000L;
        boolean includeValueStats = true;
        TSDBStatsAggregatorFactory factory = createFactory("test_create", minTimestamp, maxTimestamp, includeValueStats);

        // Create mock SearchContext
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.bigArrays()).thenReturn(new BigArrays(null, new NoneCircuitBreakerService(), "test"));

        // Act
        TSDBStatsAggregator aggregator = (TSDBStatsAggregator) factory.createInternal(
            searchContext,
            null,  // parent aggregator
            CardinalityUpperBound.NONE,
            Map.of()  // metadata
        );

        // Assert
        assertNotNull("createInternal() should return a TSDBStatsAggregator instance", aggregator);
        assertEquals("Aggregator name should match factory name", "test_create", aggregator.name());

        // Cleanup
        aggregator.close();
    }

    public void testCreateInternalWithDifferentParameters() throws Exception {
        // Arrange - Test with includeValueStats=false and custom metadata
        TSDBStatsAggregatorFactory factory = createFactory("test_no_values", 2000L, 6000L, false);

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.bigArrays()).thenReturn(new BigArrays(null, new NoneCircuitBreakerService(), "test"));

        // Act
        TSDBStatsAggregator aggregator = (TSDBStatsAggregator) factory.createInternal(
            searchContext,
            null,
            CardinalityUpperBound.NONE,
            Map.of("custom_meta", "value")
        );

        // Assert
        assertNotNull("Should create aggregator with value stats disabled", aggregator);
        assertEquals("test_no_values", aggregator.name());

        // Cleanup
        aggregator.close();
    }

    public void testCreateInternalWithExtremeTimestamps() throws Exception {
        // Arrange - Test edge case with extreme timestamp values
        TSDBStatsAggregatorFactory factory = createFactory("test_extreme", Long.MIN_VALUE, Long.MAX_VALUE, true);

        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.bigArrays()).thenReturn(new BigArrays(null, new NoneCircuitBreakerService(), "test"));

        // Act
        TSDBStatsAggregator aggregator = (TSDBStatsAggregator) factory.createInternal(
            searchContext,
            null,
            CardinalityUpperBound.NONE,
            Map.of()
        );

        // Assert
        assertNotNull("Should create aggregator with extreme timestamps", aggregator);

        // Cleanup
        aggregator.close();
    }

    /**
     * Helper method to create TSDBStatsAggregatorFactory with minimal parameters.
     * Uses null for complex OpenSearch infrastructure components that aren't needed for basic tests.
     */
    private TSDBStatsAggregatorFactory createFactory(String name, long minTimestamp, long maxTimestamp, boolean includeValueStats) {
        try {
            // Create an empty AggregatorFactories.Builder to satisfy the constructor
            AggregatorFactories.Builder subFactoriesBuilder = new AggregatorFactories.Builder();

            return new TSDBStatsAggregatorFactory(
                name,
                null, // QueryShardContext - not needed for basic tests
                null, // AggregatorFactory parent - not needed
                subFactoriesBuilder, // AggregatorFactories.Builder - required by parent constructor
                Map.of(), // metadata
                minTimestamp,
                maxTimestamp,
                includeValueStats,
                TSDBStatsConstants.DEDUP_MODE_RECOMPUTED
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to create factory for test", e);
        }
    }
}
