/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Unit tests for TimeSeriesUnfoldAggregationBuilder.
 */
public class TimeSeriesUnfoldAggregationBuilderTests extends OpenSearchTestCase {

    public void testConstructorBasic() {
        // Arrange
        List<UnaryPipelineStage> stages = List.of(new ScaleStage(2.0));
        long minTimestamp = 1000L;
        long maxTimestamp = 2000L;
        long step = 100L;

        // Act
        TimeSeriesUnfoldAggregationBuilder builder = new TimeSeriesUnfoldAggregationBuilder(
            "test_unfold",
            stages,
            minTimestamp,
            maxTimestamp,
            step
        );

        // Assert
        assertEquals("test_unfold", builder.getName());
        assertEquals(stages, builder.getStages());
        assertEquals(minTimestamp, builder.getMinTimestamp());
        assertEquals(maxTimestamp, builder.getMaxTimestamp());
        assertEquals(step, builder.getStep());
        assertEquals("time_series_unfold", builder.getType());
    }

    public void testStagesFluentMethod() {
        // Arrange
        List<UnaryPipelineStage> originalStages = List.of(new ScaleStage(2.0));
        List<UnaryPipelineStage> newStages = List.of(new ScaleStage(3.0));
        TimeSeriesUnfoldAggregationBuilder builder = new TimeSeriesUnfoldAggregationBuilder(
            "test_unfold",
            originalStages,
            1000L,
            2000L,
            100L
        );

        // Act
        builder.setStages(newStages);

        // Assert
        assertEquals(newStages, builder.getStages());
    }

    public void testBucketCardinality() {
        // Arrange
        TimeSeriesUnfoldAggregationBuilder builder = new TimeSeriesUnfoldAggregationBuilder(
            "test_cardinality",
            List.of(new ScaleStage(1.0)),
            1000L,
            2000L,
            100L
        );

        // Act & Assert
        assertEquals(AggregationBuilder.BucketCardinality.MANY, builder.bucketCardinality());
    }

    public void testShallowCopy() {
        // Arrange
        List<UnaryPipelineStage> stages = List.of(new ScaleStage(2.0));
        TimeSeriesUnfoldAggregationBuilder original = new TimeSeriesUnfoldAggregationBuilder("test_copy", stages, 1000L, 2000L, 100L);

        // Act
        AggregationBuilder copy = original.shallowCopy(null, Map.of("test", "metadata"));

        // Assert
        assertTrue("Copy should be TimeSeriesUnfoldAggregationBuilder", copy instanceof TimeSeriesUnfoldAggregationBuilder);
        TimeSeriesUnfoldAggregationBuilder typedCopy = (TimeSeriesUnfoldAggregationBuilder) copy;
        assertEquals(original.getName(), typedCopy.getName());
        assertEquals(original.getMinTimestamp(), typedCopy.getMinTimestamp());
        assertEquals(original.getMaxTimestamp(), typedCopy.getMaxTimestamp());
        assertEquals(original.getStep(), typedCopy.getStep());
        assertEquals(original.getStages(), typedCopy.getStages());
    }

    // ========== Additional Enhanced Tests ==========

    /**
     * Test XContent generation with stages.
     */
    public void testXContentGeneration() throws IOException {
        // Arrange
        List<UnaryPipelineStage> stages = List.of(new ScaleStage(2.0), new SumStage("service"));
        TimeSeriesUnfoldAggregationBuilder builder = new TimeSeriesUnfoldAggregationBuilder("test_xcontent", stages, 1000L, 2000L, 100L);

        // Act
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        builder.internalXContent(xContentBuilder, null);
        xContentBuilder.endObject();

        // Assert
        String jsonString = xContentBuilder.toString();
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("min_timestamp"));
        assertTrue(jsonString.contains("max_timestamp"));
        assertTrue(jsonString.contains("step"));
        assertTrue(jsonString.contains("stages"));
        assertTrue(jsonString.contains("1000"));
        assertTrue(jsonString.contains("2000"));
        assertTrue(jsonString.contains("100"));
    }

    /**
     * Test XContent generation with null stages.
     */
    public void testXContentGenerationWithNullStages() throws IOException {
        // Arrange
        TimeSeriesUnfoldAggregationBuilder builder = new TimeSeriesUnfoldAggregationBuilder("test_xcontent_null", null, 1000L, 2000L, 100L);

        // Act
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        builder.internalXContent(xContentBuilder, null);
        xContentBuilder.endObject();

        // Assert
        String jsonString = xContentBuilder.toString();
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("min_timestamp"));
        assertTrue(jsonString.contains("max_timestamp"));
        assertTrue(jsonString.contains("step"));
        // Should not contain stages when null
        assertFalse(jsonString.contains("stages"));
    }

    /**
     * Test doBuild method creates correct factory.
     */
    public void testDoBuild() throws Exception {
        // Arrange
        List<UnaryPipelineStage> stages = List.of(new ScaleStage(2.0));
        TimeSeriesUnfoldAggregationBuilder builder = new TimeSeriesUnfoldAggregationBuilder("test_build", stages, 1000L, 2000L, 100L);

        // Mock dependencies
        QueryShardContext mockContext = mock(QueryShardContext.class);
        AggregatorFactory mockParent = mock(AggregatorFactory.class);
        AggregatorFactories.Builder mockSubFactoriesBuilder = mock(AggregatorFactories.Builder.class);

        // Act
        AggregatorFactory factory = builder.doBuild(mockContext, mockParent, mockSubFactoriesBuilder);

        // Assert
        assertNotNull(factory);
        assertTrue(factory instanceof TimeSeriesUnfoldAggregatorFactory);
    }

    /**
     * Test registerAggregators method with mock builder.
     */
    public void testRegisterAggregatorsWithMock() throws Exception {
        // Arrange
        ValuesSourceRegistry.Builder mockBuilder = mock(ValuesSourceRegistry.Builder.class);

        // Act & Assert - Should not throw
        TimeSeriesUnfoldAggregationBuilder.registerAggregators(mockBuilder);
    }

    /**
     * Test constructor with extreme timestamp values.
     */
    public void testExtremeTimestampValues() throws Exception {
        // Arrange & Act
        TimeSeriesUnfoldAggregationBuilder builder = new TimeSeriesUnfoldAggregationBuilder(
            "extreme_values",
            List.of(),
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            1L
        );

        // Assert
        assertEquals(Long.MIN_VALUE, builder.getMinTimestamp());
        assertEquals(Long.MAX_VALUE, builder.getMaxTimestamp());
        assertEquals(1L, builder.getStep());
    }

    /**
     * Test with large number of stages.
     */
    public void testWithManyStages() throws Exception {
        // Arrange
        List<UnaryPipelineStage> manyStages = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            manyStages.add(new ScaleStage(i + 1.0));
        }

        // Act
        TimeSeriesUnfoldAggregationBuilder builder = new TimeSeriesUnfoldAggregationBuilder("many_stages", manyStages, 1000L, 2000L, 100L);

        // Assert
        assertEquals(50, builder.getStages().size());
        assertEquals(1.0, ((ScaleStage) builder.getStages().get(0)).getFactor(), 0.001);
        assertEquals(50.0, ((ScaleStage) builder.getStages().get(49)).getFactor(), 0.001);
    }

    /**
     * Test copy constructor functionality.
     */
    public void testCopyConstructor() throws Exception {
        // Arrange
        List<UnaryPipelineStage> stages = List.of(new ScaleStage(2.0));
        TimeSeriesUnfoldAggregationBuilder original = new TimeSeriesUnfoldAggregationBuilder("original", stages, 1000L, 2000L, 100L);

        AggregatorFactories.Builder mockFactoriesBuilder = mock(AggregatorFactories.Builder.class);
        Map<String, Object> metadata = Map.of("key", "value");

        // Create a copy using the protected constructor (via shallowCopy)
        AggregationBuilder copy = original.shallowCopy(mockFactoriesBuilder, metadata);

        // Assert
        assertTrue(copy instanceof TimeSeriesUnfoldAggregationBuilder);
        TimeSeriesUnfoldAggregationBuilder copyBuilder = (TimeSeriesUnfoldAggregationBuilder) copy;

        assertEquals(original.getName(), copyBuilder.getName());
        assertEquals(original.getMinTimestamp(), copyBuilder.getMinTimestamp());
        assertEquals(original.getMaxTimestamp(), copyBuilder.getMaxTimestamp());
        assertEquals(original.getStep(), copyBuilder.getStep());
        assertSame(original.getStages(), copyBuilder.getStages()); // Should be shallow copy
    }

    /**
     * Test NAME constant.
     */
    public void testNameConstant() {
        assertEquals("time_series_unfold", TimeSeriesUnfoldAggregationBuilder.NAME);
    }

    // ========== XContent Parser Tests ==========

    /**
     * Test parsing basic aggregation configuration from XContent
     */
    public void testParseBasicConfiguration() throws Exception {
        String json = """
            {
              "min_timestamp": 1000,
              "max_timestamp": 2000,
              "step": 100
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT
            TimeSeriesUnfoldAggregationBuilder result = TimeSeriesUnfoldAggregationBuilder.parse("test_agg", parser);

            assertEquals("test_agg", result.getName());
            assertEquals(1000L, result.getMinTimestamp());
            assertEquals(2000L, result.getMaxTimestamp());
            assertEquals(100L, result.getStep());
            assertTrue("Should have no stages", result.getStages().isEmpty());
        }
    }

    /**
     * Test parsing throws exception when required parameters are missing
     */
    public void testParseWithMissingParameters() throws Exception {
        String json = "{}";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            // Should throw IllegalArgumentException for missing required parameters
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesUnfoldAggregationBuilder.parse("default_agg", parser)
            );

            assertTrue(
                "Exception should mention missing parameter",
                exception.getMessage().contains("Required parameter") && exception.getMessage().contains("is missing")
            );
        }
    }

    /**
     * Test parsing throws exception when min_timestamp is missing
     */
    public void testParseMissingMinTimestamp() throws Exception {
        String json = """
            {
              "max_timestamp": 2000,
              "step": 100
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesUnfoldAggregationBuilder.parse("test_agg", parser)
            );

            assertTrue("Exception should mention min_timestamp", exception.getMessage().contains("min_timestamp"));
        }
    }

    /**
     * Test parsing throws exception when max_timestamp is missing
     */
    public void testParseMissingMaxTimestamp() throws Exception {
        String json = """
            {
              "min_timestamp": 1000,
              "step": 100
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesUnfoldAggregationBuilder.parse("test_agg", parser)
            );

            assertTrue("Exception should mention max_timestamp", exception.getMessage().contains("max_timestamp"));
        }
    }

    /**
     * Test parsing throws exception when step is missing
     */
    public void testParseMissingStep() throws Exception {
        String json = """
            {
              "min_timestamp": 1000,
              "max_timestamp": 2000
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> TimeSeriesUnfoldAggregationBuilder.parse("test_agg", parser)
            );

            assertTrue("Exception should mention step", exception.getMessage().contains("step"));
        }
    }

    /**
     * Test parsing with unknown fields (should be skipped)
     */
    public void testParseWithUnknownFields() throws Exception {
        String json = """
            {
              "unknown_field": "value",
              "min_timestamp": 1000,
              "max_timestamp": 2000,
              "step": 500,
              "unknown_object": {}
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesUnfoldAggregationBuilder result = TimeSeriesUnfoldAggregationBuilder.parse("unknown_agg", parser);

            assertEquals("unknown_agg", result.getName());
            assertEquals(1000L, result.getMinTimestamp());
            assertEquals(2000L, result.getMaxTimestamp());
            assertEquals(500L, result.getStep());
            assertTrue("Should have no stages", result.getStages().isEmpty());
        }
    }

    /**
     * Test parsing with pipeline stages
     */
    public void testParseWithStages() throws Exception {
        String json = """
            {
              "min_timestamp": 1000,
              "max_timestamp": 2000,
              "step": 100,
              "stages": [
                {
                  "type": "scale",
                  "factor": 2.0
                },
                {
                  "type": "sum",
                  "labels": ["instance"]
                }
              ]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesUnfoldAggregationBuilder result = TimeSeriesUnfoldAggregationBuilder.parse("stages_agg", parser);

            assertEquals("stages_agg", result.getName());
            assertEquals(1000L, result.getMinTimestamp());
            assertEquals(2000L, result.getMaxTimestamp());
            assertEquals(100L, result.getStep());
            assertEquals(2, result.getStages().size());

            // Verify first stage is ScaleStage
            assertTrue("First stage should be ScaleStage", result.getStages().get(0) instanceof ScaleStage);
            ScaleStage scaleStage = (ScaleStage) result.getStages().get(0);
            assertEquals(2.0, scaleStage.getFactor(), 0.001);

            // Verify second stage is SumStage
            assertTrue("Second stage should be SumStage", result.getStages().get(1) instanceof SumStage);
        }
    }

    /**
     * Test parsing with single stage
     */
    public void testParseWithSingleStage() throws Exception {
        String json = """
            {
              "min_timestamp": 1000,
              "max_timestamp": 2000,
              "step": 100,
              "stages": [
                {
                  "type": "scale",
                  "factor": 3.5
                }
              ]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesUnfoldAggregationBuilder result = TimeSeriesUnfoldAggregationBuilder.parse("single_stage_agg", parser);

            assertEquals("single_stage_agg", result.getName());
            assertEquals(1000L, result.getMinTimestamp());
            assertEquals(2000L, result.getMaxTimestamp());
            assertEquals(100L, result.getStep());
            assertEquals(1, result.getStages().size());

            assertTrue("Stage should be ScaleStage", result.getStages().get(0) instanceof ScaleStage);
            ScaleStage scaleStage = (ScaleStage) result.getStages().get(0);
            assertEquals(3.5, scaleStage.getFactor(), 0.001);
        }
    }

    /**
     * Test parsing with empty stages array
     */
    public void testParseWithEmptyStages() throws Exception {
        String json = """
            {
              "min_timestamp": 1000,
              "max_timestamp": 2000,
              "step": 100,
              "stages": []
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesUnfoldAggregationBuilder result = TimeSeriesUnfoldAggregationBuilder.parse("empty_stages_agg", parser);

            assertEquals("empty_stages_agg", result.getName());
            assertEquals(1000L, result.getMinTimestamp());
            assertEquals(2000L, result.getMaxTimestamp());
            assertEquals(100L, result.getStep());
            assertTrue("Stages should be empty", result.getStages().isEmpty());
        }
    }

    /**
     * Test error handling for invalid stage type
     */
    public void testParseInvalidStageType() throws Exception {
        String json = """
            {
              "min_timestamp": 1000,
              "max_timestamp": 2000,
              "step": 100,
              "stages": [
                {
                  "type": "invalid_stage_type"
                }
              ]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            // Should throw IllegalArgumentException for unknown stage type
            expectThrows(IllegalArgumentException.class, () -> TimeSeriesUnfoldAggregationBuilder.parse("invalid_agg", parser));
        }
    }

    /**
     * Test parsing with complex stages configuration
     */
    public void testParseWithComplexStages() throws Exception {
        String json = """
            {
              "min_timestamp": 5000,
              "max_timestamp": 10000,
              "step": 500,
              "stages": [
                {
                  "type": "scale",
                  "factor": 1.5
                },
                {
                  "type": "sum",
                  "labels": ["region", "service"]
                },
                {
                  "type": "scale",
                  "factor": 0.5
                }
              ]
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesUnfoldAggregationBuilder result = TimeSeriesUnfoldAggregationBuilder.parse("complex_agg", parser);

            assertEquals("complex_agg", result.getName());
            assertEquals(5000L, result.getMinTimestamp());
            assertEquals(10000L, result.getMaxTimestamp());
            assertEquals(500L, result.getStep());
            assertEquals(3, result.getStages().size());

            // Verify all three stages
            assertTrue("First stage should be ScaleStage", result.getStages().get(0) instanceof ScaleStage);
            assertTrue("Second stage should be SumStage", result.getStages().get(1) instanceof SumStage);
            assertTrue("Third stage should be ScaleStage", result.getStages().get(2) instanceof ScaleStage);

            ScaleStage firstScale = (ScaleStage) result.getStages().get(0);
            assertEquals(1.5, firstScale.getFactor(), 0.001);

            ScaleStage thirdScale = (ScaleStage) result.getStages().get(2);
            assertEquals(0.5, thirdScale.getFactor(), 0.001);
        }
    }

}
