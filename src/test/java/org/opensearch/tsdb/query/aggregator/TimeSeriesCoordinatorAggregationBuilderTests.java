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
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.stage.AsPercentStage;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.stage.PipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for TimeSeriesCoordinatorAggregationBuilder.
 */
public class TimeSeriesCoordinatorAggregationBuilderTests extends OpenSearchTestCase {

    public void testConstructorBasic() {
        // Arrange
        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Map.of("input", "source_agg");
        String inputReference = "input";

        // Act
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_coordinator",
            stages,
            new LinkedHashMap<>(), // No macros
            references,
            inputReference
        );

        // Assert
        assertEquals("test_coordinator", builder.getName());
        assertEquals("coordinator_pipeline", builder.getType());
        assertEquals(TimeSeriesCoordinatorAggregationBuilder.NAME, builder.getWriteableName());
    }

    public void testConstructorWithMacros() {
        // Arrange
        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Map.of("a", "agg_a", "b", "agg_b");
        String inputReference = "a";
        LinkedHashMap<String, TimeSeriesCoordinatorAggregator.MacroDefinition> macros = new LinkedHashMap<>();
        macros.put("macro1", new TimeSeriesCoordinatorAggregator.MacroDefinition("macro1", List.of(new ScaleStage(3.0)), "a"));

        // Act
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_with_macros",
            stages,
            macros,
            references,
            inputReference
        );

        // Assert
        assertEquals("test_with_macros", builder.getName());
        assertEquals("coordinator_pipeline", builder.getType());
    }

    public void testSetMetadata() {
        // Arrange
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "metadata_test",
            List.of(new ScaleStage(1.0)),
            new LinkedHashMap<>(), // No macros
            Map.of("a", "agg_a"),
            "a"
        );
        Map<String, Object> metadata = Map.of("key1", "value1", "key2", 42);

        // Act
        TimeSeriesCoordinatorAggregationBuilder result = builder.setMetadata(metadata);

        // Assert
        assertSame("setMetadata should return same instance", builder, result);
    }

    // ========== Additional Enhanced Tests ==========

    /**
     * Test XContent generation with stages and references.
     */
    public void testXContentGeneration() throws IOException {
        // Arrange
        List<PipelineStage> stages = List.of(new ScaleStage(2.0), new AsPercentStage("b"));
        Map<String, String> references = Map.of("a", "unfold_a", "b", "unfold_b");
        String inputReference = "a";

        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_xcontent",
            stages,
            new LinkedHashMap<>(), // No macros
            references,
            inputReference
        );

        // Act
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        builder.internalXContent(xContentBuilder, null);
        xContentBuilder.endObject();

        // Assert
        String jsonString = xContentBuilder.toString();
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("stages"));
        assertTrue(jsonString.contains("references"));
        assertTrue(jsonString.contains("inputReference"));
        assertTrue(jsonString.contains("unfold_a"));
        assertTrue(jsonString.contains("unfold_b"));
    }

    /**
     * Test XContent generation with null input reference.
     */
    public void testXContentGenerationNullInputReference() throws IOException {
        // Arrange
        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Map.of("a", "unfold_a");

        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_xcontent_null",
            stages,
            new LinkedHashMap<>(), // No macros
            references,
            null // null input reference
        );

        // Act
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        builder.internalXContent(xContentBuilder, null);
        xContentBuilder.endObject();

        // Assert
        String jsonString = xContentBuilder.toString();
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("stages"));
        assertTrue(jsonString.contains("references"));
        // Should not contain inputReference when null
        assertFalse(jsonString.contains("inputReference"));
    }

    /**
     * Test validation with null stages - constructor should allow it (validation happens in createInternal).
     */
    public void testValidationNullStagesBasic() throws Exception {
        // Arrange & Act - Constructor should handle null stages gracefully
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_validation_null_stages",
            null, // null stages
            new LinkedHashMap<>(), // No macros
            Map.of("a", "unfold_a"),
            "a"
        );

        // Assert - Constructor completes without throwing
        assertEquals("test_validation_null_stages", builder.getName());
    }

    /**
     * Test validation with empty stages - constructor should allow it (validation happens in createInternal).
     */
    public void testValidationEmptyStagesBasic() throws Exception {
        // Arrange & Act - Constructor should handle empty stages gracefully
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_validation_empty_stages",
            new ArrayList<>(), // empty stages
            new LinkedHashMap<>(), // No macros
            Map.of("a", "unfold_a"),
            "a"
        );

        // Assert - Constructor completes without throwing
        assertEquals("test_validation_empty_stages", builder.getName());
    }

    /**
     * Test validation with null references - constructor should allow it (validation happens in createInternal).
     */
    public void testValidationNullReferencesBasic() throws Exception {
        // Arrange & Act - Constructor should handle null references gracefully
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_validation_null_refs",
            List.of(new ScaleStage(2.0)),
            new LinkedHashMap<>(), // No macros
            null, // null references
            "a"
        );

        // Assert - Constructor completes without throwing
        assertEquals("test_validation_null_refs", builder.getName());
    }

    /**
     * Test createInternal method.
     */
    public void testCreateInternal() throws Exception {
        // Arrange
        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Map.of("a", "unfold_a");
        String inputReference = "a";

        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_create_internal",
            stages,
            new LinkedHashMap<>(), // No macros
            references,
            inputReference
        );

        Map<String, Object> metadata = Map.of("key", "value");

        // Act
        PipelineAggregator aggregator = builder.createInternal(metadata);

        // Assert
        assertNotNull(aggregator);
        assertTrue(aggregator instanceof TimeSeriesCoordinatorAggregator);
        assertEquals("test_create_internal", aggregator.name());
    }

    /**
     * Test with null macro definitions in constructor.
     */
    public void testConstructorWithNullMacros() throws Exception {
        // Arrange
        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Map.of("a", "unfold_a");
        String inputReference = "a";

        // Act
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_null_macros",
            stages,
            null, // null macros
            references,
            inputReference
        );

        // Assert
        assertEquals("test_null_macros", builder.getName());
        assertEquals("coordinator_pipeline", builder.getType());
    }

    /**
     * Test XContent generation with empty collections.
     */
    public void testXContentGenerationEmpty() throws IOException {
        // Arrange
        TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
            "test_xcontent_empty",
            new ArrayList<>(),
            new LinkedHashMap<>(), // No macros
            new LinkedHashMap<>(),
            null
        );

        // Act
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        builder.internalXContent(xContentBuilder, null);
        xContentBuilder.endObject();

        // Assert
        String jsonString = xContentBuilder.toString();
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("stages"));
        assertTrue(jsonString.contains("references"));
        // Should contain empty arrays/objects
        assertTrue(jsonString.contains("[]"));
        assertTrue(jsonString.contains("{}"));
    }

    // ========== XContent Parser Tests ==========

    /**
     * Test parsing basic coordinator configuration
     */
    public void testParseBasicConfiguration() throws Exception {
        String json = """
            {
              "references": {
                "a": "agg1",
                "b": "agg2"
              },
              "inputReference": "a"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("coord_agg", parser);

            assertEquals("coord_agg", result.getName());
            assertTrue("Should have no stages by default", result.getStages().isEmpty());
            assertEquals("Should have inputReference", "a", result.getInputReference());
            assertEquals("Should have 2 references", 2, result.getReferences().size());
            assertEquals("Reference 'a' should point to 'agg1'", "agg1", result.getReferences().get("a"));
            assertEquals("Reference 'b' should point to 'agg2'", "agg2", result.getReferences().get("b"));
        }
    }

    /**
     * Test parsing coordinator with empty references and stages (defaults)
     */
    public void testParseDefaults() throws Exception {
        String json = "{}";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("coord_default", parser);

            assertEquals("coord_default", result.getName());
            assertTrue("Should have no stages by default", result.getStages().isEmpty());
            assertTrue("Should have no references by default", result.getReferences().isEmpty());
            assertNull("Should have null inputReference by default", result.getInputReference());
        }
    }

    /**
     * Test parsing coordinator with unknown fields (should be skipped)
     */
    public void testParseWithUnknownFields() throws Exception {
        String json = """
            {
              "unknown_field": "value",
              "references": {
                "a": "agg1"
              },
              "unknown_object": {}
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("coord_unknown", parser);

            assertEquals("coord_unknown", result.getName());
            assertTrue("Should have no stages", result.getStages().isEmpty());
            assertEquals("Should have 1 reference", 1, result.getReferences().size());
            assertEquals("Reference 'a' should point to 'agg1'", "agg1", result.getReferences().get("a"));
            assertNull("Should have null inputReference", result.getInputReference());
        }
    }

    /**
     * Test parsing coordinator with multiple stages and references
     */
    public void testParseComplexConfiguration() throws Exception {
        String json = """
            {
              "stages": [
                {
                  "type": "scale",
                  "factor": 1.5
                },
                {
                  "type": "sum",
                  "labels": ["region"]
                }
              ],
              "references": {
                "input": "metrics",
                "compare": "baseline"
              },
              "inputReference": "input"
            }
            """;

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            parser.nextToken(); // Move to START_OBJECT

            TimeSeriesCoordinatorAggregationBuilder result = TimeSeriesCoordinatorAggregationBuilder.parse("coord_complex", parser);

            assertEquals("coord_complex", result.getName());
            assertEquals("Should have 2 stages", 2, result.getStages().size());
            assertTrue("First stage should be ScaleStage", result.getStages().get(0) instanceof ScaleStage);
            assertTrue("Second stage should be SumStage", result.getStages().get(1) instanceof SumStage);
            assertEquals("Should have 2 references", 2, result.getReferences().size());
            assertEquals("Reference 'input' should point to 'metrics'", "metrics", result.getReferences().get("input"));
            assertEquals("Reference 'compare' should point to 'baseline'", "baseline", result.getReferences().get("compare"));
            assertEquals("Input reference should be 'input'", "input", result.getInputReference());
        }
    }

}
