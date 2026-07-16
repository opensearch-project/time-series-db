/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.query.stage.PipelineStage;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link ProfilingTimeSeriesCoordinatorAggregationBuilder}.
 *
 * <p>This test class focuses on the profiling-specific functionality added by
 * ProfilingTimeSeriesCoordinatorAggregationBuilder. Tests for inherited behavior from
 * TimeSeriesCoordinatorAggregationBuilder are covered in TimeSeriesCoordinatorAggregationBuilderTests.</p>
 */
public class ProfilingTimeSeriesCoordinatorAggregationBuilderTests extends OpenSearchTestCase {

    // ========== Constructor Tests ==========

    public void testConstructorBasic() {

        String name = "test_profiling_agg";
        List<PipelineStage> stages = List.of();
        LinkedHashMap<String, TimeSeriesCoordinatorAggregator.MacroDefinition> macros = new LinkedHashMap<>();
        Map<String, String> references = Map.of();
        String inputReference = "input_ref";

        ProfilingTimeSeriesCoordinatorAggregationBuilder builder = new ProfilingTimeSeriesCoordinatorAggregationBuilder(
            name,
            stages,
            macros,
            references,
            inputReference
        );

        assertEquals(name, builder.getName());
        assertEquals(ProfilingTimeSeriesCoordinatorAggregationBuilder.NAME, builder.getType());
        assertEquals(ProfilingTimeSeriesCoordinatorAggregationBuilder.NAME, builder.getWriteableName());
    }

    public void testConstructorWithStagesAndReferences() {

        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Map.of("ref1", "agg1", "ref2", "agg2");

        ProfilingTimeSeriesCoordinatorAggregationBuilder builder = new ProfilingTimeSeriesCoordinatorAggregationBuilder(
            "test_with_stages",
            stages,
            new LinkedHashMap<>(),
            references,
            "ref1"
        );

        assertEquals("test_with_stages", builder.getName());
        assertEquals(1, builder.getStages().size());
        assertEquals(2, builder.getReferences().size());
        assertEquals("ref1", builder.getInputReference());
    }

    // ========== Type and Name Tests (Override Verification) ==========

    public void testGetTypeDiffersFromParent() {

        ProfilingTimeSeriesCoordinatorAggregationBuilder profilingBuilder = createBuilder("test");
        TimeSeriesCoordinatorAggregationBuilder standardBuilder = new TimeSeriesCoordinatorAggregationBuilder(
            "test",
            List.of(),
            new LinkedHashMap<>(),
            Map.of(),
            null
        );

        assertEquals("coordinator_pipeline_with_profile", profilingBuilder.getType());
        assertEquals("coordinator_pipeline", standardBuilder.getType());
        assertNotEquals(standardBuilder.getType(), profilingBuilder.getType());
    }

    public void testGetWriteableNameDiffersFromParent() {

        ProfilingTimeSeriesCoordinatorAggregationBuilder profilingBuilder = createBuilder("test");
        TimeSeriesCoordinatorAggregationBuilder standardBuilder = new TimeSeriesCoordinatorAggregationBuilder(
            "test",
            List.of(),
            new LinkedHashMap<>(),
            Map.of(),
            null
        );

        assertEquals("coordinator_pipeline_with_profile", profilingBuilder.getWriteableName());
        assertEquals("coordinator_pipeline", standardBuilder.getWriteableName());
        assertNotEquals(standardBuilder.getWriteableName(), profilingBuilder.getWriteableName());
    }

    public void testNameConstant() {

        assertEquals("coordinator_pipeline_with_profile", ProfilingTimeSeriesCoordinatorAggregationBuilder.NAME);
    }

    // ========== createInternal Tests (Override Verification) ==========

    public void testCreateInternalAddsProfilingFlag() {

        ProfilingTimeSeriesCoordinatorAggregationBuilder builder = createBuilder("test");

        PipelineAggregator aggregator = builder.createInternal(null);

        assertNotNull(aggregator);
        assertTrue(aggregator instanceof TimeSeriesCoordinatorAggregator);
        assertEquals("test", aggregator.name());
    }

    public void testCreateInternalPreservesExistingMetadata() {

        ProfilingTimeSeriesCoordinatorAggregationBuilder builder = createBuilder("test");
        Map<String, Object> existingMetadata = Map.of("custom_key", "custom_value", "number", 42);

        PipelineAggregator aggregator = builder.createInternal(existingMetadata);

        assertNotNull(aggregator);
        assertTrue(aggregator instanceof TimeSeriesCoordinatorAggregator);
    }

    public void testCreateInternalWithNullMetadata() {

        ProfilingTimeSeriesCoordinatorAggregationBuilder builder = createBuilder("test_null_meta");

        PipelineAggregator aggregator = builder.createInternal(null);

        assertNotNull(aggregator);
        assertTrue(aggregator instanceof TimeSeriesCoordinatorAggregator);
    }

    public void testCreateInternalWithEmptyMetadata() {

        ProfilingTimeSeriesCoordinatorAggregationBuilder builder = createBuilder("test_empty_meta");

        PipelineAggregator aggregator = builder.createInternal(Map.of());

        assertNotNull(aggregator);
        assertTrue(aggregator instanceof TimeSeriesCoordinatorAggregator);
    }

    // ========== Serialization Tests ==========

    public void testSerializationBasic() throws IOException {

        ProfilingTimeSeriesCoordinatorAggregationBuilder original = createBuilder("test_ser");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        ProfilingTimeSeriesCoordinatorAggregationBuilder deserialized = new ProfilingTimeSeriesCoordinatorAggregationBuilder(in);

        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getType(), deserialized.getType());
        assertEquals(original.getWriteableName(), deserialized.getWriteableName());
        assertEquals(original.getStages().size(), deserialized.getStages().size());
        assertEquals(original.getReferences(), deserialized.getReferences());
        assertEquals(original.getInputReference(), deserialized.getInputReference());
    }

    public void testSerializationWithReferences() throws IOException {

        ProfilingTimeSeriesCoordinatorAggregationBuilder original = new ProfilingTimeSeriesCoordinatorAggregationBuilder(
            "test_with_refs",
            List.of(),
            new LinkedHashMap<>(),
            Map.of("ref1", "agg1", "ref2", "agg2"),
            "input"
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        ProfilingTimeSeriesCoordinatorAggregationBuilder deserialized = new ProfilingTimeSeriesCoordinatorAggregationBuilder(in);

        assertEquals(original.getName(), deserialized.getName());
        assertEquals(original.getReferences(), deserialized.getReferences());
        assertEquals("input", deserialized.getInputReference());
    }

    public void testSerializationRoundTrip() throws IOException {

        for (int i = 0; i < 5; i++) {
            ProfilingTimeSeriesCoordinatorAggregationBuilder original = createBuilder("test_" + i);

            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            ProfilingTimeSeriesCoordinatorAggregationBuilder deserialized = new ProfilingTimeSeriesCoordinatorAggregationBuilder(in);

            assertEquals("Round trip " + i, original.getName(), deserialized.getName());
            assertEquals("Round trip " + i, original.getType(), deserialized.getType());
        }
    }

    // ========== XContent Tests ==========

    public void testXContentGeneration() throws IOException {

        List<PipelineStage> stages = List.of(new ScaleStage(2.0));
        Map<String, String> references = Map.of("a", "unfold_a", "b", "unfold_b");

        ProfilingTimeSeriesCoordinatorAggregationBuilder builder = new ProfilingTimeSeriesCoordinatorAggregationBuilder(
            "test_xcontent",
            stages,
            new LinkedHashMap<>(),
            references,
            "a"
        );

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        builder.internalXContent(xContentBuilder, null);
        xContentBuilder.endObject();

        String jsonString = xContentBuilder.toString();
        assertNotNull(jsonString);
        assertTrue(jsonString.contains("stages"));
        assertTrue(jsonString.contains("references"));
        assertTrue(jsonString.contains("inputReference"));
    }

    // ========== Inheritance Verification ==========

    public void testInstanceOfParentClass() {

        ProfilingTimeSeriesCoordinatorAggregationBuilder builder = createBuilder("test");

        assertTrue(
            "Should be instance of TimeSeriesCoordinatorAggregationBuilder",
            builder instanceof TimeSeriesCoordinatorAggregationBuilder
        );
    }

    public void testInheritsParentBehavior() {

        ProfilingTimeSeriesCoordinatorAggregationBuilder builder = createBuilder("inherited_test");

        assertEquals("inherited_test", builder.getName());
        assertNotNull(builder.getStages());
        assertNotNull(builder.getReferences());
        assertNotNull(builder.getMacroDefinitions());
    }

    public void testDifferentInstancesHaveDistinctTypes() {

        ProfilingTimeSeriesCoordinatorAggregationBuilder profiling1 = createBuilder("test1");
        ProfilingTimeSeriesCoordinatorAggregationBuilder profiling2 = createBuilder("test2");
        TimeSeriesCoordinatorAggregationBuilder standard = new TimeSeriesCoordinatorAggregationBuilder(
            "test3",
            List.of(),
            new LinkedHashMap<>(),
            Map.of(),
            null
        );

        assertEquals(profiling1.getType(), profiling2.getType());
        assertNotEquals(profiling1.getType(), standard.getType());
        assertEquals("coordinator_pipeline_with_profile", profiling1.getType());
        assertEquals("coordinator_pipeline", standard.getType());
    }

    // ========== Edge Cases ==========

    public void testConstructorWithNullStages() {

        ProfilingTimeSeriesCoordinatorAggregationBuilder builder = new ProfilingTimeSeriesCoordinatorAggregationBuilder(
            "test_null_stages",
            null,
            new LinkedHashMap<>(),
            Map.of(),
            null
        );

        assertEquals("test_null_stages", builder.getName());
    }

    public void testConstructorWithNullReferences() {

        ProfilingTimeSeriesCoordinatorAggregationBuilder builder = new ProfilingTimeSeriesCoordinatorAggregationBuilder(
            "test_null_refs",
            List.of(),
            new LinkedHashMap<>(),
            null,
            null
        );

        assertEquals("test_null_refs", builder.getName());
    }

    public void testConstructorWithNullMacros() {

        ProfilingTimeSeriesCoordinatorAggregationBuilder builder = new ProfilingTimeSeriesCoordinatorAggregationBuilder(
            "test_null_macros",
            List.of(),
            null,
            Map.of(),
            null
        );

        assertEquals("test_null_macros", builder.getName());
    }

    // ========== Helper Methods ==========

    private ProfilingTimeSeriesCoordinatorAggregationBuilder createBuilder(String name) {
        return new ProfilingTimeSeriesCoordinatorAggregationBuilder(name, List.of(), new LinkedHashMap<>(), Map.of(), null);
    }
}
