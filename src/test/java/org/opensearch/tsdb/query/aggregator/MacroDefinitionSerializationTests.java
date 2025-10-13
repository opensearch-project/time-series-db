/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.AbstractWireTestCase;
import org.opensearch.tsdb.lang.m3.stage.AsPercentStage;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.aggregator.TimeSeriesCoordinatorAggregator.MacroDefinition;
import org.opensearch.tsdb.query.stage.PipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialization tests for MacroDefinition.
 * Extends AbstractWireTestCase to automatically test wire serialization,
 * equals, and hashCode.
 */
public class MacroDefinitionSerializationTests extends AbstractWireTestCase<MacroDefinition> {

    @Override
    protected MacroDefinition createTestInstance() {
        String name = randomAlphaOfLength(10);
        List<PipelineStage> stages = createRandomStages();
        String inputReference = randomBoolean() ? null : randomAlphaOfLength(8);

        return new MacroDefinition(name, stages, inputReference);
    }

    @Override
    protected MacroDefinition copyInstance(MacroDefinition instance, Version version) throws IOException {
        // Serialize and deserialize using the full writeTo/readFrom
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            instance.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return MacroDefinition.readFrom(in);
            }
        }
    }

    @Override
    protected MacroDefinition mutateInstance(MacroDefinition instance) {
        // Create a mutated version by changing one field
        String name = instance.getName();
        List<PipelineStage> stages = instance.getStages();
        String inputReference = instance.getInputReference();

        switch (randomIntBetween(0, 2)) {
            case 0:
                // Mutate name - always append to ensure it's different
                name = instance.getName() + "_mutated";
                break;
            case 1:
                // Mutate stages - add a stage to ensure it's different
                List<PipelineStage> newStages = new ArrayList<>(stages);
                newStages.add(new ScaleStage(randomDoubleBetween(0.1, 10.0, true)));
                stages = newStages;
                break;
            case 2:
                // Mutate input reference - ensure it's different from original
                if (inputReference == null) {
                    inputReference = randomAlphaOfLength(10);
                } else {
                    inputReference = inputReference + "_mutated";
                }
                break;
        }

        return new MacroDefinition(name, stages, inputReference);
    }

    /**
     * Test serialization with writeToWithoutName/readFrom(in, name) methods.
     */
    public void testSerializationWithoutName() throws IOException {
        // Arrange
        String macroName = "test_macro";
        List<PipelineStage> stages = List.of(new ScaleStage(2.0), new SumStage("service"));
        String inputRef = "input_ref";
        MacroDefinition original = new MacroDefinition(macroName, stages, inputRef);

        // Act - Test writeToWithoutName and readFrom with name parameter
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeToWithoutName(out);

            try (StreamInput in = out.bytes().streamInput()) {
                MacroDefinition deserialized = MacroDefinition.readFrom(in, macroName);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getStages().size(), deserialized.getStages().size());
                assertEquals(original.getInputReference(), deserialized.getInputReference());

                // Verify stages
                for (int i = 0; i < original.getStages().size(); i++) {
                    assertEquals(original.getStages().get(i).getName(), deserialized.getStages().get(i).getName());
                }
            }
        }
    }

    /**
     * Test serialization with null input reference.
     */
    public void testSerializationWithNullInputReference() throws IOException {
        // Arrange
        MacroDefinition original = new MacroDefinition("test_null_ref", List.of(new ScaleStage(1.5)), null);

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                MacroDefinition deserialized = MacroDefinition.readFrom(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertNull(deserialized.getInputReference());
                assertEquals(original.getStages().size(), deserialized.getStages().size());
            }
        }
    }

    /**
     * Test serialization with empty stages list.
     */
    public void testSerializationWithEmptyStages() throws IOException {
        // Arrange
        MacroDefinition original = new MacroDefinition("empty_stages", List.of(), "input_ref");

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                MacroDefinition deserialized = MacroDefinition.readFrom(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getInputReference(), deserialized.getInputReference());
                assertTrue(deserialized.getStages().isEmpty());
            }
        }
    }

    /**
     * Test serialization with multiple complex stages.
     */
    public void testSerializationWithMultipleComplexStages() throws IOException {
        // Arrange
        List<PipelineStage> complexStages = List.of(
            new ScaleStage(2.5),
            new AsPercentStage("total"),
            new SumStage("service"),
            new ScaleStage(0.5)
        );
        MacroDefinition original = new MacroDefinition("complex_macro", complexStages, "complex_input");

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                MacroDefinition deserialized = MacroDefinition.readFrom(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getInputReference(), deserialized.getInputReference());
                assertEquals(4, deserialized.getStages().size());

                // Verify stage types and order
                for (int i = 0; i < original.getStages().size(); i++) {
                    assertEquals(original.getStages().get(i).getName(), deserialized.getStages().get(i).getName());
                }
            }
        }
    }

    /**
     * Test serialization with many stages.
     */
    public void testSerializationWithManyStages() throws IOException {
        // Arrange
        List<PipelineStage> manyStages = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            manyStages.add(new ScaleStage(i + 1.0));
        }

        MacroDefinition original = new MacroDefinition("many_stages_macro", manyStages, "input_many");

        // Act
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                MacroDefinition deserialized = MacroDefinition.readFrom(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getInputReference(), deserialized.getInputReference());
                assertEquals(50, deserialized.getStages().size());
            }
        }
    }

    /**
     * Test both serialization methods produce compatible results.
     */
    public void testBothSerializationMethodsCompatible() throws IOException {
        // Arrange
        String macroName = "compatibility_test";
        List<PipelineStage> stages = List.of(new ScaleStage(3.0), new SumStage("env"));
        String inputRef = "compat_input";
        MacroDefinition original = new MacroDefinition(macroName, stages, inputRef);

        // Act - Serialize with full writeTo
        MacroDefinition deserializedFull;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                deserializedFull = MacroDefinition.readFrom(in);
            }
        }

        // Act - Serialize with writeToWithoutName
        MacroDefinition deserializedWithoutName;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeToWithoutName(out);
            try (StreamInput in = out.bytes().streamInput()) {
                deserializedWithoutName = MacroDefinition.readFrom(in, macroName);
            }
        }

        // Assert - Both methods should produce equivalent results
        assertEquals(deserializedFull.getName(), deserializedWithoutName.getName());
        assertEquals(deserializedFull.getInputReference(), deserializedWithoutName.getInputReference());
        assertEquals(deserializedFull.getStages().size(), deserializedWithoutName.getStages().size());
    }

    // ========== Helper Methods ==========

    private List<PipelineStage> createRandomStages() {
        List<PipelineStage> stages = new ArrayList<>();
        int numStages = randomIntBetween(0, 5);

        for (int i = 0; i < numStages; i++) {
            // Randomly choose between different stage types
            switch (randomIntBetween(0, 2)) {
                case 0:
                    stages.add(new ScaleStage(randomDoubleBetween(0.1, 10.0, true)));
                    break;
                case 1:
                    stages.add(new SumStage(randomAlphaOfLength(5)));
                    break;
                case 2:
                    stages.add(new AsPercentStage(randomAlphaOfLength(5)));
                    break;
            }
        }

        return stages;
    }
}
