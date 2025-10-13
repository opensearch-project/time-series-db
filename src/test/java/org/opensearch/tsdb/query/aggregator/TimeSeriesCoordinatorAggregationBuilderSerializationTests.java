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
import org.opensearch.tsdb.query.stage.PipelineStage;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serialization tests for TimeSeriesCoordinatorAggregationBuilder.
 * Extends AbstractWireTestCase to automatically test wire serialization,
 * equals, and hashCode.
 */
public class TimeSeriesCoordinatorAggregationBuilderSerializationTests extends AbstractWireTestCase<
    TimeSeriesCoordinatorAggregationBuilder> {

    @Override
    protected TimeSeriesCoordinatorAggregationBuilder createTestInstance() {
        // Create random stages - vary between 1-3 stages with different types
        List<PipelineStage> stages;
        int numStages = randomIntBetween(1, 3);
        if (numStages == 1) {
            stages = List.of(new ScaleStage(randomDoubleBetween(0.1, 10.0, true)));
        } else if (numStages == 2) {
            stages = List.<PipelineStage>of(
                new ScaleStage(randomDoubleBetween(0.1, 10.0, true)),
                new SumStage(List.of(randomAlphaOfLength(5)))
            );
        } else {
            stages = List.<PipelineStage>of(
                new ScaleStage(randomDoubleBetween(0.1, 10.0, true)),
                new AsPercentStage(randomAlphaOfLength(4)),
                new SumStage(List.of(randomAlphaOfLength(5)))
            );
        }

        // Create references - vary between 1-3 references, including empty case
        Map<String, String> references;
        int numRefs = randomIntBetween(0, 3);
        if (numRefs == 0) {
            // Test empty references case
            references = Map.of();
        } else if (numRefs == 1) {
            references = Map.of(randomAlphaOfLength(5), randomAlphaOfLength(8));
        } else if (numRefs == 2) {
            references = Map.of(randomAlphaOfLength(5), randomAlphaOfLength(8), randomAlphaOfLength(6), randomAlphaOfLength(8));
        } else {
            references = Map.of(
                randomAlphaOfLength(5),
                randomAlphaOfLength(8),
                randomAlphaOfLength(6),
                randomAlphaOfLength(8),
                randomAlphaOfLength(7),
                randomAlphaOfLength(8)
            );
        }

        // Input reference - null, or one of the references
        String inputReference;
        if (references.isEmpty() || randomBoolean()) {
            inputReference = null;
        } else {
            inputReference = randomFrom(references.keySet());
        }

        // Create macros - vary between 0-2 macros with different stage types
        LinkedHashMap<String, TimeSeriesCoordinatorAggregator.MacroDefinition> macros;
        int numMacros = randomIntBetween(0, 2);
        if (numMacros == 0) {
            macros = new LinkedHashMap<>();
        } else if (numMacros == 1) {
            String macroInputRef = references.isEmpty() ? randomAlphaOfLength(4) : randomFrom(references.keySet());
            TimeSeriesCoordinatorAggregator.MacroDefinition macro = new TimeSeriesCoordinatorAggregator.MacroDefinition(
                randomAlphaOfLength(5),
                List.of(new ScaleStage(randomDoubleBetween(0.1, 10.0, true))),
                macroInputRef
            );
            macros = new LinkedHashMap<>(Map.of(randomAlphaOfLength(5), macro));
        } else {
            // Multiple macros with different stage types
            String macroInputRef1 = references.isEmpty() ? randomAlphaOfLength(4) : randomFrom(references.keySet());
            String macroInputRef2 = references.isEmpty() ? randomAlphaOfLength(4) : randomFrom(references.keySet());

            TimeSeriesCoordinatorAggregator.MacroDefinition macro1 = new TimeSeriesCoordinatorAggregator.MacroDefinition(
                randomAlphaOfLength(5),
                List.of(new AsPercentStage(randomAlphaOfLength(4))),
                macroInputRef1
            );
            TimeSeriesCoordinatorAggregator.MacroDefinition macro2 = new TimeSeriesCoordinatorAggregator.MacroDefinition(
                randomAlphaOfLength(6),
                List.of(new ScaleStage(randomDoubleBetween(0.1, 10.0, true))),
                macroInputRef2
            );
            macros = new LinkedHashMap<>(Map.of(randomAlphaOfLength(5), macro1, randomAlphaOfLength(6), macro2));
        }

        return new TimeSeriesCoordinatorAggregationBuilder(randomAlphaOfLength(10), stages, macros, references, inputReference);
    }

    @Override
    protected TimeSeriesCoordinatorAggregationBuilder copyInstance(TimeSeriesCoordinatorAggregationBuilder instance, Version version)
        throws IOException {
        // Serialize and deserialize
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            instance.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new TimeSeriesCoordinatorAggregationBuilder(in);
            }
        }
    }

    @Override
    protected TimeSeriesCoordinatorAggregationBuilder mutateInstance(TimeSeriesCoordinatorAggregationBuilder instance) {
        // Create a mutated version by changing one field
        List<PipelineStage> stages = instance.getStages();
        Map<String, String> references = instance.getReferences();
        String inputReference = instance.getInputReference();
        LinkedHashMap<String, TimeSeriesCoordinatorAggregator.MacroDefinition> macros = instance.getMacroDefinitions();

        switch (randomIntBetween(0, 3)) {
            case 0:
                // Mutate stages
                stages = List.of(new SumStage(List.of(randomAlphaOfLength(5))));
                break;
            case 1:
                // Mutate references
                references = Map.of(randomAlphaOfLength(7), randomAlphaOfLength(9));
                break;
            case 2:
                // Mutate input reference
                inputReference = randomBoolean() ? null : randomAlphaOfLength(6);
                break;
            case 3:
                // Mutate macros
                TimeSeriesCoordinatorAggregator.MacroDefinition newMacro = new TimeSeriesCoordinatorAggregator.MacroDefinition(
                    randomAlphaOfLength(6),
                    List.of(new AsPercentStage(randomAlphaOfLength(5))),
                    randomAlphaOfLength(4)
                );
                macros = new LinkedHashMap<>(Map.of(randomAlphaOfLength(7), newMacro));
                break;
        }

        return new TimeSeriesCoordinatorAggregationBuilder(instance.getName() + "_mutated", stages, macros, references, inputReference);
    }
}
