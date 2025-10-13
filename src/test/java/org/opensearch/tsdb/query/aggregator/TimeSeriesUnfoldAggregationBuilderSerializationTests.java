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
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialization tests for TimeSeriesUnfoldAggregationBuilder.
 * Extends AbstractWireTestCase to automatically test wire serialization,
 * equals, and hashCode.
 */
public class TimeSeriesUnfoldAggregationBuilderSerializationTests extends AbstractWireTestCase<TimeSeriesUnfoldAggregationBuilder> {

    @Override
    protected TimeSeriesUnfoldAggregationBuilder createTestInstance() {
        // Create random stages - vary between 0-3 stages with different types
        List<UnaryPipelineStage> stages;
        int numStages = randomIntBetween(0, 3);

        if (numStages == 0) {
            stages = List.of();
        } else if (numStages == 1) {
            stages = List.of(new ScaleStage(randomDoubleBetween(0.1, 10.0, true)));
        } else if (numStages == 2) {
            stages = List.<UnaryPipelineStage>of(
                new ScaleStage(randomDoubleBetween(0.1, 10.0, true)),
                new SumStage(randomAlphaOfLength(5))
            );
        } else {
            stages = List.<UnaryPipelineStage>of(
                new ScaleStage(randomDoubleBetween(0.1, 10.0, true)),
                new SumStage(randomAlphaOfLength(4)),
                new ScaleStage(randomDoubleBetween(0.1, 10.0, true))
            );
        }

        // Random timestamp values - can include extreme values
        long minTimestamp = randomBoolean() ? randomLongBetween(0, 1000000L) : Long.MIN_VALUE;
        long maxTimestamp = randomBoolean() ? randomLongBetween(minTimestamp, 2000000L) : Long.MAX_VALUE;
        long step = randomLongBetween(1L, 10000L);

        return new TimeSeriesUnfoldAggregationBuilder(randomAlphaOfLength(10), stages, minTimestamp, maxTimestamp, step);
    }

    @Override
    protected TimeSeriesUnfoldAggregationBuilder copyInstance(TimeSeriesUnfoldAggregationBuilder instance, Version version)
        throws IOException {
        // Serialize and deserialize
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            instance.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new TimeSeriesUnfoldAggregationBuilder(in);
            }
        }
    }

    @Override
    protected TimeSeriesUnfoldAggregationBuilder mutateInstance(TimeSeriesUnfoldAggregationBuilder instance) {
        // Create a mutated version by changing one field
        List<UnaryPipelineStage> stages = instance.getStages();
        long minTimestamp = instance.getMinTimestamp();
        long maxTimestamp = instance.getMaxTimestamp();
        long step = instance.getStep();

        switch (randomIntBetween(0, 3)) {
            case 0:
                // Mutate stages
                List<UnaryPipelineStage> newStages = new ArrayList<>(stages);
                newStages.add(new ScaleStage(randomDoubleBetween(0.1, 10.0, true)));
                stages = newStages;
                break;
            case 1:
                // Mutate minTimestamp
                minTimestamp = randomLongBetween(0, 1000L);
                break;
            case 2:
                // Mutate maxTimestamp
                maxTimestamp = randomLongBetween(3000000L, 4000000L);
                break;
            case 3:
                // Mutate step
                step = randomLongBetween(10001L, 20000L);
                break;
        }

        return new TimeSeriesUnfoldAggregationBuilder(instance.getName() + "_mutated", stages, minTimestamp, maxTimestamp, step);
    }

    /**
     * Test serialization with null stages (should be converted to empty list).
     */
    public void testSerializationWithNullStages() throws IOException {
        // Arrange
        TimeSeriesUnfoldAggregationBuilder original = new TimeSeriesUnfoldAggregationBuilder("null_stages", null, 1000L, 2000L, 100L);

        // Act - Serialize and deserialize
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                TimeSeriesUnfoldAggregationBuilder deserialized = new TimeSeriesUnfoldAggregationBuilder(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getMinTimestamp(), deserialized.getMinTimestamp());
                assertEquals(original.getMaxTimestamp(), deserialized.getMaxTimestamp());
                assertEquals(original.getStep(), deserialized.getStep());
                assertTrue(deserialized.getStages().isEmpty());
            }
        }
    }

    /**
     * Test serialization with extreme timestamp values.
     */
    public void testSerializationWithExtremeValues() throws IOException {
        // Arrange
        TimeSeriesUnfoldAggregationBuilder original = new TimeSeriesUnfoldAggregationBuilder(
            "extreme_values",
            List.of(new ScaleStage(1.0)),
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            1L
        );

        // Act - Serialize and deserialize
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                TimeSeriesUnfoldAggregationBuilder deserialized = new TimeSeriesUnfoldAggregationBuilder(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(Long.MIN_VALUE, deserialized.getMinTimestamp());
                assertEquals(Long.MAX_VALUE, deserialized.getMaxTimestamp());
                assertEquals(1L, deserialized.getStep());
                assertEquals(original.getStages().size(), deserialized.getStages().size());
            }
        }
    }

    /**
     * Test serialization with many complex stages.
     */
    public void testSerializationWithManyComplexStages() throws IOException {
        // Arrange
        List<UnaryPipelineStage> manyStages = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            manyStages.add(new ScaleStage(i + 1.0));
        }

        TimeSeriesUnfoldAggregationBuilder original = new TimeSeriesUnfoldAggregationBuilder("many_stages", manyStages, 500L, 5000L, 250L);

        // Act - Serialize and deserialize
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                TimeSeriesUnfoldAggregationBuilder deserialized = new TimeSeriesUnfoldAggregationBuilder(in);

                // Assert
                assertEquals(original.getName(), deserialized.getName());
                assertEquals(original.getMinTimestamp(), deserialized.getMinTimestamp());
                assertEquals(original.getMaxTimestamp(), deserialized.getMaxTimestamp());
                assertEquals(original.getStep(), deserialized.getStep());
                assertEquals(20, deserialized.getStages().size());

                // Verify stage types and order
                for (int i = 0; i < original.getStages().size(); i++) {
                    assertEquals(original.getStages().get(i).getName(), deserialized.getStages().get(i).getName());
                }
            }
        }
    }
}
