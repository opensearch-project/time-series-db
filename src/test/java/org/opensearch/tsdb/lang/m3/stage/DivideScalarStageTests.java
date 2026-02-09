/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DivideScalarStageTests extends AbstractWireSerializingTestCase<DivideScalarStage> {

    public void testConstructorAndBasicProperties() {
        DivideScalarStage stage = new DivideScalarStage(2.5);
        assertEquals(2.5, stage.getDivisor(), 0.001);
        assertEquals("divideScalar", stage.getName());
        assertTrue(stage.supportConcurrentSegmentSearch());
        assertFalse(stage.isCoordinatorOnly());
    }

    public void testConstructorThrowsOnInvalidDivisors() {
        expectThrows(IllegalArgumentException.class, () -> new DivideScalarStage(0.0));
        expectThrows(IllegalArgumentException.class, () -> new DivideScalarStage(Double.NaN));
    }

    public void testProcessDivision() {
        DivideScalarStage stage = new DivideScalarStage(2.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0), new FloatSample(3000L, -6.0));
        TimeSeries inputSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test-series");

        List<TimeSeries> result = stage.process(Arrays.asList(inputSeries));

        assertEquals(1, result.size());
        TimeSeries dividedSeries = result.get(0);
        assertEquals(3, dividedSeries.getSamples().size());
        assertEquals(5.0, dividedSeries.getSamples().getSample(0).getValue(), 0.001); // 10.0 / 2.0
        assertEquals(10.0, dividedSeries.getSamples().getSample(1).getValue(), 0.001); // 20.0 / 2.0
        assertEquals(-3.0, dividedSeries.getSamples().getSample(2).getValue(), 0.001); // -6.0 / 2.0
        assertEquals(labels, dividedSeries.getLabels());
        assertEquals("test-series", dividedSeries.getAlias());
    }

    public void testProcessPreservesNaNInput() {
        DivideScalarStage stage = new DivideScalarStage(2.0);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries inputSeries = new TimeSeries(
            Arrays.asList(new FloatSample(1000L, Double.NaN)),
            labels,
            1000L,
            1000L,
            1000L,
            "test-series"
        );

        List<TimeSeries> result = stage.process(Arrays.asList(inputSeries));

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).getSamples().size());
        assertTrue(Double.isNaN(result.get(0).getSamples().getSample(0).getValue()));
    }

    public void testProcessWithEmptyInput() {
        DivideScalarStage stage = new DivideScalarStage(2.0);
        List<TimeSeries> result = stage.process(Arrays.asList());
        assertTrue(result.isEmpty());
    }

    public void testFromArgs() {
        DivideScalarStage stage1 = DivideScalarStage.fromArgs(Map.of("divisor", 5.0));
        assertEquals(5.0, stage1.getDivisor(), 0.001);

        DivideScalarStage stage2 = DivideScalarStage.fromArgs(Map.of("divisor", 3)); // integer
        assertEquals(3.0, stage2.getDivisor(), 0.001);

        expectThrows(Exception.class, () -> DivideScalarStage.fromArgs(Map.of("divisor", 0.0)));
        expectThrows(Exception.class, () -> DivideScalarStage.fromArgs(Map.of()));
    }

    public void testWriteToAndReadFrom() throws IOException {
        DivideScalarStage originalStage = new DivideScalarStage(3.5);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStage.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                DivideScalarStage readStage = DivideScalarStage.readFrom(in);
                assertEquals(originalStage.getDivisor(), readStage.getDivisor(), 0.001);
                assertEquals(originalStage.getName(), readStage.getName());
            }
        }
    }

    public void testAnnotationAndFactoryIntegration() {
        // Verify annotation
        assertTrue(DivideScalarStage.class.isAnnotationPresent(PipelineStageAnnotation.class));
        PipelineStageAnnotation annotation = DivideScalarStage.class.getAnnotation(PipelineStageAnnotation.class);
        assertEquals("divideScalar", annotation.name());

        // Verify factory registration
        assertTrue(PipelineStageFactory.getSupportedStageTypes().contains("divideScalar"));
        DivideScalarStage factoryStage = (DivideScalarStage) PipelineStageFactory.createWithArgs("divideScalar", Map.of("divisor", 2.0));
        assertEquals(2.0, factoryStage.getDivisor(), 0.001);
    }

    public void testEquals() {
        DivideScalarStage stage1 = new DivideScalarStage(2.5);
        DivideScalarStage stage2 = new DivideScalarStage(2.5);
        DivideScalarStage stage3 = new DivideScalarStage(3.0);

        assertEquals(stage1, stage2);
        assertNotEquals(stage1, stage3);
        assertEquals(stage1, stage1);
        assertNotEquals(stage1, null);
        assertNotEquals(stage1, "string");
    }

    public void testUnaryPipelineStageNullInput() {
        DivideScalarStage stage = new DivideScalarStage(1.0);
        TestUtils.assertNullInputThrowsException(stage, "divideScalar");
    }

    @Override
    protected Writeable.Reader<DivideScalarStage> instanceReader() {
        return DivideScalarStage::readFrom;
    }

    @Override
    protected DivideScalarStage createTestInstance() {
        double divisor;
        do {
            divisor = randomDoubleBetween(-1000.0, 1000.0, false); // false excludes zero
        } while (Double.isNaN(divisor)); // Also exclude NaN
        return new DivideScalarStage(divisor);
    }
}
