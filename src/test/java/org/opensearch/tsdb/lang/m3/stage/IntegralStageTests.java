/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IntegralStageTests extends AbstractWireSerializingTestCase<IntegralStage> {

    public void testProcessWithEmptyInput() {
        IntegralStage stage = new IntegralStage(false);
        List<TimeSeries> input = new ArrayList<>();
        List<TimeSeries> result = stage.process(input);
        assertTrue(result.isEmpty());
    }

    public void testProcessWithNullInput() {
        IntegralStage stage = new IntegralStage(false);
        TestUtils.assertNullInputThrowsException(stage, "integral");
    }

    public void testProcessWithResetOnNullTrue() {
        IntegralStage stage = new IntegralStage(true);
        ByteLabels labels = ByteLabels.fromStrings("name", "name");
        List<Sample> samples = List.of(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 2.0),
            new FloatSample(3000L, Double.NaN),
            new FloatSample(4000L, 3.0),
            new FloatSample(5000L, 4.0)
        );
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 5000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> expectedSamples = List.of(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 3.0),
            new FloatSample(4000L, 3.0), // Reset to 0, then 3
            new FloatSample(5000L, 7.0)
        );
        assertSamplesEqual("ResetOnNull true", expectedSamples, result.get(0).getSamples().toList());
    }

    public void testProcessWithResetOnNullFalse() {
        IntegralStage stage = new IntegralStage(false);
        ByteLabels labels = ByteLabels.fromStrings("name", "name");
        List<Sample> samples = List.of(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 2.0),
            new FloatSample(3000L, Double.NaN),
            new FloatSample(4000L, 3.0),
            new FloatSample(5000L, 4.0)
        );
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 5000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> expectedSamples = List.of(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 3.0),
            new FloatSample(4000L, 6.0), // Continues: 3 + 3
            new FloatSample(5000L, 10.0)
        );
        assertSamplesEqual("ResetOnNull false", expectedSamples, result.get(0).getSamples().toList());
    }

    public void testProcessWithAllNaN() {
        IntegralStage stage = new IntegralStage(false);
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        List<Sample> samples = List.of(new FloatSample(1000L, Double.NaN), new FloatSample(2000L, Double.NaN));
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        assertEquals(0, result.get(0).getSamples().size()); // All NaN, nothing inserted
    }

    public void testProcessWithGap() {
        IntegralStage stage = new IntegralStage(true);
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        List<Sample> samples = List.of(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 2.0),
            new FloatSample(5000L, 3.0) // Gap: 5000 - 2000 = 3000 > step (1000)
        );
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 5000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> expectedSamples = List.of(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 3.0),
            new FloatSample(5000L, 3.0) // Reset due to gap, then 3
        );
        assertSamplesEqual("Gap test", expectedSamples, result.get(0).getSamples().toList());
    }

    public void testDefaultConstructor() {
        IntegralStage stage = new IntegralStage();
        assertFalse("Default constructor should create stage with resetOnNull=false", stage.isResetOnNull());
    }

    public void testToXContent() throws IOException {
        // Test with resetOnNull=true
        IntegralStage stageTrue = new IntegralStage(true);
        try (XContentBuilder builder = XContentFactory.jsonBuilder().startObject()) {
            stageTrue.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String json = builder.toString();
            assertEquals("{\"resetOnNull\":true}", json);
        }

        // Test with resetOnNull=false
        IntegralStage stageFalse = new IntegralStage(false);
        try (XContentBuilder builder = XContentFactory.jsonBuilder().startObject()) {
            stageFalse.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            String json = builder.toString();
            assertEquals("{\"resetOnNull\":false}", json);
        }
    }

    public void testFromArgs() {
        // Test with Boolean true
        assertTrue(IntegralStage.fromArgs(Map.of("resetOnNull", true)).isResetOnNull());
        // Test with Boolean false
        assertFalse(IntegralStage.fromArgs(Map.of("resetOnNull", false)).isResetOnNull());
        // Test with empty map (default)
        assertFalse(IntegralStage.fromArgs(Map.of()).isResetOnNull());
        // Test with null args map (default)
        assertFalse(IntegralStage.fromArgs(null).isResetOnNull());
        // Test with String "true"
        assertTrue(IntegralStage.fromArgs(Map.of("resetOnNull", "true")).isResetOnNull());
        // Test with String "false"
        assertFalse(IntegralStage.fromArgs(Map.of("resetOnNull", "false")).isResetOnNull());
        // Test with null value for resetOnNull key (should default to false)
        Map<String, Object> argsWithNull = new HashMap<>();
        argsWithNull.put("resetOnNull", null);
        assertFalse(IntegralStage.fromArgs(argsWithNull).isResetOnNull());
    }

    public void testFromArgsWithInvalidString() {
        // Test with invalid string value
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IntegralStage.fromArgs(Map.of("resetOnNull", "invalid"))
        );
        assertEquals("Invalid type for 'resetOnNull' argument. Expected boolean, but got: invalid", e.getMessage());
    }

    public void testFromArgsWithInvalidType() {
        // Test with invalid type (Integer)
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> IntegralStage.fromArgs(Map.of("resetOnNull", 123)));
        assertEquals("Invalid type for 'resetOnNull' argument. Expected Boolean or String, but got Integer", e.getMessage());
    }

    public void testEstimateMemoryOverhead() {
        IntegralStage stage = new IntegralStage(false);

        // Null and empty input should return 0
        assertEquals(0L, stage.estimateMemoryOverhead(null));
        assertEquals(0L, stage.estimateMemoryOverhead(List.of()));

        // Create test series with samples
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0), new FloatSample(3000L, 30.0));
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, null);

        // Single series should return positive overhead
        long overhead = stage.estimateMemoryOverhead(List.of(ts));
        assertTrue("Overhead should be positive for non-empty input", overhead > 0);

        // Multiple series should scale
        long doubleOverhead = stage.estimateMemoryOverhead(List.of(ts, ts));
        assertTrue("Overhead should scale with number of series", doubleOverhead > overhead);
    }

    @Override
    protected IntegralStage createTestInstance() {
        return new IntegralStage(randomBoolean());
    }

    @Override
    protected IntegralStage mutateInstance(IntegralStage instance) {
        return new IntegralStage(!instance.isResetOnNull());
    }

    @Override
    protected Writeable.Reader<IntegralStage> instanceReader() {
        return IntegralStage::readFrom;
    }
}
