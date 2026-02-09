/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

public class TransformNullStageTests extends AbstractWireSerializingTestCase<TransformNullStage> {

    /**
     * Test case 1: All null values (missing data points).
     * No samples are provided, so all values should be replaced with the fill value.
     */
    public void testAllNullValues() {
        TransformNullStage stage = new TransformNullStage(10.0);
        // Empty samples list represents all null/missing values
        List<Sample> samples = List.of();
        ByteLabels labels = ByteLabels.fromStrings("name", "metric1");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 30L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(3, resultSeries.getSamples().size());

        // All values should be filled with 10.0
        List<Sample> expectedSamples = List.of(new FloatSample(10L, 10.0), new FloatSample(20L, 10.0), new FloatSample(30L, 10.0));
        assertSamplesEqual("All null values test", expectedSamples, resultSeries.getSamples().toList());
    }

    /**
     * Test case 2: Mixed values.
     * Some samples are valid, some are missing. Missing timestamps should be filled.
     */
    public void testMixedValues() {
        TransformNullStage stage = new TransformNullStage(5.0);
        // Create sparse samples - skip some timestamps to represent null values
        List<Sample> samples = List.of(
            new FloatSample(10L, 1.0),      // valid
            // 20L is missing (null) -> should become 5.0
            new FloatSample(30L, 3.0)       // valid
            // 40L and 50L are missing (null) -> should become 5.0
        );
        ByteLabels labels = ByteLabels.fromStrings("name", "metric2");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 50L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);

        // Should have 5 samples (10, 20, 30, 40, 50)
        assertEquals(5, resultSeries.getSamples().size());

        List<Sample> expectedSamples = List.of(
            new FloatSample(10L, 1.0),  // original
            new FloatSample(20L, 5.0),  // filled
            new FloatSample(30L, 3.0),  // original
            new FloatSample(40L, 5.0),  // filled
            new FloatSample(50L, 5.0)   // filled
        );
        assertSamplesEqual("Mixed values test", expectedSamples, resultSeries.getSamples().toList());
    }

    /**
     * Test case 3: Default value (0) with mixed valid and missing data.
     * Using the no-arg constructor should fill with 0.0 by default.
     * Different from test case 1 which has all missing data.
     */
    public void testDefaultFillValue() {
        TransformNullStage stage = new TransformNullStage(); // defaults to 0.0
        // Create a diverse test with one valid value and some missing timestamps
        List<Sample> samples = List.of(new FloatSample(20L, 15.0));
        ByteLabels labels = ByteLabels.fromStrings("name", "metric3");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 40L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(4, resultSeries.getSamples().size()); // 10, 20, 30, 40

        // Expected: [0.0 at 10L, 15.0 at 20L, 0.0 at 30L, 0.0 at 40L]
        List<Sample> expectedSamples = List.of(
            new FloatSample(10L, 0.0),   // filled
            new FloatSample(20L, 15.0),  // original
            new FloatSample(30L, 0.0),   // filled
            new FloatSample(40L, 0.0)    // filled
        );
        assertSamplesEqual("Default fill value test", expectedSamples, resultSeries.getSamples().toList());
    }

    /**
     * Test case 4: NaN values should be treated as null and filled.
     * This test ensures that NaN values in the input are replaced with the fill value.
     */
    public void testNaNValuesAreTreatedAsNull() {
        TransformNullStage stage = new TransformNullStage(7.0);
        // Create samples with NaN values
        List<Sample> samples = List.of(
            new FloatSample(10L, 1.0),      // valid
            new FloatSample(20L, Double.NaN), // NaN should be treated as null -> 7.0
            new FloatSample(30L, 3.0),      // valid
            new FloatSample(40L, Double.NaN), // NaN should be treated as null -> 7.0
            new FloatSample(50L, 5.0)       // valid
        );
        ByteLabels labels = ByteLabels.fromStrings("name", "metric_nan");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 50L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(5, resultSeries.getSamples().size());

        // NaN values should be replaced with fill value (7.0)
        List<Sample> expectedSamples = List.of(
            new FloatSample(10L, 1.0),  // original
            new FloatSample(20L, 7.0),  // NaN replaced with fill value
            new FloatSample(30L, 3.0),  // original
            new FloatSample(40L, 7.0),  // NaN replaced with fill value
            new FloatSample(50L, 5.0)   // original
        );
        assertSamplesEqual("NaN values test", expectedSamples, resultSeries.getSamples().toList());
    }

    /**
     * Test case 5: Mixed NaN and missing values.
     * Both NaN values and missing timestamps should be filled with the fill value.
     */
    public void testMixedNaNAndMissingValues() {
        TransformNullStage stage = new TransformNullStage(9.0);
        // Create samples with both NaN and missing timestamps
        List<Sample> samples = List.of(
            new FloatSample(10L, 1.0),      // valid
            // 20L is missing -> should become 9.0
            new FloatSample(30L, Double.NaN), // NaN -> should become 9.0
            new FloatSample(40L, 4.0)       // valid
            // 50L is missing -> should become 9.0
        );
        ByteLabels labels = ByteLabels.fromStrings("name", "metric_mixed");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 50L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(5, resultSeries.getSamples().size());

        List<Sample> expectedSamples = List.of(
            new FloatSample(10L, 1.0),  // original
            new FloatSample(20L, 9.0),  // missing, filled
            new FloatSample(30L, 9.0),  // NaN, filled
            new FloatSample(40L, 4.0),  // original
            new FloatSample(50L, 9.0)   // missing, filled
        );
        assertSamplesEqual("Mixed NaN and missing values test", expectedSamples, resultSeries.getSamples().toList());
    }

    /**
     * Test case 6: All NaN values.
     * When all samples have NaN values, all should be replaced with fill value.
     */
    public void testAllNaNValues() {
        TransformNullStage stage = new TransformNullStage(3.0);
        // All samples have NaN values
        List<Sample> samples = List.of(
            new FloatSample(10L, Double.NaN),
            new FloatSample(20L, Double.NaN),
            new FloatSample(30L, Double.NaN)
        );
        ByteLabels labels = ByteLabels.fromStrings("name", "metric_all_nan");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 30L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(3, resultSeries.getSamples().size());

        // All NaN values should be replaced with 3.0
        List<Sample> expectedSamples = List.of(new FloatSample(10L, 3.0), new FloatSample(20L, 3.0), new FloatSample(30L, 3.0));
        assertSamplesEqual("All NaN values test", expectedSamples, resultSeries.getSamples().toList());
    }

    /**
     * Test with empty input list.
     */
    public void testWithEmptyInput() {
        TransformNullStage stage = new TransformNullStage(1.0);
        List<TimeSeries> result = stage.process(List.of());
        assertTrue(result.isEmpty());
    }

    /**
     * Test with multiple time series.
     */
    public void testMultipleTimeSeries() {
        TransformNullStage stage = new TransformNullStage(2.0);

        // First time series: has one valid sample at 20L, missing at 10L
        List<Sample> samples1 = List.of(new FloatSample(20L, 5.0));
        ByteLabels labels1 = ByteLabels.fromStrings("name", "ts1");
        TimeSeries ts1 = new TimeSeries(samples1, labels1, 10L, 20L, 10L, null);

        // Second time series: has one valid sample at 100L, missing at 110L
        List<Sample> samples2 = List.of(new FloatSample(100L, 10.0));
        ByteLabels labels2 = ByteLabels.fromStrings("name", "ts2");
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 100L, 110L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(ts1, ts2));

        assertEquals(2, result.size());

        // First time series: expected [2.0 at 10L, 5.0 at 20L]
        List<Sample> expectedSamples1 = List.of(
            new FloatSample(10L, 2.0),  // filled
            new FloatSample(20L, 5.0)   // original
        );
        assertSamplesEqual("First time series samples", expectedSamples1, result.get(0).getSamples().toList());

        // Second time series: expected [10.0 at 100L, 2.0 at 110L]
        List<Sample> expectedSamples2 = List.of(
            new FloatSample(100L, 10.0), // original
            new FloatSample(110L, 2.0)   // filled
        );
        assertSamplesEqual("Second time series samples", expectedSamples2, result.get(1).getSamples().toList());
    }

    /**
     * Test getName().
     */
    public void testGetName() {
        TransformNullStage stage = new TransformNullStage(1.0);
        assertEquals("transform_null", stage.getName());
    }

    /**
     * Test getDisplayName() with integer and decimal values.
     */
    public void testGetDisplayName() {
        TransformNullStage stage1 = new TransformNullStage(0.0);
        assertEquals("transformNull 0", stage1.getDisplayName());

        TransformNullStage stage2 = new TransformNullStage(5.0);
        assertEquals("transformNull 5", stage2.getDisplayName());

        TransformNullStage stage3 = new TransformNullStage(2.5);
        assertEquals("transformNull 2.5", stage3.getDisplayName());
    }

    /**
     * Test fromArgs() method.
     */
    public void testFromArgs() {
        Map<String, Object> args = Map.of("fill_value", 3.5);
        TransformNullStage stage = TransformNullStage.fromArgs(args);

        // Empty samples list represents missing data point
        List<Sample> samples = List.of();
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        List<Sample> expectedSamples = List.of(new FloatSample(10L, 3.5));
        assertSamplesEqual("fromArgs test", expectedSamples, result.get(0).getSamples().toList());
    }

    /**
     * Test fromArgs() with default fill value (when fill_value is not provided).
     */
    public void testFromArgsWithDefaultValue() {
        Map<String, Object> args = Map.of();
        TransformNullStage stage = TransformNullStage.fromArgs(args);

        // Empty samples list represents missing data point
        List<Sample> samples = List.of();
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        List<Sample> expectedSamples = List.of(new FloatSample(10L, 0.0));
        assertSamplesEqual("fromArgs with default value test", expectedSamples, result.get(0).getSamples().toList());
    }

    /**
     * Test PipelineStageFactory integration.
     */
    public void testPipelineStageFactory() {
        assertTrue(PipelineStageFactory.isStageTypeSupported(TransformNullStage.NAME));
        PipelineStage stage = PipelineStageFactory.createWithArgs(TransformNullStage.NAME, Map.of("fill_value", 4.0));
        assertTrue(stage instanceof TransformNullStage);
    }

    /**
     * Test PipelineStageFactory.readFrom(StreamInput).
     */
    public void testPipelineStageFactoryReadFrom_StreamInput() throws Exception {
        TransformNullStage original = new TransformNullStage(3.14);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Factory variant that reads stage name first
            out.writeString(TransformNullStage.NAME);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                PipelineStage restored = PipelineStageFactory.readFrom(in);
                assertNotNull(restored);
                assertTrue(restored instanceof TransformNullStage);
            }
        }
    }

    /**
     * Test toXContent().
     */
    public void testToXContent() throws IOException {
        TransformNullStage stage = new TransformNullStage(2.5);
        try (XContentBuilder builder = XContentFactory.jsonBuilder().startObject()) {
            stage.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            String json = builder.toString();
            assertEquals("{\"fill_value\":2.5}", json);
        }
    }

    /**
     * Test supportConcurrentSegmentSearch().
     */
    public void testSupportConcurrentSegmentSearch() {
        TransformNullStage stage = new TransformNullStage(1.0);
        assertFalse(stage.supportConcurrentSegmentSearch());
    }

    @Override
    protected TransformNullStage createTestInstance() {
        double fillValue = randomDouble() * 100;
        return new TransformNullStage(fillValue);
    }

    @Override
    protected Writeable.Reader<TransformNullStage> instanceReader() {
        return TransformNullStage::readFrom;
    }

    public void testNullInputThrowsException() {
        TransformNullStage stage = new TransformNullStage(0.0);
        assertNullInputThrowsException(stage, "transform_null");
    }

    /**
     * Test that TransformNullStage can be serialized to JSON (toXContent) and
     * deserialized back (fromArgs) without losing information.
     *
     * <p>This ensures round-trip serialization works correctly and that Number
     * type conversions (Integer/Long/Double) are handled properly.</p>
     */
    public void testXContentDeserialization() throws IOException {
        // Create original stage with custom fill value
        TransformNullStage original = new TransformNullStage(3.14);

        // Serialize to JSON and parse back to args Map
        Map<String, Object> args = PipelineStageTestUtils.serializeToArgs(original);

        // Deserialize from args
        TransformNullStage deserialized = TransformNullStage.fromArgs(args);

        // Verify round-trip preserves all fields
        assertEquals("Stage names should match", original.getName(), deserialized.getName());
        assertEquals("Stages should be equal after round-trip", original, deserialized);
    }

    public void testEstimateMemoryOverhead() {
        TransformNullStage stage = new TransformNullStage(0.0);

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
}
