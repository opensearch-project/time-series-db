/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.core.model.FloatSample;

/**
 * Unit tests for KeepLastValueStage functionality.
 */
public class KeepLastValueStageTests extends AbstractWireSerializingTestCase<KeepLastValueStage> {

    /**
     * Test keepLastValue without interval limit - fills all missing points.
     */
    public void testKeepLastValueUnlimited() {
        // Setup: Create time series with gaps
        // Expected timeline: 1000, 1100, 1200, 1300, 1400, 1500, 1600, 1700 (step=100ms)
        // Missing: 1100, 1300
        List<Sample> samples = List.of(
            new FloatSample(1000L, 10.0),  // t=1000
            // missing 1100 - should be filled with 10.0
            new FloatSample(1200L, 30.0),  // t=1200
            // missing 1300 - should be filled with 30.0
            new FloatSample(1400L, 50.0), // t=1400
            // missing 1500 - should be filled with 50.0
            // missing 1600 - should be filled with 50.0
            new FloatSample(1700L, 80.0)   // t=1700
        );

        TimeSeries inputSeries = new TimeSeries(samples, (Labels) null, 1000L, 1700L, 100L, null);
        List<TimeSeries> input = List.of(inputSeries);

        // Execute: Apply KeepLastValueStage without interval limit
        KeepLastValueStage stage = new KeepLastValueStage();
        List<TimeSeries> result = stage.process(input);

        List<Sample> expectedSamples = List.of(
            new FloatSample(1000L, 10.0),
            new FloatSample(1100L, 10.0),  // filled
            new FloatSample(1200L, 30.0),
            new FloatSample(1300L, 30.0),  // filled
            new FloatSample(1400L, 50.0),
            new FloatSample(1500L, 50.0),  // filled
            new FloatSample(1600L, 50.0),  // filled
            new FloatSample(1700L, 80.0)
        );

        // Verify: Should fill missing values with last non-null value
        TestUtils.assertSamplesEqual("Filled samples do not match expected", expectedSamples, result.getFirst().getSamples().toList());
    }

    /**
     * Test keepLastValue with interval limit - only fills within interval.
     */
    public void testKeepLastValueWithInterval() {
        // Setup: Create time series with gaps and 200ms lookback interval
        // Timeline: 1000, 1100, 1200, 1300, 1400, 1500 (step=100ms)
        // Data: 1000=10.0, 1200=30.0, 1500=60.0
        // Missing: 1100, 1300, 1400
        // With 200ms interval (exclusive start, inclusive end):
        // 1100: window (900, 1100] includes 1000 ✓
        // 1300: window (1100, 1300] includes 1200 ✓
        // 1400: window (1200, 1400] excludes 1200 (exactly at boundary), no samples ✗
        List<Sample> samples = List.of(
            new FloatSample(1000L, 10.0),  // t=1000
            new FloatSample(1200L, 30.0),  // t=1200
            new FloatSample(1500L, 60.0)   // t=1500
        );

        TimeSeries inputSeries = new TimeSeries(samples, (Labels) null, 1000L, 1500L, 100L, null);
        List<TimeSeries> input = List.of(inputSeries);

        // Execute: Apply KeepLastValueStage with 200ms interval
        KeepLastValueStage stage = new KeepLastValueStage(200L);
        List<TimeSeries> result = stage.process(input);

        // Verify: Should fill values within interval
        List<Sample> expectedSamples = List.of(
            new FloatSample(1000L, 10.0),
            new FloatSample(1100L, 10.0),  // filled from 1000
            new FloatSample(1200L, 30.0),
            new FloatSample(1300L, 30.0), // filled from 1200
            new FloatSample(1400L, 30.0), // filled from 1200
            new FloatSample(1500L, 60.0)
        );

        // Verify: Should fill missing values with last non-null value within interval
        TestUtils.assertSamplesEqual("Filled samples do not match expected", expectedSamples, result.getFirst().getSamples().toList());
    }

    /**
     * Test keepLastValue with restrictive interval - some points can't be filled.
     */
    public void testKeepLastValueRestrictiveInterval() {
        // Setup: Create time series with gaps and very short 50ms lookback interval
        // Timeline: 1000, 1100, 1200, 1300 (step=100ms)
        // Data: 1000=10.0, 1300=40.0
        // Missing: 1100, 1200
        // With 50ms interval:
        // 1100: 1000 is 100ms ago > 50ms limit ✗
        // 1200: 1000 is 200ms ago > 50ms limit ✗
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(1300L, 40.0));

        TimeSeries inputSeries = new TimeSeries(samples, (Labels) null, 1000L, 1300L, 100L, null);
        List<TimeSeries> input = List.of(inputSeries);

        // Execute: Apply KeepLastValueStage with 50ms interval
        KeepLastValueStage stage = new KeepLastValueStage(50L);
        List<TimeSeries> result = stage.process(input);

        // Verify: Should only include existing values (gaps can't be filled due to interval limit)
        List<Sample> expectedSamples = List.of(
            new FloatSample(1000L, 10.0),
            // 1100 missing - can't fill
            // 1200 missing - can't fill
            new FloatSample(1300L, 40.0)
        );
        TestUtils.assertSamplesEqual("Filled samples do not match expected", expectedSamples, result.getFirst().getSamples().toList());
    }

    /**
     * Test keepLastValue with no missing values - should be pass-through.
     */
    public void testKeepLastValueNoMissingValues() {
        // Setup: Complete time series with no gaps
        List<Sample> samples = List.of(
            new FloatSample(1000L, 10.0),
            new FloatSample(1100L, 20.0),
            new FloatSample(1200L, 30.0),
            new FloatSample(1300L, 40.0)
        );

        TimeSeries inputSeries = new TimeSeries(samples, (Labels) null, 1000L, 1300L, 100L, null);
        List<TimeSeries> input = List.of(inputSeries);

        // Execute: Apply KeepLastValueStage
        KeepLastValueStage stage = new KeepLastValueStage();
        List<TimeSeries> result = stage.process(input);

        // Verify: Should return unchanged data
        assertEquals(1, result.size());
        List<Sample> outputSamples = result.getFirst().getSamples().toList();
        TestUtils.assertSamplesEqual("Filled samples do not match expected", inputSeries.getSamples().toList(), outputSamples);
    }

    /**
     * Test keepLastValue with empty input.
     */
    public void testKeepLastValueEmptyInput() {
        KeepLastValueStage stage = new KeepLastValueStage();
        List<TimeSeries> result = stage.process(List.of());
        assertTrue("Empty input should return empty result", result.isEmpty());
    }

    /**
     * Test keepLastValue with all missing values - can't fill without any non-null values.
     */
    public void testKeepLastValueAllMissing() {
        // Setup: Time series metadata indicates expected range, but no actual samples
        List<Sample> emptySamples = List.of();
        TimeSeries inputSeries = new TimeSeries(emptySamples, (Labels) null, 1000L, 1300L, 100L, null);
        List<TimeSeries> input = List.of(inputSeries);

        // Execute: Apply KeepLastValueStage
        KeepLastValueStage stage = new KeepLastValueStage();
        List<TimeSeries> result = stage.process(input);

        // Verify: Should return empty (can't fill without any reference values)
        assertEquals(1, result.size());
        List<Sample> outputSamples = result.get(0).getSamples().toList();
        assertEquals(0, outputSamples.size());
    }

    /**
     * Test that labels and metadata are preserved correctly.
     */
    public void testKeepLastValuePreservesMetadata() {
        // Setup: Time series with labels and metadata
        Map<String, String> labels = Map.of("service", "api", "region", "us-west");
        String alias = "test_alias";
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(1200L, 30.0));

        TimeSeries inputSeries = new TimeSeries(samples, ByteLabels.fromMap(labels), 1000L, 1200L, 100L, alias);
        List<TimeSeries> input = List.of(inputSeries);

        // Execute: Apply KeepLastValueStage
        KeepLastValueStage stage = new KeepLastValueStage();
        List<TimeSeries> result = stage.process(input);

        // Verify: Should preserve all metadata
        assertEquals(1, result.size());
        TimeSeries outputSeries = result.get(0);

        assertEquals(inputSeries.getLabelsMap(), outputSeries.getLabelsMap());
        assertEquals(inputSeries.getMinTimestamp(), outputSeries.getMinTimestamp());
        assertEquals(inputSeries.getMaxTimestamp(), outputSeries.getMaxTimestamp());
        assertEquals(inputSeries.getStep(), outputSeries.getStep());
        assertEquals(inputSeries.getAlias(), outputSeries.getAlias());
    }

    /**
     * Test fromArgs method with various argument combinations.
     */
    public void testKeepLastValueFromArgs() {
        // Test with null args - should throw exception
        try {
            KeepLastValueStage.fromArgs(null);
            fail("Should throw IllegalArgumentException for null args");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Args cannot be null"));
        }

        // Test with empty args - should create unlimited keepLastValue
        Map<String, Object> emptyArgs = new HashMap<>();
        KeepLastValueStage unlimited = KeepLastValueStage.fromArgs(emptyArgs);
        assertEquals("keep_last_value", unlimited.getName());
        assertNull(unlimited.getLookBackWindow());

        // Test with null look_back_window - should create unlimited keepLastValue
        Map<String, Object> nullWindowArgs = new HashMap<>();
        nullWindowArgs.put(KeepLastValueStage.LOOK_BACK_WINDOW, null);
        KeepLastValueStage nullWindow = KeepLastValueStage.fromArgs(nullWindowArgs);
        assertEquals("keep_last_value", nullWindow.getName());
        assertNull(nullWindow.getLookBackWindow());

        // Test with numeric value
        Map<String, Object> numericArgs = new HashMap<>();
        numericArgs.put(KeepLastValueStage.LOOK_BACK_WINDOW, 300000L);
        KeepLastValueStage numericWindow = KeepLastValueStage.fromArgs(numericArgs);
        assertEquals("keep_last_value", numericWindow.getName());
        assertNotNull(numericWindow.getLookBackWindow());
        assertEquals(300000L, numericWindow.getLookBackWindow().longValue()); // 5 minutes = 300000ms

        // Test with string numeric value
        Map<String, Object> stringArgs = new HashMap<>();
        stringArgs.put(KeepLastValueStage.LOOK_BACK_WINDOW, "7200000");
        KeepLastValueStage stringWindow = KeepLastValueStage.fromArgs(stringArgs);
        assertEquals("keep_last_value", stringWindow.getName());
        assertNotNull(stringWindow.getLookBackWindow());
        assertEquals(7200000L, stringWindow.getLookBackWindow().longValue()); // 2 hours = 7200000ms

        // Test with invalid numeric value - should throw exception
        Map<String, Object> invalidArgs = new HashMap<>();
        invalidArgs.put(KeepLastValueStage.LOOK_BACK_WINDOW, "invalid_number");
        try {
            KeepLastValueStage.fromArgs(invalidArgs);
            fail("Should throw NumberFormatException for invalid numeric value");
        } catch (NumberFormatException e) {
            // Expected exception
        }
    }

    /**
     * Test toXContent method for different KeepLastValueStage configurations.
     */
    public void testKeepLastValueToXContent() throws IOException {
        // Test unlimited keepLastValue
        KeepLastValueStage unlimited = new KeepLastValueStage();
        XContentBuilder unlimitedBuilder = JsonXContent.contentBuilder().startObject();
        unlimited.toXContent(unlimitedBuilder, ToXContent.EMPTY_PARAMS);
        unlimitedBuilder.endObject();
        String unlimitedJson = unlimitedBuilder.toString();
        assertEquals("Unlimited keepLastValue should produce empty JSON object", "{}", unlimitedJson);

        // Test keepLastValue with time window
        KeepLastValueStage withWindow = new KeepLastValueStage(600000L); // 10 minutes
        XContentBuilder windowBuilder = JsonXContent.contentBuilder().startObject();
        withWindow.toXContent(windowBuilder, ToXContent.EMPTY_PARAMS);
        windowBuilder.endObject();
        String windowJson = windowBuilder.toString();
        assertEquals("KeepLastValue with 600000ms window should produce correct JSON", "{\"look_back_window\":600000}", windowJson);

        // Test keepLastValue with seconds window
        KeepLastValueStage withSeconds = new KeepLastValueStage(30000L); // 30 seconds
        XContentBuilder secondsBuilder = JsonXContent.contentBuilder().startObject();
        withSeconds.toXContent(secondsBuilder, ToXContent.EMPTY_PARAMS);
        secondsBuilder.endObject();
        String secondsJson = secondsBuilder.toString();
        assertEquals("KeepLastValue with 30000ms window should produce correct JSON", "{\"look_back_window\":30000}", secondsJson);

        // Test keepLastValue with milliseconds window
        KeepLastValueStage withMillis = new KeepLastValueStage(500L);
        XContentBuilder millisBuilder = JsonXContent.contentBuilder().startObject();
        withMillis.toXContent(millisBuilder, ToXContent.EMPTY_PARAMS);
        millisBuilder.endObject();
        String millisJson = millisBuilder.toString();
        assertEquals("KeepLastValue with 500ms window should produce correct JSON", "{\"look_back_window\":500}", millisJson);
    }

    /**
     * Test KeepLastValueStage with null input throws exception.
     */
    public void testNullInputThrowsException() {
        KeepLastValueStage stage = new KeepLastValueStage();
        TestUtils.assertNullInputThrowsException(stage, "keep_last_value");
    }

    @Override
    protected Writeable.Reader<KeepLastValueStage> instanceReader() {
        return KeepLastValueStage::readFrom;
    }

    @Override
    protected KeepLastValueStage createTestInstance() {
        // Randomly create one of 5 different KeepLastValueStage instances
        int choice = randomInt(4); // 0-4 for 5 different instances

        return switch (choice) {
            case 0 -> new KeepLastValueStage(); // No interval limit
            case 1 -> new KeepLastValueStage(30000L); // 30 seconds
            case 2 -> new KeepLastValueStage(300000L); // 5 minutes
            case 3 -> new KeepLastValueStage(3600000L); // 1 hour
            case 4 -> new KeepLastValueStage(500L); // 500ms
            default -> new KeepLastValueStage();
        };
    }

    public void testEstimateMemoryOverhead() {
        KeepLastValueStage stage = new KeepLastValueStage();

        // Null and empty input should return 0
        assertEquals(0L, stage.estimateMemoryOverhead(null));
        assertEquals(0L, stage.estimateMemoryOverhead(List.of()));

        // Create test series with potential for expansion (sparse data)
        Labels labels = ByteLabels.fromStrings("name", "test");
        List<Sample> sparseSamples = List.of(
            new FloatSample(0L, 10.0),
            new FloatSample(100L, 20.0)  // Only 2 samples over 100ms span with 10ms step = potential 11 samples
        );
        TimeSeries sparseSeries = new TimeSeries(sparseSamples, labels, 0L, 100L, 10L, null);

        // Should return positive overhead for sparse series that can expand
        long overhead = stage.estimateMemoryOverhead(List.of(sparseSeries));
        assertTrue("Overhead should be positive for sparse series with expansion potential", overhead > 0);
    }

    public void testEstimateMemoryOverheadWithZeroStep() {
        KeepLastValueStage stage = new KeepLastValueStage();

        // Series with zero step should be handled gracefully
        Labels labels = ByteLabels.fromStrings("name", "test");
        List<Sample> samples = List.of(new FloatSample(0L, 10.0));
        TimeSeries ts = new TimeSeries(samples, labels, 0L, 100L, 0L, null);

        // Should not throw and return reasonable value
        long overhead = stage.estimateMemoryOverhead(List.of(ts));
        assertTrue("Overhead should be >= 0 for zero step", overhead >= 0);
    }

    public void testEstimateMemoryOverheadWithInvalidTimestamps() {
        KeepLastValueStage stage = new KeepLastValueStage();

        // Series with maxTs < minTs should be handled gracefully
        Labels labels = ByteLabels.fromStrings("name", "test");
        List<Sample> samples = List.of(new FloatSample(100L, 10.0));
        TimeSeries ts = new TimeSeries(samples, labels, 100L, 0L, 10L, null);  // maxTs < minTs

        // Should not throw
        long overhead = stage.estimateMemoryOverhead(List.of(ts));
        assertTrue("Overhead should be >= 0 for invalid timestamps", overhead >= 0);
    }

}
