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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

public class ExcludeByTagStageTests extends AbstractWireSerializingTestCase<ExcludeByTagStage> {

    /**
     * Test case 1: Exclude time series with single matching pattern.
     * The time series with env=production should be excluded.
     */
    public void testExcludeTimeSeriesWithSingleMatchingPattern() {
        ExcludeByTagStage stage = new ExcludeByTagStage("env", List.of("production"));

        // Time series with env=production (should be excluded)
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "production", "service", "api");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(0, result.size());
    }

    /**
     * Test case 2: Keep time series with non-matching pattern.
     * The time series with env=staging should be kept.
     */
    public void testKeepTimeSeriesWithNonMatchingPattern() {
        ExcludeByTagStage stage = new ExcludeByTagStage("env", List.of("production"));

        // Time series with env=staging (should be kept)
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "staging", "service", "api");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals(labels, result.get(0).getLabels());
    }

    /**
     * Test case 3: Exclude time series with regex pattern matching.
     * Tests that regex patterns work correctly (e.g., "prod.*" matches "production").
     */
    public void testExcludeWithRegexPattern() {
        ExcludeByTagStage stage = new ExcludeByTagStage("env", List.of("prod.*"));

        // Time series with env=production (should match "prod.*" and be excluded)
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "production", "service", "api");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(0, result.size());
    }

    /**
     * Test case 4: Exclude time series with multiple patterns.
     * If any pattern matches, the time series should be excluded.
     */
    public void testExcludeWithMultiplePatterns() {
        ExcludeByTagStage stage = new ExcludeByTagStage("env", List.of("production", "staging", "dev.*"));

        // First time series: env=production (matches first pattern)
        List<Sample> samples1 = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels1 = ByteLabels.fromStrings("env", "production");
        TimeSeries ts1 = new TimeSeries(samples1, labels1, 1000L, 1000L, 1000L, null);

        // Second time series: env=development (matches "dev.*" pattern)
        List<Sample> samples2 = List.of(new FloatSample(1000L, 20.0));
        ByteLabels labels2 = ByteLabels.fromStrings("env", "development");
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 1000L, 1000L, 1000L, null);

        // Third time series: env=qa (doesn't match any pattern, should be kept)
        List<Sample> samples3 = List.of(new FloatSample(1000L, 30.0));
        ByteLabels labels3 = ByteLabels.fromStrings("env", "qa");
        TimeSeries ts3 = new TimeSeries(samples3, labels3, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(ts1, ts2, ts3));

        assertEquals(1, result.size());
        assertEquals(labels3, result.get(0).getLabels());
    }

    /**
     * Test case 5: Keep time series without the specified tag.
     * Time series that don't have the tag should always be kept.
     */
    public void testKeepTimeSeriesWithoutSpecifiedTag() {
        ExcludeByTagStage stage = new ExcludeByTagStage("env", List.of("production"));

        // Time series without "env" tag (should be kept)
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("service", "api", "region", "us-east");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals(labels, result.get(0).getLabels());
    }

    /**
     * Test case 6: Keep time series with null labels.
     * Time series with null labels should be kept (no tag to match).
     */
    public void testKeepTimeSeriesWithNullLabels() {
        ExcludeByTagStage stage = new ExcludeByTagStage("env", List.of("production"));

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries timeSeries = new TimeSeries(samples, null, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertNull(result.get(0).getLabels());
    }

    /**
     * Test case 7: Multiple time series with mixed results.
     * Some time series match and are excluded, others don't match and are kept.
     */
    public void testMultipleTimeSeriesMixedResults() {
        ExcludeByTagStage stage = new ExcludeByTagStage("env", List.of("production"));

        List<TimeSeries> input = new ArrayList<>();

        // Series 1: env=production (excluded)
        ByteLabels labels1 = ByteLabels.fromStrings("env", "production", "service", "api");
        input.add(new TimeSeries(List.of(new FloatSample(1000L, 10.0)), labels1, 1000L, 1000L, 1000L, null));

        // Series 2: env=staging (kept)
        ByteLabels labels2 = ByteLabels.fromStrings("env", "staging", "service", "web");
        input.add(new TimeSeries(List.of(new FloatSample(1000L, 20.0)), labels2, 1000L, 1000L, 1000L, null));

        // Series 3: env=production (excluded)
        ByteLabels labels3 = ByteLabels.fromStrings("env", "production", "service", "db");
        input.add(new TimeSeries(List.of(new FloatSample(1000L, 30.0)), labels3, 1000L, 1000L, 1000L, null));

        // Series 4: no env tag (kept)
        ByteLabels labels4 = ByteLabels.fromStrings("service", "cache");
        input.add(new TimeSeries(List.of(new FloatSample(1000L, 40.0)), labels4, 1000L, 1000L, 1000L, null));

        List<TimeSeries> result = stage.process(input);

        assertEquals(2, result.size());
        assertEquals(labels2, result.get(0).getLabels());
        assertEquals(labels4, result.get(1).getLabels());
    }

    /**
     * Test case 8: Empty patterns list.
     * If there are no patterns, no time series should be excluded.
     */
    public void testWithEmptyPatternsList() {
        ExcludeByTagStage stage = new ExcludeByTagStage("env", List.of());

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "production");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        expectThrows(IllegalArgumentException.class, () -> stage.process(List.of(timeSeries)));
    }

    /**
     * Test case 9: Empty time series list.
     * Empty input should return empty output.
     */
    public void testWithEmptyInput() {
        ExcludeByTagStage stage = new ExcludeByTagStage("env", List.of("production"));
        List<TimeSeries> result = stage.process(List.of());
        assertTrue(result.isEmpty());
    }

    /**
     * Test case 10: Null input throws exception.
     */
    public void testNullInputThrowsException() {
        ExcludeByTagStage stage = new ExcludeByTagStage("env", List.of("production"));
        assertNullInputThrowsException(stage, "exclude_by_tag");
    }

    /**
     * Test case 11: Complex regex patterns.
     * Tests more complex regex patterns like ".*-prod" to match "us-prod", "eu-prod", etc.
     */
    public void testComplexRegexPatterns() {
        ExcludeByTagStage stage = new ExcludeByTagStage("region", List.of(".*-prod", "us-.*"));

        // Series 1: region=us-prod (matches both patterns, excluded)
        ByteLabels labels1 = ByteLabels.fromStrings("region", "us-prod");
        TimeSeries ts1 = new TimeSeries(List.of(new FloatSample(1000L, 10.0)), labels1, 1000L, 1000L, 1000L, null);

        // Series 2: region=eu-prod (matches ".*-prod", excluded)
        ByteLabels labels2 = ByteLabels.fromStrings("region", "eu-prod");
        TimeSeries ts2 = new TimeSeries(List.of(new FloatSample(1000L, 20.0)), labels2, 1000L, 1000L, 1000L, null);

        // Series 3: region=us-staging (matches "us-.*", excluded)
        ByteLabels labels3 = ByteLabels.fromStrings("region", "us-staging");
        TimeSeries ts3 = new TimeSeries(List.of(new FloatSample(1000L, 30.0)), labels3, 1000L, 1000L, 1000L, null);

        // Series 4: region=eu-staging (doesn't match any, kept)
        ByteLabels labels4 = ByteLabels.fromStrings("region", "eu-staging");
        TimeSeries ts4 = new TimeSeries(List.of(new FloatSample(1000L, 40.0)), labels4, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(ts1, ts2, ts3, ts4));

        assertEquals(1, result.size());
        assertEquals(labels4, result.get(0).getLabels());
    }

    /**
     * Test case 12: Plain text no regex.
     * Test exact string matching without regex metacharacters.
     * NOTE: due to compatibility with m3 query, we decided to allow partial matching of the label,
     * so 'error' in the test will match against 'errors'
     */
    public void testPlainPatternMatch() {
        ExcludeByTagStage stage = new ExcludeByTagStage("status", List.of("error"));

        // Series 1: status=error (exact match, excluded)
        ByteLabels labels1 = ByteLabels.fromStrings("status", "error");
        TimeSeries ts1 = new TimeSeries(List.of(new FloatSample(1000L, 10.0)), labels1, 1000L, 1000L, 1000L, null);

        // Series 2: status=errors (no match, kept)
        ByteLabels labels2 = ByteLabels.fromStrings("status", "errors");
        TimeSeries ts2 = new TimeSeries(List.of(new FloatSample(1000L, 20.0)), labels2, 1000L, 1000L, 1000L, null);

        // Series 3: status=warning (no match, kept)
        ByteLabels labels3 = ByteLabels.fromStrings("status", "warning");
        TimeSeries ts3 = new TimeSeries(List.of(new FloatSample(1000L, 30.0)), labels3, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(ts1, ts2, ts3));

        assertEquals(1, result.size());
        assertEquals(labels3, result.get(0).getLabels());
    }

    /**
     * Test against case where the pattern only matches partial of the label
     */
    public void testPartialMatch() {

        // test for plain texts
        ExcludeByTagStage stage = new ExcludeByTagStage("status", List.of("2"));

        ByteLabels labels1 = ByteLabels.fromStrings("status", "200");
        TimeSeries ts1 = new TimeSeries(List.of(new FloatSample(1000L, 10.0)), labels1, 1000L, 1000L, 1000L, null);

        ByteLabels labels2 = ByteLabels.fromStrings("status", "020");
        TimeSeries ts2 = new TimeSeries(List.of(new FloatSample(1000L, 20.0)), labels2, 1000L, 1000L, 1000L, null);

        ByteLabels labels3 = ByteLabels.fromStrings("status", "002");
        TimeSeries ts3 = new TimeSeries(List.of(new FloatSample(1000L, 30.0)), labels3, 1000L, 1000L, 1000L, null);

        ByteLabels labels4 = ByteLabels.fromStrings("status", "3030");
        TimeSeries ts4 = new TimeSeries(List.of(new FloatSample(1000L, 30.0)), labels4, 1000L, 1000L, 1000L, null);

        ByteLabels labels5 = ByteLabels.fromStrings("status", "2");
        TimeSeries ts5 = new TimeSeries(List.of(new FloatSample(1000L, 30.0)), labels5, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(ts1, ts2, ts3, ts4, ts5));

        assertEquals(1, result.size());
        assertEquals(labels4, result.get(0).getLabels());

        // test for regex pattern
        stage = new ExcludeByTagStage("status", List.of("2.*d"));
        labels1 = ByteLabels.fromStrings("status", "200");
        ts1 = new TimeSeries(List.of(new FloatSample(1000L, 10.0)), labels1, 1000L, 1000L, 1000L, null);

        labels2 = ByteLabels.fromStrings("status", "020d");
        ts2 = new TimeSeries(List.of(new FloatSample(1000L, 20.0)), labels2, 1000L, 1000L, 1000L, null);

        labels3 = ByteLabels.fromStrings("status", "2d32");
        ts3 = new TimeSeries(List.of(new FloatSample(1000L, 30.0)), labels3, 1000L, 1000L, 1000L, null);

        labels4 = ByteLabels.fromStrings("status", "d32");
        ts4 = new TimeSeries(List.of(new FloatSample(1000L, 30.0)), labels4, 1000L, 1000L, 1000L, null);

        result = stage.process(List.of(ts1, ts2, ts3, ts4));

        assertEquals(2, result.size());
        assertEquals(labels1, result.get(0).getLabels());
        assertEquals(labels4, result.get(1).getLabels());
    }

    /**
     * Test getName().
     */
    public void testGetName() {
        ExcludeByTagStage stage = new ExcludeByTagStage("env", List.of("production"));
        assertEquals("exclude_by_tag", stage.getName());
    }

    /**
     * Test fromArgs() method with valid arguments.
     */
    public void testFromArgs() {
        Map<String, Object> args = Map.of("tag_name", "env", "patterns", List.of("production", "staging"));
        ExcludeByTagStage stage = ExcludeByTagStage.fromArgs(args);
        assertNotNull(stage);
        assertEquals("exclude_by_tag", stage.getName());
    }

    /**
     * Test fromArgs() method with invalid arguments.
     */
    public void testFromArgsInvalid() {
        expectThrows(IllegalArgumentException.class, () -> ExcludeByTagStage.fromArgs(null));

        Map<String, Object> args1 = Map.of("patterns", List.of("production", "staging"));
        expectThrows(IllegalArgumentException.class, () -> ExcludeByTagStage.fromArgs(args1));

        Map<String, Object> args2 = Map.of("tag_name", "");
        expectThrows(IllegalArgumentException.class, () -> ExcludeByTagStage.fromArgs(args2));

        Map<String, Object> args3 = Map.of("tag_name", "env");
        expectThrows(IllegalArgumentException.class, () -> ExcludeByTagStage.fromArgs(args3));

        Map<String, Object> args4 = Map.of("tag_name", "env", "patterns", Collections.emptyList());
        expectThrows(IllegalArgumentException.class, () -> ExcludeByTagStage.fromArgs(args4));
    }

    /**
     * Test PipelineStageFactory integration.
     */
    public void testPipelineStageFactory() {
        assertTrue(PipelineStageFactory.isStageTypeSupported(ExcludeByTagStage.NAME));
        Map<String, Object> args = Map.of("tag_name", "env", "patterns", List.of("production"));
        PipelineStage stage = PipelineStageFactory.createWithArgs(ExcludeByTagStage.NAME, args);
        assertTrue(stage instanceof ExcludeByTagStage);
    }

    /**
     * Test PipelineStageFactory.readFrom(StreamInput).
     */
    public void testPipelineStageFactoryReadFrom_StreamInput() throws Exception {
        ExcludeByTagStage original = new ExcludeByTagStage("env", List.of("production", "staging"));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Factory variant that reads stage name first
            out.writeString(ExcludeByTagStage.NAME);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                PipelineStage restored = PipelineStageFactory.readFrom(in);
                assertNotNull(restored);
                assertTrue(restored instanceof ExcludeByTagStage);
            }
        }
    }

    /**
     * Test toXContent().
     */
    public void testToXContent() throws IOException {
        ExcludeByTagStage stage = new ExcludeByTagStage("env", List.of("production", "staging"));
        try (XContentBuilder builder = XContentFactory.jsonBuilder().startObject()) {
            stage.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            String json = builder.toString();
            assertTrue(json.contains("\"tag_name\":\"env\""));
            assertTrue(json.contains("\"patterns\":[\"production\",\"staging\"]"));
        }
    }

    /**
     * Test equals() and hashCode() - different instances with same values.
     */
    public void testEqualsAndHashCode() {
        ExcludeByTagStage stage1 = new ExcludeByTagStage("env", List.of("production"));
        ExcludeByTagStage stage2 = new ExcludeByTagStage("env", List.of("production"));

        assertNotEquals(stage1, null);
        assertNotEquals(stage1, new Object());
        assertEquals(stage1, stage2);
        assertEquals(stage1, stage1);
    }

    @Override
    protected ExcludeByTagStage createTestInstance() {
        return new ExcludeByTagStage(randomAlphaOfLength(5), List.of(randomAlphaOfLength(8), randomAlphaOfLength(8)));
    }

    @Override
    protected Writeable.Reader<ExcludeByTagStage> instanceReader() {
        return ExcludeByTagStage::readFrom;
    }
}
