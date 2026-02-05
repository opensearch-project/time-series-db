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
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

public class TagSubStageTests extends AbstractWireSerializingTestCase<TagSubStage> {

    /**
     * Test case 1: Simple string replacement without regex.
     */
    public void testSimpleStringReplacement() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod", "service", "api");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("production", result.get(0).getLabels().get("env"));
        assertEquals("api", result.get(0).getLabels().get("service"));
    }

    /**
     * Test case 2: Regex replacement with backreference.
     * Replace "prod-east" with "production-east" using pattern "^prod-(.*)$" and replacement "production-$1".
     */
    public void testRegexWithBackreference() {
        TagSubStage stage = new TagSubStage("env", "^prod-(.*)$", "production-$1");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod-east", "service", "api");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("production-east", result.get(0).getLabels().get("env"));
    }

    /**
     * Test case 3: Multiple backreferences.
     * Replace "prod-us-east" with "production_us_east" using pattern "^(\\w+)-(\\w+)-(\\w+)$".
     */
    public void testMultipleBackreferences() {
        TagSubStage stage = new TagSubStage("region", "^(\\w+)-(\\w+)-(\\w+)$", "$1_$2_$3");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("region", "prod-us-east");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("prod_us_east", result.get(0).getLabels().get("region"));
    }

    /**
     * Test case 4: Remove substring by replacing with empty string.
     * Remove version suffix like "-v123" from service names.
     */
    public void testRemoveSubstring() {
        TagSubStage stage = new TagSubStage("service", "-v[0-9]+$", "");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("service", "api-v123");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("api", result.get(0).getLabels().get("service"));
    }

    /**
     * Test case 5: Extract substring using regex group.
     * Extract region from "us-east-1-host123" to get "us-east-1".
     */
    public void testExtractSubstring() {
        TagSubStage stage = new TagSubStage("host", "^([a-z]+-[a-z]+-[0-9]+)-.*$", "$1");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("host", "us-east-1-host123");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("us-east-1", result.get(0).getLabels().get("host"));
    }

    /**
     * Test case 6: Non-matching pattern leaves tag unchanged.
     */
    public void testNonMatchingPatternLeavesTagUnchanged() {
        TagSubStage stage = new TagSubStage("env", "^prod-(.*)$", "production-$1");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "staging", "service", "api");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("staging", result.get(0).getLabels().get("env"));
    }

    /**
     * Test case 7: Time series without the specified tag passes through unchanged.
     */
    public void testTimeSeriesWithoutSpecifiedTag() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("service", "api", "region", "us-east");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals(labels, result.get(0).getLabels());
    }

    /**
     * Test case 8: Time series with null labels passes through unchanged.
     */
    public void testTimeSeriesWithNullLabels() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries timeSeries = new TimeSeries(samples, null, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertNull(result.get(0).getLabels());
    }

    /**
     * Test case 9: Multiple time series with different tag values.
     */
    public void testMultipleTimeSeriesWithDifferentValues() {
        TagSubStage stage = new TagSubStage("env", "^prod-(.*)$", "production-$1");

        List<TimeSeries> input = new ArrayList<>();

        // Series 1: env=prod-east (should be replaced)
        ByteLabels labels1 = ByteLabels.fromStrings("env", "prod-east");
        input.add(new TimeSeries(List.of(new FloatSample(1000L, 10.0)), labels1, 1000L, 1000L, 1000L, null));

        // Series 2: env=prod-west (should be replaced)
        ByteLabels labels2 = ByteLabels.fromStrings("env", "prod-west");
        input.add(new TimeSeries(List.of(new FloatSample(1000L, 20.0)), labels2, 1000L, 1000L, 1000L, null));

        // Series 3: env=staging (no match, unchanged)
        ByteLabels labels3 = ByteLabels.fromStrings("env", "staging");
        input.add(new TimeSeries(List.of(new FloatSample(1000L, 30.0)), labels3, 1000L, 1000L, 1000L, null));

        // Series 4: no env tag (unchanged)
        ByteLabels labels4 = ByteLabels.fromStrings("service", "api");
        input.add(new TimeSeries(List.of(new FloatSample(1000L, 40.0)), labels4, 1000L, 1000L, 1000L, null));

        List<TimeSeries> result = stage.process(input);

        assertEquals(4, result.size());
        assertEquals("production-east", result.get(0).getLabels().get("env"));
        assertEquals("production-west", result.get(1).getLabels().get("env"));
        assertEquals("staging", result.get(2).getLabels().get("env"));
        assertFalse(result.get(3).getLabels().has("env"));
    }

    /**
     * Test case 10: Preserve other labels when modifying one tag.
     */
    public void testPreserveOtherLabels() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod", "service", "api", "region", "us-east", "version", "v1");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("production", result.get(0).getLabels().get("env"));
        assertEquals("api", result.get(0).getLabels().get("service"));
        assertEquals("us-east", result.get(0).getLabels().get("region"));
        assertEquals("v1", result.get(0).getLabels().get("version"));
    }

    /**
     * Test case 11: Preserve alias when modifying labels.
     */
    public void testPreserveAlias() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("env", "prod");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "my-alias");

        List<TimeSeries> result = stage.process(List.of(timeSeries));

        assertEquals(1, result.size());
        assertEquals("production", result.get(0).getLabels().get("env"));
        assertEquals("my-alias", result.get(0).getAlias());
    }

    /**
     * Test case 12: Empty input list returns empty output.
     */
    public void testWithEmptyInput() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");
        List<TimeSeries> result = stage.process(List.of());

        assertEquals(0, result.size());
    }

    /**
     * Test case 13: Null input should throw NullPointerException.
     */
    public void testWithNullInput() {
        TagSubStage stage = new TagSubStage("env", "prod", "production");
        assertNullInputThrowsException(stage, "tag_sub");
    }

    /**
     * Test getName returns correct stage name.
     */
    public void testGetName() {
        TagSubStage stage = new TagSubStage("env", "pattern", "replacement");
        assertEquals("tag_sub", stage.getName());
    }

    /**
     * Test equals and hashCode.
     */
    public void testEqualsAndHashCode() {
        TagSubStage stage1 = new TagSubStage("env", "prod", "production");
        TagSubStage stage2 = new TagSubStage("env", "prod", "production");
        TagSubStage stage3 = new TagSubStage("region", "prod", "production");
        TagSubStage stage4 = new TagSubStage("env", "staging", "production");
        TagSubStage stage5 = new TagSubStage("env", "prod", "staging");

        assertEquals(stage1, stage2);
        assertEquals(stage1.hashCode(), stage2.hashCode());

        assertNotEquals(stage1, stage3);
        assertNotEquals(stage1, stage4);
        assertNotEquals(stage1, stage5);
    }

    /**
     * Test fromArgs factory method with valid arguments.
     */
    public void testFromArgsWithValidArguments() {
        Map<String, Object> args = Map.of("tag_name", "env", "search_pattern", "prod", "replacement", "production");

        TagSubStage stage = TagSubStage.fromArgs(args);

        assertEquals("tag_sub", stage.getName());
    }

    /**
     * Test fromArgs validation for invalid arguments.
     */
    public void testFromArgsValidation() {
        // Null args
        assertThrows(IllegalArgumentException.class, () -> TagSubStage.fromArgs(null));

        // Missing required arguments
        assertThrows(
            IllegalArgumentException.class,
            () -> TagSubStage.fromArgs(Map.of("search_pattern", "prod", "replacement", "production"))
        );
        assertThrows(IllegalArgumentException.class, () -> TagSubStage.fromArgs(Map.of("tag_name", "env", "replacement", "production")));
        assertThrows(IllegalArgumentException.class, () -> TagSubStage.fromArgs(Map.of("tag_name", "env", "search_pattern", "prod")));

        // Empty tag_name or search_pattern
        assertThrows(
            IllegalArgumentException.class,
            () -> TagSubStage.fromArgs(Map.of("tag_name", "", "search_pattern", "prod", "replacement", "production"))
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> TagSubStage.fromArgs(Map.of("tag_name", "env", "search_pattern", "", "replacement", "production"))
        );

        // Empty replacement is valid
        TagSubStage stage = TagSubStage.fromArgs(Map.of("tag_name", "env", "search_pattern", "prod", "replacement", ""));
        assertNotNull(stage);
    }

    /**
     * Test serialization and XContent.
     */
    public void testSerializationAndXContent() throws IOException {
        TagSubStage original = new TagSubStage("env", "^prod-(.*)$", "production-$1");

        // Test writeTo/readFrom
        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        TagSubStage deserialized = TagSubStage.readFrom(input);
        assertEquals(original, deserialized);

        // Test toXContent
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        original.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"tag_name\":\"env\""));
        assertTrue(json.contains("\"search_pattern\":\"^prod-(.*)$\""));
        assertTrue(json.contains("\"replacement\":\"production-$1\""));

        // Test through PipelineStageFactory
        BytesStreamOutput factoryOutput = new BytesStreamOutput();
        factoryOutput.writeString(original.getName());
        original.writeTo(factoryOutput);
        StreamInput factoryInput = factoryOutput.bytes().streamInput();
        PipelineStage factoryDeserialized = PipelineStageFactory.readFrom(factoryInput);
        assertTrue(factoryDeserialized instanceof TagSubStage);
        assertEquals(original, factoryDeserialized);
    }

    @Override
    protected TagSubStage createTestInstance() {
        return new TagSubStage(randomAlphaOfLength(5), randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<TagSubStage> instanceReader() {
        return TagSubStage::readFrom;
    }
}
