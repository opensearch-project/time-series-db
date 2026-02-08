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
import org.opensearch.tsdb.lang.m3.common.TagComparisonOperator;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

public class TagCompareStageTests extends AbstractWireSerializingTestCase<TagCompareStage> {

    /**
     * Test lexicographic comparison with city names (from documentation example).
     */
    public void testLexicographicComparison() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.LT, "city", "denver");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        // Create test time series
        TimeSeries atlanta = new TimeSeries(
            samples,
            ByteLabels.fromStrings("city", "atlanta", "name", "actions"),
            1000L,
            1000L,
            1000L,
            "actions"
        );
        TimeSeries boston = new TimeSeries(
            samples,
            ByteLabels.fromStrings("city", "boston", "name", "bikes"),
            1000L,
            1000L,
            1000L,
            "bikes"
        );
        TimeSeries chicago = new TimeSeries(
            samples,
            ByteLabels.fromStrings("city", "chicago", "name", "drivers"),
            1000L,
            1000L,
            1000L,
            "drivers"
        );
        TimeSeries denver = new TimeSeries(
            samples,
            ByteLabels.fromStrings("city", "denver", "name", "cities"),
            1000L,
            1000L,
            1000L,
            "cities"
        );

        List<TimeSeries> input = List.of(atlanta, boston, chicago, denver);
        List<TimeSeries> result = stage.process(input);

        // Should include atlanta, boston, chicago (all < "denver" lexicographically)
        // Should exclude denver (not < "denver")
        assertEquals(3, result.size());

        // Verify the included series
        assertTrue(result.stream().anyMatch(ts -> "atlanta".equals(ts.getLabels().get("city"))));
        assertTrue(result.stream().anyMatch(ts -> "boston".equals(ts.getLabels().get("city"))));
        assertTrue(result.stream().anyMatch(ts -> "chicago".equals(ts.getLabels().get("city"))));

        // Verify denver is excluded
        assertFalse(result.stream().anyMatch(ts -> "denver".equals(ts.getLabels().get("city"))));
    }

    /**
     * Test semantic version comparison (from documentation example).
     */
    public void testSemanticVersionComparison() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.LE, "version", "30.500.100");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        // Create test time series with version tags
        TimeSeries actions = new TimeSeries(
            samples,
            ByteLabels.fromStrings("version", "30.500.1", "name", "actions"),
            1000L,
            1000L,
            1000L,
            "actions"
        );
        TimeSeries bikes = new TimeSeries(
            samples,
            ByteLabels.fromStrings("version", "29.5", "name", "bikes"),
            1000L,
            1000L,
            1000L,
            "bikes"
        );
        TimeSeries drivers = new TimeSeries(
            samples,
            ByteLabels.fromStrings("version", "30.5", "name", "drivers"),
            1000L,
            1000L,
            1000L,
            "drivers"
        );
        TimeSeries cities = new TimeSeries(
            samples,
            ByteLabels.fromStrings("version", "30.600", "name", "cities"),
            1000L,
            1000L,
            1000L,
            "cities"
        );
        TimeSeries errors = new TimeSeries(
            samples,
            ByteLabels.fromStrings("version", "30.500.100", "name", "errors"),
            1000L,
            1000L,
            1000L,
            "errors"
        );

        List<TimeSeries> input = List.of(actions, bikes, drivers, cities, errors);
        List<TimeSeries> result = stage.process(input);

        // Should include bikes (29.5), drivers (30.5), actions (30.500.1), errors (30.500.100)
        // Should exclude cities (30.600) as it's > 30.500.100
        assertEquals(4, result.size());

        // Verify the included series
        assertTrue(result.stream().anyMatch(ts -> "29.5".equals(ts.getLabels().get("version"))));
        assertTrue(result.stream().anyMatch(ts -> "30.5".equals(ts.getLabels().get("version"))));
        assertTrue(result.stream().anyMatch(ts -> "30.500.1".equals(ts.getLabels().get("version"))));
        assertTrue(result.stream().anyMatch(ts -> "30.500.100".equals(ts.getLabels().get("version"))));

        // Verify cities is excluded
        assertFalse(result.stream().anyMatch(ts -> "30.600".equals(ts.getLabels().get("version"))));
    }

    /**
     * Test that series missing the target tag are excluded.
     */
    public void testMissingTagExcluded() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.EQ, "env", "prod");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        TimeSeries withTag = new TimeSeries(samples, ByteLabels.fromStrings("env", "prod", "service", "api"), 1000L, 1000L, 1000L, "api");
        TimeSeries withoutTag = new TimeSeries(samples, ByteLabels.fromStrings("service", "web"), 1000L, 1000L, 1000L, "web");

        List<TimeSeries> result = stage.process(List.of(withTag, withoutTag));

        assertEquals(1, result.size());
        assertEquals("prod", result.get(0).getLabels().get("env"));
    }

    /**
     * Test that invalid semantic versions are excluded when comparison value is semantic.
     */
    public void testInvalidVersionsExcluded() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.GT, "version", "1.0.0");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        TimeSeries validVersion = new TimeSeries(
            samples,
            ByteLabels.fromStrings("version", "2.0.0", "name", "valid"),
            1000L,
            1000L,
            1000L,
            "valid"
        );
        TimeSeries invalidVersion = new TimeSeries(
            samples,
            ByteLabels.fromStrings("version", "invalid", "name", "invalid"),
            1000L,
            1000L,
            1000L,
            "invalid"
        );

        List<TimeSeries> result = stage.process(List.of(validVersion, invalidVersion));

        assertEquals(1, result.size());
        assertEquals("2.0.0", result.get(0).getLabels().get("version"));
        assertEquals("valid", result.get(0).getLabels().get("name"));
    }

    /**
     * Test equality comparison.
     */
    public void testEqualityComparison() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.EQ, "status", "active");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        TimeSeries active = new TimeSeries(samples, ByteLabels.fromStrings("status", "active"), 1000L, 1000L, 1000L, null);
        TimeSeries inactive = new TimeSeries(samples, ByteLabels.fromStrings("status", "inactive"), 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(active, inactive));

        assertEquals(1, result.size());
        assertEquals("active", result.get(0).getLabels().get("status"));
    }

    /**
     * Test not-equality comparison.
     */
    public void testNotEqualityComparison() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.NE, "status", "disabled");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        TimeSeries active = new TimeSeries(samples, ByteLabels.fromStrings("status", "active"), 1000L, 1000L, 1000L, null);
        TimeSeries disabled = new TimeSeries(samples, ByteLabels.fromStrings("status", "disabled"), 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(active, disabled));

        assertEquals(1, result.size());
        assertEquals("active", result.get(0).getLabels().get("status"));
    }

    /**
     * Test that null input throws an exception.
     */
    public void testProcessWithNullInput() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.EQ, "tag", "value");
        assertNullInputThrowsException(stage, "tag_compare");
    }

    /**
     * Test processing with empty input.
     */
    public void testProcessWithEmptyInput() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.EQ, "tag", "value");
        List<TimeSeries> result = stage.process(new ArrayList<>());
        assertTrue(result.isEmpty());
    }

    /**
     * Test serialization and deserialization.
     */
    public void testSerializationCustom() throws IOException {
        TagCompareStage original = new TagCompareStage(TagComparisonOperator.LE, "version", "1.2.3");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        TagCompareStage deserialized = TagCompareStage.readFrom(in);

        assertEquals(original, deserialized);
    }

    /**
     * Test fromArgs factory method.
     */
    public void testFromArgs() {
        Map<String, Object> args = Map.of("operator", ">=", "tag_key", "version", "compare_value", "2.0.0");

        TagCompareStage stage = TagCompareStage.fromArgs(args);
        assertEquals(TagComparisonOperator.GE, stage.getOperator());
        assertEquals("version", stage.getTagKey());
        assertEquals("2.0.0", stage.getCompareValue());
    }

    /**
     * Test fromArgs with invalid arguments.
     */
    public void testFromArgsInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> TagCompareStage.fromArgs(null));
        assertThrows(IllegalArgumentException.class, () -> TagCompareStage.fromArgs(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> TagCompareStage.fromArgs(Map.of("operator", "==")));
        assertThrows(
            IllegalArgumentException.class,
            () -> TagCompareStage.fromArgs(Map.of("operator", "invalid", "tag_key", "tag", "compare_value", "value"))
        );
    }

    /**
     * Test toXContent serialization.
     */
    public void testToXContent() throws IOException {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.LT, "tag", "value");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stage.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue(json.contains("\"operator\":\"<\""));
        assertTrue(json.contains("\"tag_key\":\"tag\""));
        assertTrue(json.contains("\"compare_value\":\"value\""));
    }

    /**
     * Test stage name.
     */
    public void testGetName() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.EQ, "tag", "value");
        assertEquals("tag_compare", stage.getName());
    }

    /**
     * Test fromArgs factory integration.
     */
    public void testFromArgsFactoryIntegration() {
        Map<String, Object> args = Map.of("operator", "==", "tag_key", "env", "compare_value", "prod");

        TagCompareStage stage = TagCompareStage.fromArgs(args);
        assertNotNull(stage);

        assertEquals(TagComparisonOperator.EQ, stage.getOperator());
        assertEquals("env", stage.getTagKey());
        assertEquals("prod", stage.getCompareValue());
    }

    @Override
    protected TagCompareStage createTestInstance() {
        TagComparisonOperator operator = randomFrom(TagComparisonOperator.values());
        String tagKey = randomAlphaOfLengthBetween(1, 10);
        String compareValue = randomAlphaOfLengthBetween(1, 10);
        return new TagCompareStage(operator, tagKey, compareValue);
    }

    @Override
    protected Writeable.Reader<TagCompareStage> instanceReader() {
        return TagCompareStage::readFrom;
    }

    @Override
    protected TagCompareStage mutateInstance(TagCompareStage instance) {
        switch (randomIntBetween(0, 2)) {
            case 0:
                TagComparisonOperator[] operators = TagComparisonOperator.values();
                TagComparisonOperator newOperator = operators[(java.util.Arrays.asList(operators).indexOf(instance.getOperator()) + 1)
                    % operators.length];
                return new TagCompareStage(newOperator, instance.getTagKey(), instance.getCompareValue());
            case 1:
                return new TagCompareStage(instance.getOperator(), instance.getTagKey() + "modified", instance.getCompareValue());
            case 2:
                return new TagCompareStage(instance.getOperator(), instance.getTagKey(), instance.getCompareValue() + "modified");
            default:
                throw new AssertionError();
        }
    }

    // Need to add getters to TagCompareStage for the tests to work
    public void testGetters() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.GE, "tag", "value");
        assertEquals(TagComparisonOperator.GE, stage.getOperator());
        assertEquals("tag", stage.getTagKey());
        assertEquals("value", stage.getCompareValue());
    }
}
