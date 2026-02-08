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
import org.opensearch.tsdb.lang.m3.common.WhereOperator;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

public class WhereStageTests extends AbstractWireSerializingTestCase<WhereStage> {

    /**
     * Test basic equality example from documentation.
     */
    public void testBasicEqualityExample() {
        WhereStage stage = new WhereStage(WhereOperator.EQ, "region", "uber_region");

        List<Sample> samples = List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0));

        TimeSeries pass = new TimeSeries(
            samples,
            ByteLabels.fromStrings("name", "pass", "region", "dca", "uber_region", "dca"),
            1000L,
            3000L,
            1000L,
            "pass"
        );
        TimeSeries fail = new TimeSeries(
            samples,
            ByteLabels.fromStrings("name", "fail", "region", "dca", "uber_region", "lax"),
            1000L,
            3000L,
            1000L,
            "fail"
        );

        List<TimeSeries> result = stage.process(List.of(pass, fail));

        assertEquals(1, result.size());
        assertEquals("pass", result.get(0).getLabels().get("name"));
        assertEquals("dca", result.get(0).getLabels().get("region"));
        assertEquals("dca", result.get(0).getLabels().get("uber_region"));
    }

    /**
     * Test basic negation example from documentation.
     */
    public void testBasicNegationExample() {
        WhereStage stage = new WhereStage(WhereOperator.NEQ, "city", "action");

        List<Sample> samples = List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0));

        TimeSeries pass = new TimeSeries(
            samples,
            ByteLabels.fromStrings("name", "pass", "action", "delete", "city", "atlanta"),
            1000L,
            3000L,
            1000L,
            "pass"
        );
        TimeSeries fail = new TimeSeries(
            samples,
            ByteLabels.fromStrings("name", "fail", "action", "atlanta", "city", "atlanta"),
            1000L,
            3000L,
            1000L,
            "fail"
        );

        List<TimeSeries> result = stage.process(List.of(pass, fail));

        assertEquals(1, result.size());
        assertEquals("pass", result.get(0).getLabels().get("name"));
        assertEquals("delete", result.get(0).getLabels().get("action"));
        assertEquals("atlanta", result.get(0).getLabels().get("city"));
    }

    /**
     * Test that series missing either tag are excluded.
     */
    public void testMissingTagsExcluded() {
        WhereStage stage = new WhereStage(WhereOperator.EQ, "tag1", "tag2");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        TimeSeries bothTags = new TimeSeries(samples, ByteLabels.fromStrings("tag1", "value", "tag2", "value"), 1000L, 1000L, 1000L, null);
        TimeSeries missingTag1 = new TimeSeries(samples, ByteLabels.fromStrings("tag2", "value"), 1000L, 1000L, 1000L, null);
        TimeSeries missingTag2 = new TimeSeries(samples, ByteLabels.fromStrings("tag1", "value"), 1000L, 1000L, 1000L, null);
        TimeSeries noTags = new TimeSeries(samples, ByteLabels.fromStrings("other", "value"), 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(bothTags, missingTag1, missingTag2, noTags));

        assertEquals(1, result.size());
        assertEquals("value", result.get(0).getLabels().get("tag1"));
        assertEquals("value", result.get(0).getLabels().get("tag2"));
    }

    /**
     * Test multiple series with mixed matches.
     */
    public void testMultipleSeriesMixedMatches() {
        WhereStage stage = new WhereStage(WhereOperator.EQ, "env", "region");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        TimeSeries match1 = new TimeSeries(samples, ByteLabels.fromStrings("env", "prod", "region", "prod"), 1000L, 1000L, 1000L, null);
        TimeSeries match2 = new TimeSeries(samples, ByteLabels.fromStrings("env", "dev", "region", "dev"), 1000L, 1000L, 1000L, null);
        TimeSeries noMatch1 = new TimeSeries(samples, ByteLabels.fromStrings("env", "prod", "region", "dev"), 1000L, 1000L, 1000L, null);
        TimeSeries noMatch2 = new TimeSeries(samples, ByteLabels.fromStrings("env", "test", "region", "prod"), 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(match1, match2, noMatch1, noMatch2));

        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(ts -> "prod".equals(ts.getLabels().get("env"))));
        assertTrue(result.stream().anyMatch(ts -> "dev".equals(ts.getLabels().get("env"))));
    }

    /**
     * Test not-equality operator with various values.
     */
    public void testNotEqualityOperator() {
        WhereStage stage = new WhereStage(WhereOperator.NEQ, "tag1", "tag2");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        TimeSeries equal = new TimeSeries(samples, ByteLabels.fromStrings("tag1", "same", "tag2", "same"), 1000L, 1000L, 1000L, null);
        TimeSeries notEqual = new TimeSeries(
            samples,
            ByteLabels.fromStrings("tag1", "different1", "tag2", "different2"),
            1000L,
            1000L,
            1000L,
            null
        );

        List<TimeSeries> result = stage.process(List.of(equal, notEqual));

        assertEquals(1, result.size());
        assertEquals("different1", result.get(0).getLabels().get("tag1"));
        assertEquals("different2", result.get(0).getLabels().get("tag2"));
    }

    /**
     * Test that null input throws an exception.
     */
    public void testProcessWithNullInput() {
        WhereStage stage = new WhereStage(WhereOperator.EQ, "tag1", "tag2");
        assertNullInputThrowsException(stage, "where");
    }

    /**
     * Test processing with empty input.
     */
    public void testProcessWithEmptyInput() {
        WhereStage stage = new WhereStage(WhereOperator.EQ, "tag1", "tag2");
        List<TimeSeries> result = stage.process(new ArrayList<>());
        assertTrue(result.isEmpty());
    }

    /**
     * Test serialization and deserialization.
     */
    public void testSerializationCustom() throws IOException {
        WhereStage original = new WhereStage(WhereOperator.NEQ, "tag1", "tag2");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        WhereStage deserialized = WhereStage.readFrom(in);

        assertEquals(original, deserialized);
    }

    /**
     * Test fromArgs factory method.
     */
    public void testFromArgs() {
        Map<String, Object> args = Map.of("operator", "neq", "tag_key1", "env", "tag_key2", "region");

        WhereStage stage = WhereStage.fromArgs(args);
        assertEquals(WhereOperator.NEQ, stage.getOperator());
        assertEquals("env", stage.getTagKey1());
        assertEquals("region", stage.getTagKey2());
    }

    /**
     * Test fromArgs with invalid arguments.
     */
    public void testFromArgsInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> WhereStage.fromArgs(null));
        assertThrows(IllegalArgumentException.class, () -> WhereStage.fromArgs(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> WhereStage.fromArgs(Map.of("operator", "eq")));
        assertThrows(
            IllegalArgumentException.class,
            () -> WhereStage.fromArgs(Map.of("operator", "invalid", "tag_key1", "tag1", "tag_key2", "tag2"))
        );
    }

    /**
     * Test toXContent serialization.
     */
    public void testToXContent() throws IOException {
        WhereStage stage = new WhereStage(WhereOperator.EQ, "tag1", "tag2");

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stage.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue(json.contains("\"operator\":\"eq\""));
        assertTrue(json.contains("\"tag_key1\":\"tag1\""));
        assertTrue(json.contains("\"tag_key2\":\"tag2\""));
    }

    /**
     * Test stage name.
     */
    public void testGetName() {
        WhereStage stage = new WhereStage(WhereOperator.EQ, "tag1", "tag2");
        assertEquals("where", stage.getName());
    }

    /**
     * Test readFrom through PipelineStageFactory.
     */
    public void testReadFromThroughFactory() throws IOException {
        WhereStage original = new WhereStage(WhereOperator.EQ, "env", "region");

        BytesStreamOutput output = new BytesStreamOutput();
        output.writeString(original.getName());
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        PipelineStage deserialized = PipelineStageFactory.readFrom(input);

        assertTrue(deserialized instanceof WhereStage);
        assertEquals(original, deserialized);
    }

    @Override
    protected WhereStage createTestInstance() {
        WhereOperator operator = randomFrom(WhereOperator.values());
        String tagKey1 = randomAlphaOfLengthBetween(1, 10);
        String tagKey2 = randomAlphaOfLengthBetween(1, 10);
        return new WhereStage(operator, tagKey1, tagKey2);
    }

    @Override
    protected Writeable.Reader<WhereStage> instanceReader() {
        return WhereStage::readFrom;
    }

    @Override
    protected WhereStage mutateInstance(WhereStage instance) {
        switch (randomIntBetween(0, 2)) {
            case 0:
                WhereOperator[] operators = WhereOperator.values();
                WhereOperator newOperator = operators[(java.util.Arrays.asList(operators).indexOf(instance.getOperator()) + 1)
                    % operators.length];
                return new WhereStage(newOperator, instance.getTagKey1(), instance.getTagKey2());
            case 1:
                return new WhereStage(instance.getOperator(), instance.getTagKey1() + "modified", instance.getTagKey2());
            case 2:
                return new WhereStage(instance.getOperator(), instance.getTagKey1(), instance.getTagKey2() + "modified");
            default:
                throw new AssertionError();
        }
    }
}
