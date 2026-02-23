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
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.lang.m3.common.TagComparisonOperator;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

public class TagCompareStageTests extends AbstractWireSerializingTestCase<TagCompareStage> {

    private static final List<Sample> SAMPLES = List.of(new FloatSample(1000L, 10.0));

    private static TimeSeries ts(String... keyValues) {
        return new TimeSeries(SAMPLES, ByteLabels.fromStrings(keyValues), 1000L, 1000L, 1000L, null);
    }

    public void testLexicographicLessThan() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.LT, "city", "denver");
        List<TimeSeries> result = stage.process(
            List.of(ts("city", "atlanta"), ts("city", "boston"), ts("city", "chicago"), ts("city", "denver"))
        );
        assertEquals(3, result.size());
        assertFalse(result.stream().anyMatch(t -> "denver".equals(t.getLabels().get("city"))));
    }

    public void testSemanticVersionLessEqual() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.LE, "version", "30.500.100");
        List<TimeSeries> result = stage.process(
            List.of(
                ts("version", "30.500.1"),
                ts("version", "29.5"),
                ts("version", "30.5"),
                ts("version", "30.600"),
                ts("version", "30.500.100")
            )
        );
        assertEquals(4, result.size());
        assertFalse(result.stream().anyMatch(t -> "30.600".equals(t.getLabels().get("version"))));
    }

    public void testMissingTagExcluded() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.EQ, "env", "prod");
        List<TimeSeries> result = stage.process(List.of(ts("env", "prod"), ts("service", "web")));
        assertEquals(1, result.size());
        assertEquals("prod", result.get(0).getLabels().get("env"));
    }

    public void testInvalidVersionsExcluded() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.GT, "version", "1.0.0");
        List<TimeSeries> result = stage.process(List.of(ts("version", "2.0.0"), ts("version", "invalid")));
        assertEquals(1, result.size());
        assertEquals("2.0.0", result.get(0).getLabels().get("version"));
    }

    public void testEqualityAndNotEquality() {
        List<TimeSeries> input = List.of(ts("status", "active"), ts("status", "inactive"));
        assertEquals(1, new TagCompareStage(TagComparisonOperator.EQ, "status", "active").process(input).size());
        assertEquals(1, new TagCompareStage(TagComparisonOperator.NE, "status", "active").process(input).size());
    }

    public void testNullAndEmptyInput() {
        TagCompareStage stage = new TagCompareStage(TagComparisonOperator.EQ, "tag", "value");
        assertNullInputThrowsException(stage, "tag_compare");
        assertTrue(stage.process(List.of()).isEmpty());
    }

    public void testFromArgs() {
        TagCompareStage stage = TagCompareStage.fromArgs(Map.of("operator", ">=", "tag_key", "version", "compare_value", "2.0.0"));
        assertEquals(TagComparisonOperator.GE, stage.getOperator());
        assertEquals("version", stage.getTagKey());
        assertEquals("2.0.0", stage.getCompareValue());
    }

    public void testFromArgsInvalid() {
        assertThrows(IllegalArgumentException.class, () -> TagCompareStage.fromArgs(null));
        assertThrows(IllegalArgumentException.class, () -> TagCompareStage.fromArgs(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> TagCompareStage.fromArgs(Map.of("operator", "==")));
        assertThrows(
            IllegalArgumentException.class,
            () -> TagCompareStage.fromArgs(Map.of("operator", "invalid", "tag_key", "t", "compare_value", "v"))
        );
    }

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

    public void testGetName() {
        assertEquals("tag_compare", new TagCompareStage(TagComparisonOperator.EQ, "t", "v").getName());
    }

    public void testReadFromThroughFactory() throws IOException {
        TagCompareStage original = new TagCompareStage(TagComparisonOperator.LE, "version", "1.0");
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeString(original.getName());
        original.writeTo(output);
        org.opensearch.tsdb.query.stage.PipelineStage deserialized = org.opensearch.tsdb.query.stage.PipelineStageFactory.readFrom(
            output.bytes().streamInput()
        );
        assertTrue(deserialized instanceof TagCompareStage);
        assertEquals(original, deserialized);
    }

    @Override
    protected TagCompareStage createTestInstance() {
        return new TagCompareStage(
            randomFrom(TagComparisonOperator.values()),
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
    }

    @Override
    protected Writeable.Reader<TagCompareStage> instanceReader() {
        return TagCompareStage::readFrom;
    }

    @Override
    protected TagCompareStage mutateInstance(TagCompareStage instance) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> {
                TagComparisonOperator[] ops = TagComparisonOperator.values();
                yield new TagCompareStage(
                    ops[(Arrays.asList(ops).indexOf(instance.getOperator()) + 1) % ops.length],
                    instance.getTagKey(),
                    instance.getCompareValue()
                );
            }
            case 1 -> new TagCompareStage(instance.getOperator(), instance.getTagKey() + "x", instance.getCompareValue());
            default -> new TagCompareStage(instance.getOperator(), instance.getTagKey(), instance.getCompareValue() + "x");
        };
    }
}
