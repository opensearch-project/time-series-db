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
import org.opensearch.tsdb.lang.m3.common.WhereOperator;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

public class WhereStageTests extends AbstractWireSerializingTestCase<WhereStage> {

    private static final List<Sample> SAMPLES = List.of(new FloatSample(1000L, 1.0));

    private static TimeSeries ts(String... keyValues) {
        return new TimeSeries(SAMPLES, ByteLabels.fromStrings(keyValues), 1000L, 1000L, 1000L, null);
    }

    public void testEqualityFilter() {
        WhereStage stage = new WhereStage(WhereOperator.EQ, "region", "uber_region");
        List<TimeSeries> result = stage.process(
            List.of(ts("name", "pass", "region", "dca", "uber_region", "dca"), ts("name", "fail", "region", "dca", "uber_region", "lax"))
        );
        assertEquals(1, result.size());
        assertEquals("pass", result.get(0).getLabels().get("name"));
    }

    public void testNotEqualityFilter() {
        WhereStage stage = new WhereStage(WhereOperator.NEQ, "city", "action");
        List<TimeSeries> result = stage.process(
            List.of(ts("action", "delete", "city", "atlanta"), ts("action", "atlanta", "city", "atlanta"))
        );
        assertEquals(1, result.size());
        assertEquals("delete", result.get(0).getLabels().get("action"));
    }

    public void testMissingTagsExcluded() {
        WhereStage stage = new WhereStage(WhereOperator.EQ, "tag1", "tag2");
        List<TimeSeries> result = stage.process(List.of(ts("tag1", "v", "tag2", "v"), ts("tag2", "v"), ts("tag1", "v"), ts("other", "v")));
        assertEquals(1, result.size());
    }

    public void testMultipleSeriesMixedMatches() {
        WhereStage stage = new WhereStage(WhereOperator.EQ, "env", "region");
        List<TimeSeries> result = stage.process(
            List.of(
                ts("env", "prod", "region", "prod"),
                ts("env", "dev", "region", "dev"),
                ts("env", "prod", "region", "dev"),
                ts("env", "test", "region", "prod")
            )
        );
        assertEquals(2, result.size());
    }

    public void testNullAndEmptyInput() {
        WhereStage stage = new WhereStage(WhereOperator.EQ, "tag1", "tag2");
        assertNullInputThrowsException(stage, "where");
        assertTrue(stage.process(List.of()).isEmpty());
    }

    public void testFromArgs() {
        WhereStage stage = WhereStage.fromArgs(Map.of("operator", "neq", "tag_key1", "env", "tag_key2", "region"));
        assertEquals(WhereOperator.NEQ, stage.getOperator());
        assertEquals("env", stage.getTagKey1());
        assertEquals("region", stage.getTagKey2());
    }

    public void testFromArgsInvalid() {
        assertThrows(IllegalArgumentException.class, () -> WhereStage.fromArgs(null));
        assertThrows(IllegalArgumentException.class, () -> WhereStage.fromArgs(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> WhereStage.fromArgs(Map.of("operator", "eq")));
        assertThrows(
            IllegalArgumentException.class,
            () -> WhereStage.fromArgs(Map.of("operator", "invalid", "tag_key1", "t1", "tag_key2", "t2"))
        );
    }

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

    public void testGetName() {
        assertEquals("where", new WhereStage(WhereOperator.EQ, "t1", "t2").getName());
    }

    public void testReadFromThroughFactory() throws IOException {
        WhereStage original = new WhereStage(WhereOperator.EQ, "env", "region");
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeString(original.getName());
        original.writeTo(output);
        PipelineStage deserialized = PipelineStageFactory.readFrom(output.bytes().streamInput());
        assertTrue(deserialized instanceof WhereStage);
        assertEquals(original, deserialized);
    }

    @Override
    protected WhereStage createTestInstance() {
        return new WhereStage(randomFrom(WhereOperator.values()), randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected Writeable.Reader<WhereStage> instanceReader() {
        return WhereStage::readFrom;
    }

    @Override
    protected WhereStage mutateInstance(WhereStage instance) {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> {
                WhereOperator[] ops = WhereOperator.values();
                yield new WhereStage(
                    ops[(Arrays.asList(ops).indexOf(instance.getOperator()) + 1) % ops.length],
                    instance.getTagKey1(),
                    instance.getTagKey2()
                );
            }
            case 1 -> new WhereStage(instance.getOperator(), instance.getTagKey1() + "x", instance.getTagKey2());
            default -> new WhereStage(instance.getOperator(), instance.getTagKey1(), instance.getTagKey2() + "x");
        };
    }
}
