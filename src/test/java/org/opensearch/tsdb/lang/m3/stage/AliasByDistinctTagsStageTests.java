/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

public class AliasByDistinctTagsStageTests extends AbstractWireSerializingTestCase<AliasByDistinctTagsStage> {

    public void testAutoDetectVaryingTags() {
        AliasByDistinctTagsStage stage = new AliasByDistinctTagsStage(false, null);

        List<TimeSeries> input = List.of(
            new TimeSeries(List.of(new FloatSample(10L, 1.0)), ByteLabels.fromStrings("env", "prod", "dc", "us1"), 10L, 10L, 10L, null),
            new TimeSeries(List.of(new FloatSample(10L, 2.0)), ByteLabels.fromStrings("env", "prod", "dc", "us2"), 10L, 10L, 10L, null)
        );

        List<TimeSeries> result = stage.process(input);
        assertEquals("us1", result.get(0).getAlias());
        assertEquals("us2", result.get(1).getAlias());
    }

    public void testIncludeKeys() {
        AliasByDistinctTagsStage stage = new AliasByDistinctTagsStage(true, null);

        List<TimeSeries> input = List.of(
            new TimeSeries(List.of(new FloatSample(10L, 1.0)), ByteLabels.fromStrings("env", "prod", "dc", "us1"), 10L, 10L, 10L, null),
            new TimeSeries(List.of(new FloatSample(10L, 2.0)), ByteLabels.fromStrings("env", "prod", "dc", "us2"), 10L, 10L, 10L, null)
        );

        List<TimeSeries> result = stage.process(input);
        assertEquals("dc:us1", result.get(0).getAlias());
        assertEquals("dc:us2", result.get(1).getAlias());
    }

    public void testSpecificTags() {
        AliasByDistinctTagsStage stage = new AliasByDistinctTagsStage(false, List.of("dc"));

        List<TimeSeries> input = List.of(
            new TimeSeries(List.of(new FloatSample(10L, 1.0)), ByteLabels.fromStrings("env", "staging", "dc", "us1"), 10L, 10L, 10L, null),
            new TimeSeries(List.of(new FloatSample(10L, 2.0)), ByteLabels.fromStrings("env", "prod", "dc", "us2"), 10L, 10L, 10L, null)
        );

        // Varying tags (sorted): dc, env. Then append specified: dc.
        // So alias = "<dc> <env> <dc>"
        List<TimeSeries> result = stage.process(input);
        assertEquals("us1 staging us1", result.get(0).getAlias());
        assertEquals("us2 prod us2", result.get(1).getAlias());
    }

    public void testSingleSeries() {
        AliasByDistinctTagsStage stage = new AliasByDistinctTagsStage(false, null);
        List<TimeSeries> input = List.of(
            new TimeSeries(List.of(new FloatSample(10L, 1.0)), ByteLabels.fromStrings("env", "prod"), 10L, 10L, 10L, null)
        );

        List<TimeSeries> result = stage.process(input);
        assertNull(result.get(0).getAlias());
    }

    public void testCoordinatorOnly() {
        AliasByDistinctTagsStage stage = new AliasByDistinctTagsStage(false, null);
        assertTrue(stage.isCoordinatorOnly());
    }

    public void testFromArgs() {
        AliasByDistinctTagsStage stage = AliasByDistinctTagsStage.fromArgs(Map.of("include_keys", true, "tag_names", List.of("env", "dc")));
        assertTrue(stage.isIncludeKeys());
        assertEquals(List.of("env", "dc"), stage.getTagNames());

        AliasByDistinctTagsStage defaultStage = AliasByDistinctTagsStage.fromArgs(null);
        assertFalse(defaultStage.isIncludeKeys());
        assertNull(defaultStage.getTagNames());
    }

    public void testFromArgsWithQuotedStrings() {
        // Test with quoted "true"
        AliasByDistinctTagsStage trueStage = AliasByDistinctTagsStage.fromArgs(Map.of("include_keys", "true"));
        assertTrue(trueStage.isIncludeKeys());

        // Test with quoted "false"
        AliasByDistinctTagsStage falseStage = AliasByDistinctTagsStage.fromArgs(Map.of("include_keys", "false"));
        assertFalse(falseStage.isIncludeKeys());

        // Test case insensitivity
        AliasByDistinctTagsStage upperCaseStage = AliasByDistinctTagsStage.fromArgs(Map.of("include_keys", "TRUE"));
        assertTrue(upperCaseStage.isIncludeKeys());

        // Test invalid quoted string
        assertThrows(IllegalArgumentException.class, () -> AliasByDistinctTagsStage.fromArgs(Map.of("include_keys", "invalid")));
    }

    public void testFactoryCreation() {
        Map<String, Object> args = Map.of("include_keys", true);
        AliasByDistinctTagsStage stage = (AliasByDistinctTagsStage) PipelineStageFactory.createWithArgs("aliasByDistinctTags", args);
        assertTrue(stage.isIncludeKeys());
    }

    public void testGetName() {
        AliasByDistinctTagsStage stage = new AliasByDistinctTagsStage(false, null);
        assertEquals("aliasByDistinctTags", stage.getName());
    }

    public void testNullInputThrowsException() {
        AliasByDistinctTagsStage stage = new AliasByDistinctTagsStage(false, null);
        assertThrows(NullPointerException.class, () -> stage.process(null));
    }

    public void testXContentSerialization() throws Exception {
        AliasByDistinctTagsStage stage = new AliasByDistinctTagsStage(true, List.of("env"));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stage.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue(json.contains("include_keys"));
        assertTrue(json.contains("tag_names"));
    }

    @Override
    protected AliasByDistinctTagsStage createTestInstance() {
        return new AliasByDistinctTagsStage(randomBoolean(), randomBoolean() ? List.of(randomAlphaOfLength(5)) : null);
    }

    @Override
    protected Writeable.Reader<AliasByDistinctTagsStage> instanceReader() {
        return AliasByDistinctTagsStage::readFrom;
    }

    @Override
    protected AliasByDistinctTagsStage mutateInstance(AliasByDistinctTagsStage instance) {
        return new AliasByDistinctTagsStage(!instance.isIncludeKeys(), instance.getTagNames());
    }
}
