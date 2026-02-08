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

public class AliasSubStageTests extends AbstractWireSerializingTestCase<AliasSubStage> {

    public void testRegexSubstitution() {
        AliasSubStage stage = new AliasSubStage("(.+)_(.+)", "$2-$1");
        TimeSeries ts = new TimeSeries(
            List.of(new FloatSample(10L, 1.0)),
            ByteLabels.fromStrings("name", "value"),
            10L,
            10L,
            10L,
            "metric_name"
        );

        List<TimeSeries> result = stage.process(List.of(ts));
        assertEquals("name-metric", result.get(0).getAlias());
    }

    public void testNoMatch() {
        AliasSubStage stage = new AliasSubStage("nomatch", "replacement");
        TimeSeries ts = new TimeSeries(
            List.of(new FloatSample(10L, 1.0)),
            ByteLabels.fromStrings("name", "value"),
            10L,
            10L,
            10L,
            "original"
        );

        List<TimeSeries> result = stage.process(List.of(ts));
        assertEquals("original", result.get(0).getAlias());
    }

    public void testNullAlias() {
        AliasSubStage stage = new AliasSubStage(".*", "test");
        TimeSeries ts = new TimeSeries(List.of(new FloatSample(10L, 1.0)), ByteLabels.fromStrings("name", "value"), 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(ts));
        assertEquals(ts, result.get(0)); // Unchanged
    }

    public void testFromArgs() {
        Map<String, Object> args = Map.of("search_pattern", "(.+)", "replacement", "new_$1");
        AliasSubStage stage = AliasSubStage.fromArgs(args);
        assertEquals("(.+)", stage.getSearchPattern());
        assertEquals("new_$1", stage.getReplacement());
    }

    public void testFromArgsValidation() {
        assertThrows(IllegalArgumentException.class, () -> AliasSubStage.fromArgs(null));
        assertThrows(IllegalArgumentException.class, () -> AliasSubStage.fromArgs(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> AliasSubStage.fromArgs(Map.of("search_pattern", "")));
    }

    public void testFactoryCreation() {
        Map<String, Object> args = Map.of("search_pattern", "test", "replacement", "replaced");
        AliasSubStage stage = (AliasSubStage) PipelineStageFactory.createWithArgs("aliasSub", args);
        assertEquals("test", stage.getSearchPattern());
        assertEquals("replaced", stage.getReplacement());
    }

    public void testGetName() {
        AliasSubStage stage = new AliasSubStage("pattern", "replacement");
        assertEquals("aliasSub", stage.getName());
    }

    public void testNullInputThrowsException() {
        AliasSubStage stage = new AliasSubStage("pattern", "replacement");
        assertThrows(NullPointerException.class, () -> stage.process(null));
    }

    public void testXContentSerialization() throws Exception {
        AliasSubStage stage = new AliasSubStage("(.+)", "$1_suffix");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stage.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue(json.contains("search_pattern"));
        assertTrue(json.contains("replacement"));
    }

    @Override
    protected AliasSubStage createTestInstance() {
        return new AliasSubStage(randomAlphaOfLength(5), randomAlphaOfLength(5));
    }

    @Override
    protected Writeable.Reader<AliasSubStage> instanceReader() {
        return AliasSubStage::readFrom;
    }

    @Override
    protected AliasSubStage mutateInstance(AliasSubStage instance) {
        return new AliasSubStage(instance.getSearchPattern() + "_mutated", instance.getReplacement() + "_mutated");
    }
}
