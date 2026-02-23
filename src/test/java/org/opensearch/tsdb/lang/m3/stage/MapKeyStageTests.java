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
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

public class MapKeyStageTests extends AbstractWireSerializingTestCase<MapKeyStage> {

    private static final List<Sample> SAMPLES = List.of(new FloatSample(1000L, 1.0));

    private static TimeSeries ts(String alias, String... keyValues) {
        return new TimeSeries(SAMPLES, ByteLabels.fromStrings(keyValues), 1000L, 1000L, 1000L, alias);
    }

    public void testBasicKeyMapping() {
        MapKeyStage stage = new MapKeyStage("city", "cityName");
        List<TimeSeries> result = stage.process(
            List.of(
                ts("a", "city", "atlanta", "name", "actions"),
                ts("b", "city", "boston", "name", "bikes"),
                ts("c", "dc", "dca1", "name", "cities")
            )
        );
        assertEquals(3, result.size());

        TimeSeries actions = result.stream().filter(t -> "a".equals(t.getAlias())).findFirst().orElseThrow();
        assertFalse(actions.getLabels().has("city"));
        assertEquals("atlanta", actions.getLabels().get("cityName"));

        // Series without the old key passes through unchanged
        TimeSeries cities = result.stream().filter(t -> "c".equals(t.getAlias())).findFirst().orElseThrow();
        assertFalse(cities.getLabels().has("cityName"));
        assertTrue(cities.getLabels().has("dc"));
    }

    public void testMissingKeyPassThrough() {
        MapKeyStage stage = new MapKeyStage("env", "environment");
        List<TimeSeries> result = stage.process(
            List.of(ts(null, "env", "prod", "service", "api"), ts(null, "service", "web", "region", "us-east"))
        );
        assertEquals(2, result.size());
        assertTrue(result.stream().anyMatch(t -> t.getLabels().has("environment")));
        assertTrue(result.stream().anyMatch(t -> t.getLabels().has("region")));
    }

    public void testSamplesAndMetadataPreserved() {
        List<Sample> samples = List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0));
        TimeSeries original = new TimeSeries(
            samples,
            ByteLabels.fromStrings("old", "value", "other", "tag"),
            1000L,
            3000L,
            1000L,
            "test-alias"
        );
        List<TimeSeries> result = new MapKeyStage("old", "new").process(List.of(original));

        TimeSeries t = result.get(0);
        assertEquals(2, t.getSamples().size());
        assertEquals(1000L, t.getMinTimestamp());
        assertEquals(3000L, t.getMaxTimestamp());
        assertEquals("test-alias", t.getAlias());
        assertEquals("value", t.getLabels().get("new"));
        assertFalse(t.getLabels().has("old"));
    }

    public void testNullLabelsPassThrough() {
        TimeSeries withNullLabels = new TimeSeries(SAMPLES, null, 1000L, 1000L, 1000L, null);
        List<TimeSeries> result = new MapKeyStage("old", "new").process(List.of(withNullLabels));
        assertEquals(1, result.size());
        assertEquals(withNullLabels, result.get(0));
    }

    public void testKeyOverwriting() {
        List<TimeSeries> result = new MapKeyStage("old", "existing").process(
            List.of(ts(null, "old", "new_value", "existing", "old_value"))
        );
        assertEquals("new_value", result.get(0).getLabels().get("existing"));
        assertFalse(result.get(0).getLabels().has("old"));
    }

    public void testNullAndEmptyInput() {
        MapKeyStage stage = new MapKeyStage("old", "new");
        assertNullInputThrowsException(stage, "map_key");
        assertTrue(stage.process(List.of()).isEmpty());
    }

    public void testFromArgs() {
        MapKeyStage stage = MapKeyStage.fromArgs(Map.of("old_key", "old", "new_key", "new"));
        assertEquals("old", stage.getOldKey());
        assertEquals("new", stage.getNewKey());
    }

    public void testFromArgsInvalid() {
        assertThrows(IllegalArgumentException.class, () -> MapKeyStage.fromArgs(null));
        assertThrows(IllegalArgumentException.class, () -> MapKeyStage.fromArgs(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> MapKeyStage.fromArgs(Map.of("old_key", "old")));
        assertThrows(IllegalArgumentException.class, () -> MapKeyStage.fromArgs(Map.of("new_key", "new")));
        assertThrows(IllegalArgumentException.class, () -> MapKeyStage.fromArgs(Map.of("old_key", "", "new_key", "new")));
    }

    public void testToXContent() throws IOException {
        MapKeyStage stage = new MapKeyStage("oldKey", "newKey");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stage.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();
        String json = builder.toString();
        assertTrue(json.contains("\"old_key\":\"oldKey\""));
        assertTrue(json.contains("\"new_key\":\"newKey\""));
    }

    public void testGetName() {
        assertEquals("map_key", new MapKeyStage("a", "b").getName());
    }

    public void testReadFromThroughFactory() throws IOException {
        MapKeyStage original = new MapKeyStage("env", "environment");
        BytesStreamOutput output = new BytesStreamOutput();
        output.writeString(original.getName());
        original.writeTo(output);
        PipelineStage deserialized = PipelineStageFactory.readFrom(output.bytes().streamInput());
        assertTrue(deserialized instanceof MapKeyStage);
        assertEquals(original, deserialized);
    }

    @Override
    protected MapKeyStage createTestInstance() {
        return new MapKeyStage(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10));
    }

    @Override
    protected Writeable.Reader<MapKeyStage> instanceReader() {
        return MapKeyStage::readFrom;
    }

    @Override
    protected MapKeyStage mutateInstance(MapKeyStage instance) {
        if (randomBoolean()) {
            return new MapKeyStage(instance.getOldKey() + "x", instance.getNewKey());
        } else {
            return new MapKeyStage(instance.getOldKey(), instance.getNewKey() + "x");
        }
    }
}
