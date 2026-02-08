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

public class MapKeyStageTests extends AbstractWireSerializingTestCase<MapKeyStage> {

    /**
     * Test basic key mapping from documentation example.
     */
    public void testBasicKeyMapping() {
        MapKeyStage stage = new MapKeyStage("city", "cityName");

        List<Sample> samples1 = List.of(new FloatSample(1000L, 1.0), new FloatSample(4000L, 4.0), new FloatSample(2000L, 2.0));
        List<Sample> samples2 = List.of(new FloatSample(2000L, 2.0), new FloatSample(4000L, 4.0), new FloatSample(5000L, 5.0));
        List<Sample> samples3 = List.of(new FloatSample(5000L, 5.0), new FloatSample(8000L, 8.0), new FloatSample(15000L, 15.0));

        TimeSeries actions = new TimeSeries(
            samples1,
            ByteLabels.fromStrings("city", "atlanta", "name", "actions"),
            1000L,
            2000L,
            1000L,
            "actions"
        );
        TimeSeries bikes = new TimeSeries(
            samples2,
            ByteLabels.fromStrings("city", "boston", "name", "bikes"),
            2000L,
            5000L,
            1000L,
            "bikes"
        );
        TimeSeries cities = new TimeSeries(
            samples3,
            ByteLabels.fromStrings("dc", "dca1", "name", "cities"),
            5000L,
            15000L,
            1000L,
            "cities"
        );

        List<TimeSeries> result = stage.process(List.of(actions, bikes, cities));

        assertEquals(3, result.size());

        // Check that city was renamed to cityName for actions and bikes
        TimeSeries actionsResult = result.stream().filter(ts -> "actions".equals(ts.getAlias())).findFirst().orElse(null);
        assertNotNull(actionsResult);
        assertFalse(actionsResult.getLabels().has("city"));
        assertTrue(actionsResult.getLabels().has("cityName"));
        assertEquals("atlanta", actionsResult.getLabels().get("cityName"));

        TimeSeries bikesResult = result.stream().filter(ts -> "bikes".equals(ts.getAlias())).findFirst().orElse(null);
        assertNotNull(bikesResult);
        assertFalse(bikesResult.getLabels().has("city"));
        assertTrue(bikesResult.getLabels().has("cityName"));
        assertEquals("boston", bikesResult.getLabels().get("cityName"));

        // Check that cities series is unchanged (no city tag)
        TimeSeries citiesResult = result.stream().filter(ts -> "cities".equals(ts.getAlias())).findFirst().orElse(null);
        assertNotNull(citiesResult);
        assertTrue(citiesResult.getLabels().has("dc"));
        assertEquals("dca1", citiesResult.getLabels().get("dc"));
        assertFalse(citiesResult.getLabels().has("city"));
        assertFalse(citiesResult.getLabels().has("cityName"));
    }

    /**
     * Test that series without the old key pass through unchanged.
     */
    public void testMissingKeyPassThrough() {
        MapKeyStage stage = new MapKeyStage("env", "environment");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        TimeSeries withKey = new TimeSeries(samples, ByteLabels.fromStrings("env", "prod", "service", "api"), 1000L, 1000L, 1000L, null);
        TimeSeries withoutKey = new TimeSeries(
            samples,
            ByteLabels.fromStrings("service", "web", "region", "us-east"),
            1000L,
            1000L,
            1000L,
            null
        );

        List<TimeSeries> result = stage.process(List.of(withKey, withoutKey));

        assertEquals(2, result.size());

        // Series with key should be transformed
        TimeSeries transformedSeries = result.stream().filter(ts -> ts.getLabels().has("environment")).findFirst().orElse(null);
        assertNotNull(transformedSeries);
        assertEquals("prod", transformedSeries.getLabels().get("environment"));
        assertFalse(transformedSeries.getLabels().has("env"));

        // Series without key should be unchanged
        TimeSeries unchangedSeries = result.stream().filter(ts -> ts.getLabels().has("region")).findFirst().orElse(null);
        assertNotNull(unchangedSeries);
        assertEquals("us-east", unchangedSeries.getLabels().get("region"));
        assertEquals("web", unchangedSeries.getLabels().get("service"));
    }

    /**
     * Test key renaming preserves all samples and metadata.
     */
    public void testSamplesAndMetadataPreserved() {
        MapKeyStage stage = new MapKeyStage("old", "new");

        List<Sample> samples = List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0));

        TimeSeries original = new TimeSeries(
            samples,
            ByteLabels.fromStrings("old", "value", "other", "tag"),
            1000L,
            3000L,
            1000L,
            "test-alias"
        );

        List<TimeSeries> result = stage.process(List.of(original));

        assertEquals(1, result.size());
        TimeSeries transformed = result.get(0);

        // Check samples are preserved
        assertEquals(3, transformed.getSamples().size());
        assertEquals(1.0, transformed.getSamples().getValue(0), 0.001);
        assertEquals(2.0, transformed.getSamples().getValue(1), 0.001);
        assertEquals(3.0, transformed.getSamples().getValue(2), 0.001);

        // Check timestamps and metadata are preserved
        assertEquals(1000L, transformed.getMinTimestamp());
        assertEquals(3000L, transformed.getMaxTimestamp());
        assertEquals(1000L, transformed.getStep());
        assertEquals("test-alias", transformed.getAlias());

        // Check label transformation
        assertTrue(transformed.getLabels().has("new"));
        assertEquals("value", transformed.getLabels().get("new"));
        assertFalse(transformed.getLabels().has("old"));
        assertTrue(transformed.getLabels().has("other"));
        assertEquals("tag", transformed.getLabels().get("other"));
    }

    /**
     * Test renaming with null labels.
     */
    public void testNullLabelsHandling() {
        MapKeyStage stage = new MapKeyStage("old", "new");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        TimeSeries withNullLabels = new TimeSeries(samples, null, 1000L, 1000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(withNullLabels));

        assertEquals(1, result.size());
        // Series should pass through unchanged
        assertEquals(withNullLabels, result.get(0));
    }

    /**
     * Test key overwriting scenario.
     */
    public void testKeyOverwriting() {
        MapKeyStage stage = new MapKeyStage("old", "existing");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        TimeSeries series = new TimeSeries(
            samples,
            ByteLabels.fromStrings("old", "new_value", "existing", "old_value"),
            1000L,
            1000L,
            1000L,
            null
        );

        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        TimeSeries transformed = result.get(0);

        // The new key should have the value from the old key
        assertTrue(transformed.getLabels().has("existing"));
        assertEquals("new_value", transformed.getLabels().get("existing"));
        assertFalse(transformed.getLabels().has("old"));
    }

    /**
     * Test that null input throws an exception.
     */
    public void testProcessWithNullInput() {
        MapKeyStage stage = new MapKeyStage("old", "new");
        assertNullInputThrowsException(stage, "map_key");
    }

    /**
     * Test processing with empty input.
     */
    public void testProcessWithEmptyInput() {
        MapKeyStage stage = new MapKeyStage("old", "new");
        List<TimeSeries> result = stage.process(new ArrayList<>());
        assertTrue(result.isEmpty());
    }

    /**
     * Test serialization and deserialization.
     */
    public void testSerializationCustom() throws IOException {
        MapKeyStage original = new MapKeyStage("oldKey", "newKey");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        MapKeyStage deserialized = MapKeyStage.readFrom(in);

        assertEquals(original, deserialized);
    }

    /**
     * Test fromArgs factory method.
     */
    public void testFromArgs() {
        Map<String, Object> args = Map.of("old_key", "old", "new_key", "new");

        MapKeyStage stage = MapKeyStage.fromArgs(args);
        assertEquals("old", stage.getOldKey());
        assertEquals("new", stage.getNewKey());
    }

    /**
     * Test fromArgs with invalid arguments.
     */
    public void testFromArgsInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> MapKeyStage.fromArgs(null));
        assertThrows(IllegalArgumentException.class, () -> MapKeyStage.fromArgs(Map.of()));
        assertThrows(IllegalArgumentException.class, () -> MapKeyStage.fromArgs(Map.of("old_key", "old")));
        assertThrows(IllegalArgumentException.class, () -> MapKeyStage.fromArgs(Map.of("new_key", "new")));
        assertThrows(IllegalArgumentException.class, () -> MapKeyStage.fromArgs(Map.of("old_key", "", "new_key", "new")));
        assertThrows(IllegalArgumentException.class, () -> MapKeyStage.fromArgs(Map.of("old_key", "old", "new_key", "")));
    }

    /**
     * Test toXContent serialization.
     */
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

    /**
     * Test stage name.
     */
    public void testGetName() {
        MapKeyStage stage = new MapKeyStage("old", "new");
        assertEquals("map_key", stage.getName());
    }

    /**
     * Test readFrom through PipelineStageFactory.
     */
    public void testReadFromThroughFactory() throws IOException {
        MapKeyStage original = new MapKeyStage("env", "environment");

        BytesStreamOutput output = new BytesStreamOutput();
        output.writeString(original.getName());
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        PipelineStage deserialized = PipelineStageFactory.readFrom(input);

        assertTrue(deserialized instanceof MapKeyStage);
        assertEquals(original, deserialized);
    }

    @Override
    protected MapKeyStage createTestInstance() {
        String oldKey = randomAlphaOfLengthBetween(1, 10);
        String newKey = randomAlphaOfLengthBetween(1, 10);
        return new MapKeyStage(oldKey, newKey);
    }

    @Override
    protected Writeable.Reader<MapKeyStage> instanceReader() {
        return MapKeyStage::readFrom;
    }

    @Override
    protected MapKeyStage mutateInstance(MapKeyStage instance) {
        if (randomBoolean()) {
            return new MapKeyStage(instance.getOldKey() + "modified", instance.getNewKey());
        } else {
            return new MapKeyStage(instance.getOldKey(), instance.getNewKey() + "modified");
        }
    }
}
