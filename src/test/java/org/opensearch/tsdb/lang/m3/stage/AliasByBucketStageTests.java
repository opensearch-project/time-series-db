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

public class AliasByBucketStageTests extends AbstractWireSerializingTestCase<AliasByBucketStage> {

    public void testValueBucket() {
        AliasByBucketStage stage = new AliasByBucketStage("bucket");
        TimeSeries ts = new TimeSeries(List.of(new FloatSample(10L, 1.0)), ByteLabels.fromStrings("bucket", "5-10.5"), 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(ts));
        assertEquals("10.5", result.get(0).getAlias());
    }

    public void testInvalidBucketFormatThrowsError() {
        AliasByBucketStage stage = new AliasByBucketStage("le");
        TimeSeries ts = new TimeSeries(List.of(new FloatSample(10L, 1.0)), ByteLabels.fromStrings("le", "0.1"), 10L, 10L, 10L, null);

        assertThrows(IllegalArgumentException.class, () -> stage.process(List.of(ts)));
    }

    public void testDurationBucket() {
        AliasByBucketStage stage = new AliasByBucketStage("bucket");
        TimeSeries ts = new TimeSeries(List.of(new FloatSample(10L, 1.0)), ByteLabels.fromStrings("bucket", "1s-5s"), 10L, 10L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(ts));
        assertEquals("5000", result.get(0).getAlias()); // 5s = 5000ms
    }

    public void testInfinityBucket() {
        AliasByBucketStage stage = new AliasByBucketStage("le");
        TimeSeries ts = new TimeSeries(
            List.of(new FloatSample(10L, 1.0)),
            ByteLabels.fromStrings("le", "10-infinity"),
            10L,
            10L,
            10L,
            null
        );

        List<TimeSeries> result = stage.process(List.of(ts));
        assertEquals("10", result.get(0).getAlias()); // Fallback to lower bound
    }

    public void testConstructorRequiresTagName() {
        assertThrows(IllegalArgumentException.class, () -> new AliasByBucketStage(null));
        assertThrows(IllegalArgumentException.class, () -> new AliasByBucketStage(""));
        assertThrows(IllegalArgumentException.class, () -> new AliasByBucketStage("   "));
    }

    public void testGetName() {
        AliasByBucketStage stage = new AliasByBucketStage("bucket");
        assertEquals("aliasByBucket", stage.getName());
    }

    public void testNullInputThrowsException() {
        AliasByBucketStage stage = new AliasByBucketStage("bucket");
        assertThrows(NullPointerException.class, () -> stage.process(null));
    }

    public void testMissingBucketTag() {
        AliasByBucketStage stage = new AliasByBucketStage("missing");
        TimeSeries ts = new TimeSeries(List.of(new FloatSample(10L, 1.0)), ByteLabels.fromStrings("other", "value"), 10L, 10L, 10L, null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> stage.process(List.of(ts)));
        assertEquals("Required bucket tag 'missing' does not exist in series", exception.getMessage());
    }

    public void testMissingBucketTagWithSeriesAlias() {
        AliasByBucketStage stage = new AliasByBucketStage("le");
        TimeSeries ts = new TimeSeries(
            List.of(new FloatSample(10L, 1.0)),
            ByteLabels.fromStrings("other", "value"),
            10L,
            10L,
            10L,
            "my_series"  // Series with alias
        );

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> stage.process(List.of(ts)));
        assertEquals("Required bucket tag 'le' does not exist in series 'my_series'", exception.getMessage());
    }

    public void testEmptyBucketTagValue() {
        AliasByBucketStage stage = new AliasByBucketStage("le");
        TimeSeries ts = new TimeSeries(
            List.of(new FloatSample(10L, 1.0)),
            ByteLabels.fromStrings("le", ""),  // Empty value
            10L,
            10L,
            10L,
            "test_series"
        );

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> stage.process(List.of(ts)));
        assertEquals("Bucket tag 'le' exists but has no value in series 'test_series'", exception.getMessage());
    }

    public void testFromArgs() {
        AliasByBucketStage stage = AliasByBucketStage.fromArgs(Map.of("tag_name", "bucket"));
        assertEquals("bucket", stage.getTagName());

        // Test missing argument
        assertThrows(IllegalArgumentException.class, () -> AliasByBucketStage.fromArgs(null));
        assertThrows(IllegalArgumentException.class, () -> AliasByBucketStage.fromArgs(Map.of()));
    }

    public void testFactoryCreation() {
        Map<String, Object> args = Map.of("tag_name", "le");
        AliasByBucketStage stage = (AliasByBucketStage) PipelineStageFactory.createWithArgs("aliasByBucket", args);
        assertEquals("le", stage.getTagName());
    }

    public void testXContentSerialization() throws Exception {
        AliasByBucketStage stage = new AliasByBucketStage("bucket");
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stage.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue(json.contains("tag_name"));
    }

    @Override
    protected AliasByBucketStage createTestInstance() {
        return new AliasByBucketStage(randomAlphaOfLength(5));
    }

    @Override
    protected Writeable.Reader<AliasByBucketStage> instanceReader() {
        return AliasByBucketStage::readFrom;
    }

    @Override
    protected AliasByBucketStage mutateInstance(AliasByBucketStage instance) {
        return new AliasByBucketStage(instance.getTagName() + "_mutated");
    }
}
