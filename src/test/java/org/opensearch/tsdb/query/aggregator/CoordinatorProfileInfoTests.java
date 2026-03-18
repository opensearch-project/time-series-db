/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Unit tests for {@link CoordinatorProfileInfo}.
 */
public class CoordinatorProfileInfoTests extends OpenSearchTestCase {

    // ========== Constructor Tests ==========

    public void testConstructorBasic() {

        String stages = "mockFetch(0): 1000000 ns, 100 bytes;scale(2): 500000 ns, 50 bytes";
        long reduceTimeNanos = 2500000L;
        long totalTimeNanos = 1500000L;
        long internalReduceNanos = 300000L;
        long serializeNanos = 400000L;
        long deserializeNanos = 200000L;

        CoordinatorProfileInfo profile = new CoordinatorProfileInfo(
            stages,
            reduceTimeNanos,
            totalTimeNanos,
            internalReduceNanos,
            serializeNanos,
            deserializeNanos
        );

        assertEquals(stages, profile.getStages());
        assertEquals(reduceTimeNanos, profile.getReduceTimeNanos());
        assertEquals(totalTimeNanos, profile.getTotalTimeNanos());
        assertEquals(internalReduceNanos, profile.getInternalReduceTimeNanos());
        assertEquals(serializeNanos, profile.getSerializeTimeNanos());
        assertEquals(deserializeNanos, profile.getDeserializeTimeNanos());
    }

    public void testConstructorWithEmptyStages() {

        CoordinatorProfileInfo profile = new CoordinatorProfileInfo("", 0L, 0L, 0L, 0L, 0L);

        assertEquals("", profile.getStages());
        assertEquals(0L, profile.getReduceTimeNanos());
        assertEquals(0L, profile.getTotalTimeNanos());
    }

    public void testConstructorWithLargeValues() {

        String stages = "very_long_stage_description_with_lots_of_details";
        long largeValue1 = Long.MAX_VALUE / 2;
        long largeValue2 = Long.MAX_VALUE / 3;

        CoordinatorProfileInfo profile = new CoordinatorProfileInfo(
            stages,
            largeValue1,
            largeValue2,
            Long.MAX_VALUE / 4,
            Long.MAX_VALUE / 5,
            Long.MAX_VALUE / 6
        );

        assertEquals(stages, profile.getStages());
        assertEquals(largeValue1, profile.getReduceTimeNanos());
        assertEquals(largeValue2, profile.getTotalTimeNanos());
    }

    // ========== Serialization Tests ==========

    public void testSerializationBasic() throws IOException {

        CoordinatorProfileInfo original = new CoordinatorProfileInfo("test_stages", 123456L, 789012L, 345678L, 901234L, 567890L);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        CoordinatorProfileInfo deserialized = new CoordinatorProfileInfo(in);

        assertEquals(original.getStages(), deserialized.getStages());
        assertEquals(original.getReduceTimeNanos(), deserialized.getReduceTimeNanos());
        assertEquals(original.getTotalTimeNanos(), deserialized.getTotalTimeNanos());
        assertEquals(original.getInternalReduceTimeNanos(), deserialized.getInternalReduceTimeNanos());
        assertEquals(original.getSerializeTimeNanos(), deserialized.getSerializeTimeNanos());
        assertEquals(original.getDeserializeTimeNanos(), deserialized.getDeserializeTimeNanos());
    }

    public void testSerializationRoundTrip() throws IOException {

        for (int i = 0; i < 10; i++) {
            CoordinatorProfileInfo original = new CoordinatorProfileInfo(
                "stage_" + i,
                i * 1000L,
                i * 2000L,
                i * 3000L,
                i * 4000L,
                i * 5000L
            );

            BytesStreamOutput out = new BytesStreamOutput();
            original.writeTo(out);

            StreamInput in = out.bytes().streamInput();
            CoordinatorProfileInfo deserialized = new CoordinatorProfileInfo(in);

            assertEquals(original.getStages(), deserialized.getStages());
            assertEquals(original.getReduceTimeNanos(), deserialized.getReduceTimeNanos());
            assertEquals(original.getTotalTimeNanos(), deserialized.getTotalTimeNanos());
            assertEquals(original.getInternalReduceTimeNanos(), deserialized.getInternalReduceTimeNanos());
            assertEquals(original.getSerializeTimeNanos(), deserialized.getSerializeTimeNanos());
            assertEquals(original.getDeserializeTimeNanos(), deserialized.getDeserializeTimeNanos());
        }
    }

    // ========== XContent Tests ==========

    public void testToXContentBasic() throws IOException {

        CoordinatorProfileInfo profile = new CoordinatorProfileInfo("mockFetch: 1000 ns", 2000000L, 1500000L, 500000L, 300000L, 200000L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        profile.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertNotNull(json);
        assertTrue(json.contains("\"stages\":\"mockFetch: 1000 ns\""));
        assertTrue(json.contains("\"reduce_time_ns\":2000000"));
        assertTrue(json.contains("\"total_profiled_stages_time_ns\":1500000"));
        assertTrue(json.contains("\"internal_reduce_ns\":500000"));
        assertTrue(json.contains("\"serialize_ns\":300000"));
        assertTrue(json.contains("\"deserialize_ns\":200000"));
    }

    public void testToXContentWithZeroValues() throws IOException {

        CoordinatorProfileInfo profile = new CoordinatorProfileInfo("", 0L, 0L, 0L, 0L, 0L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        profile.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertNotNull(json);
        assertTrue(json.contains("\"stages\":\"\""));
        assertTrue(json.contains("\"reduce_time_ns\":0"));
        assertTrue(json.contains("\"total_profiled_stages_time_ns\":0"));
        assertTrue(json.contains("\"internal_reduce_ns\":0"));
        assertTrue(json.contains("\"serialize_ns\":0"));
        assertTrue(json.contains("\"deserialize_ns\":0"));
    }

    public void testToXContentFieldNaming() throws IOException {

        CoordinatorProfileInfo profile = new CoordinatorProfileInfo("test", 1L, 2L, 3L, 4L, 5L);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        profile.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertTrue(json.contains("reduce_time_ns"));
        assertTrue(json.contains("total_profiled_stages_time_ns"));
        assertTrue(json.contains("internal_reduce_ns"));
        assertTrue(json.contains("serialize_ns"));
        assertTrue(json.contains("deserialize_ns"));
    }
}
