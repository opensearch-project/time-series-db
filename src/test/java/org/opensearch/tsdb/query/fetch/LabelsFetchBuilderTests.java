/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.fetch;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class LabelsFetchBuilderTests extends OpenSearchTestCase {

    public void testWriteableName() {
        LabelsFetchBuilder builder = new LabelsFetchBuilder();
        assertEquals("tsdb_labels", builder.getWriteableName());
    }

    public void testSerializationRoundTrip() throws IOException {
        LabelsFetchBuilder original = new LabelsFetchBuilder();

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        LabelsFetchBuilder deserialized = new LabelsFetchBuilder(in);

        assertEquals(original, deserialized);
        assertEquals(original.hashCode(), deserialized.hashCode());
    }

    public void testFromXContentWithEmptyObject() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().endObject();

        BytesReference bytes = BytesReference.bytes(xContentBuilder);
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), bytes)) {
            parser.nextToken();
            LabelsFetchBuilder builder = LabelsFetchBuilder.fromXContent(parser);
            assertNotNull(builder);
        }
    }

    public void testFromXContentWithFieldsIgnored() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().field("some_field", "some_value").endObject();

        BytesReference bytes = BytesReference.bytes(xContentBuilder);
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), bytes)) {
            parser.nextToken();
            LabelsFetchBuilder builder = LabelsFetchBuilder.fromXContent(parser);
            assertNotNull(builder);
        }
    }

    public void testFromXContentWithNestedObjectsIgnored() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("nested")
            .field("deep", "value")
            .endObject()
            .endObject();

        BytesReference bytes = BytesReference.bytes(xContentBuilder);
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), bytes)) {
            parser.nextToken();
            LabelsFetchBuilder builder = LabelsFetchBuilder.fromXContent(parser);
            assertNotNull(builder);
        }
    }

    public void testToXContent() throws IOException {
        LabelsFetchBuilder builder = new LabelsFetchBuilder();

        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        builder.toXContent(xContentBuilder, null);
        xContentBuilder.endObject();

        String json = xContentBuilder.toString();
        assertTrue(json.contains("tsdb_labels"));
    }

    public void testEqualsAndHashCode() {
        LabelsFetchBuilder builder1 = new LabelsFetchBuilder();
        LabelsFetchBuilder builder2 = new LabelsFetchBuilder();

        assertEquals(builder1, builder1);
        assertEquals(builder1, builder2);
        assertEquals(builder1.hashCode(), builder2.hashCode());
        assertNotEquals(builder1, null);
        assertNotEquals(builder1, "not a builder");
    }

    public void testFromXContentWithInvalidToken() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().value(123);

        BytesReference bytes = BytesReference.bytes(xContentBuilder);
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), bytes)) {
            parser.nextToken();
            expectThrows(org.opensearch.core.common.ParsingException.class, () -> LabelsFetchBuilder.fromXContent(parser));
        }
    }

    public void testFromXContentWithNullValue() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().nullValue();

        BytesReference bytes = BytesReference.bytes(xContentBuilder);
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), bytes)) {
            parser.nextToken();
            LabelsFetchBuilder builder = LabelsFetchBuilder.fromXContent(parser);
            assertNotNull(builder);
        }
    }
}
