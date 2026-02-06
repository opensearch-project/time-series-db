/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.action;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class LoadBlockResponseTests extends OpenSearchTestCase {

    public void testSuccessResponse() {
        LoadBlockResponse response = new LoadBlockResponse(2, List.of("block_1", "block_2"), List.of());
        assertTrue(response.isSuccess());
        assertEquals(2, response.getBlocksLoaded());
        assertEquals(List.of("block_1", "block_2"), response.getLoadedBlocks());
        assertTrue(response.getFailedBlocks().isEmpty());
        assertTrue(response.getMessage().contains("success"));
    }

    public void testPartialFailureResponse() {
        LoadBlockResponse response = new LoadBlockResponse(1, List.of("block_1"), List.of("block_2 (not found)"));
        assertFalse(response.isSuccess());
        assertEquals(1, response.getBlocksLoaded());
        assertEquals(1, response.getFailedBlocks().size());
    }

    public void testErrorResponse() {
        LoadBlockResponse response = new LoadBlockResponse("Index not found");
        assertFalse(response.isSuccess());
        assertEquals(0, response.getBlocksLoaded());
        assertTrue(response.getLoadedBlocks().isEmpty());
        assertEquals("Index not found", response.getMessage());
    }

    public void testSerialization() throws IOException {
        LoadBlockResponse response = new LoadBlockResponse(1, List.of("block_1_2_abc"), List.of("block_other (error)"));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                LoadBlockResponse read = new LoadBlockResponse(in);
                assertEquals(response.isSuccess(), read.isSuccess());
                assertEquals(response.getBlocksLoaded(), read.getBlocksLoaded());
                assertEquals(response.getLoadedBlocks(), read.getLoadedBlocks());
                assertEquals(response.getFailedBlocks(), read.getFailedBlocks());
                assertEquals(response.getMessage(), read.getMessage());
            }
        }
    }

    public void testToXContent() throws IOException {
        LoadBlockResponse response = new LoadBlockResponse(1, List.of("block_1"), List.of());
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = builder.toString();
            assertTrue(json.contains("\"success\":true"));
            assertTrue(json.contains("\"blocks_loaded\":1"));
            assertTrue(json.contains("block_1"));
        }
    }
}
