/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.fetch;

import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchExtBuilder;

import java.io.IOException;

/**
 * Search extension builder for TSDB labels fetch sub-phase.
 * Parses the ext section of search requests to enable label fetching.
 *
 * <p>The label storage type is automatically read from the index setting
 * {@code index.tsdb_engine.labels.storage_type} during fetch phase execution.
 *
 * <p>Example usage in search request:
 * <pre>
 * GET /tsdb-index/_search
 * {
 *   "ext": {
 *     "tsdb_labels": {}
 *   }
 * }
 * </pre>
 */
public class LabelsFetchBuilder extends SearchExtBuilder {

    public LabelsFetchBuilder() {}

    public LabelsFetchBuilder(StreamInput in) throws IOException {
        // No fields to read
    }

    public static LabelsFetchBuilder fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_OBJECT) {
            parser.skipChildren();
        } else if (token != XContentParser.Token.VALUE_NULL && token != XContentParser.Token.VALUE_STRING) {
            throw new ParsingException(parser.getTokenLocation(), "Expected START_OBJECT but got " + token);
        }
        return new LabelsFetchBuilder();
    }

    @Override
    public String getWriteableName() {
        return LabelsFetchSubPhase.NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // No fields to write
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(LabelsFetchSubPhase.NAME);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return o != null && getClass() == o.getClass();
    }

    @Override
    public int hashCode() {
        return LabelsFetchSubPhase.NAME.hashCode();
    }
}
