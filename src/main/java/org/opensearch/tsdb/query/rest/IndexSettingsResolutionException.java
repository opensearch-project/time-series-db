/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.rest;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;

/**
 * Exception thrown when there are issues resolving index settings,
 * particularly step size settings, across partitions.
 */
public class IndexSettingsResolutionException extends OpenSearchException {

    public IndexSettingsResolutionException(String msg) {
        super(msg);
    }

    public IndexSettingsResolutionException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public IndexSettingsResolutionException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
