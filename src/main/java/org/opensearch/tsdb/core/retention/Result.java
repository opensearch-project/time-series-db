/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.retention;

import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;

import java.util.List;

/**
 * A record class representing the result of the retention operation.
 * @param success A list of the indexes that were removed successfully.
 * @param failure A list of the indexes that were failed to be removed.
 */
public record Result(List<ClosedChunkIndex> success, List<ClosedChunkIndex> failure) {
}
