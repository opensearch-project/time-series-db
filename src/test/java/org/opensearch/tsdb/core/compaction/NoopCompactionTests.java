/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import org.mockito.Mockito;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;

import java.io.IOException;
import java.util.List;

public class NoopCompactionTests extends OpenSearchTestCase {

    public void testPlan() {
        ClosedChunkIndex realIndex1 = Mockito.mock(ClosedChunkIndex.class);
        ClosedChunkIndex realIndex2 = Mockito.mock(ClosedChunkIndex.class);
        ClosedChunkIndex realIndex3 = Mockito.mock(ClosedChunkIndex.class);

        var compaction = new NoopCompaction();
        assertEquals(0, compaction.plan(List.of(realIndex1, realIndex2, realIndex3, realIndex1)).size());
    }

    public void testGetFrequency() {
        var compaction = new NoopCompaction();
        assertEquals(Long.MAX_VALUE, compaction.getFrequency());
    }

    public void testCompact() throws IOException {
        ClosedChunkIndex sourceIndex1 = Mockito.mock(ClosedChunkIndex.class);
        ClosedChunkIndex sourceIndex2 = Mockito.mock(ClosedChunkIndex.class);
        ClosedChunkIndex sourceIndex3 = Mockito.mock(ClosedChunkIndex.class);
        ClosedChunkIndex dest = Mockito.mock(ClosedChunkIndex.class);

        var compaction = new NoopCompaction();
        compaction.compact(List.of(sourceIndex1, sourceIndex2, sourceIndex3), dest);
        Mockito.verify(sourceIndex1, Mockito.times(0)).copyTo(Mockito.any());
        Mockito.verify(sourceIndex2, Mockito.times(0)).copyTo(Mockito.any());
        Mockito.verify(sourceIndex3, Mockito.times(0)).copyTo(Mockito.any());
        Mockito.verify(dest, Mockito.times(0)).forceMerge(Mockito.anyInt());
    }
}
