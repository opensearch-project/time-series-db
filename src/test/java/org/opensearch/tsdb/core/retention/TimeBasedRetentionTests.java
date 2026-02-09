/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.retention;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexManager;
import org.opensearch.tsdb.core.utils.Constants;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;

public class TimeBasedRetentionTests extends OpenSearchTestCase {

    private static final long TEST_BLOCK_DURATION = TimeValue.timeValueHours(2).getMillis();

    private ClosedChunkIndexManager closedChunkIndexManager;
    private Path metricsDirectory;
    private TimeBasedRetention retention;
    private Instant start;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        metricsDirectory = createTempDir();
        start = Instant.parse("2026-01-01T12:00:00Z");
        Clock fixedClock = Clock.fixed(Instant.parse("2026-01-01T21:00:00Z"), ZoneId.systemDefault());
        retention = new TimeBasedRetention(TEST_BLOCK_DURATION, 0, fixedClock);
    }

    public void testPlanWithEmptyResults() {
        var result = retention.plan(Collections.emptyList());
        assertTrue(result.isEmpty());
    }

    public void testDropWithMultipleIndexesSuccess() throws Exception {
        // Create 4 indexes: 3 old (to be removed) and 1 recent (to serve as reference for TTL)
        Path indexPath1 = metricsDirectory.resolve("block_1");
        Path indexPath2 = metricsDirectory.resolve("block_2");
        Path indexPath3 = metricsDirectory.resolve("block_3");
        Path indexPath4 = metricsDirectory.resolve("block_4");

        // With TTL=DEFAULT_BLOCK_DURATION , first 3 should be removed
        ClosedChunkIndex.Metadata metadata1 = new ClosedChunkIndex.Metadata(
            "block_1",
            start.toEpochMilli(),
            start.toEpochMilli() + TEST_BLOCK_DURATION
        );
        ClosedChunkIndex.Metadata metadata2 = new ClosedChunkIndex.Metadata(
            "block_2",
            metadata1.maxTimestamp(),
            metadata1.maxTimestamp() + TEST_BLOCK_DURATION
        );
        ClosedChunkIndex.Metadata metadata3 = new ClosedChunkIndex.Metadata(
            "block_3",
            metadata2.maxTimestamp(),
            metadata2.maxTimestamp() + TEST_BLOCK_DURATION
        );
        ClosedChunkIndex.Metadata metadata4 = new ClosedChunkIndex.Metadata(
            "block_4",
            metadata3.maxTimestamp(),
            metadata3.maxTimestamp() + TEST_BLOCK_DURATION
        );

        ClosedChunkIndex realIndex1 = new ClosedChunkIndex(indexPath1, metadata1, Constants.Time.DEFAULT_TIME_UNIT, Settings.EMPTY);
        ClosedChunkIndex realIndex2 = new ClosedChunkIndex(indexPath2, metadata2, Constants.Time.DEFAULT_TIME_UNIT, Settings.EMPTY);
        ClosedChunkIndex realIndex3 = new ClosedChunkIndex(indexPath3, metadata3, Constants.Time.DEFAULT_TIME_UNIT, Settings.EMPTY);
        ClosedChunkIndex realIndex4 = new ClosedChunkIndex(indexPath4, metadata4, Constants.Time.DEFAULT_TIME_UNIT, Settings.EMPTY);

        var result = retention.plan(List.of(realIndex1, realIndex2, realIndex3, realIndex4));

        assertEquals(3, result.size());

        // Clean up the recent index
        realIndex1.close();
        realIndex2.close();
        realIndex3.close();
        realIndex4.close();
    }

    public void testPlanWithMinusOneDurationReturnsEmpty() throws Exception {
        // Create retention with duration = -1 (disabled)
        TimeBasedRetention disabledRetention = new TimeBasedRetention(-1, 0);

        // Create some indexes
        Path indexPath1 = metricsDirectory.resolve("block_100");
        Path indexPath2 = metricsDirectory.resolve("block_200");

        ClosedChunkIndex.Metadata metadata1 = new ClosedChunkIndex.Metadata("block_100", 0L, TEST_BLOCK_DURATION);
        ClosedChunkIndex.Metadata metadata2 = new ClosedChunkIndex.Metadata("block_200", TEST_BLOCK_DURATION, TEST_BLOCK_DURATION * 2);

        ClosedChunkIndex realIndex1 = new ClosedChunkIndex(indexPath1, metadata1, Constants.Time.DEFAULT_TIME_UNIT, Settings.EMPTY);
        ClosedChunkIndex realIndex2 = new ClosedChunkIndex(indexPath2, metadata2, Constants.Time.DEFAULT_TIME_UNIT, Settings.EMPTY);

        // With duration=-1, should not delete anything
        var result = disabledRetention.plan(List.of(realIndex1, realIndex2));

        assertEquals(0, result.size());

        realIndex1.close();
        realIndex2.close();
    }
}
