/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.compaction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.IndexSettings;
import org.opensearch.tsdb.TSDBPlugin;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Factory class for creating compaction strategy instances based on index settings.
 * <p>
 * This factory determines the appropriate compaction strategy to use for an index
 * based on its configuration. Currently supported strategies include:
 * <ul>
 *   <li>SizeTieredCompaction - Size-tiered compaction with configurable time ranges</li>
 *   <li>ForceMergeCompaction - In-place force merge optimization for multi-segment indexes</li>
 *   <li>NoopCompaction - Default strategy that performs no compaction</li>
 * </ul>
 */
public class CompactionFactory {
    private static final Logger logger = LogManager.getLogger(CompactionFactory.class);

    public enum CompactionType {
        SizeTieredCompaction("SizeTieredCompaction"),
        ForceMergeCompaction("ForceMergeCompaction"),
        Noop("Noop"),
        Invalid("Invalid");

        public final String name;

        CompactionType(String name) {
            this.name = name;
        }

        public static CompactionType from(String compactionType) {
            return switch (compactionType) {
                case "SizeTieredCompaction" -> CompactionType.SizeTieredCompaction;
                case "ForceMergeCompaction" -> CompactionType.ForceMergeCompaction;
                case "Noop" -> CompactionType.Noop;
                default -> CompactionType.Invalid;
            };
        }
    }

    /**
     * Creates a compaction strategy instance based on the provided index settings.
     *
     * @param indexSettings the index settings containing compaction and retention configuration
     * @return a Compaction instance configured according to the index settings
     */
    public static Compaction create(IndexSettings indexSettings) {
        var compaction = getCompactionFor(indexSettings);
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY, newFrequency -> {
            logger.info("Updating compaction frequency to: {}", newFrequency);
            compaction.setFrequency(newFrequency.getMillis());
        });
        return compaction;
    }

    private static Compaction getCompactionFor(IndexSettings indexSettings) {
        CompactionType compactionType = CompactionType.from(TSDBPlugin.TSDB_ENGINE_COMPACTION_TYPE.get(indexSettings.getSettings()));

        // Read common settings used by multiple compaction types
        long frequency = TSDBPlugin.TSDB_ENGINE_COMPACTION_FREQUENCY.get(indexSettings.getSettings()).getMillis();
        TimeUnit resolution = TimeUnit.valueOf(TSDBPlugin.TSDB_ENGINE_TIME_UNIT.get(indexSettings.getSettings()));

        switch (compactionType) {
            case SizeTieredCompaction:
                long retentionTime = TSDBPlugin.TSDB_ENGINE_RETENTION_TIME.get(indexSettings.getSettings()).getHours();
                long ttl = retentionTime != -1 ? retentionTime : Long.MAX_VALUE;

                // Cap the max index size as minimum of 1/10 of TTL or 31D(744H).
                List<Integer> tiers = new ArrayList<>();
                for (int tier = 2;; tier *= 3) {
                    if (tier > ttl * 0.1) {
                        if (tier > 744) {
                            tiers.add(744);
                        }
                        break;
                    }
                    tiers.add(tier);
                }

                return new SizeTieredCompaction(tiers.stream().map(Duration::ofHours).toArray(Duration[]::new), frequency, resolution);
            case ForceMergeCompaction:
                int minSegmentCount = TSDBPlugin.TSDB_ENGINE_FORCE_MERGE_MIN_SEGMENT_COUNT.get(indexSettings.getSettings());
                int maxSegmentsAfterForceMerge = TSDBPlugin.TSDB_ENGINE_FORCE_MERGE_MAX_SEGMENTS_AFTER_MERGE.get(
                    indexSettings.getSettings()
                );
                long oooCutoffWindow = TSDBPlugin.TSDB_ENGINE_OOO_CUTOFF.get(indexSettings.getSettings()).getMillis();
                long blockDuration = TSDBPlugin.TSDB_ENGINE_BLOCK_DURATION.get(indexSettings.getSettings()).getMillis();
                return new ForceMergeCompaction(
                    frequency,
                    minSegmentCount,
                    maxSegmentsAfterForceMerge,
                    oooCutoffWindow,
                    blockDuration,
                    resolution
                );
            case Noop:
                return new NoopCompaction();
            default:
                throw new IllegalArgumentException("Unknown compaction type: " + compactionType);
        }
    }
}
