/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesNormalizer;
import org.opensearch.tsdb.query.stage.BinaryPipelineStage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Abstract base class for binary pipeline projection stages that provides common functionality
 * for label matching and time alignment operations when handling left and right time series operands.
 */
public abstract class AbstractBinaryProjectionStage implements BinaryPipelineStage {

    /**
     * Default constructor for AbstractBinaryProjectionStage.
     */
    protected AbstractBinaryProjectionStage() {}

    /** The parameter name for label keys. */
    public static final String LABELS_PARAM_KEY = "labels";

    protected abstract boolean hasKeepNansOption();

    /**
     * Get the normalization strategy to use for this stage.
     *
     * @return The normalization strategy (NONE, PAIRWISE, or GLOBAL)
     */
    protected abstract NormalizationStrategy getNormalizationStrategy();

    /**
     * Get the consolidation strategy to use for normalization.
     *
     * @return The consolidation strategy (defaults to AVG)
     */
    protected TimeSeriesNormalizer.ConsolidationStrategy getConsolidationStrategy() {
        return TimeSeriesNormalizer.ConsolidationStrategy.AVG;
    }

    /**
     * Find a time series in the list that matches the target labels for the provided label keys.
     * If we only have 1 time series, ignore labels.
     * If labelKeys is null or empty, performs full label matching.
     *
     * @param timeSeriesList The list of time series to search
     * @param targetLabels The target labels to match against
     * @param labelKeys The specific label keys to consider for matching, or null/empty for full matching
     * @return The matching time series, or null if no match found
     */
    protected List<TimeSeries> findMatchingTimeSeries(List<TimeSeries> timeSeriesList, Labels targetLabels, List<String> labelKeys) {
        List<TimeSeries> matchingTimeSeriesList = new ArrayList<>();
        for (TimeSeries timeSeries : timeSeriesList) {
            if (labelsMatch(targetLabels, timeSeries.getLabels(), labelKeys)) {
                matchingTimeSeriesList.add(timeSeries);
            }
        }
        return matchingTimeSeriesList;
    }

    /**
     * Check if two Labels objects match for all specified label keys.
     * If labelKeys is null or empty, performs full label matching.
     *
     * @param leftLabels The left labels
     * @param rightLabels The right labels
     * @param labelKeys The specific label keys to consider for matching, or null/empty for full matching
     * @return true if labels match for all specified keys, false otherwise
     */
    protected boolean labelsMatch(Labels leftLabels, Labels rightLabels, List<String> labelKeys) {
        if (leftLabels == null || rightLabels == null) {
            return false;
        }

        // If no specific tag provided, use full label matching
        if (labelKeys == null || labelKeys.isEmpty()) {
            return leftLabels.equals(rightLabels);
        }

        // Check that all specified label keys match
        for (String labelKey : labelKeys) {
            String leftValue = leftLabels.get(labelKey);
            String rightValue = rightLabels.get(labelKey);

            if (leftValue.equals(rightValue) == false) {
                return false;
            }
        }

        return true;
    }

    /**
     * Align two time series by timestamp and process the samples.
     * Both left and right time series are expected to be sorted by timestamp.
     * This method walks through both series, finds matching timestamps, and processes
     * samples at those timestamps. Mismatched timestamps are handled based on hasKeepNansOption().
     *
     * @param leftSeries The left time series
     * @param rightSeries The right time series
     * @return A new time series, or null if no matching timestamps are found.
     */
    private TimeSeries alignTimestampsAndProcess(TimeSeries leftSeries, TimeSeries rightSeries) {
        if (leftSeries == null || rightSeries == null) {
            return null;
        }

        SampleList leftSamples = leftSeries.getSamples();
        SampleList rightSamples = rightSeries.getSamples();

        if (leftSamples == null || rightSamples == null) {
            return null;
        }

        FloatSampleList.Builder resultBuilder = new FloatSampleList.Builder();
        boolean hasKeepNansOptions = hasKeepNansOption();

        // Find matching timestamps between the two sorted time series.
        // The input time series is expected to be sorted by timestamp in increasing order.
        Iterator<Sample> leftIter = leftSamples.iterator();
        Iterator<Sample> rightIter = rightSamples.iterator();
        Sample leftSample = advanceIterOrNull(leftIter);
        Sample rightSample = advanceIterOrNull(rightIter);

        while (leftSample != null || rightSample != null) {
            Long leftTimestamp = Long.MAX_VALUE;
            Long rightTimestamp = Long.MAX_VALUE;
            if (leftSample != null) {
                leftTimestamp = leftSample.getTimestamp();
            }
            if (rightSample != null) {
                rightTimestamp = rightSample.getTimestamp();
            }

            Double resultValue;
            long timestamp;
            if (leftTimestamp < rightTimestamp) {
                // If stage doesn't have keepNans option, we skip processing
                if (hasKeepNansOptions) {
                    resultValue = processSampleValues(leftSample.getValue(), null);
                } else {
                    resultValue = null;
                }
                timestamp = leftTimestamp;
                leftSample = advanceIterOrNull(leftIter);

            } else if (rightTimestamp < leftTimestamp) {
                // If stage doesn't have keepNans option, we skip processing
                if (hasKeepNansOptions) {
                    resultValue = processSampleValues(null, rightSample.getValue());
                } else {
                    resultValue = null;
                }
                timestamp = rightTimestamp;
                rightSample = advanceIterOrNull(rightIter);
            } else {
                resultValue = processSampleValues(leftSample.getValue(), rightSample.getValue());
                timestamp = leftTimestamp;
                leftSample = advanceIterOrNull(leftIter);
                rightSample = advanceIterOrNull(rightIter);
            }
            if (resultValue != null) {
                resultBuilder.add(timestamp, resultValue);
            }
        }

        if (resultBuilder.isEmpty()) {
            return null;
        }

        // Calculate min/max timestamps from the union of both series
        long minTimestamp = Math.min(leftSeries.getMinTimestamp(), rightSeries.getMinTimestamp());
        long maxTimestamp = Math.max(leftSeries.getMaxTimestamp(), rightSeries.getMaxTimestamp());

        // Transform labels if needed (can be overridden by subclasses)
        Labels transformedLabels = transformLabels(leftSeries.getLabels());

        return new TimeSeries(
            resultBuilder.build(),
            transformedLabels,
            minTimestamp,
            maxTimestamp,
            leftSeries.getStep(),
            leftSeries.getAlias()
        );
    }

    /**
     * Get the label keys to use for selective matching.
     * If null or empty, full label matching will be performed.
     *
     * @return The list of label keys for selective matching, or null for full matching
     */
    protected abstract List<String> getLabelKeys();

    /**
     * Check if this stage should extract common tag keys when no label keys are specified.
     * Subclasses can override this to enable common tag key extraction (e.g., asPercent, divide, subtract).
     *
     * @return true if common tag keys should be extracted when no label keys are specified, false otherwise
     */
    protected boolean shouldExtractCommonTagKeys() {
        return false;
    }

    /**
     * Process two time series inputs and return the resulting time series aligning timestamps and matching labels.
     * When the right operand has a single series, all left series are processed against it without label matching.
     * When the right operand has multiple series, label matching is used to pair left and right series.
     *
     * @param left The left operand time series
     * @param right The right operand time series
     * @return The result time series
     */
    @Override
    public List<TimeSeries> process(List<TimeSeries> left, List<TimeSeries> right) {
        if (left == null) {
            throw new NullPointerException(getName() + " stage received null left input");
        }
        if (right == null) {
            throw new NullPointerException(getName() + " stage received null right input");
        }
        if (left.isEmpty() || right.isEmpty()) {
            return new ArrayList<>();
        }

        // If right operand has single series, project all left operand time series onto
        // the right time series without label matching.
        if (right.size() == 1) {
            return processWithoutLabelMatching(left, right.getFirst());
        } else {
            return processWithLabelMatching(left, right);
        }
    }

    /**
     * Process left time series against a single right time series without label matching.
     * This method is called when a single right time series is provided.
     *
     * For BATCH normalization, all left series and the right series are normalized together first.
     *
     * @param left The left operand time series list
     * @param rightSeries The single right operand time series
     * @return The result time series list
     */
    protected List<TimeSeries> processWithoutLabelMatching(List<TimeSeries> left, TimeSeries rightSeries) {
        List<TimeSeries> result = new ArrayList<>();

        // For BATCH normalization, normalize all series together first
        if (getNormalizationStrategy() == NormalizationStrategy.BATCH) {
            // Combine all left series + right series for normalization
            List<TimeSeries> allSeries = new ArrayList<>(left.size() + 1);
            allSeries.addAll(left);
            allSeries.add(rightSeries);

            // Normalize all together
            List<TimeSeries> normalized = TimeSeriesNormalizer.normalize(
                allSeries,
                TimeSeriesNormalizer.StepSizeStrategy.LCM,
                getConsolidationStrategy()
            );

            // Last normalized series is the right series
            TimeSeries normalizedRight = normalized.getLast();

            // Process each normalized left series with normalized right
            for (int i = 0; i < left.size(); i++) {
                TimeSeries normalizedLeft = normalized.get(i);
                // Note: Don't call alignAndProcess since BATCH already normalized, just process directly
                TimeSeries processedSeries = alignTimestampsAndProcess(normalizedLeft, normalizedRight);
                if (processedSeries != null) {
                    result.add(processedSeries);
                }
            }
        } else {
            // For NONE or PAIRWISE, process each pair individually
            for (TimeSeries leftSeries : left) {
                if (leftSeries == null) {
                    continue;
                }

                TimeSeries processedSeries;
                // Apply pairwise normalization if configured
                if (getNormalizationStrategy() == NormalizationStrategy.PAIRWISE) {
                    List<TimeSeries> normalized = TimeSeriesNormalizer.normalize(
                        List.of(leftSeries, rightSeries),
                        TimeSeriesNormalizer.StepSizeStrategy.LCM,
                        getConsolidationStrategy()
                    );
                    processedSeries = alignTimestampsAndProcess(normalized.get(0), normalized.get(1));
                } else {
                    // NONE strategy - process without normalization
                    processedSeries = alignTimestampsAndProcess(leftSeries, rightSeries);
                }

                if (processedSeries != null) {
                    result.add(processedSeries);
                }
            }
        }

        return result;
    }

    /**
     * Process left time series against multiple right time series.
     * Matches time series by labels using selective matching if labelKeys are provided. Otherwise match entire labels set.
     * Groups right series first for efficiency. If only one group remains after grouping, skips tag matching.
     * After matching, delegates to processWithoutLabelMatching for the actual processing.
     *
     * @param left The left operand time series list
     * @param right The right operand time series list
     * @return The result time series list
     */
    protected List<TimeSeries> processWithLabelMatching(List<TimeSeries> left, List<TimeSeries> right) {
        List<String> labelKeys = getLabelKeys();

        // If no label keys specified and this stage should extract common tag keys, do so
        if ((labelKeys == null || labelKeys.isEmpty()) && shouldExtractCommonTagKeys()) {
            labelKeys = extractCommonTagKeys(left, right);
        }

        // Group and merge right series by label keys for efficient processing
        Map<ByteLabels, TimeSeries> rightGroups = groupAndMergeRightSeriesByLabels(right, labelKeys);

        // If only one group after grouping, process all left series against that single merged right series
        if (rightGroups.size() == 1) {
            TimeSeries mergedRightSeries = rightGroups.values().iterator().next();
            return processWithoutLabelMatching(left, mergedRightSeries);
        }

        // Multiple groups - perform tag matching for each left series using the grouped map
        // Build matched pairs: group left series by their matching right series for efficient normalization
        Map<TimeSeries, List<TimeSeries>> rightToLeftMap = new HashMap<>();
        for (TimeSeries leftSeries : left) {
            ByteLabels leftGroupLabels = extractGroupLabels(leftSeries, labelKeys);
            if (leftGroupLabels != null) {
                TimeSeries matchingRightSeries = rightGroups.get(leftGroupLabels);
                if (matchingRightSeries != null) {
                    rightToLeftMap.computeIfAbsent(matchingRightSeries, k -> new ArrayList<>()).add(leftSeries);
                }
            }
        }

        // Process each group of left series with their matching right series
        List<TimeSeries> result = new ArrayList<>();
        for (Map.Entry<TimeSeries, List<TimeSeries>> entry : rightToLeftMap.entrySet()) {
            TimeSeries rightSeries = entry.getKey();
            List<TimeSeries> matchedLeftSeries = entry.getValue();

            // Delegate to processWithoutLabelMatching which handles normalization
            List<TimeSeries> processed = processWithoutLabelMatching(matchedLeftSeries, rightSeries);
            result.addAll(processed);
        }

        return result;
    }

    /**
     * Extract common tag keys from left and right time series.
     * Finds tag keys that are present in at least one left series and at least one right series.
     * This allows matching series by their common tags even when not all series have all tags.
     *
     * @param left The left operand time series list
     * @param right The right operand time series list
     * @return List of common tag keys, or empty list if none found
     */
    protected List<String> extractCommonTagKeys(List<TimeSeries> left, List<TimeSeries> right) {
        if (left.isEmpty() || right.isEmpty()) {
            return new ArrayList<>();
        }

        // Collect all Labels from left and right series
        List<Labels> allLabels = new ArrayList<>();
        for (TimeSeries series : left) {
            allLabels.add(series.getLabels());
        }
        for (TimeSeries series : right) {
            allLabels.add(series.getLabels());
        }

        // Use Labels.findCommonLabelNames() to find names present in all Labels instances
        // This method handles null and empty labels internally
        return Labels.findCommonLabelNames(allLabels);
    }

    /**
     * Group and merge right time series by the specified label keys.
     * If labelKeys is null or empty, groups by all labels (full label matching).
     * Series with the same group labels are merged together.
     *
     * @param right The right operand time series list
     * @param labelKeys The label keys to group by, or null/empty for full label matching
     * @return Map of grouped labels to merged time series
     */
    protected Map<ByteLabels, TimeSeries> groupAndMergeRightSeriesByLabels(List<TimeSeries> right, List<String> labelKeys) {
        Map<ByteLabels, List<TimeSeries>> groups = new HashMap<>();

        // First, group series by labels
        for (TimeSeries series : right) {
            ByteLabels groupLabels = extractGroupLabels(series, labelKeys);
            if (groupLabels != null) {
                groups.computeIfAbsent(groupLabels, k -> new ArrayList<>()).add(series);
            }
        }

        // Then, merge series in each group
        Map<ByteLabels, TimeSeries> mergedGroups = new HashMap<>();
        for (Map.Entry<ByteLabels, List<TimeSeries>> entry : groups.entrySet()) {
            TimeSeries mergedSeries = mergeMatchingSeries(entry.getValue());
            if (mergedSeries != null) {
                mergedGroups.put(entry.getKey(), mergedSeries);
            }
        }

        return mergedGroups;
    }

    /**
     * Extract group labels from a time series based on the specified label keys.
     * If labelKeys is null or empty, returns all labels (for full label matching).
     *
     * @param series The time series to extract labels from
     * @param labelKeys The label keys to extract, or null/empty for all labels
     * @return ByteLabels containing the extracted labels, or null if required labels are missing
     */
    protected ByteLabels extractGroupLabels(TimeSeries series, List<String> labelKeys) {
        Labels seriesLabels = series.getLabels();
        if (seriesLabels == null) {
            return null;
        }

        // If no label keys specified, use all labels
        if (labelKeys == null || labelKeys.isEmpty()) {
            if (seriesLabels instanceof ByteLabels) {
                return (ByteLabels) seriesLabels;
            } else {
                return ByteLabels.fromMap(seriesLabels.toMapView());
            }
        }

        // Extract only the specified label keys
        Map<String, String> groupLabelMap = new HashMap<>();
        for (String labelName : labelKeys) {
            if (seriesLabels.has(labelName)) {
                String labelValue = seriesLabels.get(labelName);
                groupLabelMap.put(labelName, labelValue);
            } else {
                // Missing required label - return null to drop this series
                return null;
            }
        }

        return ByteLabels.fromMap(groupLabelMap);
    }

    protected abstract TimeSeries mergeMatchingSeries(List<TimeSeries> rightTimeSeries);

    /**
     * Transform labels before creating the result time series.
     * The default implementation returns labels unchanged.
     * Subclasses can override this to add, modify, or remove labels as needed.
     *
     * @param originalLabels The original labels from the left series
     * @return The transformed labels to use in the result time series
     */
    protected Labels transformLabels(Labels originalLabels) {
        return originalLabels;
    }

    /**
     * Process sample values from left and right time series and return a result sample value.
     * This method should be overridden by subclasses to implement their specific logic.
     * Both values are expected to be non-null and have matching timestamps.
     *
     * @param leftValue The left sample value
     * @param rightValue The right sample value
     * @return The result value, null if the result value is not supposed to be added to the final result series
     */
    protected abstract Double processSampleValues(Double leftValue, Double rightValue);

    @Override
    public int hashCode() {
        List<String> labelKeys = getLabelKeys();
        return labelKeys != null ? labelKeys.hashCode() : 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AbstractBinaryProjectionStage that = (AbstractBinaryProjectionStage) obj;
        List<String> labelKeys = getLabelKeys();
        List<String> thatLabelKeys = that.getLabelKeys();
        if (labelKeys == null && thatLabelKeys == null) {
            return true;
        }
        if (labelKeys == null || thatLabelKeys == null) {
            return false;
        }
        return labelKeys.equals(thatLabelKeys);
    }

    private static <T> T advanceIterOrNull(Iterator<T> iter) {
        return iter.hasNext() ? iter.next() : null;
    }
}
