/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.BinaryPipelineStage;

import java.util.ArrayList;
import java.util.List;

/**
 * Abstract base class for binary pipeline projection stages that provides common functionality
 * for label matching and time alignment operations when handling left and right time series operands.
 */
public abstract class AbstractBinaryProjectionStage implements BinaryPipelineStage {

    /**
     * Default constructor for AbstractBinaryProjectionStage.
     */
    protected AbstractBinaryProjectionStage() {}

    /**
     * Find a time series in the list that matches the target labels for the provided label keys.
     * If labelKeys is null or empty, performs full label matching.
     *
     * @param timeSeriesList The list of time series to search
     * @param targetLabels The target labels to match against
     * @param labelKeys The specific label keys to consider for matching, or null/empty for full matching
     * @return The matching time series, or null if no match found
     */
    protected TimeSeries findMatchingTimeSeries(List<TimeSeries> timeSeriesList, Labels targetLabels, List<String> labelKeys) {
        for (TimeSeries timeSeries : timeSeriesList) {
            if (labelsMatch(targetLabels, timeSeries.getLabels(), labelKeys)) {
                return timeSeries;
            }
        }
        return null;
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
     * Resulting time series includes only timestamps where both left and right samples exist.
     * Both left and right time series are expected to be sorted by timestamp.
     *
     * @param leftSeries The left time series
     * @param rightSeries The right time series
     * @return A new time series, or null if no matching timestamps are found.
     */
    protected TimeSeries alignAndProcess(TimeSeries leftSeries, TimeSeries rightSeries) {
        if (leftSeries == null || rightSeries == null) {
            return null;
        }

        List<Sample> leftSamples = leftSeries.getSamples();
        List<Sample> rightSamples = rightSeries.getSamples();

        if (leftSamples == null || rightSamples == null) {
            return null;
        }

        List<Sample> resultSamples = new ArrayList<>();

        // Find matching timestamps between the two sorted time series.
        // The input time series is expected to be sorted by timestamp in increasing order.
        int leftIndex = 0;
        int rightIndex = 0;

        while (leftIndex < leftSamples.size() && rightIndex < rightSamples.size()) {
            Sample leftSample = leftSamples.get(leftIndex);
            Sample rightSample = rightSamples.get(rightIndex);

            long leftTimestamp = leftSample.getTimestamp();
            long rightTimestamp = rightSample.getTimestamp();

            if (leftTimestamp < rightTimestamp) {
                // Left timestamp is earlier, advance left index
                leftIndex++;
            } else if (rightTimestamp < leftTimestamp) {
                // Right timestamp is earlier, advance right index
                rightIndex++;
            } else {
                Sample resultSample = processSamples(leftSample, rightSample);
                if (resultSample != null) {
                    resultSamples.add(resultSample);
                }
                leftIndex++;
                rightIndex++;
            }
        }

        // Return null if no matching timestamps were found
        if (resultSamples.isEmpty()) {
            return null;
        }

        // Calculate min/max timestamps from the union of both series
        long minTimestamp = Math.min(leftSeries.getMinTimestamp(), rightSeries.getMinTimestamp());
        long maxTimestamp = Math.max(leftSeries.getMaxTimestamp(), rightSeries.getMaxTimestamp());

        // Transform labels if needed (can be overridden by subclasses)
        Labels transformedLabels = transformLabels(leftSeries.getLabels());

        return new TimeSeries(resultSamples, transformedLabels, minTimestamp, maxTimestamp, leftSeries.getStep(), leftSeries.getAlias());
    }

    /**
     * Get the label keys to use for selective matching.
     * If null or empty, full label matching will be performed.
     *
     * @return The list of label keys for selective matching, or null for full matching
     */
    protected abstract List<String> getLabelKeys();

    /**
     * Process two time series inputs and return the resulting time series aligning timestamps and matching labels.
     * When labelKeys are specified, standard label matching is always used regardless of the number of right series.
     * When no labelKeys are specified and there's a single right series, all left series are processed against it.
     *
     * @param left The left operand time series
     * @param right The right operand time series
     * @return The result time series
     */
    @Override
    public List<TimeSeries> process(List<TimeSeries> left, List<TimeSeries> right) {
        if (left.isEmpty() || right.isEmpty()) {
            return new ArrayList<>();
        }

        List<String> labelKeys = getLabelKeys();

        // If labelKeys are specified, always use the standard label matching logic
        // regardless of the number of right series
        if (labelKeys != null && labelKeys.isEmpty() == false) {
            return processWithLabelMatching(left, right);
        }

        // If no label keys are provided and right operand has single series, project all left operand time series onto
        // the right time series without label matching.
        if (right.size() == 1) {
            return processWithoutLabelMatching(left, right.getFirst());
        } else {
            return processWithLabelMatching(left, right);
        }
    }

    /**
     * Process left time series against a single right time series without label matching.
     * This method is called when no labelKeys are specified and a single right time series is provided.
     *
     * @param left The left operand time series list
     * @param rightSeries The single right operand time series
     * @return The result time series list
     */
    protected List<TimeSeries> processWithoutLabelMatching(List<TimeSeries> left, TimeSeries rightSeries) {
        List<TimeSeries> result = new ArrayList<>();

        for (TimeSeries leftSeries : left) {
            TimeSeries processedSeries = alignAndProcess(leftSeries, rightSeries);
            if (processedSeries != null) {
                result.add(processedSeries);
            }
        }

        return result;
    }

    /**
     * Process left time series against multiple right time series.
     * Matches time series by labels using selective matching if labelKeys are provided. Otherwise match entire labels set.
     *
     * @param left The left operand time series list
     * @param right The right operand time series list
     * @return The result time series list
     */
    protected List<TimeSeries> processWithLabelMatching(List<TimeSeries> left, List<TimeSeries> right) {
        List<TimeSeries> result = new ArrayList<>();
        List<String> labelKeys = getLabelKeys();

        for (TimeSeries leftSeries : left) {
            TimeSeries matchingRightSeries = findMatchingTimeSeries(right, leftSeries.getLabels(), labelKeys);
            if (matchingRightSeries != null) {
                TimeSeries processedSeries = alignAndProcess(leftSeries, matchingRightSeries);
                if (processedSeries != null) {
                    result.add(processedSeries);
                }
            }
        }

        return result;
    }

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
     * Process samples from left and right time series and return a result sample.
     * This method should be overridden by subclasses to implement their specific logic.
     * Both samples are expected to be non-null and have matching timestamps.
     *
     * @param leftSample The left sample
     * @param rightSample The right sample
     * @return The result sample
     */
    protected abstract Sample processSamples(Sample leftSample, Sample rightSample);

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
}
