/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.utils;

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for parsing histogram bucket ranges and related operations.
 * Extracted from HistogramPercentileStage to enable reuse across multiple stages.
 */
public final class BucketParsingUtils {

    private BucketParsingUtils() {
        // Utility class - prevent instantiation
    }

    private static final Pattern DURATION_PATTERN = Pattern.compile("(-?\\d+(?:\\.\\d+)?)(ns|us|µs|ms|s|m|h|d)");
    private static final Map<String, Function<Double, Duration>> unitMap = Map.of(
        "ns",
        value -> Duration.ofNanos(Math.round(value)),
        "us",
        value -> Duration.ofNanos(Math.round(value * 1000)), // 1 microsecond = 1000 nanoseconds
        "µs",
        value -> Duration.ofNanos(Math.round(value * 1000)), // 1 microsecond = 1000 nanoseconds
        "ms",
        value -> Duration.ofNanos(Math.round(value * 1_000_000)),
        "s",
        value -> Duration.ofNanos(Math.round(value * 1_000_000_000)),
        "m",
        value -> Duration.ofNanos(Math.round(value * 60_000_000_000L)),
        "h",
        value -> Duration.ofNanos(Math.round(value * 3_600_000_000_000L))
    );

    /**
     * Interface to represent histogram bucket ranges (duration or value based).
     */
    public interface HistogramRange {
        /**
         * Returns the lower bound as double
         */
        double lower();

        /**
         * Returns the upper bound as double
         */
        double upper();
    }

    /**
     * Implementation for value-based histogram ranges (e.g., "10-20").
     */
    public static class HistogramValueRange implements HistogramRange {
        private final double low;
        private final double high;

        public HistogramValueRange(double low, double high) {
            this.low = low;
            this.high = high;
        }

        @Override
        public double lower() {
            return low;
        }

        @Override
        public double upper() {
            return high;
        }
    }

    /**
     * Implementation for duration-based histogram ranges (e.g., "10ms-20ms").
     */
    public static class HistogramDurationRange implements HistogramRange {
        private final Duration low;
        private final Duration high;

        public HistogramDurationRange(Duration low, Duration high) {
            this.low = low;
            this.high = high;
        }

        @Override
        public double lower() {
            // cannot use low.toMillis() directly as that will discard everything under 1ms for small durations
            return low.toNanos() / 1_000_000.0; // Convert to milliseconds
        }

        @Override
        public double upper() {
            // cannot use high.toMillis() directly as that will discard everything under 1ms for small durations
            return high.toNanos() / 1_000_000.0; // Convert to milliseconds
        }
    }

    /**
     * Helper class to represent bucket information for grouping.
     */
    public static class BucketInfo {
        private final String bucketId;
        private final String bucketRange;
        private final HistogramRange parsedRange;

        public BucketInfo(String bucketId, String bucketRange) throws IllegalArgumentException {
            this.bucketId = bucketId;
            this.bucketRange = bucketRange;
            this.parsedRange = parseBucket(bucketRange);
        }

        public double getUpperBound() {
            return parsedRange.upper();
        }

        public double getLowerBound() {
            return parsedRange.lower();
        }

        public String getBucketId() {
            return bucketId;
        }

        public String getBucketRange() {
            return bucketRange;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            BucketInfo that = (BucketInfo) o;
            return bucketId.equals(that.bucketId) && bucketRange.equals(that.bucketRange);
        }

        @Override
        public int hashCode() {
            return bucketId.hashCode() * 31 + bucketRange.hashCode();
        }

        @Override
        public String toString() {
            return "BucketInfo{bucketId='" + bucketId + "', bucketRange='" + bucketRange + "', upper=" + getUpperBound() + "}";
        }
    }

    /**
     * Parse bucket range string into HistogramRange.
     * Supports both duration ranges (e.g., "10ms-20ms") and value ranges (e.g., "10-20").
     */
    public static HistogramRange parseBucket(String bucketRange) throws IllegalArgumentException {
        if (bucketRange == null || bucketRange.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket range cannot be null or empty");
        }

        // Handle single value or infinity cases
        if (bucketRange.equals("infinity") || bucketRange.equals("+Inf")) {
            return new HistogramValueRange(0, Double.POSITIVE_INFINITY);
        }

        // Find the delimiter dash, handling negative numbers correctly
        int delimiterIndex = findDelimiterDash(bucketRange);
        if (delimiterIndex == -1) {
            throw new IllegalArgumentException("Invalid bucket range format: " + bucketRange + ". Expected format: 'low-high'");
        }

        String left = bucketRange.substring(0, delimiterIndex).trim();
        String right = bucketRange.substring(delimiterIndex + 1).trim();

        // Try parsing as duration range first
        try {
            return parseDurationRange(left, right);
        } catch (IllegalArgumentException de) {
            // If duration parsing fails, try value range
            try {
                return parseValueRange(left, right);
            } catch (IllegalArgumentException ve) {
                throw new IllegalArgumentException(
                    "Cannot parse as duration range: " + de.getMessage() + "; cannot parse as value range: " + ve.getMessage()
                );
            }
        }
    }

    /**
     * Parse value range (e.g., "10-20").
     */
    public static HistogramRange parseValueRange(String left, String right) throws IllegalArgumentException {
        try {
            double low;
            double high;
            if ("-Inf".equals(left) || "-infinity".equals(left)) {
                low = Double.NEGATIVE_INFINITY;
            } else {
                low = Double.parseDouble(left);
            }

            if ("infinity".equals(right) || "+Inf".equals(right)) {
                high = low; // For infinity buckets, upper bound equals lower bound
            } else {
                high = Double.parseDouble(right);
                if (high <= low) {
                    throw new IllegalArgumentException("High value " + high + " must exceed low value " + low);
                }
            }

            return new HistogramValueRange(low, high);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Cannot parse value range: " + left + "-" + right, e);
        }
    }

    /**
     * Parse duration range (e.g., "10ms-20ms").
     */
    public static HistogramRange parseDurationRange(String left, String right) throws IllegalArgumentException {
        try {
            Duration low;

            // Handle special cases
            if ("0".equals(left)) {
                low = Duration.ZERO;
            } else if ("-Inf".equals(left) || "-infinity".equals(left)) {
                // negative infinity : use the minimal duration possible (–9223372036854775808 nanoseconds (~–292.47 years))
                low = Duration.ofNanos(Long.MIN_VALUE);
            } else {
                low = parseDuration(left);
            }

            Duration high;
            if ("infinity".equals(right) || "+Inf".equals(right)) {
                // positive infinity : set high=low which is the current m3 behavior
                high = low; // For infinity buckets
            } else {
                high = parseDuration(right);

                if (high.compareTo(low) <= 0) {
                    throw new IllegalArgumentException("High duration " + high + " must exceed low duration " + low);
                }
            }

            return new HistogramDurationRange(low, high);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse duration range: " + left + "-" + right, e);
        }
    }

    /**
     * Parse a duration string supporting both single-unit (e.g., "1ms", "10s") and
     * multi-unit Go-style formats (e.g., "2m11.072s", "1h30m").
     * Supports the specific postfixes: "ns", "us"/"µs", "ms", "s", "m", "h"
     */
    public static Duration parseDuration(String durationString) throws IllegalArgumentException {
        if (durationString == null || durationString.trim().isEmpty()) {
            throw new IllegalArgumentException("Duration string cannot be null or empty");
        }

        // Handle the __ → µ replacement workaround for bad data
        String cleanedDuration = durationString.replace("__", "µ");

        // Find all duration components and validate for Go-compatibility
        Matcher matcher = DURATION_PATTERN.matcher(cleanedDuration);
        Duration total = Duration.ZERO;
        Set<String> seenUnits = new HashSet<>(); // Track units to prevent duplicates
        boolean hasMatches = false;
        int lastEnd = 0;

        while (matcher.find()) {
            hasMatches = true;

            // Check for gaps between matches (invalid characters)
            if (matcher.start() > lastEnd) {
                String gap = cleanedDuration.substring(lastEnd, matcher.start());
                if (!gap.isEmpty()) {
                    throw new IllegalArgumentException("Invalid characters '" + gap + "' in duration: " + durationString);
                }
            }
            lastEnd = matcher.end();

            double value = Double.parseDouble(matcher.group(1));
            String unit = matcher.group(2);

            // Reject duplicate units (Go-compatible behavior)
            if (!seenUnits.add(unit)) {
                throw new IllegalArgumentException("Duplicate time unit '" + unit + "' in duration: " + durationString);
            }

            Function<Double, Duration> durationCreator = unitMap.get(unit);
            if (durationCreator == null) {
                throw new IllegalArgumentException("Unknown time unit: " + unit);
            }

            Duration component = durationCreator.apply(value);
            total = total.plus(component);
        }

        if (!hasMatches) {
            throw new IllegalArgumentException("Invalid duration string format: " + durationString);
        }

        // Validate that entire string was consumed (no leftover characters at the end)
        if (lastEnd < cleanedDuration.length()) {
            String leftover = cleanedDuration.substring(lastEnd);
            if (!leftover.isEmpty()) {
                throw new IllegalArgumentException("Invalid characters '" + leftover + "' in duration: " + durationString);
            }
        }

        return total;
    }

    /**
     * Find the delimiter dash in a bucket range string, properly handling negative numbers.
     * Examples:
     * - "10-20" -> index 2
     * - "-10-5" -> index 3 (skip initial negative sign)
     * - "-10--5" -> index 3 (skip initial negative sign)
     * - "1.5s-2.5s" -> index 4
     */
    private static int findDelimiterDash(String bucketRange) {
        // Start from index 1 to skip potential negative sign at the beginning
        int startIndex = bucketRange.startsWith("-") ? 1 : 0;

        // Look for a dash that's not immediately after a numeric character followed by 'e' or 'E'
        // (to handle scientific notation like 1e-5)
        for (int i = startIndex; i < bucketRange.length(); i++) {
            if (bucketRange.charAt(i) == '-') {
                // Check if this dash is part of scientific notation
                if (i > 0 && (bucketRange.charAt(i - 1) == 'e' || bucketRange.charAt(i - 1) == 'E')) {
                    continue; // Skip scientific notation dashes
                }
                return i; // Found delimiter dash
            }
        }
        return -1; // No delimiter dash found
    }
}
