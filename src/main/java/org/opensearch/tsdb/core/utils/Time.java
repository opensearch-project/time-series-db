/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for converting between {@link Instant} and numeric timestamps in different time units.
 *
 * <p>This class provides bidirectional conversion between Java's {@link Instant} representation
 * and numeric timestamp values in either milliseconds or nanoseconds since the Unix epoch (January 1, 1970, 00:00:00 UTC).
 *
 * <p>Supported time units:
 * <ul>
 *   <li>{@link TimeUnit#MILLISECONDS} - Standard millisecond precision (loses sub-millisecond data)</li>
 *   <li>{@link TimeUnit#NANOSECONDS} - Full nanosecond precision (preserves all temporal data)</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * // Convert Instant to millisecond timestamp
 * Instant instant = Instant.now();
 * long millisTimestamp = Time.toTimestamp(instant, TimeUnit.MILLISECONDS);
 *
 * // Convert millisecond timestamp back to Instant
 * Instant reconstructed = Time.toInstant(millisTimestamp, TimeUnit.MILLISECONDS);
 *
 * // For full precision, use nanoseconds
 * long nanosTimestamp = Time.toTimestamp(instant, TimeUnit.NANOSECONDS);
 * Instant fullPrecision = Time.toInstant(nanosTimestamp, TimeUnit.NANOSECONDS);
 * }</pre>
 *
 * @see java.time.Instant
 * @see java.util.concurrent.TimeUnit
 */
public class Time {

    /**
     * Private constructor to prevent instantiation of this utility class.
     */
    private Time() {}

    /**
     * Converts an {@link Instant} to a numeric timestamp in the specified time unit.
     *
     * <p>The conversion behavior depends on the time unit:
     * <ul>
     *   <li><b>MILLISECONDS:</b> Converts to milliseconds since epoch. Sub-millisecond precision
     *       from the nanosecond field is included (rounded down to nearest millisecond).</li>
     *   <li><b>NANOSECONDS:</b> Converts to nanoseconds since epoch, preserving full precision.
     *       The result is calculated as {@code (epochSeconds * 1,000,000,000) + nanos}.</li>
     * </ul>
     *
     * <p><b>Precision considerations:</b>
     * <ul>
     *   <li>When using MILLISECONDS, sub-millisecond data is truncated (e.g., 123.456789ms becomes 123ms)</li>
     *   <li>When using NANOSECONDS, full precision is preserved</li>
     * </ul>
     *
     * @param time the {@link Instant} to convert (must not be null)
     * @param timeUnit the target time unit for the timestamp ({@link TimeUnit#MILLISECONDS} or {@link TimeUnit#NANOSECONDS})
     * @return the numeric timestamp in the specified time unit since the Unix epoch
     * @throws UnsupportedOperationException if an unsupported time unit is specified (only MILLISECONDS and NANOSECONDS are supported)
     *
     * @see #toInstant(long, TimeUnit)
     */
    public static long toTimestamp(Instant time, TimeUnit timeUnit) {
        return switch (timeUnit) {
            case TimeUnit.MILLISECONDS -> time.toEpochMilli();
            case TimeUnit.NANOSECONDS -> (time.getEpochSecond() * 1_000_000_000L) + time.getNano();
            default -> throw new UnsupportedOperationException("Unsupported time unit:" + timeUnit);
        };
    }

    /**
     * Converts a numeric timestamp in the specified time unit to an {@link Instant}.
     *
     * <p>The conversion behavior depends on the time unit:
     * <ul>
     *   <li><b>MILLISECONDS:</b> Interprets the timestamp as milliseconds since epoch and creates
     *       an Instant with millisecond precision.</li>
     *   <li><b>NANOSECONDS:</b> Interprets the timestamp as nanoseconds since epoch, preserving
     *       full precision. The epoch seconds and nanosecond adjustment are calculated as
     *       {@code epochSecond = timestamp / 1,000,000,000} and {@code nanoAdjustment = timestamp % 1,000,000,000}.</li>
     * </ul>
     *
     * <p><b>Precision considerations:</b>
     * <ul>
     *   <li>MILLISECONDS: The resulting Instant will have zero sub-millisecond precision</li>
     *   <li>NANOSECONDS: The resulting Instant preserves full nanosecond precision</li>
     * </ul>
     *
     * <p><b>Round-trip guarantees:</b>
     * <ul>
     *   <li>For MILLISECONDS: {@code toInstant(toTimestamp(instant, MILLISECONDS), MILLISECONDS)} preserves millisecond precision</li>
     *   <li>For NANOSECONDS: {@code toInstant(toTimestamp(instant, NANOSECONDS), NANOSECONDS)} preserves full precision</li>
     * </ul>
     *
     * @param timestamp the numeric timestamp in the specified time unit since the Unix epoch
     * @param timeUnit the time unit of the input timestamp ({@link TimeUnit#MILLISECONDS} or {@link TimeUnit#NANOSECONDS})
     * @return an {@link Instant} representing the specified timestamp
     * @throws UnsupportedOperationException if an unsupported time unit is specified (only MILLISECONDS and NANOSECONDS are supported)
     * @throws NullPointerException if {@code timeUnit} is null
     * @throws java.time.DateTimeException if the instant exceeds the maximum or minimum instant (rare for reasonable timestamps)
     *
     * @see #toTimestamp(Instant, TimeUnit)
     */
    public static Instant toInstant(long timestamp, TimeUnit timeUnit) {
        return switch (timeUnit) {
            case TimeUnit.MILLISECONDS -> Instant.ofEpochMilli(timestamp);
            case TimeUnit.NANOSECONDS -> Instant.ofEpochSecond(timestamp / 1_000_000_000, timestamp % 1_000_000_000);
            default -> throw new UnsupportedOperationException("Unsupported time unit:" + timeUnit);
        };
    }
}
