/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.utils;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.utils.Time;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

public class TimeTests extends OpenSearchTestCase {

    public void testToTimestamp_Milliseconds() {
        assertEquals(0L, Time.toTimestamp(Instant.ofEpochMilli(0), TimeUnit.MILLISECONDS));
        assertEquals(1609459200000L, Time.toTimestamp(Instant.ofEpochMilli(1609459200000L), TimeUnit.MILLISECONDS));
        assertEquals(1609459200500L, Time.toTimestamp(Instant.ofEpochSecond(1609459200L, 500_000_000), TimeUnit.MILLISECONDS));
        assertEquals(-1000L, Time.toTimestamp(Instant.ofEpochMilli(-1000L), TimeUnit.MILLISECONDS));
    }

    public void testToTimestamp_Nanoseconds() {
        assertEquals(0L, Time.toTimestamp(Instant.ofEpochMilli(0), TimeUnit.NANOSECONDS));
        assertEquals(1_123_456_789L, Time.toTimestamp(Instant.ofEpochSecond(1, 123_456_789), TimeUnit.NANOSECONDS));
        assertEquals(1609459200000L * 1_000_000L, Time.toTimestamp(Instant.ofEpochMilli(1609459200000L), TimeUnit.NANOSECONDS));
    }

    public void testToInstant_Milliseconds() {
        assertEquals(Instant.ofEpochMilli(0), Time.toInstant(0L, TimeUnit.MILLISECONDS));
        assertEquals(Instant.ofEpochMilli(1609459200000L), Time.toInstant(1609459200000L, TimeUnit.MILLISECONDS));
        assertEquals(Instant.ofEpochMilli(-1000L), Time.toInstant(-1000L, TimeUnit.MILLISECONDS));
    }

    public void testToInstant_Nanoseconds() {
        assertEquals(Instant.ofEpochMilli(0), Time.toInstant(0L, TimeUnit.NANOSECONDS));
        assertEquals(Instant.ofEpochSecond(1, 123_456_789), Time.toInstant(1_123_456_789L, TimeUnit.NANOSECONDS));
        assertEquals(Instant.ofEpochMilli(1609459200000L), Time.toInstant(1609459200000L * 1_000_000L, TimeUnit.NANOSECONDS));
    }

    public void testRoundTrip_Milliseconds() {
        Instant epoch = Instant.ofEpochMilli(0);
        assertEquals(epoch, Time.toInstant(Time.toTimestamp(epoch, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS));

        Instant now = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        assertEquals(now, Time.toInstant(Time.toTimestamp(now, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS));

        Instant negative = Instant.ofEpochMilli(-86400000L);
        assertEquals(negative, Time.toInstant(Time.toTimestamp(negative, TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS));
    }

    public void testRoundTrip_Nanoseconds() {
        Instant epoch = Instant.ofEpochSecond(0, 0);
        assertEquals(epoch, Time.toInstant(Time.toTimestamp(epoch, TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS));

        Instant withNanos = Instant.ofEpochSecond(1000, 123_456_789);
        assertEquals(withNanos, Time.toInstant(Time.toTimestamp(withNanos, TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS));
    }

    public void testUnsupportedTimeUnit() {
        Instant instant = Instant.ofEpochMilli(1000);
        assertThrows(UnsupportedOperationException.class, () -> Time.toTimestamp(instant, TimeUnit.SECONDS));
        assertThrows(UnsupportedOperationException.class, () -> Time.toInstant(1000L, TimeUnit.MICROSECONDS));
    }
}
