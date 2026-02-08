/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.utils;

import org.apache.lucene.util.BytesRef;
import org.opensearch.index.mapper.RangeFieldMapper;
import org.opensearch.index.mapper.RangeType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Unit tests for TimestampRangeEncoding utility class.
 */
public class TimestampRangeEncodingTests extends OpenSearchTestCase {

    public void testEncodeRange() throws IOException {
        long min = 1234567890123L;
        long max = 9876543210987L;

        BytesRef encoded = TimestampRangeEncoding.encodeRange(min, max);

        assertNotNull(encoded);
        assertEquals(new RangeFieldMapper.Range(RangeType.LONG, min, max, true, true), TimestampRangeEncoding.decodeRange(encoded));
    }

    public void testEncodeRangeWithZero() throws IOException {
        BytesRef encoded = TimestampRangeEncoding.encodeRange(0, 0);
        assertEquals(new RangeFieldMapper.Range(RangeType.LONG, 0L, 0L, true, true), TimestampRangeEncoding.decodeRange(encoded));
    }

    public void testEncodeRangeWithNegative() throws IOException {
        long min = -1000L;
        long max = 1000L;

        BytesRef encoded = TimestampRangeEncoding.encodeRange(min, max);
        assertEquals(new RangeFieldMapper.Range(RangeType.LONG, min, max, true, true), TimestampRangeEncoding.decodeRange(encoded));
    }

    public void testEncodeRangeWithLongMinMax() throws IOException {
        long min = Long.MIN_VALUE;
        long max = Long.MAX_VALUE;

        BytesRef encoded = TimestampRangeEncoding.encodeRange(min, max);
        assertEquals(new RangeFieldMapper.Range(RangeType.LONG, min, max, true, true), TimestampRangeEncoding.decodeRange(encoded));
    }

    public void testDecodeMin() throws IOException {
        long expectedMin = 12345L;
        long expectedMax = 67890L;

        BytesRef encoded = TimestampRangeEncoding.encodeRange(expectedMin, expectedMax);

        assertEquals(
            new RangeFieldMapper.Range(RangeType.LONG, expectedMin, expectedMax, true, true),
            TimestampRangeEncoding.decodeRange(encoded)
        );
    }

    public void testDecodeMax() throws IOException {
        long expectedMin = 12345L;
        long expectedMax = 67890L;

        BytesRef encoded = TimestampRangeEncoding.encodeRange(expectedMin, expectedMax);

        assertEquals(
            new RangeFieldMapper.Range(RangeType.LONG, expectedMin, expectedMax, true, true),
            TimestampRangeEncoding.decodeRange(encoded)
        );
    }

    public void testDecodeMinMax() throws IOException {
        long expectedMin = 100L;
        long expectedMax = 200L;

        BytesRef encoded = TimestampRangeEncoding.encodeRange(expectedMin, expectedMax);

        assertEquals(
            new RangeFieldMapper.Range(RangeType.LONG, expectedMin, expectedMax, true, true),
            TimestampRangeEncoding.decodeRange(encoded)
        );
    }

    public void testEncodeDecodeRoundTrip() throws IOException {
        // Test various ranges
        long[][] testCases = {
            { 0, 100 },
            { 100, 200 },
            { -100, 100 },
            { Long.MIN_VALUE, 0 },
            { 0, Long.MAX_VALUE },
            { Long.MIN_VALUE, Long.MAX_VALUE },
            { 1000000000000L, 2000000000000L },
            { -5000, -1000 } };

        for (long[] testCase : testCases) {
            long min = testCase[0];
            long max = testCase[1];

            BytesRef encoded = TimestampRangeEncoding.encodeRange(min, max);
            assertEquals(new RangeFieldMapper.Range(RangeType.LONG, min, max, true, true), TimestampRangeEncoding.decodeRange(encoded));
        }
    }

    public void testSameValueEncodedTwiceIsIdentical() throws IOException {
        long min = 100L;
        long max = 200L;

        BytesRef encoded1 = TimestampRangeEncoding.encodeRange(min, max);
        BytesRef encoded2 = TimestampRangeEncoding.encodeRange(min, max);

        assertEquals(encoded1.length, encoded2.length);

        for (int i = 0; i < encoded1.length; i++) {
            assertEquals("Byte mismatch at position " + i, encoded1.bytes[encoded1.offset + i], encoded2.bytes[encoded2.offset + i]);
        }
    }

    public void testGetMinMax() throws IOException {
        long expectedMin = 100L;
        long expectedMax = 200L;

        BytesRef encoded = TimestampRangeEncoding.encodeRange(expectedMin, expectedMax);
        long[] minMax = TimestampRangeEncoding.getMinMax(encoded);

        assertEquals(2, minMax.length);
        assertEquals(expectedMin, minMax[0]);
        assertEquals(expectedMax, minMax[1]);
    }
}
