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

import java.io.IOException;
import java.util.Set;

/**
 * Utility class for encoding and decoding timestamp ranges to/from binary format.
 * Uses OpenSearch's RangeType.LONG for encoding/decoding to maintain compatibility.
 */
public final class TimestampRangeEncoding {

    private TimestampRangeEncoding() {
        // Utility class - prevent instantiation
    }

    /**
     * Encodes a timestamp range into binary format for efficient storage.
     * Uses OpenSearch's RangeType.LONG encoding which uses VarInt for compact storage.
     *
     * @param min the minimum timestamp (inclusive)
     * @param max the maximum timestamp (inclusive)
     * @return BytesRef containing the encoded range
     */
    public static BytesRef encodeRange(long min, long max) throws IOException {
        return RangeType.LONG.encodeRanges(Set.of(new RangeFieldMapper.Range(RangeType.LONG, min, max, true, true)));
    }

    /**
     * Decodes a range of long values encoded using encodeRange.
     *
     * @param bytes the byte array containing the encoded long range
     * @return the decoded long range
     */
    public static RangeFieldMapper.Range decodeRange(BytesRef bytes) throws IOException {
        return RangeType.LONG.decodeRanges(bytes).getFirst();
    }

    /**
     * Extracts both min and max timestamps from an encoded range.
     *
     * @param bytes the byte array containing the encoded long range
     * @return array with [min, max] timestamps
     */
    public static long[] getMinMax(BytesRef bytes) throws IOException {
        RangeFieldMapper.Range range = decodeRange(bytes);
        long min = ((Number) range.getFrom()).longValue();
        long max = ((Number) range.getTo()).longValue();
        return new long[] { min, max };
    }
}
