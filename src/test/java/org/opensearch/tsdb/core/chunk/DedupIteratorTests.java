/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.chunk;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.Sample;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for DedupIterator.
 */
public class DedupIteratorTests extends OpenSearchTestCase {

    /**
     * Test iterator that returns a predefined list of samples.
     * Useful for testing deduplication since XORAppender doesn't support duplicate timestamps.
     */
    private static class TestIterator implements ChunkIterator {
        private final List<ChunkIterator.TimestampValue> samples;
        private int currentIndex = -1;
        private final Exception error;

        TestIterator(List<ChunkIterator.TimestampValue> samples) {
            this(samples, null);
        }

        TestIterator(List<ChunkIterator.TimestampValue> samples, Exception error) {
            this.samples = samples;
            this.error = error;
        }

        @Override
        public ValueType next() {
            if (error != null) {
                return ValueType.NONE;
            }
            currentIndex++;
            if (currentIndex >= samples.size()) {
                return ValueType.NONE;
            }
            return ValueType.FLOAT;
        }

        @Override
        public ChunkIterator.TimestampValue at() {
            if (currentIndex < 0 || currentIndex >= samples.size()) {
                return null;
            }
            return samples.get(currentIndex);
        }

        @Override
        public Exception error() {
            return error;
        }

        @Override
        public int totalSamples() {
            return error != null ? -1 : samples.size();
        }
    }

    private static List<ChunkIterator.TimestampValue> createSamples(long... timestampsAndValues) {
        if (timestampsAndValues.length % 2 != 0) {
            throw new IllegalArgumentException("Must provide pairs of timestamp and value");
        }
        List<ChunkIterator.TimestampValue> result = new ArrayList<>();
        for (int i = 0; i < timestampsAndValues.length; i += 2) {
            result.add(new ChunkIterator.TimestampValue(timestampsAndValues[i], (double) timestampsAndValues[i + 1]));
        }
        return result;
    }

    public void testFirstPolicySkipsDuplicates() {
        // Create iterator with duplicate timestamps
        List<ChunkIterator.TimestampValue> samples = createSamples(1000L, 10L, 1000L, 11L, 1000L, 12L, 2000L, 20L, 2000L, 21L, 3000L, 30L);

        DedupIterator dedup = new DedupIterator(new TestIterator(samples), DedupIterator.DuplicatePolicy.FIRST);

        List<Sample> result = dedup.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(3, result.size());

        // Should keep only first value for each timestamp
        assertEquals(1000L, result.get(0).getTimestamp());
        assertEquals(10.0, result.get(0).getValue(), 0.0);
        assertEquals(2000L, result.get(1).getTimestamp());
        assertEquals(20.0, result.get(1).getValue(), 0.0);
        assertEquals(3000L, result.get(2).getTimestamp());
        assertEquals(30.0, result.get(2).getValue(), 0.0);
    }

    public void testLastPolicyKeepsLastValue() {
        // Create iterator with duplicate timestamps
        List<ChunkIterator.TimestampValue> samples = createSamples(1000L, 10L, 1000L, 11L, 1000L, 12L, 2000L, 20L, 2000L, 21L, 3000L, 30L);

        DedupIterator dedup = new DedupIterator(new TestIterator(samples), DedupIterator.DuplicatePolicy.LAST);

        List<Sample> result = dedup.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(3, result.size());

        // Should keep only last value for each timestamp
        assertEquals(1000L, result.get(0).getTimestamp());
        assertEquals(12.0, result.get(0).getValue(), 0.0);
        assertEquals(2000L, result.get(1).getTimestamp());
        assertEquals(21.0, result.get(1).getValue(), 0.0);
        assertEquals(3000L, result.get(2).getTimestamp());
        assertEquals(30.0, result.get(2).getValue(), 0.0);
        assertEquals(ChunkIterator.ValueType.NONE, dedup.next());
    }

    public void testNoDuplicatesPassThrough() {
        // Create iterator with no duplicates
        List<ChunkIterator.TimestampValue> samples = createSamples(1000L, 10L, 2000L, 20L, 3000L, 30L);

        DedupIterator dedupFirst = new DedupIterator(new TestIterator(samples), DedupIterator.DuplicatePolicy.FIRST);

        List<Sample> result = dedupFirst.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(3, result.size());

        for (int i = 0; i < 3; i++) {
            assertEquals((i + 1) * 1000L, result.get(i).getTimestamp());
            assertEquals((i + 1) * 10.0, result.get(i).getValue(), 0.0);
        }
    }

    public void testAllDuplicatesFirstPolicy() {
        // All values have the same timestamp
        List<ChunkIterator.TimestampValue> samples = createSamples(1000L, 10L, 1000L, 20L, 1000L, 30L, 1000L, 40L, 1000L, 50L);

        DedupIterator dedup = new DedupIterator(new TestIterator(samples), DedupIterator.DuplicatePolicy.FIRST);

        List<Sample> result = dedup.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(1, result.size());
        assertEquals(1000L, result.get(0).getTimestamp());
        assertEquals(10.0, result.get(0).getValue(), 0.0);
    }

    public void testAllDuplicatesLastPolicy() {
        // All values have the same timestamp
        List<ChunkIterator.TimestampValue> samples = createSamples(1000L, 10L, 1000L, 20L, 1000L, 30L, 1000L, 40L, 1000L, 50L);

        DedupIterator dedup = new DedupIterator(new TestIterator(samples), DedupIterator.DuplicatePolicy.LAST);

        List<Sample> result = dedup.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(1, result.size());
        assertEquals(1000L, result.get(0).getTimestamp());
        assertEquals(50.0, result.get(0).getValue(), 0.0);  // Last value
    }

    public void testInterleavedDuplicatesFirstPolicy() {
        List<ChunkIterator.TimestampValue> samples = createSamples(
            1000L,
            10L,
            1000L,
            11L,
            2000L,
            20L,
            3000L,
            30L,
            3000L,
            31L,
            3000L,
            32L,
            4000L,
            40L
        );

        DedupIterator dedup = new DedupIterator(new TestIterator(samples), DedupIterator.DuplicatePolicy.FIRST);

        List<Sample> result = dedup.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(4, result.size());
        assertEquals(1000L, result.get(0).getTimestamp());
        assertEquals(10.0, result.get(0).getValue(), 0.0);
        assertEquals(2000L, result.get(1).getTimestamp());
        assertEquals(20.0, result.get(1).getValue(), 0.0);
        assertEquals(3000L, result.get(2).getTimestamp());
        assertEquals(30.0, result.get(2).getValue(), 0.0);
        assertEquals(4000L, result.get(3).getTimestamp());
        assertEquals(40.0, result.get(3).getValue(), 0.0);
    }

    public void testWithMergeIteratorFirstPolicy() {
        // Test integration with MergeIterator to ensure stable sorting + dedup works
        XORChunk chunk1 = new XORChunk();
        XORAppender appender1 = (XORAppender) chunk1.appender();
        appender1.append(1000L, 10.0);
        appender1.append(2000L, 20.0);

        XORChunk chunk2 = new XORChunk();
        XORAppender appender2 = (XORAppender) chunk2.appender();
        appender2.append(1000L, 100.0);
        appender2.append(2000L, 200.0);

        XORChunk chunk3 = new XORChunk();
        XORAppender appender3 = (XORAppender) chunk3.appender();
        appender3.append(1000L, 1000.0);
        appender3.append(3000L, 3000.0);

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(new XORIterator(chunk1.bytes()));
        iterators.add(new XORIterator(chunk2.bytes()));
        iterators.add(new XORIterator(chunk3.bytes()));

        MergeIterator merged = new MergeIterator(iterators);
        DedupIterator dedup = new DedupIterator(merged, DedupIterator.DuplicatePolicy.FIRST);

        List<Sample> samples = dedup.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(3, samples.size());

        // Should keep first value from each timestamp (stable sort ensures chunk1, chunk2, chunk3 order)
        assertEquals(1000L, samples.get(0).getTimestamp());
        assertEquals(10.0, samples.get(0).getValue(), 0.0);  // From chunk1 (index 0)
        assertEquals(2000L, samples.get(1).getTimestamp());
        assertEquals(20.0, samples.get(1).getValue(), 0.0);  // From chunk1 (index 0)
        assertEquals(3000L, samples.get(2).getTimestamp());
        assertEquals(3000.0, samples.get(2).getValue(), 0.0);  // From chunk3 (only one)
    }

    public void testErrorPropagation() {
        // Test that errors from underlying iterator are propagated
        Exception testError = new RuntimeException("Test error");
        List<ChunkIterator.TimestampValue> samples = createSamples(1000L, 10L);

        DedupIterator dedup = new DedupIterator(new TestIterator(samples, testError), DedupIterator.DuplicatePolicy.FIRST);

        assertNotNull(dedup.error());
        assertEquals("Test error", dedup.error().getMessage());
    }

    /**
     * Test iterator that can return an error after returning some values.
     */
    private static class ErrorAfterNIterator implements ChunkIterator {
        private final List<ChunkIterator.TimestampValue> samples;
        private int currentIndex = -1;
        private final int errorAfterIndex;
        private Exception error;

        ErrorAfterNIterator(List<ChunkIterator.TimestampValue> samples, int errorAfterIndex) {
            this.samples = samples;
            this.errorAfterIndex = errorAfterIndex;
        }

        @Override
        public ValueType next() {
            currentIndex++;
            if (currentIndex > errorAfterIndex) {
                error = new RuntimeException("Error after index " + errorAfterIndex);
                return ValueType.NONE;
            }
            if (currentIndex >= samples.size()) {
                return ValueType.NONE;
            }
            return ValueType.FLOAT;
        }

        @Override
        public ChunkIterator.TimestampValue at() {
            if (currentIndex < 0 || currentIndex >= samples.size()) {
                return null;
            }
            return samples.get(currentIndex);
        }

        @Override
        public Exception error() {
            return error;
        }

        @Override
        public int totalSamples() {
            return samples.size();
        }
    }

    public void testErrorDuringIterationFirstPolicy() {
        // Create iterator with duplicates that will error after 2 values
        List<ChunkIterator.TimestampValue> samples = createSamples(1000L, 10L, 1000L, 11L, 2000L, 20L, 3000L, 30L);

        DedupIterator dedup = new DedupIterator(new ErrorAfterNIterator(samples, 1), DedupIterator.DuplicatePolicy.FIRST);

        // Should get first value
        assertEquals(ChunkIterator.ValueType.FLOAT, dedup.next());
        assertEquals(1000L, dedup.at().timestamp());
        assertEquals(10.0, dedup.at().value(), 0.0);

        // Second next() should detect the error and return NONE
        assertEquals(ChunkIterator.ValueType.NONE, dedup.next());

        // Error should be available
        assertNotNull(dedup.error());
        assertTrue(dedup.error().getMessage().contains("Error after index"));
    }

    public void testErrorDuringIterationLastPolicy() {
        // Create iterator with duplicates that will error after 2 values
        List<ChunkIterator.TimestampValue> samples = createSamples(1000L, 10L, 1000L, 11L, 2000L, 20L, 3000L, 30L);

        DedupIterator dedup = new DedupIterator(new ErrorAfterNIterator(samples, 1), DedupIterator.DuplicatePolicy.LAST);

        // Should get first deduplicated value (last of duplicates at 1000)
        assertEquals(ChunkIterator.ValueType.FLOAT, dedup.next());
        assertEquals(1000L, dedup.at().timestamp());
        assertEquals(11.0, dedup.at().value(), 0.0);

        // Second next() should detect the error and return NONE
        assertEquals(ChunkIterator.ValueType.NONE, dedup.next());

        // Error should be available
        assertNotNull(dedup.error());
        assertTrue(dedup.error().getMessage().contains("Error after index"));
    }

    public void testErrorDuringDecodeSamplesFirstPolicy() {
        // Test that decodeSamples properly throws exception when error occurs
        List<ChunkIterator.TimestampValue> samples = createSamples(1000L, 10L, 2000L, 20L, 3000L, 30L, 4000L, 40L);

        DedupIterator dedup = new DedupIterator(new ErrorAfterNIterator(samples, 1), DedupIterator.DuplicatePolicy.FIRST);

        // decodeSamples should throw the error
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> dedup.decodeSamples(0L, Long.MAX_VALUE));

        assertTrue(thrown.getMessage().contains("Error during chunk iteration") || thrown.getMessage().contains("Error after index"));
    }

    public void testNullIteratorThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> new DedupIterator(null, DedupIterator.DuplicatePolicy.FIRST));
    }

    public void testNullPolicyThrowsException() {
        List<ChunkIterator.TimestampValue> samples = createSamples(1000L, 10L);

        assertThrows(IllegalArgumentException.class, () -> new DedupIterator(new TestIterator(samples), null));
    }

    public void testTotalSamplesReturnsUnknown() {
        // DedupIterator can't predict sample count since duplicates are unknown
        List<ChunkIterator.TimestampValue> samples = createSamples(1000L, 10L, 1000L, 11L, 2000L, 20L);

        DedupIterator dedup = new DedupIterator(new TestIterator(samples), DedupIterator.DuplicatePolicy.FIRST);

        assertEquals(-1, dedup.totalSamples());
    }

    public void testEmptyIterator() {
        List<ChunkIterator.TimestampValue> samples = new ArrayList<>();

        DedupIterator dedup = new DedupIterator(new TestIterator(samples), DedupIterator.DuplicatePolicy.FIRST);

        List<Sample> result = dedup.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(0, result.size());
    }
}
