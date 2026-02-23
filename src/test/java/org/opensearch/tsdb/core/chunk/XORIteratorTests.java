/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.chunk;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;

import java.util.List;

/**
 * Unit tests for XORIterator.
 */
public class XORIteratorTests extends OpenSearchTestCase {

    public void testXORIteratorBasicFunctionality() {
        // Create a simple XORChunk with test data
        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();

        // Add some test samples
        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);
        appender.append(3000L, 30.0);

        XORIterator iterator = new XORIterator(chunk.bytes());

        // Test basic iteration
        assertEquals("First sample should be available", ChunkIterator.ValueType.FLOAT, iterator.next());
        ChunkIterator.TimestampValue tv1 = iterator.at();
        assertEquals("First timestamp", 1000L, tv1.timestamp());
        assertEquals("First value", 10.0, tv1.value(), 0.0);

        assertEquals("Second sample should be available", ChunkIterator.ValueType.FLOAT, iterator.next());
        ChunkIterator.TimestampValue tv2 = iterator.at();
        assertEquals("Second timestamp", 2000L, tv2.timestamp());
        assertEquals("Second value", 20.0, tv2.value(), 0.0);

        assertEquals("Third sample should be available", ChunkIterator.ValueType.FLOAT, iterator.next());
        ChunkIterator.TimestampValue tv3 = iterator.at();
        assertEquals("Third timestamp", 3000L, tv3.timestamp());
        assertEquals("Third value", 30.0, tv3.value(), 0.0);

        assertEquals("No more samples", ChunkIterator.ValueType.NONE, iterator.next());
    }

    public void testXORIteratorDecodeSamplesDefaultMethod() {
        // Create a simple XORChunk with test data
        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();

        // Add some test samples
        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);
        appender.append(3000L, 30.0);

        XORIterator iterator = new XORIterator(chunk.bytes());

        // Test the default method
        List<Sample> samples = iterator.decodeSamples(1500L, 2500L).samples().toList();

        assertEquals("Should have 1 sample in range", 1, samples.size());
        assertEquals("Sample timestamp should be 2000L", 2000L, samples.get(0).getTimestamp());
        assertEquals("Sample value should be 20.0", 20.0, ((FloatSample) samples.get(0)).getValue(), 0.0);
    }

    public void testXORIteratorDecodeSamplesFullRange() {
        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();

        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);
        appender.append(3000L, 30.0);

        XORIterator iterator = new XORIterator(chunk.bytes());

        // Test decoding with full range
        List<Sample> samples = iterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList();

        assertEquals(samples, List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0), new FloatSample(3000L, 30.0)));
    }

    public void testXORIteratorDecodeSamplesEmptyRange() {
        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();

        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);
        appender.append(3000L, 30.0);

        XORIterator iterator = new XORIterator(chunk.bytes());

        // Test decoding with no matching range
        List<Sample> samples = iterator.decodeSamples(5000L, 6000L).samples().toList();

        assertTrue("Should have no samples in range", samples.isEmpty());
    }

    public void testXORIteratorDecodeSamplesExactBoundaries() {
        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();

        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);
        appender.append(3000L, 30.0);

        XORIterator iterator = new XORIterator(chunk.bytes());

        // Test decoding with exact boundaries [1000, 3000) - inclusive start, exclusive end
        List<Sample> samples = iterator.decodeSamples(1000L, 3000L).samples().toList();

        // Sample at 3000L is NOT included because maxTimestamp is exclusive
        assertEquals(samples, List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0)));
    }

    public void testXORIteratorDecodeSamplesEmptyChunk() {
        XORChunk chunk = new XORChunk();
        XORIterator iterator = new XORIterator(chunk.bytes());

        List<Sample> samples = iterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList();

        assertTrue("Should have no samples from empty chunk", samples.isEmpty());
    }

    public void testXORIteratorDecodeSamplesSingleSample() {
        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();

        appender.append(1500L, 15.0);

        XORIterator iterator = new XORIterator(chunk.bytes());

        List<Sample> samples = iterator.decodeSamples(1000L, 2000L).samples().toList();

        assertEquals("Should have 1 sample", 1, samples.size());
        assertEquals("Sample timestamp", 1500L, samples.get(0).getTimestamp());
        assertEquals("Sample value", 15.0, ((FloatSample) samples.get(0)).getValue(), 0.0);
    }

    public void testXORIteratorReset() {
        XORChunk chunk1 = new XORChunk();
        XORAppender appender1 = (XORAppender) chunk1.appender();
        appender1.append(1000L, 10.0);
        appender1.append(2000L, 20.0);

        XORChunk chunk2 = new XORChunk();
        XORAppender appender2 = (XORAppender) chunk2.appender();
        appender2.append(3000L, 30.0);
        appender2.append(4000L, 40.0);

        XORIterator iterator = new XORIterator(chunk1.bytes());

        // Test first chunk
        List<Sample> samples1 = iterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(samples1, List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0)));

        // Reset with second chunk
        iterator.reset(chunk2.bytes());

        // Test second chunk
        List<Sample> samples2 = iterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(samples2, List.of(new FloatSample(3000L, 30.0), new FloatSample(4000L, 40.0)));
    }

    public void testXORIteratorErrorHandling() {
        // Test with invalid data
        XORIterator iterator = new XORIterator(new byte[] { 0x01 }); // Invalid data

        // Should handle gracefully
        assertEquals("Should return NONE for invalid data", ChunkIterator.ValueType.NONE, iterator.next());
        assertTrue("Should have no samples from invalid data", iterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList().isEmpty());
    }

    public void testChunkIteratorDecodeSamplesExceptionHandling() {
        // This test is mainly for code coverage of exception handling paths in ChunkIterator.decodeSamples()

        // Helper function to create mock iterators with specific exceptions
        java.util.function.Function<Exception, ChunkIterator> iteratorCreator = (ex) -> new ChunkIterator() {
            @Override
            public ValueType next() {
                return ValueType.NONE;
            }

            @Override
            public TimestampValue at() {
                return new TimestampValue(1000L, 10.0);
            }

            @Override
            public Exception error() {
                return ex;
            }

            @Override
            public int totalSamples() {
                return 1;
            }
        };

        // Test IllegalStateException path
        ChunkIterator illegalStateIterator = iteratorCreator.apply(new IllegalStateException("Test error"));
        assertThrows(IllegalStateException.class, () -> illegalStateIterator.decodeSamples(0L, Long.MAX_VALUE));

        // Test IllegalArgumentException path
        ChunkIterator illegalArgIterator = iteratorCreator.apply(new IllegalArgumentException("Test error"));
        assertThrows(IllegalArgumentException.class, () -> illegalArgIterator.decodeSamples(0L, Long.MAX_VALUE));

        // Test RuntimeException path
        ChunkIterator runtimeIterator = iteratorCreator.apply(new RuntimeException("Test error"));
        assertThrows(RuntimeException.class, () -> runtimeIterator.decodeSamples(0L, Long.MAX_VALUE));
    }

    public void testProcessedSampleCountAllInRange() {
        // Test that processedSampleCount equals returned samples when all are in range
        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();
        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);
        appender.append(3000L, 30.0);

        XORIterator iterator = new XORIterator(chunk.bytes());
        ChunkIterator.DecodeResult result = iterator.decodeSamples(0L, Long.MAX_VALUE);

        assertEquals("Should have 3 samples", 3, result.samples().toList().size());
        assertEquals("Should have processed 3 samples", 3, result.processedSampleCount());
    }

    public void testProcessedSampleCountPartialRange() {
        // Test that processedSampleCount counts all processed samples, not just returned ones
        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();
        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);
        appender.append(3000L, 30.0);
        appender.append(4000L, 40.0);
        appender.append(5000L, 50.0);

        XORIterator iterator = new XORIterator(chunk.bytes());
        // Request range [1500, 3500) - should return only 2000 and 3000
        ChunkIterator.DecodeResult result = iterator.decodeSamples(1500L, 4000L);

        assertEquals("Should have 2 samples in range", 2, result.samples().toList().size());
        assertEquals("Should have processed 4 samples (1000, 2000, 3000, 4000)", 4, result.processedSampleCount());
    }

    public void testProcessedSampleCountBeforeRange() {
        // Test that samples before the range are counted as processed
        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();
        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);
        appender.append(3000L, 30.0);

        XORIterator iterator = new XORIterator(chunk.bytes());
        // Request range starting at 2500 - should skip 1000 and 2000
        ChunkIterator.DecodeResult result = iterator.decodeSamples(2500L, Long.MAX_VALUE);

        assertEquals("Should have 1 sample in range", 1, result.samples().toList().size());
        assertEquals("Should have processed all 3 samples", 3, result.processedSampleCount());
    }

    public void testProcessedSampleCountEmptyResult() {
        // Test that processedSampleCount is correct even when no samples are returned
        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();
        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);
        appender.append(3000L, 30.0);

        XORIterator iterator = new XORIterator(chunk.bytes());
        // Request range with no matching samples
        ChunkIterator.DecodeResult result = iterator.decodeSamples(5000L, 6000L);

        assertEquals("Should have 0 samples in range", 0, result.samples().toList().size());
        assertEquals("Should have processed 3 samples until exhausted", 3, result.processedSampleCount());
    }
}
