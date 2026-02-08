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
 * Unit tests for MultiIterator.
 */
public class MergeIteratorTests extends OpenSearchTestCase {

    public void testMultiIteratorBasicMerge() {
        // Create two chunks with interleaved timestamps
        XORChunk chunk1 = new XORChunk();
        XORAppender appender1 = (XORAppender) chunk1.appender();
        appender1.append(1000L, 10.0);
        appender1.append(3000L, 30.0);
        appender1.append(5000L, 50.0);

        XORChunk chunk2 = new XORChunk();
        XORAppender appender2 = (XORAppender) chunk2.appender();
        appender2.append(2000L, 20.0);
        appender2.append(4000L, 40.0);
        appender2.append(6000L, 60.0);

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(new XORIterator(chunk1.bytes()));
        iterators.add(new XORIterator(chunk2.bytes()));

        MergeIterator multiIterator = new MergeIterator(iterators);

        // Verify merged order
        long[] expectedTimestamps = { 1000L, 2000L, 3000L, 4000L, 5000L, 6000L };
        double[] expectedValues = { 10.0, 20.0, 30.0, 40.0, 50.0, 60.0 };

        for (int i = 0; i < expectedTimestamps.length; i++) {
            assertEquals(ChunkIterator.ValueType.FLOAT, multiIterator.next());
            assertEquals(expectedTimestamps[i], multiIterator.at().timestamp());
            assertEquals(expectedValues[i], multiIterator.at().value(), 0.0);
        }

        assertEquals(ChunkIterator.ValueType.NONE, multiIterator.next());
    }

    public void testMultiIteratorThreeWayMerge() {
        // Create three chunks with interleaved timestamps
        XORChunk chunk1 = new XORChunk();
        XORAppender appender1 = (XORAppender) chunk1.appender();
        appender1.append(1000L, 10.0);
        appender1.append(4000L, 40.0);

        XORChunk chunk2 = new XORChunk();
        XORAppender appender2 = (XORAppender) chunk2.appender();
        appender2.append(2000L, 20.0);
        appender2.append(5000L, 50.0);

        XORChunk chunk3 = new XORChunk();
        XORAppender appender3 = (XORAppender) chunk3.appender();
        appender3.append(3000L, 30.0);
        appender3.append(6000L, 60.0);

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(new XORIterator(chunk1.bytes()));
        iterators.add(new XORIterator(chunk2.bytes()));
        iterators.add(new XORIterator(chunk3.bytes()));

        MergeIterator multiIterator = new MergeIterator(iterators);

        // Verify all values are in order
        List<Sample> samples = multiIterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(6, samples.size());
        assertEquals(1000L, samples.get(0).getTimestamp());
        assertEquals(2000L, samples.get(1).getTimestamp());
        assertEquals(3000L, samples.get(2).getTimestamp());
        assertEquals(4000L, samples.get(3).getTimestamp());
        assertEquals(5000L, samples.get(4).getTimestamp());
        assertEquals(6000L, samples.get(5).getTimestamp());
    }

    public void testMultiIteratorSingleIterator() {
        // Test with just one iterator
        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();
        appender.append(1000L, 10.0);
        appender.append(2000L, 20.0);

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(new XORIterator(chunk.bytes()));

        MergeIterator multiIterator = new MergeIterator(iterators);

        List<Sample> samples = multiIterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(2, samples.size());
        assertEquals(1000L, samples.get(0).getTimestamp());
        assertEquals(2000L, samples.get(1).getTimestamp());
    }

    public void testMultiIteratorWithEmptyIterator() {
        // Create one chunk with data and one empty chunk
        XORChunk chunk1 = new XORChunk();
        XORAppender appender1 = (XORAppender) chunk1.appender();
        appender1.append(1000L, 10.0);
        appender1.append(2000L, 20.0);

        XORChunk chunk2 = new XORChunk(); // Empty chunk

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(new XORIterator(chunk1.bytes()));
        iterators.add(new XORIterator(chunk2.bytes()));

        MergeIterator multiIterator = new MergeIterator(iterators);

        List<Sample> samples = multiIterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(2, samples.size());
        assertEquals(1000L, samples.get(0).getTimestamp());
        assertEquals(2000L, samples.get(1).getTimestamp());
    }

    public void testMultiIteratorTotalSamples() {
        // Test that totalSamples is correctly aggregated
        XORChunk chunk1 = new XORChunk();
        XORAppender appender1 = (XORAppender) chunk1.appender();
        appender1.append(1000L, 10.0);
        appender1.append(2000L, 20.0);

        XORChunk chunk2 = new XORChunk();
        XORAppender appender2 = (XORAppender) chunk2.appender();
        appender2.append(3000L, 30.0);
        appender2.append(4000L, 40.0);
        appender2.append(5000L, 50.0);

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(new XORIterator(chunk1.bytes()));
        iterators.add(new XORIterator(chunk2.bytes()));

        MergeIterator multiIterator = new MergeIterator(iterators);

        assertEquals(5, multiIterator.totalSamples());
    }

    public void testMultiIteratorNullIteratorHandling() {
        // Test that null iterators in the list are skipped
        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();
        appender.append(1000L, 10.0);

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(null);
        iterators.add(new XORIterator(chunk.bytes()));
        iterators.add(null);

        MergeIterator multiIterator = new MergeIterator(iterators);

        List<Sample> samples = multiIterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(1, samples.size());
        assertEquals(1000L, samples.get(0).getTimestamp());
    }

    public void testMultiIteratorNullListThrowsException() {
        // Test that null list throws exception
        assertThrows(IllegalArgumentException.class, () -> new MergeIterator(null));
    }

    public void testMultiIteratorEmptyListThrowsException() {
        // Test that empty list throws exception
        assertThrows(IllegalArgumentException.class, () -> new MergeIterator(new ArrayList<>()));
    }

    public void testMultiIteratorDecodeSamplesWithRange() {
        // Test decodeSamples with a specific range
        XORChunk chunk1 = new XORChunk();
        XORAppender appender1 = (XORAppender) chunk1.appender();
        appender1.append(1000L, 10.0);
        appender1.append(3000L, 30.0);

        XORChunk chunk2 = new XORChunk();
        XORAppender appender2 = (XORAppender) chunk2.appender();
        appender2.append(2000L, 20.0);
        appender2.append(4000L, 40.0);

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(new XORIterator(chunk1.bytes()));
        iterators.add(new XORIterator(chunk2.bytes()));

        MergeIterator multiIterator = new MergeIterator(iterators);

        List<Sample> samples = multiIterator.decodeSamples(1500L, 3500L).samples().toList();
        assertEquals(2, samples.size());
        assertEquals(2000L, samples.get(0).getTimestamp());
        assertEquals(3000L, samples.get(1).getTimestamp());
    }

    public void testMultiIteratorErrorHandling() {
        // Create a mock iterator that returns an error
        ChunkIterator errorIterator = new ChunkIterator() {
            private final Exception error = new RuntimeException("Test error");

            @Override
            public ValueType next() {
                return ValueType.NONE;
            }

            @Override
            public TimestampValue at() {
                return null;
            }

            @Override
            public Exception error() {
                return error;
            }
        };

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(errorIterator);

        MergeIterator multiIterator = new MergeIterator(iterators);

        // Should have captured the error during initialization
        assertNotNull(multiIterator.error());
        assertEquals(ChunkIterator.ValueType.NONE, multiIterator.next());
    }

    public void testErrorDuringInitialAdvance() {
        // Test error that occurs during the initial advance() call in constructor
        ChunkIterator errorIterator = new ChunkIterator() {
            private boolean firstCall = true;
            private final Exception error = new RuntimeException("Error during advance");

            @Override
            public ValueType next() {
                if (firstCall) {
                    firstCall = false;
                    return ValueType.FLOAT;
                }
                return ValueType.NONE;
            }

            @Override
            public TimestampValue at() {
                return new TimestampValue(1000L, 10.0);
            }

            @Override
            public Exception error() {
                // Return error only after first next() call
                return firstCall ? null : error;
            }
        };

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(errorIterator);

        MergeIterator multiIterator = new MergeIterator(iterators);

        // Should have captured the error during initialization
        assertNotNull(multiIterator.error());
        assertEquals("Error during advance", multiIterator.error().getMessage());
    }

    public void testErrorDuringNextWithTwoIterators() {
        // Test error handling in the k=2 optimized path
        // Note: next() is called once during initialization, then again on each multiIterator.next()
        ChunkIterator errorIterator = new ChunkIterator() {
            private int callCount = 0;
            private final Exception error = new RuntimeException("Error during iteration");

            @Override
            public ValueType next() {
                callCount++;
                if (callCount <= 3) {  // Allow 3 calls: 1 for init, 2 for next()
                    return ValueType.FLOAT;
                }
                return ValueType.NONE;
            }

            @Override
            public TimestampValue at() {
                // Use small timestamps to ensure this iterator is selected
                return new TimestampValue(callCount * 100L, callCount * 10.0);
            }

            @Override
            public Exception error() {
                // Return error after third call to simulate error during advance
                return callCount > 3 ? error : null;
            }
        };

        XORChunk chunk = new XORChunk();
        XORAppender appender = (XORAppender) chunk.appender();
        appender.append(5000L, 50.0);
        appender.append(6000L, 60.0);

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(errorIterator);
        iterators.add(new XORIterator(chunk.bytes()));

        MergeIterator multiIterator = new MergeIterator(iterators);

        // First call should succeed (returns timestamp 100 from init, then advances to 200)
        assertEquals(ChunkIterator.ValueType.FLOAT, multiIterator.next());
        assertEquals(100L, multiIterator.at().timestamp());
        assertNull(multiIterator.error());

        // Second call should succeed (returns timestamp 200, then advances to 300)
        assertEquals(ChunkIterator.ValueType.FLOAT, multiIterator.next());
        assertEquals(200L, multiIterator.at().timestamp());
        assertNull(multiIterator.error());

        // Third call returns 300, then tries to advance which triggers the error
        assertEquals(ChunkIterator.ValueType.FLOAT, multiIterator.next());
        assertEquals(300L, multiIterator.at().timestamp());
        assertNotNull(multiIterator.error());
        assertEquals("Error during iteration", multiIterator.error().getMessage());
    }

    public void testErrorDuringNextWithMultipleIterators() {
        // Test error handling in the k>=3 heap path
        // Note: next() is called once during initialization, then again on each multiIterator.next()
        ChunkIterator errorIterator = new ChunkIterator() {
            private int callCount = 0;
            private final Exception error = new RuntimeException("Heap error during iteration");

            @Override
            public ValueType next() {
                callCount++;
                if (callCount <= 3) {  // Allow 3 calls: 1 for init, 2 for next()
                    return ValueType.FLOAT;
                }
                return ValueType.NONE;
            }

            @Override
            public TimestampValue at() {
                // Use timestamps that ensure this iterator stays at the top
                return new TimestampValue(callCount * 100L, callCount * 10.0);
            }

            @Override
            public Exception error() {
                // Return error after third call
                return callCount > 3 ? error : null;
            }
        };

        XORChunk chunk1 = new XORChunk();
        XORAppender appender1 = (XORAppender) chunk1.appender();
        appender1.append(5000L, 50.0);
        appender1.append(6000L, 60.0);

        XORChunk chunk2 = new XORChunk();
        XORAppender appender2 = (XORAppender) chunk2.appender();
        appender2.append(7000L, 70.0);
        appender2.append(8000L, 80.0);

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(errorIterator);
        iterators.add(new XORIterator(chunk1.bytes()));
        iterators.add(new XORIterator(chunk2.bytes()));

        MergeIterator multiIterator = new MergeIterator(iterators);

        // First call should succeed (returns timestamp 100 from init, then advances to 200)
        assertEquals(ChunkIterator.ValueType.FLOAT, multiIterator.next());
        assertEquals(100L, multiIterator.at().timestamp());
        assertNull(multiIterator.error());

        // Second call should succeed (returns timestamp 200, then advances to 300)
        assertEquals(ChunkIterator.ValueType.FLOAT, multiIterator.next());
        assertEquals(200L, multiIterator.at().timestamp());
        assertNull(multiIterator.error());

        // Third call returns 300, then tries to advance which triggers the error
        assertEquals(ChunkIterator.ValueType.FLOAT, multiIterator.next());
        assertEquals(300L, multiIterator.at().timestamp());
        assertNotNull(multiIterator.error());
        assertEquals("Heap error during iteration", multiIterator.error().getMessage());
    }

    public void testErrorWithNullIteratorMixed() {
        // Test error handling when null iterators are mixed with error iterators
        ChunkIterator errorIterator = new ChunkIterator() {
            private final Exception error = new RuntimeException("Mixed null error");

            @Override
            public ValueType next() {
                return ValueType.NONE;
            }

            @Override
            public TimestampValue at() {
                return null;
            }

            @Override
            public Exception error() {
                return error;
            }
        };

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(null);
        iterators.add(errorIterator);
        iterators.add(null);

        MergeIterator multiIterator = new MergeIterator(iterators);

        // Should have captured the error even with null iterators
        assertNotNull(multiIterator.error());
        assertEquals("Mixed null error", multiIterator.error().getMessage());
    }

    public void testHeapSiftUpWithDescendingTimestamps() {
        // Test the siftUp logic by creating iterators with descending initial timestamps
        // This forces the heap to perform swaps during initialization
        XORChunk chunk1 = new XORChunk();
        XORAppender appender1 = (XORAppender) chunk1.appender();
        appender1.append(1000L, 10.0);
        appender1.append(2000L, 20.0);

        XORChunk chunk2 = new XORChunk();
        XORAppender appender2 = (XORAppender) chunk2.appender();
        appender2.append(500L, 5.0);
        appender2.append(1500L, 15.0);

        XORChunk chunk3 = new XORChunk();
        XORAppender appender3 = (XORAppender) chunk3.appender();
        appender3.append(300L, 3.0);
        appender3.append(800L, 8.0);

        XORChunk chunk4 = new XORChunk();
        XORAppender appender4 = (XORAppender) chunk4.appender();
        appender4.append(100L, 1.0);
        appender4.append(600L, 6.0);

        // Add iterators in descending order of first timestamp
        // This triggers siftUp to perform swaps: 1000 > 500 > 300 > 100
        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(new XORIterator(chunk1.bytes()));
        iterators.add(new XORIterator(chunk2.bytes()));
        iterators.add(new XORIterator(chunk3.bytes()));
        iterators.add(new XORIterator(chunk4.bytes()));

        MergeIterator multiIterator = new MergeIterator(iterators);

        // Verify the merge produces correctly sorted output
        List<Sample> samples = multiIterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(8, samples.size());

        // Verify timestamps are in ascending order
        long[] expectedTimestamps = { 100L, 300L, 500L, 600L, 800L, 1000L, 1500L, 2000L };
        double[] expectedValues = { 1.0, 3.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0 };

        for (int i = 0; i < expectedTimestamps.length; i++) {
            assertEquals(expectedTimestamps[i], samples.get(i).getTimestamp());
            assertEquals(expectedValues[i], samples.get(i).getValue(), 0.0);
        }
    }

    public void testHeapSiftUpWithMultipleSwaps() {
        // Test siftUp with a scenario that requires multiple levels of swapping
        // Create 7 iterators to build a deeper heap (requires 3 levels)
        List<ChunkIterator> iterators = new ArrayList<>();

        // Create chunks in descending timestamp order to trigger heap siftUp
        for (int i = 7; i >= 1; i--) {
            XORChunk chunk = new XORChunk();
            XORAppender appender = (XORAppender) chunk.appender();
            appender.append(i * 1000L, i * 10.0);
            iterators.add(new XORIterator(chunk.bytes()));
        }

        MergeIterator multiIterator = new MergeIterator(iterators);

        // Verify correct order
        for (int i = 1; i <= 7; i++) {
            assertEquals(ChunkIterator.ValueType.FLOAT, multiIterator.next());
            assertEquals(i * 1000L, multiIterator.at().timestamp());
        }

        assertEquals(ChunkIterator.ValueType.NONE, multiIterator.next());
    }

    public void testStableSortingWithFourIteratorsAllDuplicates() {
        // Test stable sorting with k=4: all iterators have same timestamps
        List<ChunkIterator> iterators = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            XORChunk chunk = new XORChunk();
            XORAppender appender = (XORAppender) chunk.appender();
            appender.append(1000L, i * 1.0);
            appender.append(2000L, i + 10.0);
            iterators.add(new XORIterator(chunk.bytes()));
        }

        MergeIterator multiIterator = new MergeIterator(iterators);

        List<Sample> samples = multiIterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(8, samples.size());

        // Timestamp 1000: should be in order 1.0, 2.0, 3.0, 4.0
        for (int i = 0; i < 4; i++) {
            assertEquals(1000L, samples.get(i).getTimestamp());
            assertEquals((i + 1) * 1.0, samples.get(i).getValue(), 0.0);
        }

        // Timestamp 2000: should be in order 11.0, 12.0, 13.0, 14.0
        for (int i = 0; i < 4; i++) {
            assertEquals(2000L, samples.get(i + 4).getTimestamp());
            assertEquals((i + 1) + 10.0, samples.get(i + 4).getValue(), 0.0);
        }
    }

    public void testStableSortingDuringSiftUp() {
        // Test the siftUp break condition: timestamp == parentEntry.timestamp && entryIndex >= parentEntry.index
        // When all iterators have the same timestamp, siftUp must maintain stable order
        List<ChunkIterator> iterators = new ArrayList<>();
        for (int i = 1; i <= 4; i++) {
            XORChunk chunk = new XORChunk();
            XORAppender appender = (XORAppender) chunk.appender();
            appender.append(1000L, i * 10.0);
            iterators.add(new XORIterator(chunk.bytes()));
        }

        MergeIterator multiIterator = new MergeIterator(iterators);

        List<Sample> samples = multiIterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(4, samples.size());

        // Should emit in stable order: index 0, 1, 2, 3
        for (int i = 0; i < 4; i++) {
            assertEquals(1000L, samples.get(i).getTimestamp());
            assertEquals((i + 1) * 10.0, samples.get(i).getValue(), 0.0);
        }
    }

    public void testStableSortingDuringSiftDown() {
        // Test stable sorting when siftDown is triggered after advancement
        // After emitting and advancing an iterator, siftDown with duplicate timestamps must maintain stable order
        XORChunk chunk1 = new XORChunk();
        XORAppender appender1 = (XORAppender) chunk1.appender();
        appender1.append(1000L, 10.0);
        appender1.append(3000L, 30.0);

        XORChunk chunk2 = new XORChunk();
        XORAppender appender2 = (XORAppender) chunk2.appender();
        appender2.append(2000L, 20.0);
        appender2.append(3000L, 31.0);

        XORChunk chunk3 = new XORChunk();
        XORAppender appender3 = (XORAppender) chunk3.appender();
        appender3.append(2000L, 21.0);
        appender3.append(3000L, 32.0);

        List<ChunkIterator> iterators = new ArrayList<>();
        iterators.add(new XORIterator(chunk1.bytes()));
        iterators.add(new XORIterator(chunk2.bytes()));
        iterators.add(new XORIterator(chunk3.bytes()));

        MergeIterator multiIterator = new MergeIterator(iterators);

        List<Sample> samples = multiIterator.decodeSamples(0L, Long.MAX_VALUE).samples().toList();
        assertEquals(6, samples.size());

        // 1000 from chunk1
        assertEquals(1000L, samples.get(0).getTimestamp());
        assertEquals(10.0, samples.get(0).getValue(), 0.0);

        // 2000 from chunk2, then chunk3 (stable order)
        assertEquals(2000L, samples.get(1).getTimestamp());
        assertEquals(20.0, samples.get(1).getValue(), 0.0);
        assertEquals(2000L, samples.get(2).getTimestamp());
        assertEquals(21.0, samples.get(2).getValue(), 0.0);

        // 3000 from chunk1, chunk2, chunk3 (stable order)
        for (int i = 0; i < 3; i++) {
            assertEquals(3000L, samples.get(i + 3).getTimestamp());
            assertEquals(30.0 + i, samples.get(i + 3).getValue(), 0.0);
        }
    }
}
