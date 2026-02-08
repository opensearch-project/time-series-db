/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for SampleList interface and its implementations.
 */
public class SampleListTests extends OpenSearchTestCase {

    /**
     * Tests that estimateBytes() returns a reasonable value for an empty list.
     */
    public void testEstimateBytesEmptyList() {
        SampleList sampleList = SampleList.fromList(new ArrayList<>());

        long estimatedBytes = sampleList.estimateBytes();

        // Empty list should still have ArrayList overhead
        assertTrue("Empty list should have at least ArrayList overhead", estimatedBytes >= SampleList.ARRAYLIST_OVERHEAD);
    }

    /**
     * Tests that estimateBytes() returns a reasonable value for a populated list.
     */
    public void testEstimateBytesWithSamples() {
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0));
        SampleList sampleList = SampleList.fromList(samples);

        long estimatedBytes = sampleList.estimateBytes();

        // Should include ArrayList overhead + array header + sample objects
        long expectedMinimum = SampleList.ARRAYLIST_OVERHEAD + SampleList.ARRAY_HEADER_OVERHEAD + (3 * (SampleList.REFERENCE_SIZE
            + SampleList.ESTIMATED_SAMPLE_SIZE));

        assertEquals("estimateBytes should match expected calculation", expectedMinimum, estimatedBytes);
    }

    /**
     * Tests that estimateBytes() scales with the number of samples.
     */
    public void testEstimateBytesScalesWithSampleCount() {
        List<Sample> smallList = Arrays.asList(new FloatSample(1000L, 1.0));
        List<Sample> largeList = Arrays.asList(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 2.0),
            new FloatSample(3000L, 3.0),
            new FloatSample(4000L, 4.0),
            new FloatSample(5000L, 5.0)
        );

        SampleList smallSampleList = SampleList.fromList(smallList);
        SampleList largeSampleList = SampleList.fromList(largeList);

        long smallEstimate = smallSampleList.estimateBytes();
        long largeEstimate = largeSampleList.estimateBytes();

        assertTrue(
            "Larger sample list should have larger estimate. Small: " + smallEstimate + ", Large: " + largeEstimate,
            largeEstimate > smallEstimate
        );

        // The difference should be proportional to the sample count difference
        long expectedDifference = 4 * (SampleList.REFERENCE_SIZE + SampleList.ESTIMATED_SAMPLE_SIZE);
        assertEquals("Difference should be proportional to sample count difference", expectedDifference, largeEstimate - smallEstimate);
    }

    /**
     * Tests that the memory estimation constants are reasonable.
     */
    public void testMemoryEstimationConstantsAreReasonable() {
        // ArrayList overhead should be positive
        assertTrue("ARRAYLIST_OVERHEAD should be positive", SampleList.ARRAYLIST_OVERHEAD > 0);

        // Array header should be positive
        assertTrue("ARRAY_HEADER_OVERHEAD should be positive", SampleList.ARRAY_HEADER_OVERHEAD > 0);

        // Sample size should be at least timestamp + value = 16 bytes
        assertTrue("ESTIMATED_SAMPLE_SIZE should be at least 16 bytes", SampleList.ESTIMATED_SAMPLE_SIZE >= 16);

        // Reference size should be positive (typically 4 or 8 bytes)
        assertTrue("REFERENCE_SIZE should be positive", SampleList.REFERENCE_SIZE > 0);
    }
}
