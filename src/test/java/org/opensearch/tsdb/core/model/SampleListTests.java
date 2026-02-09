/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for SampleList interface and its implementations.
 */
public class SampleListTests extends OpenSearchTestCase {

    /**
     * Tests that ramBytesUsed() returns a reasonable value for an empty list.
     */
    public void testRamBytesUsedEmptyList() {
        SampleList sampleList = SampleList.fromList(new ArrayList<>());

        long ramBytes = sampleList.ramBytesUsed();

        // Empty list should still have ArrayList overhead
        assertTrue("Empty list should have at least ArrayList overhead", ramBytes >= SampleList.ARRAYLIST_OVERHEAD);
    }

    /**
     * Tests that ramBytesUsed() returns a reasonable value for a populated list.
     */
    public void testRamBytesUsedWithSamples() {
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0));
        SampleList sampleList = SampleList.fromList(samples);

        long ramBytes = sampleList.ramBytesUsed();

        // Estimate should be positive and include wrapper + ArrayList + array + samples
        assertTrue("ramBytesUsed should be positive", ramBytes > 0);

        // Should scale with sample count - base overhead plus per-sample cost
        long perSampleCost = SampleList.REFERENCE_SIZE + SampleList.ESTIMATED_SAMPLE_SIZE;
        assertTrue("Should include per-sample overhead", ramBytes >= 3 * perSampleCost);
    }

    /**
     * Tests that ramBytesUsed() scales with the number of samples.
     */
    public void testRamBytesUsedScalesWithSampleCount() {
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

        long smallEstimate = smallSampleList.ramBytesUsed();
        long largeEstimate = largeSampleList.ramBytesUsed();

        assertTrue(
            "Larger sample list should have larger estimate. Small: " + smallEstimate + ", Large: " + largeEstimate,
            largeEstimate > smallEstimate
        );

        // The difference should be proportional to the sample count difference
        long expectedDifference = 4 * (SampleList.REFERENCE_SIZE + SampleList.ESTIMATED_SAMPLE_SIZE);
        assertEquals("Difference should be proportional to sample count difference", expectedDifference, largeEstimate - smallEstimate);
    }

    /**
     * Tests that the memory estimation constants are reasonable and JVM-aware.
     */
    public void testMemoryEstimationConstantsAreReasonable() {
        // ArrayList overhead should be positive and match RamUsageEstimator
        assertTrue("ARRAYLIST_OVERHEAD should be positive", SampleList.ARRAYLIST_OVERHEAD > 0);
        assertEquals(
            "ARRAYLIST_OVERHEAD should use RamUsageEstimator",
            RamUsageEstimator.shallowSizeOfInstance(ArrayList.class),
            SampleList.ARRAYLIST_OVERHEAD
        );

        // Array header should match RamUsageEstimator
        assertEquals(
            "ARRAY_HEADER_SIZE should use RamUsageEstimator",
            RamUsageEstimator.NUM_BYTES_ARRAY_HEADER,
            SampleList.ARRAY_HEADER_SIZE
        );

        // Sample size should be at least timestamp + value = 16 bytes
        assertTrue("ESTIMATED_SAMPLE_SIZE should be at least 16 bytes", SampleList.ESTIMATED_SAMPLE_SIZE >= 16);

        // Reference size should match RamUsageEstimator (4 with compressed OOPs, 8 without)
        assertEquals("REFERENCE_SIZE should use RamUsageEstimator", RamUsageEstimator.NUM_BYTES_OBJECT_REF, SampleList.REFERENCE_SIZE);
    }
}
