/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for SampleMerger.
 */
public class SampleMergerTests extends OpenSearchTestCase {

    public void testMergeSortedSamplesAnyWins() {
        SampleMerger merger = new SampleMerger(SampleMerger.DeduplicatePolicy.ANY_WINS);

        List<Sample> samples1 = Arrays.asList(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0), new FloatSample(3000L, 30.0));

        List<Sample> samples2 = Arrays.asList(new FloatSample(1500L, 15.0), new FloatSample(2500L, 25.0), new FloatSample(3500L, 35.0));

        List<Sample> result = merger.merge(SampleList.fromList(samples1), SampleList.fromList(samples2), true).toList();

        assertEquals("Should have 6 samples", 6, result.size());
        assertThat("First sample should be from samples1", result.get(0).getTimestamp(), equalTo(1000L));
        assertThat("Second sample should be from samples2", result.get(1).getTimestamp(), equalTo(1500L));
        assertThat("Last sample should be from samples2", result.get(5).getTimestamp(), equalTo(3500L));
    }

    public void testMergeSortedSamplesWithDuplicatesAnyWins() {
        SampleMerger merger = new SampleMerger(SampleMerger.DeduplicatePolicy.ANY_WINS);

        List<Sample> samples1 = Arrays.asList(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));

        List<Sample> samples2 = Arrays.asList(
            new FloatSample(1000L, 15.0), // Duplicate timestamp
            new FloatSample(2000L, 25.0)  // Duplicate timestamp
        );

        List<Sample> result = merger.merge(SampleList.fromList(samples1), SampleList.fromList(samples2), true).toList();

        assertEquals("Should have 2 samples (duplicates merged)", 2, result.size());
        assertThat("First sample should be from samples2 (newer)", result.get(0).getTimestamp(), equalTo(1000L));
        assertThat("First sample value should be from samples2", ((FloatSample) result.get(0)).getValue(), equalTo(15.0));
        assertThat("Second sample should be from samples2 (newer)", result.get(1).getTimestamp(), equalTo(2000L));
        assertThat("Second sample value should be from samples2", ((FloatSample) result.get(1)).getValue(), equalTo(25.0));
    }

    public void testMergeSortedSamplesWithDuplicatesSumValues() {
        SampleMerger merger = new SampleMerger(SampleMerger.DeduplicatePolicy.SUM_VALUES);

        List<Sample> samples1 = Arrays.asList(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));

        List<Sample> samples2 = Arrays.asList(
            new FloatSample(1000L, 15.0), // Duplicate timestamp
            new FloatSample(2000L, 25.0)  // Duplicate timestamp
        );

        List<Sample> result = merger.merge(SampleList.fromList(samples1), SampleList.fromList(samples2), true).toList();

        assertEquals("Should have 2 samples (duplicates merged)", 2, result.size());
        assertThat("First sample should be merged", result.get(0).getTimestamp(), equalTo(1000L));
        assertThat("First sample value should be summed", ((FloatSample) result.get(0)).getValue(), equalTo(25.0));
        assertThat("Second sample should be merged", result.get(1).getTimestamp(), equalTo(2000L));
        assertThat("Second sample value should be summed", ((FloatSample) result.get(1)).getValue(), equalTo(45.0));
    }

    public void testMergeUnsortedSamples() {
        SampleMerger merger = new SampleMerger(SampleMerger.DeduplicatePolicy.ANY_WINS);

        List<Sample> samples1 = Arrays.asList(new FloatSample(3000L, 30.0), new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0));

        List<Sample> samples2 = Arrays.asList(new FloatSample(2500L, 25.0), new FloatSample(1500L, 15.0), new FloatSample(3500L, 35.0));

        List<Sample> result = merger.merge(SampleList.fromList(samples1), SampleList.fromList(samples2), false).toList();

        assertEquals("Should have 6 samples", 6, result.size());
        // Verify result is sorted
        for (int i = 1; i < result.size(); i++) {
            assertTrue("Result should be sorted", result.get(i - 1).getTimestamp() <= result.get(i).getTimestamp());
        }
    }

    public void testMergeEmptyLists() {
        SampleMerger merger = new SampleMerger();

        List<Sample> empty1 = new ArrayList<>();
        List<Sample> empty2 = new ArrayList<>();
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 10.0));

        // Both empty
        List<Sample> result1 = merger.merge(SampleList.fromList(empty1), SampleList.fromList(empty2), true).toList();
        assertTrue("Empty merge should return empty list", result1.isEmpty());

        // First empty
        List<Sample> result2 = merger.merge(SampleList.fromList(empty1), SampleList.fromList(samples), true).toList();
        assertEquals("Should return second list", samples.size(), result2.size());
        assertThat("Should return second list", result2.get(0).getTimestamp(), equalTo(1000L));

        // Second empty
        List<Sample> result3 = merger.merge(SampleList.fromList(samples), SampleList.fromList(empty2), true).toList();
        assertEquals("Should return first list", samples.size(), result3.size());
        assertThat("Should return first list", result3.get(0).getTimestamp(), equalTo(1000L));
    }

    public void testDefaultConstructor() {
        SampleMerger merger = new SampleMerger();

        // Should use ANY_WINS policy by default
        List<Sample> samples1 = Arrays.asList(new FloatSample(1000L, 10.0));
        List<Sample> samples2 = Arrays.asList(new FloatSample(1000L, 20.0));

        List<Sample> result = merger.merge(SampleList.fromList(samples1), SampleList.fromList(samples2), true).toList();

        assertEquals("Should have 1 sample (duplicate merged)", 1, result.size());
        assertThat("Should use ANY_WINS policy", ((FloatSample) result.get(0)).getValue(), equalTo(20.0));
    }

    public void testSumValuesWithFloatSamples() {
        SampleMerger merger = new SampleMerger(SampleMerger.DeduplicatePolicy.SUM_VALUES);

        // Test that SUM_VALUES policy correctly sums FloatSample values
        List<Sample> samples1 = Arrays.asList(new FloatSample(1000L, 10.0));
        List<Sample> samples2 = Arrays.asList(new FloatSample(1000L, 20.0));

        List<Sample> result = merger.merge(SampleList.fromList(samples1), SampleList.fromList(samples2), true).toList();

        assertEquals("Should have 1 sample", 1, result.size());
        // Should sum the values since both are FloatSample
        assertThat("Should sum the values", result.get(0).getValue(), equalTo(30.0));
    }
}
