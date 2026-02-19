/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleType;
import org.opensearch.tsdb.core.model.MultiValueSample;
import org.opensearch.tsdb.core.model.MinMaxSample;
import org.opensearch.tsdb.core.model.SumCountSample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public class DenseSampleContainerTests extends OpenSearchTestCase {
    public void testConstructor() {
        // Verify all sample types can be used in constructor
        for (SampleType sampleType : SampleType.values()) {
            DenseSampleContainer container = new DenseSampleContainer(sampleType, 1000L);
            assertEquals(0L, container.size());
            assertEquals(Long.MAX_VALUE, container.getMinTimestamp());
            assertEquals(Long.MIN_VALUE, container.getMaxTimestamp());
        }
    }

    public void testConstructor_WithInitialCapacity() {
        // Verify all sample types can be used in constructor
        for (SampleType sampleType : SampleType.values()) {
            DenseSampleContainer container = new DenseSampleContainer(sampleType, 1000L, 100);
            assertEquals(0L, container.size());
            assertEquals(Long.MAX_VALUE, container.getMinTimestamp());
            assertEquals(Long.MIN_VALUE, container.getMaxTimestamp());
        }
    }

    public void testFloatSample_Append() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.FLOAT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));
        assertEquals(expected, container.iterator());
        assertEquals(1000, container.getMinTimestamp());
        assertEquals(1000000, container.getMaxTimestamp());
    }

    public void testFloatSample_AppendWithGap() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 4000, 5, SampleType.FLOAT_SAMPLE);
        expected.stream().filter(Objects::nonNull).forEach(sample -> container.append(sample.getTimestamp(), sample));
        assertEquals(expected, container.iterator());
    }

    public void testFloatSample_AppendThrowsExceptionOnOutOfOrderSample() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.FLOAT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));
        expectThrows(IllegalArgumentException.class, () -> container.append(500, expected.getFirst()));
    }

    public void testFloatSample_GetSampleFor() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.FLOAT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        Sample sample = container.getSampleFor(2000L);
        assertEquals(2000L, sample.getTimestamp());
        assertEquals(101.0, sample.getValue(), 0.001);
    }

    public void testFloatSample_GetSampleForThrowsExceptionOnInvalidTimestamp() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.FLOAT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        expectThrows(IllegalArgumentException.class, () -> container.getSampleFor(0L));
    }

    public void testFloatSample_GetSampleForReturnsNullForGap() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        container.append(1000L, new FloatSample(1000L, 10.0));
        container.append(3000L, new FloatSample(3000L, 30.0)); // gap at 2000L

        assertNull(container.getSampleFor(2000L));
    }

    public void testFloatSample_UpdateSampleFor() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 2, SampleType.FLOAT_SAMPLE);
        expected.stream().filter(Objects::nonNull).forEach(sample -> container.append(sample.getTimestamp(), sample));
        Sample sample = container.getSampleFor(3000L);
        assertNull(sample);
        container.updateSampleFor(3000L, new FloatSample(2000L, 25.0));

        sample = container.getSampleFor(3000L);
        assertEquals(25.0, sample.getValue(), 0.001);
        assertEquals(1000, container.getMinTimestamp());
        assertEquals(1000000, container.getMaxTimestamp());

        container.updateSampleFor(3000L, null);
        sample = container.getSampleFor(3000L);
        assertNull(sample);

    }

    public void testFloatSample_UpdateSampleForThrowsExceptionOnInvalidSampleType() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.FLOAT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        expectThrows(IllegalArgumentException.class, () -> container.updateSampleFor(2000L, new SumCountSample(2000L, 25.0, 1)));
    }

    public void testFloatSample_UpdateSampleForThrowsExceptionOnInvalidTimestamp() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.FLOAT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        expectThrows(IllegalArgumentException.class, () -> container.updateSampleFor(2500L, new FloatSample(2500L, 2)));
    }

    public void testFloatSample_EmptyContainer() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        assertEquals(0L, container.size());
        assertFalse(container.iterator().hasNext());
    }

    public void testSumCountSample_Append() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.SUM_COUNT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));
        assertEquals(expected, container.iterator());
        assertEquals(1000, container.getMinTimestamp());
        assertEquals(1000000, container.getMaxTimestamp());
    }

    public void testSumCountSample_AppendWithGap() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 4000, 5, SampleType.SUM_COUNT_SAMPLE);
        expected.stream().filter(Objects::nonNull).forEach(sample -> container.append(sample.getTimestamp(), sample));
        assertEquals(expected, container.iterator());
    }

    public void testSumCountSample_AppendThrowsExceptionOnOutOfOrderSample() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.SUM_COUNT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));
        expectThrows(IllegalArgumentException.class, () -> container.append(500, expected.getFirst()));
    }

    public void testSumCountSample_GetSampleFor() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.SUM_COUNT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        Sample sample = container.getSampleFor(2000L);
        assertTrue(sample instanceof SumCountSample);
        SumCountSample sumCountSample = (SumCountSample) sample;
        assertEquals(2000L, sumCountSample.getTimestamp());
        assertEquals(101.0, sumCountSample.sum(), 0.001);
        assertEquals(11L, sumCountSample.count());
    }

    public void testSumCountSample_GetSampleForThrowsExceptionOnInvalidTimestamp() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.SUM_COUNT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        expectThrows(IllegalArgumentException.class, () -> container.getSampleFor(0L));
    }

    public void testSumCountSample_UpdateSampleFor() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 2, SampleType.SUM_COUNT_SAMPLE);
        expected.stream().filter(Objects::nonNull).forEach(sample -> container.append(sample.getTimestamp(), sample));
        Sample sample = container.getSampleFor(3000L);
        assertNull(sample);
        container.updateSampleFor(3000L, new SumCountSample(2000L, 25.0, 1));

        sample = container.getSampleFor(3000L);
        assertEquals(25.0, sample.getValue(), 0.001);
        assertEquals(1000, container.getMinTimestamp());
        assertEquals(1000000, container.getMaxTimestamp());

        container.updateSampleFor(3000L, null);
        sample = container.getSampleFor(3000L);
        assertNull(sample);
    }

    public void testSumCountSample_UpdateSampleForThrowsExceptionOnInvalidTimestamp() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.SUM_COUNT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        expectThrows(IllegalArgumentException.class, () -> container.updateSampleFor(2500L, new SumCountSample(2500L, 25.0, 1)));
    }

    public void testMultiValueSample_Append() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.MULTI_VALUE_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.MULTI_VALUE_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        assertEquals(expected, container.iterator());
        assertEquals(1000, container.getMinTimestamp());
        assertEquals(1000000, container.getMaxTimestamp());
    }

    public void testMultiValueSample_AppendThrowsExceptionOnOutOfOrderSample() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.MULTI_VALUE_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.MULTI_VALUE_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));
        expectThrows(IllegalArgumentException.class, () -> container.append(500, expected.getFirst()));
    }

    public void testMultiValueSample_GetSampleFor() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.MULTI_VALUE_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.MULTI_VALUE_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));
        Sample sample = container.getSampleFor(2000L);
        assertTrue(sample instanceof MultiValueSample);
        MultiValueSample multiValueSample = (MultiValueSample) sample;
        assertEquals(2000L, multiValueSample.getTimestamp());
        assertEquals(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0), multiValueSample.getSortedValueList());
    }

    public void testMultiValueSample_GetSampleForThrowsExceptionOnInvalidTimestamp() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.MULTI_VALUE_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.MULTI_VALUE_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        expectThrows(IllegalArgumentException.class, () -> container.getSampleFor(0L));
    }

    public void testMultiValueSample_UpdateSampleFor() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.MULTI_VALUE_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 2, SampleType.MULTI_VALUE_SAMPLE);
        expected.stream().filter(Objects::nonNull).forEach(sample -> container.append(sample.getTimestamp(), sample));
        Sample sample = container.getSampleFor(3000L);
        assertNull(sample);
        container.updateSampleFor(3000L, new MultiValueSample(2000L, List.of(1.0, 2.0, 3.0, 4.0, 5.0)));

        sample = container.getSampleFor(3000L);
        assertEquals(List.of(1.0, 2.0, 3.0, 4.0, 5.0), ((MultiValueSample) sample).getSortedValueList());
        assertEquals(1000, container.getMinTimestamp());
        assertEquals(1000000, container.getMaxTimestamp());

        container.updateSampleFor(3000L, null);
        sample = container.getSampleFor(3000L);
        assertNull(sample);
    }

    public void testMultiValueSample_UpdateSampleForThrowsExceptionOnInvalidTimestamp() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.MULTI_VALUE_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, 0, SampleType.MULTI_VALUE_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));
        container.updateSampleFor(2000L, new MultiValueSample(2000L, Arrays.asList(10.0, 11.0, 12.0)));

        expectThrows(
            IllegalArgumentException.class,
            () -> container.updateSampleFor(2500L, new MultiValueSample(2500L, Arrays.asList(10.0, 11.0, 12.0)))
        );
    }

    // ========== MIN_MAX_SAMPLE ==========

    public void testMinMaxSample_Append() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.MIN_MAX_SAMPLE, 1000L);
        container.append(1000L, new MinMaxSample(1000L, 1.0, 10.0));
        container.append(2000L, new MinMaxSample(2000L, 2.0, 20.0));
        container.append(3000L, new MinMaxSample(3000L, 3.0, 30.0));

        assertEquals(3L, container.size());
        assertEquals(1000L, container.getMinTimestamp());
        assertEquals(3000L, container.getMaxTimestamp());
    }

    public void testMinMaxSample_GetSampleFor() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.MIN_MAX_SAMPLE, 1000L);
        container.append(1000L, new MinMaxSample(1000L, 1.0, 10.0));
        container.append(2000L, new MinMaxSample(2000L, 2.0, 20.0));

        Sample sample = container.getSampleFor(2000L);
        assertTrue(sample instanceof MinMaxSample);
        MinMaxSample minMax = (MinMaxSample) sample;
        assertEquals(2000L, minMax.getTimestamp());
        assertEquals(2.0, minMax.min(), 0.001);
        assertEquals(20.0, minMax.max(), 0.001);
    }

    public void testMinMaxSample_UpdateSampleFor() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.MIN_MAX_SAMPLE, 1000L);
        container.append(1000L, new MinMaxSample(1000L, 1.0, 10.0));
        container.append(2000L, new MinMaxSample(2000L, 2.0, 20.0));
        container.append(4000L, new MinMaxSample(4000L, 4.0, 40.0)); // gap at 3000

        assertNull(container.getSampleFor(3000L));
        container.updateSampleFor(3000L, new MinMaxSample(3000L, 3.0, 30.0));

        Sample sample = container.getSampleFor(3000L);
        assertTrue(sample instanceof MinMaxSample);
        MinMaxSample minMax = (MinMaxSample) sample;
        assertEquals(3.0, minMax.min(), 0.001);
        assertEquals(30.0, minMax.max(), 0.001);

        container.updateSampleFor(3000L, null);
        assertNull(container.getSampleFor(3000L));
    }

    public void testMinMaxSample_UpdateSampleForThrowsExceptionOnInvalidTimestamp() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.MIN_MAX_SAMPLE, 1000L);
        container.append(1000L, new MinMaxSample(1000L, 1.0, 10.0));

        expectThrows(IllegalArgumentException.class, () -> container.updateSampleFor(2500L, new MinMaxSample(2500L, 2.5, 25.0)));
    }

    public void testMinMaxSample_GetSampleForThrowsExceptionOnInvalidTimestamp() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.MIN_MAX_SAMPLE, 1000L);
        container.append(1000L, new MinMaxSample(1000L, 1.0, 10.0));

        expectThrows(IllegalArgumentException.class, () -> container.getSampleFor(0L));
    }

    // ========== Edge Cases and Error Handling ==========

    public void testIterator_NoSuchElementException() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        container.append(1000L, new FloatSample(1000L, 10.0));

        Iterator<Sample> iter = container.iterator();
        assertTrue(iter.hasNext());
        iter.next(); // Consume the only element
        assertFalse(iter.hasNext());

        expectThrows(NoSuchElementException.class, iter::next);
    }

    public void testSize_FloatSample() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);

        assertEquals(0L, container.size());

        container.append(1000L, new FloatSample(1000L, 10.0));
        assertEquals(1L, container.size());

        container.append(2000L, new FloatSample(2000L, 20.0));
        assertEquals(2L, container.size());

        container.append(3000L, new FloatSample(3000L, 30.0));
        assertEquals(3L, container.size());
    }

    public void testSize_SumCountSample() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);

        assertEquals(0L, container.size());

        container.append(1000L, new SumCountSample(1000L, 100.0, 10L));
        assertEquals(1L, container.size());

        container.append(2000L, new SumCountSample(2000L, 200.0, 20L));
        assertEquals(2L, container.size());
    }

    public void testSize_MultiValueSample() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.MULTI_VALUE_SAMPLE, 1000L);

        assertEquals(0L, container.size());

        container.append(1000L, new MultiValueSample(1000L, Arrays.asList(1.0, 2.0, 3.0)));
        assertEquals(1L, container.size());

        container.append(2000L, new MultiValueSample(2000L, Arrays.asList(4.0, 5.0, 6.0)));
        assertEquals(2L, container.size());
    }

    public void testMultipleIterators_Independent() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);

        container.append(1000L, new FloatSample(1000L, 10.0));
        container.append(2000L, new FloatSample(2000L, 20.0));
        container.append(3000L, new FloatSample(3000L, 30.0));

        Iterator<Sample> iter1 = container.iterator();
        Iterator<Sample> iter2 = container.iterator();

        assertTrue(iter1.hasNext());
        assertTrue(iter2.hasNext());

        iter1.next(); // Advance first iterator

        // Second iterator should still be at the beginning
        assertTrue(iter1.hasNext());
        assertTrue(iter2.hasNext());
    }

    public void testAppend_UpdatesMinMaxTimestamps() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);

        container.append(1000L, new FloatSample(1000L, 10.0));
        Sample sample2 = container.getSampleFor(1000L);
        assertEquals(1000L, sample2.getTimestamp());

        container.append(2000L, new FloatSample(2000L, 20.0));
        Sample sample1 = container.getSampleFor(2000L);
        assertEquals(2000L, sample1.getTimestamp());

        container.append(3000L, new FloatSample(3000L, 30.0));
        Sample sample3 = container.getSampleFor(3000L);
        assertEquals(3000L, sample3.getTimestamp());
    }

    public void testIterator_WithSingleElement() {
        DenseSampleContainer container = new DenseSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        container.append(1000L, new FloatSample(1000L, 10.0));

        List<Sample> samples = new ArrayList<>();
        for (Sample sample : container) {
            samples.add(sample);
        }

        assertEquals(1, samples.size());
        assertEquals(10.0, samples.get(0).getValue(), 0.001);
    }

    private List<Sample> createSamples(long timestamp, int count, long step, long nullInterval, SampleType sampleType) {
        List<Sample> samples = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            if (nullInterval != 0 && i != 0 && i % nullInterval == 0) {
                samples.add(null);
            } else {
                switch (sampleType) {
                    case FLOAT_SAMPLE -> samples.add(new FloatSample(timestamp, 100.0 + i));
                    case SUM_COUNT_SAMPLE -> samples.add(new SumCountSample(timestamp, 100.0 + i, 10 + i));
                    case MULTI_VALUE_SAMPLE -> samples.add(
                        new MultiValueSample(timestamp, List.of((double) i, (double) i + 1, (double) i + 2, (double) i + 3, (double) i + 4))
                    );
                }
            }
            timestamp = timestamp + step;
        }
        return samples;
    }

    private void assertEquals(List<Sample> expected, Iterator<Sample> actual) {
        var actualSize = 0;
        var timestamp = 0L;
        while (actual.hasNext()) {
            var actuaSample = actual.next();
            var expectedSample = expected.get(actualSize);
            if (expectedSample == null) {
                assertNull(actuaSample);
                actualSize++;
            } else if (expectedSample.getTimestamp() > timestamp + 1000L) {
                assertNull(actuaSample);
            } else {
                if (expectedSample.getSampleType() == SampleType.FLOAT_SAMPLE) {
                    assertEquals(expectedSample.getValue(), actuaSample.getValue(), 0.001);
                } else if (expectedSample.getSampleType() == SampleType.SUM_COUNT_SAMPLE) {
                    assertEquals(((SumCountSample) expectedSample).sum(), ((SumCountSample) actuaSample).sum(), 0.001);
                    assertEquals(((SumCountSample) expectedSample).count(), ((SumCountSample) actuaSample).count());
                } else if (expectedSample.getSampleType() == SampleType.MULTI_VALUE_SAMPLE) {
                    assertEquals(
                        ((MultiValueSample) expectedSample).getSortedValueList(),
                        ((MultiValueSample) actuaSample).getSortedValueList()
                    );
                }
                actualSize++;
            }
            timestamp += 1000L;
        }
        assertEquals(expected.size(), actualSize);
    }
}
