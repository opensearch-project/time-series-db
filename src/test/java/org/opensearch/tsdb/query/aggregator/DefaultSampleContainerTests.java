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
import org.opensearch.tsdb.core.model.SumCountSample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class DefaultSampleContainerTests extends OpenSearchTestCase {
    public void testConstructor() {
        // Verify all sample types can be used in constructor
        for (SampleType sampleType : SampleType.values()) {
            DefaultSampleContainer container = new DefaultSampleContainer(sampleType, 1000L);
            assertEquals(0L, container.size());
            assertEquals(Long.MAX_VALUE, container.getMinTimestamp());
            assertEquals(Long.MIN_VALUE, container.getMaxTimestamp());
        }
    }

    public void testConstructor_WithInitialCapacity() {
        // Verify all sample types can be used in constructor
        for (SampleType sampleType : SampleType.values()) {
            DefaultSampleContainer container = new DefaultSampleContainer(sampleType, 1000L, 100);
            assertEquals(0L, container.size());
            assertEquals(Long.MAX_VALUE, container.getMinTimestamp());
            assertEquals(Long.MIN_VALUE, container.getMaxTimestamp());
        }
    }

    public void testFloatSample_Append() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.FLOAT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));
        assertEquals(expected, container.iterator());
        assertEquals(1000, container.getMinTimestamp());
        assertEquals(1000000, container.getMaxTimestamp());
    }

    public void testFloatSample_AppendThrowsExceptionOnInvalidSampleType() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.SUM_COUNT_SAMPLE);
        expectThrows(IllegalArgumentException.class, () -> container.append(expected.getFirst().getTimestamp(), expected.getFirst()));
    }

    public void testFloatSample_AppendThrowsExceptionOnInvalidTimestamp() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.FLOAT_SAMPLE);
        container.append(expected.getFirst().getTimestamp(), expected.getFirst());
        expectThrows(IllegalArgumentException.class, () -> container.append(expected.getLast().getTimestamp(), expected.getLast()));
    }

    public void testFloatSample_GetSampleFor() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.FLOAT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        Sample sample = container.getSampleFor(2000L);
        assertEquals(2000L, sample.getTimestamp());
        assertEquals(101.0, sample.getValue(), 0.001);
    }

    public void testFloatSample_GetSampleForThrowsExceptionOnInvalidTimestamp() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.FLOAT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        expectThrows(IllegalArgumentException.class, () -> container.getSampleFor(0L));
    }

    public void testFloatSample_UpdateSampleFor() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.FLOAT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        container.updateSampleFor(2000L, new FloatSample(2000L, 25.0));

        Sample sample = container.getSampleFor(2000L);
        assertEquals(25.0, sample.getValue(), 0.001);
        assertEquals(1000, container.getMinTimestamp());
        assertEquals(1000000, container.getMaxTimestamp());
    }

    public void testFloatSample_UpdateSampleForThrowsExceptionOnInvalidSampleType() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.FLOAT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        expectThrows(IllegalArgumentException.class, () -> container.updateSampleFor(2000L, new SumCountSample(2000L, 25.0, 1)));
    }

    public void testFloatSample_UpdateSampleForThrowsExceptionOnInvalidTimestamp() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.FLOAT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        expectThrows(IllegalArgumentException.class, () -> container.updateSampleFor(2500L, new FloatSample(2500L, 2)));
    }

    public void testFloatSample_EmptyContainer() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        assertEquals(0L, container.size());
        assertFalse(container.iterator().hasNext());
    }

    public void testSumCountSample_Append() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.SUM_COUNT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));
        assertEquals(expected, container.iterator());
    }

    public void testSumCountSample_GetSampleFor() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.SUM_COUNT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        Sample sample = container.getSampleFor(2000L);
        assertTrue(sample instanceof SumCountSample);
        SumCountSample sumCountSample = (SumCountSample) sample;
        assertEquals(2000L, sumCountSample.getTimestamp());
        assertEquals(101.0, sumCountSample.sum(), 0.001);
        assertEquals(11L, sumCountSample.count());
    }

    public void testSumCountSample_UpdateSampleFor() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.SUM_COUNT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        container.updateSampleFor(2000L, new SumCountSample(2000L, 250.0, 25L));

        Sample sample = container.getSampleFor(2000L);
        SumCountSample sumCountSample = (SumCountSample) sample;
        assertEquals(250.0, sumCountSample.sum(), 0.001);
        assertEquals(25L, sumCountSample.count());
    }

    public void testSumCountSample_UpdateSampleForThrowsExceptionOnInvalidTimestamp() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.SUM_COUNT_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        expectThrows(IllegalArgumentException.class, () -> container.updateSampleFor(2500L, new SumCountSample(2500L, 25.0, 1)));
    }

    public void testMultiValueSample_Append() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.MULTI_VALUE_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.MULTI_VALUE_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));

        assertEquals(expected, container.iterator());
    }

    public void testMultiValueSample_GetSampleFor() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.MULTI_VALUE_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.MULTI_VALUE_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));
        Sample sample = container.getSampleFor(2000L);
        assertTrue(sample instanceof MultiValueSample);
        MultiValueSample multiValueSample = (MultiValueSample) sample;
        assertEquals(2000L, multiValueSample.getTimestamp());
        assertEquals(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0), multiValueSample.getSortedValueList());
    }

    public void testMultiValueSample_UpdateSampleFor() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.MULTI_VALUE_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.MULTI_VALUE_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));
        container.updateSampleFor(2000L, new MultiValueSample(2000L, Arrays.asList(10.0, 11.0, 12.0)));

        Sample sample = container.getSampleFor(2000L);
        MultiValueSample multiValueSample = (MultiValueSample) sample;
        assertEquals(Arrays.asList(10.0, 11.0, 12.0), multiValueSample.getSortedValueList());
    }

    public void testMultiValueSample_UpdateSampleForThrowsExceptionOnInvalidTimestamp() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.MULTI_VALUE_SAMPLE, 1000L);
        var expected = createSamples(1000, 1000, 1000, SampleType.MULTI_VALUE_SAMPLE);
        expected.forEach(sample -> container.append(sample.getTimestamp(), sample));
        container.updateSampleFor(2000L, new MultiValueSample(2000L, Arrays.asList(10.0, 11.0, 12.0)));

        expectThrows(
            IllegalArgumentException.class,
            () -> container.updateSampleFor(2500L, new MultiValueSample(2500L, Arrays.asList(10.0, 11.0, 12.0)))
        );
    }

    // ========== Edge Cases and Error Handling ==========

    public void testIterator_NoSuchElementException() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        container.append(1000L, new FloatSample(1000L, 10.0));

        Iterator<Sample> iter = container.iterator();
        assertTrue(iter.hasNext());
        iter.next(); // Consume the only element
        assertFalse(iter.hasNext());

        expectThrows(NoSuchElementException.class, iter::next);
    }

    public void testSize_FloatSample() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);

        assertEquals(0L, container.size());

        container.append(1000L, new FloatSample(1000L, 10.0));
        assertEquals(1L, container.size());

        container.append(2000L, new FloatSample(2000L, 20.0));
        assertEquals(2L, container.size());

        container.append(3000L, new FloatSample(3000L, 30.0));
        assertEquals(3L, container.size());
    }

    public void testSize_SumCountSample() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.SUM_COUNT_SAMPLE, 1000L);

        assertEquals(0L, container.size());

        container.append(1000L, new SumCountSample(1000L, 100.0, 10L));
        assertEquals(1L, container.size());

        container.append(2000L, new SumCountSample(2000L, 200.0, 20L));
        assertEquals(2L, container.size());
    }

    public void testSize_MultiValueSample() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.MULTI_VALUE_SAMPLE, 1000L);

        assertEquals(0L, container.size());

        container.append(1000L, new MultiValueSample(1000L, Arrays.asList(1.0, 2.0, 3.0)));
        assertEquals(1L, container.size());

        container.append(2000L, new MultiValueSample(2000L, Arrays.asList(4.0, 5.0, 6.0)));
        assertEquals(2L, container.size());
    }

    public void testMultipleIterators_Independent() {
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);

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
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);

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
        DefaultSampleContainer container = new DefaultSampleContainer(SampleType.FLOAT_SAMPLE, 1000L);
        container.append(1000L, new FloatSample(1000L, 10.0));

        List<Sample> samples = new ArrayList<>();
        for (Sample sample : container) {
            samples.add(sample);
        }

        assertEquals(1, samples.size());
        assertEquals(10.0, samples.get(0).getValue(), 0.001);
    }

    private List<Sample> createSamples(long timestamp, int count, long step, SampleType sampleType) {
        List<Sample> samples = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            switch (sampleType) {
                case FLOAT_SAMPLE -> samples.add(new FloatSample(timestamp, 100.0 + i));
                case SUM_COUNT_SAMPLE -> samples.add(new SumCountSample(timestamp, 100.0 + i, 10 + i));
                case MULTI_VALUE_SAMPLE -> samples.add(
                    new MultiValueSample(timestamp, List.of((double) i, (double) i + 1, (double) i + 2, (double) i + 3, (double) i + 4))
                );
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
            if (expectedSample == null || expectedSample.getTimestamp() > timestamp + 1000L) {
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
