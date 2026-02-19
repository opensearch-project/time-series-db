/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MultiValueSampleTests extends AbstractWireSerializingTestCase<MultiValueSample> {

    @Override
    protected Writeable.Reader<MultiValueSample> instanceReader() {
        // We need a custom reader since MultiValueSample.readFrom expects the timestamp already read
        return in -> {
            long timestamp = in.readLong();
            SampleType type = SampleType.readFrom(in);
            if (type != SampleType.MULTI_VALUE_SAMPLE) {
                throw new IOException("Expected MULTI_VALUE_SAMPLE but got " + type);
            }
            return MultiValueSample.readFrom(in, timestamp);
        };
    }

    @Override
    protected MultiValueSample createTestInstance() {
        long timestamp = randomLong();

        // Randomly create either a single-value or multi-value sample
        if (randomBoolean()) {
            // Single value
            return new MultiValueSample(timestamp, randomDouble());
        } else {
            // Multiple values (unsorted)
            int count = randomIntBetween(2, 10);
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                values.add(randomDouble());
            }
            return new MultiValueSample(timestamp, values);
        }
    }

    public void testSingleValue() {
        MultiValueSample sample = new MultiValueSample(1000L, 42.0);

        assertEquals(1000L, sample.getTimestamp());
        assertEquals(ValueType.FLOAT64, sample.valueType());
        assertEquals(SampleType.MULTI_VALUE_SAMPLE, sample.getSampleType());
        assertEquals(1, sample.getValueList().size());
        assertEquals(42.0, sample.getValueList().get(0), 0.0001);
    }

    public void testGetValueThrowsUnsupportedOperationException() {
        MultiValueSample sample = new MultiValueSample(1000L, List.of(10.0, 20.0, 30.0));
        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, sample::getValue);
        assertTrue(e.getMessage().contains("does not support getValue()"));
        assertTrue(e.getMessage().contains("getValueList()"));
    }

    public void testMultipleValues() {
        List<Double> values = List.of(30.0, 10.0, 20.0); // Unsorted
        MultiValueSample sample = new MultiValueSample(2000L, values);

        assertEquals(2000L, sample.getTimestamp());
        assertEquals(3, sample.getValueList().size());
        // getValueList() should return unsorted values as-is
        assertEquals(values, sample.getValueList());
    }

    public void testGetSortedValueList() {
        List<Double> unsortedValues = List.of(30.0, 10.0, 20.0, 50.0, 40.0);
        MultiValueSample sample = new MultiValueSample(1000L, unsortedValues);

        // getValueList() should return unsorted
        assertEquals(unsortedValues, sample.getValueList());

        // getSortedValueList() should return a sorted copy
        List<Double> sortedValues = sample.getSortedValueList();
        assertEquals(List.of(10.0, 20.0, 30.0, 40.0, 50.0), sortedValues);

        // Original list should remain unsorted
        assertEquals(unsortedValues, sample.getValueList());
    }

    public void testInsert() {
        MultiValueSample sample = new MultiValueSample(1000L, 10.0);
        sample.insert(20.0);
        sample.insert(15.0);
        // Values should be in insertion order (unsorted)
        assertEquals(List.of(10.0, 20.0, 15.0), sample.getValueList());
    }

    public void testMergeThrowsUnsupportedOperationException() {
        MultiValueSample sample1 = new MultiValueSample(1000L, List.of(10.0, 20.0));
        MultiValueSample sample2 = new MultiValueSample(1000L, List.of(30.0, 40.0));

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> sample1.merge(sample2));
        assertTrue(e.getMessage().contains("does not support merge"));
        assertTrue(e.getMessage().contains("insert()"));

        // merge(Sample) also throws
        Sample other = new MultiValueSample(1000L, 50.0);
        expectThrows(UnsupportedOperationException.class, () -> sample1.merge(other));

        // merge with wrong type also throws UnsupportedOperationException (not IllegalArgumentException)
        expectThrows(UnsupportedOperationException.class, () -> sample1.merge(new FloatSample(1000L, 20.0)));
    }

    public void testEquals() {
        MultiValueSample sample1 = new MultiValueSample(1000L, List.of(10.0, 20.0, 30.0));
        MultiValueSample sample2 = new MultiValueSample(1000L, List.of(10.0, 20.0, 30.0));
        MultiValueSample sample3 = new MultiValueSample(1000L, List.of(10.0, 20.0));
        MultiValueSample sample4 = new MultiValueSample(2000L, List.of(10.0, 20.0, 30.0));
        MultiValueSample sample5 = new MultiValueSample(1000L, List.of(30.0, 20.0, 10.0)); // Different order

        assertEquals(sample1, sample2);
        assertNotEquals(sample1, sample3); // Different values
        assertNotEquals(sample1, sample4); // Different timestamp
        assertNotEquals(sample1, sample5); // Different order (matters for unsorted)
        assertNotEquals(sample1, null);
        assertNotEquals(sample1, new FloatSample(1000L, 10.0));
    }

    public void testHashCode() {
        MultiValueSample sample1 = new MultiValueSample(1000L, List.of(10.0, 20.0, 30.0));
        MultiValueSample sample2 = new MultiValueSample(1000L, List.of(10.0, 20.0, 30.0));

        assertEquals(sample1.hashCode(), sample2.hashCode());
    }

    public void testDeepCopy() {
        MultiValueSample original = new MultiValueSample(1000L, List.of(10.0, 20.0, 30.0));
        MultiValueSample copy = (MultiValueSample) original.deepCopy();

        assertEquals(original, copy);

        // Modify copy - should not affect original
        copy.insert(40.0);
        assertEquals(3, original.getValueList().size());
        assertEquals(4, copy.getValueList().size());
    }

    public void testWithCapacity() {
        MultiValueSample sample = MultiValueSample.withCapacity(1000L, 5);
        assertEquals(1000L, sample.getTimestamp());
        assertTrue(sample.getValueList().isEmpty());

        sample.insert(10.0);
        sample.insert(20.0);
        sample.insert(30.0);
        assertEquals(List.of(10.0, 20.0, 30.0), sample.getValueList());
    }
}
