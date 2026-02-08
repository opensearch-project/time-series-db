/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.model;

import org.opensearch.test.OpenSearchTestCase;

public class FloatSampleListTests extends OpenSearchTestCase {

    public void testBuilder() {
        FloatSampleList.Builder builder = new FloatSampleList.Builder();

        for (int i = 0; i < 100; i++) {
            builder.add(i, i * 2);
        }

        for (int i = 0; i < 100; i += 2) {
            builder.set(i, i, -i * 2);
        }

        SampleList sampleList = builder.build();

        // cannot call twice
        expectThrows(IllegalStateException.class, builder::build);

        assertEquals(100, sampleList.size());
        for (int i = 0; i < sampleList.size(); i++) {
            if (i % 2 == 0) {
                assertEquals(-i * 2, sampleList.getValue(i), 0.0001);
            } else {
                assertEquals(i * 2, sampleList.getValue(i), 0.0001);
            }
        }
    }

    public void testIteration() {
        FloatSampleList.Builder builder = new FloatSampleList.Builder();
        for (int i = 0; i < 100; i++) {
            builder.add(i, i * 2);
        }
        SampleList sampleList = builder.build();

        int cnt = 0;
        for (Sample sample : sampleList) {
            assertEquals(cnt, sample.getTimestamp());
            assertEquals(cnt * 2, sample.getValue(), 0.0001);
            assertEquals(SampleType.FLOAT_SAMPLE, sample.getSampleType());
            expectThrows(UnsupportedOperationException.class, () -> sample.merge(new FloatSample(1, 2)));
            assertTrue(sample.deepCopy() instanceof FloatSample);
            cnt++;
        }
    }

    public void testBinarySearch() {
        FloatSampleList.Builder builder = new FloatSampleList.Builder();
        for (int i = 0; i < 100; i++) {
            builder.add(i * 2, i * 2);
        }
        SampleList sampleList = builder.build();

        for (int i = 0; i < 200; i++) {
            if (i % 2 == 0) {
                // in the list
                assertEquals(i / 2, sampleList.search(i));
            } else {
                int insertionPoint = (i + 1) / 2; // the index of the first element greater than the key
                assertEquals(-insertionPoint - 1, sampleList.search(i));
            }
        }
    }

    public void testSubList() {
        FloatSampleList.Builder builder = new FloatSampleList.Builder();
        for (int i = 0; i < 100; i++) {
            builder.add(i, i * 2);
        }
        SampleList sampleList = builder.build().subList(25, 75);

        assertEquals(50, sampleList.size());
        for (int i = 0; i < sampleList.size(); i++) {
            assertEquals(i + 25, sampleList.getTimestamp(i));
            assertEquals((i + 25) * 2, sampleList.getValue(i), 0.0001);
        }
    }

    public void testConstantList() {
        FloatSampleList.ConstantList constantList = new FloatSampleList.ConstantList(0, 10000, 2000, 1.5);

        assertEquals(6, constantList.size());

        long nextTimestamp = 0;
        for (Sample sample : constantList) {
            assertEquals(nextTimestamp, sample.getTimestamp());
            assertEquals(1.5, sample.getValue(), 0.0001);
            nextTimestamp += 2000;
        }

        assertEquals(-1, constantList.search(-1000));
        assertEquals(2, constantList.search(4000));
        assertEquals(-3 - 1, constantList.search(5000));
        assertEquals(-6 - 1, constantList.search(11000));

        SampleList subList = constantList.subList(1, 3);
        assertEquals(2, subList.size());
        assertEquals(4000, subList.getTimestamp(1));
    }
}
