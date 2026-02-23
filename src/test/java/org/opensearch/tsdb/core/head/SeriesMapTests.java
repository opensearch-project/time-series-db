/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.core.head;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.Labels;

import java.util.List;

public class SeriesMapTests extends OpenSearchTestCase {

    public void testEmptySeriesMap() {
        SeriesMap seriesMap = new SeriesMap();
        assertEquals(0, seriesMap.size());
        assertTrue(seriesMap.getSeriesMap().isEmpty());
        assertNull(seriesMap.getByReference(123L));
    }

    public void testAddAndGetByReference() {
        SeriesMap seriesMap = new SeriesMap();
        Labels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels labels2 = ByteLabels.fromStrings("k3", "v3", "k4", "v4");

        MemSeries series1 = new MemSeries(123L, labels1, SeriesEventListener.NOOP);
        MemSeries series2 = new MemSeries(456L, labels2, SeriesEventListener.NOOP);

        seriesMap.add(series1);
        seriesMap.add(series2);

        assertEquals(2, seriesMap.size());
        assertEquals(series1, seriesMap.getByReference(123L));
        assertEquals(series2, seriesMap.getByReference(456L));
        assertNull(seriesMap.getByReference(789L));
    }

    public void testAddDuplicateReference() {
        SeriesMap seriesMap = new SeriesMap();
        Labels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels labels2 = ByteLabels.fromStrings("k3", "v3", "k4", "v4");

        MemSeries series1 = new MemSeries(123L, labels1, SeriesEventListener.NOOP);
        MemSeries series2 = new MemSeries(123L, labels2, SeriesEventListener.NOOP);

        seriesMap.add(series1);
        seriesMap.add(series2);

        assertEquals(1, seriesMap.size());
        assertEquals(series2, seriesMap.getByReference(123L));
    }

    public void testDelete() {
        SeriesMap seriesMap = new SeriesMap();
        Labels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels labels2 = ByteLabels.fromStrings("k3", "v3", "k4", "v4");

        MemSeries series1 = new MemSeries(123L, labels1, SeriesEventListener.NOOP);
        MemSeries series2 = new MemSeries(456L, labels2, SeriesEventListener.NOOP);

        seriesMap.add(series1);
        seriesMap.add(series2);
        assertEquals(2, seriesMap.size());

        seriesMap.delete(series1);
        assertEquals(1, seriesMap.size());
        assertNull(seriesMap.getByReference(123L));
        assertEquals(series2, seriesMap.getByReference(456L));

        seriesMap.delete(series2);
        assertEquals(0, seriesMap.size());
        assertNull(seriesMap.getByReference(456L));
    }

    public void testDeleteNonExistentSeries() {
        SeriesMap seriesMap = new SeriesMap();
        Labels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels labels2 = ByteLabels.fromStrings("k3", "v3", "k4", "v4");

        MemSeries series1 = new MemSeries(123L, labels1, SeriesEventListener.NOOP);
        MemSeries series2 = new MemSeries(456L, labels2, SeriesEventListener.NOOP);

        seriesMap.add(series1);
        assertEquals(1, seriesMap.size());

        seriesMap.delete(series2);
        assertEquals(1, seriesMap.size());
        assertEquals(series1, seriesMap.getByReference(123L));
    }

    public void testGetSeriesMapSnapshot() {
        SeriesMap seriesMap = new SeriesMap();
        Labels labels1 = ByteLabels.fromStrings("k1", "v1", "k2", "v2");
        Labels labels2 = ByteLabels.fromStrings("k3", "v3", "k4", "v4");

        MemSeries series1 = new MemSeries(123L, labels1, SeriesEventListener.NOOP);
        MemSeries series2 = new MemSeries(456L, labels2, SeriesEventListener.NOOP);

        seriesMap.add(series1);
        seriesMap.add(series2);

        List<MemSeries> snapshot = seriesMap.getSeriesMap();
        assertEquals(2, snapshot.size());
        assertTrue(snapshot.contains(series1));
        assertTrue(snapshot.contains(series2));

        seriesMap.add(new MemSeries(789L, ByteLabels.fromStrings("k5", "v5"), SeriesEventListener.NOOP));
        assertEquals(2, snapshot.size());
        assertEquals(3, seriesMap.size());
    }

    public void testSize() {
        SeriesMap seriesMap = new SeriesMap();
        assertEquals(0, seriesMap.size());

        for (int i = 0; i < 5; i++) {
            Labels labels = ByteLabels.fromStrings("key", "value" + i);
            MemSeries series = new MemSeries(i, labels, SeriesEventListener.NOOP);
            seriesMap.add(series);
            assertEquals(i + 1, seriesMap.size());
        }

        for (int i = 0; i < 5; i++) {
            Labels labels = ByteLabels.fromStrings("key", "value" + i);
            MemSeries series = new MemSeries(i, labels, SeriesEventListener.NOOP);
            seriesMap.delete(series);
            assertEquals(4 - i, seriesMap.size());
        }
    }

    public void testPutIfAbsent() {
        SeriesMap seriesMap = new SeriesMap();
        Labels labels1 = ByteLabels.fromStrings("k1", "v1");
        Labels labels2 = ByteLabels.fromStrings("k2", "v2");

        MemSeries series1 = new MemSeries(123L, labels1, SeriesEventListener.NOOP);
        MemSeries series2 = new MemSeries(123L, labels2, SeriesEventListener.NOOP);

        MemSeries result1 = seriesMap.putIfAbsent(series1);
        assertEquals(series1, result1);
        assertEquals(1, seriesMap.size());
        assertEquals(series1, seriesMap.getByReference(123L));

        MemSeries result2 = seriesMap.putIfAbsent(series2);
        assertEquals(series1, result2);
        assertEquals(1, seriesMap.size());
        assertEquals(series1, seriesMap.getByReference(123L));
    }
}
