/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SumCountSample;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TimeSeriesTests extends OpenSearchTestCase {

    public void testConstructor() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("region", "us-east", "service", "api"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0));
        long minTimestamp = 1000L;
        long maxTimestamp = 2000L;
        long step = 1000L;
        String alias = "test-alias";

        // Act
        TimeSeries timeSeries = new TimeSeries(samples, labels, minTimestamp, maxTimestamp, step, alias);

        // Assert
        assertEquals(samples, timeSeries.getSamples());
        assertEquals(labels, timeSeries.getLabels());
        assertEquals(minTimestamp, timeSeries.getMinTimestamp());
        assertEquals(maxTimestamp, timeSeries.getMaxTimestamp());
        assertEquals(step, timeSeries.getStep());
        assertEquals(alias, timeSeries.getAlias());
    }

    public void testSetAlias() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "test"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        // Act
        timeSeries.setAlias("new-alias");

        // Assert
        assertEquals("new-alias", timeSeries.getAlias());
    }

    public void testSetAliasToNull() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "test"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "original-alias");

        // Act
        timeSeries.setAlias(null);

        // Assert
        assertNull(timeSeries.getAlias());
    }

    public void testGetLabelsMap() {
        // Arrange
        Map<String, String> expectedLabels = Map.of("region", "us-east", "service", "api", "version", "v1");
        Labels labels = ByteLabels.fromMap(expectedLabels);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        // Act
        Map<String, String> actualLabels = timeSeries.getLabelsMap();

        // Assert
        assertEquals(expectedLabels, actualLabels);
    }

    public void testGetLabelsMapWithNullLabels() {
        // Arrange
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0));
        TimeSeries timeSeries = new TimeSeries(samples, (Labels) null, 1000L, 2000L, 1000L, null);

        // Act
        Map<String, String> labelsMap = timeSeries.getLabelsMap();

        // Assert
        assertNotNull(labelsMap);
        assertTrue(labelsMap.isEmpty());
    }

    public void testEqualsAndHashCode() {
        // Arrange
        Labels labels1 = ByteLabels.fromMap(Map.of("service", "api"));
        Labels labels2 = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples1 = Arrays.asList(new FloatSample(1000L, 1.0));
        List<Sample> samples2 = Arrays.asList(new FloatSample(1000L, 1.0));

        TimeSeries timeSeries1 = new TimeSeries(samples1, labels1, 1000L, 2000L, 1000L, "alias");
        TimeSeries timeSeries2 = new TimeSeries(samples2, labels2, 1000L, 2000L, 1000L, "alias");
        TimeSeries timeSeries3 = new TimeSeries(samples1, labels1, 1000L, 2000L, 1000L, "different-alias");

        // Test equals
        assertEquals(timeSeries1, timeSeries1); // Same instance
        assertEquals(timeSeries1, timeSeries2); // Same values
        assertNotEquals(timeSeries1, timeSeries3); // Different alias
        assertNotEquals(timeSeries1, null); // Null
        assertNotEquals(timeSeries1, "not a time series"); // Different type

        // Test hashCode
        assertEquals(timeSeries1.hashCode(), timeSeries2.hashCode());
        assertNotEquals(timeSeries1.hashCode(), timeSeries3.hashCode());
    }

    public void testEqualsWithDifferentLabels() {
        // Arrange
        Labels labels1 = ByteLabels.fromMap(Map.of("service", "api"));
        Labels labels2 = ByteLabels.fromMap(Map.of("service", "db"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0));

        TimeSeries timeSeries1 = new TimeSeries(samples, labels1, 1000L, 2000L, 1000L, null);
        TimeSeries timeSeries2 = new TimeSeries(samples, labels2, 1000L, 2000L, 1000L, null);

        // Assert
        assertNotEquals(timeSeries1, timeSeries2);
        assertNotEquals(timeSeries1.hashCode(), timeSeries2.hashCode());
    }

    public void testEqualsWithDifferentSamples() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples1 = Arrays.asList(new FloatSample(1000L, 1.0));
        List<Sample> samples2 = Arrays.asList(new FloatSample(1000L, 2.0));

        TimeSeries timeSeries1 = new TimeSeries(samples1, labels, 1000L, 2000L, 1000L, null);
        TimeSeries timeSeries2 = new TimeSeries(samples2, labels, 1000L, 2000L, 1000L, null);

        // Assert
        assertNotEquals(timeSeries1, timeSeries2);
    }

    public void testToString() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "api", "region", "us-east"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "test-alias");

        // Act
        String str = timeSeries.toString();

        // Assert
        assertTrue(str.contains("TimeSeries"));
        assertTrue(str.contains("samples=" + samples.toString()));
        assertTrue(str.contains("labels=" + labels.toString()));
        assertTrue(str.contains("alias='test-alias'"));
        assertTrue(str.contains("minTimestamp=1000"));
        assertTrue(str.contains("maxTimestamp=2000"));
        assertTrue(str.contains("step=1000"));
    }

    public void testToStringWithNullAlias() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        // Act
        String str = timeSeries.toString();

        // Assert
        assertTrue(str.contains("TimeSeries"));
        assertTrue(str.contains("alias='null'"));
    }

    public void testToStringWithZeroMetadata() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 0L, 0L, 0L, null);

        // Act
        String str = timeSeries.toString();

        // Assert
        assertTrue(str.contains("minTimestamp=0"));
        assertTrue(str.contains("maxTimestamp=0"));
        assertTrue(str.contains("step=0"));
    }

    public void testWithSumCountSamples() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(new SumCountSample(1000L, 10.0, 2), new SumCountSample(2000L, 20.0, 3));

        // Act
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        // Assert
        assertEquals(samples, timeSeries.getSamples());
        assertEquals(labels, timeSeries.getLabels());
        assertEquals(1000L, timeSeries.getMinTimestamp());
        assertEquals(2000L, timeSeries.getMaxTimestamp());
        assertEquals(1000L, timeSeries.getStep());
    }

    public void testWithMixedSampleTypes() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "mixed"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new SumCountSample(2000L, 10.0, 2));

        // Act
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, null);

        // Assert
        assertEquals(samples, timeSeries.getSamples());
        assertEquals(2, timeSeries.getSamples().size());
        assertTrue(timeSeries.getSamples().get(0) instanceof FloatSample);
        assertTrue(timeSeries.getSamples().get(1) instanceof SumCountSample);
    }

    public void testWithEmptySamples() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "empty"));
        List<Sample> emptySamples = Arrays.asList();

        // Act
        TimeSeries timeSeries = new TimeSeries(emptySamples, labels, 0L, 0L, 0L, null);

        // Assert
        assertEquals(emptySamples, timeSeries.getSamples());
        assertTrue(timeSeries.getSamples().isEmpty());
    }

    public void testWithLargeMetadataValues() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "large"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0));
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        long step = Long.MAX_VALUE;

        // Act
        TimeSeries timeSeries = new TimeSeries(samples, labels, minTimestamp, maxTimestamp, step, null);

        // Assert
        assertEquals(minTimestamp, timeSeries.getMinTimestamp());
        assertEquals(maxTimestamp, timeSeries.getMaxTimestamp());
        assertEquals(step, timeSeries.getStep());
    }
}
