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
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.core.model.SumCountSample;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
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
        assertEquals(samples, timeSeries.getSamples().toList());
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
        assertEquals(samples, timeSeries.getSamples().toList());
        assertEquals(labels, timeSeries.getLabels());
        assertEquals(1000L, timeSeries.getMinTimestamp());
        assertEquals(2000L, timeSeries.getMaxTimestamp());
        assertEquals(1000L, timeSeries.getStep());
    }

    public void testWithEmptySamples() {
        // Arrange
        Labels labels = ByteLabels.fromMap(Map.of("service", "empty"));
        List<Sample> emptySamples = Arrays.asList();

        // Act
        TimeSeries timeSeries = new TimeSeries(emptySamples, labels, 0L, 0L, 0L, null);

        // Assert
        assertEquals(emptySamples, timeSeries.getSamples().toList());
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

    public void testCalculateAlignedMaxTimestamp_PerfectlyAligned() {
        // queryStart=1000, queryEnd=2000 (exclusive), step=1000
        // Expected: 1000 (not 2000, since queryEnd is exclusive)
        long result = TimeSeries.calculateAlignedMaxTimestamp(1000L, 2000L, 1000L);
        assertEquals(1000L, result);
    }

    public void testCalculateAlignedMaxTimestamp_NotAligned() {
        // queryStart=1000, queryEnd=1995, step=200
        // Expected: 1800 (largest value = 1000 + N*200 where result < 1995)
        long result = TimeSeries.calculateAlignedMaxTimestamp(1000L, 1995L, 200L);
        assertEquals(1800L, result);
    }

    public void testCalculateAlignedMaxTimestamp_ThrowsOnInvalidStep() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TimeSeries.calculateAlignedMaxTimestamp(1000L, 2000L, 0L)
        );
        assertTrue(exception.getMessage().contains("Step must be positive"));
    }

    public void testCalculateAlignedMaxTimestamp_ThrowsOnInvalidRange() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> TimeSeries.calculateAlignedMaxTimestamp(2000L, 1000L, 100L)
        );
        assertTrue(exception.getMessage().contains("Query end must be greater than query start"));
    }

    /**
     * Validate that ESTIMATED_MEMORY_OVERHEAD constant matches actual JVM object layout.
     * This test uses JOL (Java Object Layout) to calculate the actual memory overhead.
     *
     * <p>If this test fails, update TimeSeries.ESTIMATED_MEMORY_OVERHEAD to the value
     * shown in the failure message.</p>
     */
    public void testEstimatedMemoryOverheadIsAccurate() {
        try {
            // Create a minimal TimeSeries instance
            Labels labels = ByteLabels.fromStrings();
            List<Sample> samples = List.of();
            TimeSeries timeSeries = new TimeSeries(samples, labels, 0L, 0L, 1L, null);

            // Get actual JVM layout
            org.openjdk.jol.info.ClassLayout layout = org.openjdk.jol.info.ClassLayout.parseInstance(timeSeries);
            long actualOverhead = layout.instanceSize();

            long constantOverhead = TimeSeries.getEstimatedMemoryOverhead();

            // Allow small variance for JVM-specific differences
            long allowedDelta = 8;
            long difference = Math.abs(actualOverhead - constantOverhead);

            if (difference > allowedDelta) {
                fail(
                    String.format(
                        Locale.ROOT,
                        "ESTIMATED_MEMORY_OVERHEAD constant (%d bytes) does not match actual JVM layout (%d bytes)!\n"
                            + "\n"
                            + "TimeSeries object layout:\n%s\n"
                            + "\n"
                            + "ACTION REQUIRED: Update TimeSeries.ESTIMATED_MEMORY_OVERHEAD to %d\n"
                            + "\n"
                            + "This usually happens when:\n"
                            + "  1. Fields were added/removed from TimeSeries\n"
                            + "  2. JVM version or vendor changed\n"
                            + "  3. JVM flags changed (e.g., -XX:+UseCompressedOops)",
                        constantOverhead,
                        actualOverhead,
                        layout.toPrintable(),
                        actualOverhead
                    )
                );
            }

            // Test passes - log for documentation
            logger.info(
                "TimeSeries memory overhead validation passed:\n"
                    + "  ESTIMATED_MEMORY_OVERHEAD: {} bytes\n"
                    + "  Actual JVM layout: {} bytes",
                constantOverhead,
                actualOverhead
            );

        } catch (Exception e) {
            fail("Failed to validate TimeSeries memory overhead using JOL: " + e.getMessage());
        }
    }

    /**
     * Validate that ESTIMATED_SAMPLE_SIZE is reasonable.
     * Note: Sample size cannot be precisely validated with JOL because:
     * 1. Sample is an interface, not a concrete class
     * 2. JVM may apply scalar replacement optimization (no object allocation)
     * 3. Different Sample implementations have different sizes
     *
     * <p>This test validates the constant is in a reasonable range and documents
     * the expected behavior.</p>
     */
    public void testEstimatedSampleSizeIsReasonable() {
        long sampleSize = TimeSeries.getEstimatedSampleSize();

        // Sample should be at least the data size (timestamp + value = 16 bytes)
        assertTrue("ESTIMATED_SAMPLE_SIZE should be at least 16 bytes (timestamp + value)", sampleSize >= 16);

        // Sample should not exceed full object size (with header ~32 bytes is max reasonable)
        assertTrue("ESTIMATED_SAMPLE_SIZE should not exceed 32 bytes (object header + data)", sampleSize <= 32);

        // Validate documented assumption: favors scalar replacement (16 bytes)
        assertEquals(
            "ESTIMATED_SAMPLE_SIZE assumes scalar replacement optimization (16 bytes). "
                + "If changing this, update the Javadoc in TimeSeries.java",
            16,
            sampleSize
        );

        logger.info(
            "Sample size constant validation passed: {} bytes\n"
                + "  Assumes JVM scalar replacement for hot path optimization\n"
                + "  Without scalar replacement: ~32 bytes (object header + data)\n"
                + "  With scalar replacement: 16 bytes (just timestamp + value)",
            sampleSize
        );
    }

    /**
     * Tests that ramBytesUsed() returns a reasonable estimate for a TimeSeries.
     * This validates the delegation pattern where TimeSeries.ramBytesUsed() aggregates
     * its components' memory estimates.
     */
    public void testRamBytesUsedReturnsReasonableValue() {
        // Arrange: Create a TimeSeries with known components
        Labels labels = ByteLabels.fromMap(Map.of("region", "us-east", "service", "api"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0));
        String alias = "test-alias";
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, alias);

        // Act
        long ramBytes = ts.ramBytesUsed();

        // Assert
        // Should be at least the object overhead
        assertTrue("ramBytesUsed should be at least ESTIMATED_MEMORY_OVERHEAD", ramBytes >= TimeSeries.ESTIMATED_MEMORY_OVERHEAD);

        // Should include samples contribution
        assertTrue(
            "ramBytesUsed should be greater than object overhead alone (should include samples)",
            ramBytes > TimeSeries.ESTIMATED_MEMORY_OVERHEAD
        );

        // Sanity check: should be reasonable (not absurdly large)
        // A TimeSeries with 3 samples, labels, and alias should be under 1KB
        assertTrue("ramBytesUsed should be under 1KB for small TimeSeries", ramBytes < 1024);

        logger.info("TimeSeries.ramBytesUsed() returned {} bytes for 3-sample series with labels and alias", ramBytes);
    }

    /**
     * Tests that ramBytesUsed() correctly handles null labels and alias.
     */
    public void testRamBytesUsedWithNullComponents() {
        // Arrange: Create a minimal TimeSeries with null labels and alias
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0));
        TimeSeries ts = new TimeSeries(samples, null, 1000L, 1000L, 1000L, null);

        // Act
        long ramBytes = ts.ramBytesUsed();

        // Assert: Should be at least object overhead + samples
        assertTrue(
            "ramBytesUsed should be at least ESTIMATED_MEMORY_OVERHEAD with null labels/alias",
            ramBytes >= TimeSeries.ESTIMATED_MEMORY_OVERHEAD
        );
    }

    /**
     * Tests that ramBytesUsed() scales with the number of samples.
     */
    public void testRamBytesUsedScalesWithSamples() {
        // Arrange: Create two TimeSeries with different sample counts
        List<Sample> smallSamples = Arrays.asList(new FloatSample(1000L, 1.0));
        List<Sample> largeSamples = Arrays.asList(
            new FloatSample(1000L, 1.0),
            new FloatSample(2000L, 2.0),
            new FloatSample(3000L, 3.0),
            new FloatSample(4000L, 4.0),
            new FloatSample(5000L, 5.0)
        );

        TimeSeries smallTs = new TimeSeries(smallSamples, null, 1000L, 1000L, 1000L, null);
        TimeSeries largeTs = new TimeSeries(largeSamples, null, 1000L, 5000L, 1000L, null);

        // Act
        long smallEstimate = smallTs.ramBytesUsed();
        long largeEstimate = largeTs.ramBytesUsed();

        // Assert: Larger sample count should result in larger estimate
        assertTrue(
            "TimeSeries with more samples should have larger estimate. Small: " + smallEstimate + ", Large: " + largeEstimate,
            largeEstimate > smallEstimate
        );
    }

    /**
     * Tests that getChildResources() returns the samples as a child resource.
     */
    public void testGetChildResourcesReturnsSamples() {
        // Arrange: Create a TimeSeries with samples
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0));
        TimeSeries ts = new TimeSeries(samples, null, 1000L, 2000L, 1000L, null);

        // Act
        var children = ts.getChildResources();

        // Assert: Should contain exactly one child (the samples)
        assertNotNull("getChildResources should not return null", children);
        assertEquals("Should have exactly 1 child resource (samples)", 1, children.size());

        // The child should be the SampleList
        var child = children.iterator().next();
        assertEquals("Child should be the samples", ts.getSamples(), child);
    }

    /**
     * Tests that getChildResources() returns empty collection when samples is null.
     */
    public void testGetChildResourcesWithNullSamples() {
        // Arrange: Create a TimeSeries with null samples (using SampleList directly)
        TimeSeries ts = new TimeSeries((SampleList) null, null, 1000L, 2000L, 1000L, null);

        // Act
        var children = ts.getChildResources();

        // Assert: Should return empty collection
        assertNotNull("getChildResources should not return null even with null samples", children);
        assertTrue("Should have no child resources when samples is null", children.isEmpty());
    }
}
