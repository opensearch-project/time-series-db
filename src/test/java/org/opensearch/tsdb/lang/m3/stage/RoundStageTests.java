/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.aggregator.TimeSeriesProvider;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RoundStageTests extends AbstractWireSerializingTestCase<RoundStage> {

    // ========== Constructor Tests ==========

    public void testDefaultConstructor() {
        // Arrange & Act
        RoundStage roundStage = new RoundStage();

        // Assert
        assertEquals("round", roundStage.getName());

        // Test behavior to verify precision = 0 (rounds to integers)
        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        TimeSeries inputSeries = new TimeSeries(Arrays.asList(new FloatSample(1000L, 1.7)), labels, 1000L, 1000L, 1000L, "test");
        List<TimeSeries> result = roundStage.process(Arrays.asList(inputSeries));
        assertEquals(2.0, result.get(0).getSamples().getValue(0), 0.001); // 1.7 -> 2 (precision=0)
    }

    public void testConstructorWithPrecision() {
        // Arrange & Act
        RoundStage roundStage = new RoundStage(2);

        // Assert
        assertEquals("round", roundStage.getName());

        // Test behavior to verify precision = 2 (rounds to 2 decimal places)
        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        TimeSeries inputSeries = new TimeSeries(Arrays.asList(new FloatSample(1000L, 1.234)), labels, 1000L, 1000L, 1000L, "test");
        List<TimeSeries> result = roundStage.process(Arrays.asList(inputSeries));
        assertEquals(1.23, result.get(0).getSamples().getValue(0), 0.001); // 1.234 -> 1.23 (precision=2)
    }

    public void testConstructorWithNegativePrecision() {
        // Arrange & Act
        RoundStage roundStage = new RoundStage(-2);

        // Assert
        assertEquals("round", roundStage.getName());

        // Test behavior to verify precision = -2 (returns original value)
        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        TimeSeries inputSeries = new TimeSeries(Arrays.asList(new FloatSample(1000L, 150.0)), labels, 1000L, 1000L, 1000L, "test");
        List<TimeSeries> result = roundStage.process(Arrays.asList(inputSeries));
        assertEquals(150.0, result.get(0).getSamples().getValue(0), 0.001); // 150 -> 150 (precision=-2)
    }

    // ========== Basic Functionality Tests ==========

    public void testProcessWithDefaultPrecision() {
        // Arrange
        RoundStage roundStage = new RoundStage();
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.7), new FloatSample(2000L, 2.3), new FloatSample(3000L, 3.8));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = roundStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        TimeSeries roundedTimeSeries = result.get(0);
        assertEquals(3, roundedTimeSeries.getSamples().size());
        assertEquals(2.0, roundedTimeSeries.getSamples().getValue(0), 0.001); // 1.7 -> 2
        assertEquals(2.0, roundedTimeSeries.getSamples().getValue(1), 0.001); // 2.3 -> 2
        assertEquals(4.0, roundedTimeSeries.getSamples().getValue(2), 0.001); // 3.8 -> 4
        assertEquals(labels, roundedTimeSeries.getLabels());
        assertEquals("test-series", roundedTimeSeries.getAlias());
    }

    public void testProcessWithPositivePrecision() {
        // Arrange
        RoundStage roundStage = new RoundStage(2);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.234), new FloatSample(2000L, 2.567), new FloatSample(3000L, 3.891));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = roundStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        TimeSeries roundedTimeSeries = result.get(0);
        assertEquals(3, roundedTimeSeries.getSamples().size());
        assertEquals(1.23, roundedTimeSeries.getSamples().getValue(0), 0.001); // 1.234 -> 1.23
        assertEquals(2.57, roundedTimeSeries.getSamples().getValue(1), 0.001); // 2.567 -> 2.57
        assertEquals(3.89, roundedTimeSeries.getSamples().getValue(2), 0.001); // 3.891 -> 3.89
    }

    public void testProcessWithNegativePrecision() {
        // Arrange
        RoundStage roundStage = new RoundStage(-2);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        List<Sample> samples = Arrays.asList(
            new FloatSample(1000L, 123.45),
            new FloatSample(2000L, 267.89),
            new FloatSample(3000L, 391.23)
        );
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = roundStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        assertEquals(1, result.size());
        TimeSeries roundedTimeSeries = result.get(0);
        assertEquals(3, roundedTimeSeries.getSamples().size());
        assertEquals(123.45, roundedTimeSeries.getSamples().getValue(0), 0.001); // 123.45 -> 123.45 (unchanged)
        assertEquals(267.89, roundedTimeSeries.getSamples().getValue(1), 0.001); // 267.89 -> 267.89 (unchanged)
        assertEquals(391.23, roundedTimeSeries.getSamples().getValue(2), 0.001); // 391.23 -> 391.23 (unchanged)
    }

    public void testProcessWithMultipleTimeSeries() {
        // Arrange
        RoundStage roundStage = new RoundStage(1);
        Labels labels1 = ByteLabels.fromMap(Map.of("service", "api"));
        Labels labels2 = ByteLabels.fromMap(Map.of("service", "db"));

        TimeSeries timeSeries1 = new TimeSeries(Arrays.asList(new FloatSample(1000L, 4.25)), labels1, 1000L, 1000L, 1000L, "api-series");
        TimeSeries timeSeries2 = new TimeSeries(Arrays.asList(new FloatSample(2000L, 8.76)), labels2, 2000L, 2000L, 1000L, "db-series");

        // Act
        List<TimeSeries> result = roundStage.process(Arrays.asList(timeSeries1, timeSeries2));

        // Assert
        assertEquals(2, result.size());
        assertEquals(4.3, result.get(0).getSamples().getValue(0), 0.001); // 4.25 -> 4.3
        assertEquals(8.8, result.get(1).getSamples().getValue(0), 0.001); // 8.76 -> 8.8
    }

    public void testProcessWithEmptyInput() {
        // Arrange
        RoundStage roundStage = new RoundStage(2);

        // Act
        List<TimeSeries> result = roundStage.process(Arrays.asList());

        // Assert
        assertTrue(result.isEmpty());
    }

    public void testProcessWithZeroValues() {
        // Arrange
        RoundStage roundStage = new RoundStage(2);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries inputTimeSeries = new TimeSeries(Arrays.asList(new FloatSample(1000L, 0.0)), labels, 1000L, 1000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = roundStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        assertEquals(0.0, result.get(0).getSamples().getValue(0), 0.001);
    }

    public void testProcessWithNegativeValues() {
        // Arrange
        RoundStage roundStage = new RoundStage(1);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries inputTimeSeries = new TimeSeries(
            Arrays.asList(new FloatSample(1000L, -3.27), new FloatSample(2000L, -5.83)),
            labels,
            1000L,
            2000L,
            1000L,
            "test-series"
        );

        // Act
        List<TimeSeries> result = roundStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        assertEquals(-3.3, result.get(0).getSamples().getValue(0), 0.001); // -3.27 -> -3.3
        assertEquals(-5.8, result.get(0).getSamples().getValue(1), 0.001); // -5.83 -> -5.8
    }

    // ========== Edge Cases Tests ==========

    public void testProcessWithSmallValues() {
        // Arrange
        RoundStage roundStage = new RoundStage(6);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api"));
        TimeSeries inputTimeSeries = new TimeSeries(
            Arrays.asList(new FloatSample(1000L, 0.0000123456789)),
            labels,
            1000L,
            1000L,
            1000L,
            "test-series"
        );

        // Act
        List<TimeSeries> result = roundStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        assertEquals(0.000012, result.get(0).getSamples().getValue(0), 0.0000001);
    }

    // ========== Metadata Preservation Tests ==========

    public void testProcessPreservesMetadata() {
        // Arrange
        RoundStage roundStage = new RoundStage(1);
        Labels labels = ByteLabels.fromMap(Map.of("service", "api", "region", "us-east"));
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.25));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "preserved-alias");

        // Act
        List<TimeSeries> result = roundStage.process(Arrays.asList(inputTimeSeries));

        // Assert
        TimeSeries roundedTimeSeries = result.get(0);
        assertEquals(labels, roundedTimeSeries.getLabels());
        assertEquals("preserved-alias", roundedTimeSeries.getAlias());
        assertEquals(1000L, roundedTimeSeries.getMinTimestamp());
        assertEquals(2000L, roundedTimeSeries.getMaxTimestamp());
        assertEquals(1000L, roundedTimeSeries.getStep());
    }

    public void testSupportConcurrentSegmentSearch() {
        // Arrange
        RoundStage roundStage = new RoundStage(2);

        // Act & Assert
        // RoundStage uses default UnaryPipelineStage implementation which returns false
        assertTrue(roundStage.supportConcurrentSegmentSearch());
    }

    public void testGetName() {
        // Arrange
        RoundStage roundStage = new RoundStage(1);

        // Act & Assert
        assertEquals("round", roundStage.getName());
    }

    public void testReduceMethod() {
        // Test the reduce method from UnaryPipelineStage interface
        RoundStage stage = new RoundStage(2);

        // Create mock TimeSeriesProvider instances
        List<TimeSeriesProvider> aggregations = Arrays.asList(
            createMockTimeSeriesProvider("provider1"),
            createMockTimeSeriesProvider("provider2")
        );

        // Test the reduce method - should throw UnsupportedOperationException for unary stages
        UnsupportedOperationException exception = assertThrows(
            UnsupportedOperationException.class,
            () -> stage.reduce(aggregations, false)
        );

        assertTrue("Exception message should contain class name", exception.getMessage().contains("RoundStage"));
        assertTrue("Exception message should mention reduce function", exception.getMessage().contains("reduce function"));
    }

    // ========== Mathematical Properties Tests ==========

    public void testIdempotentProperty() {
        // Test that applying round multiple times with same precision has same effect
        RoundStage roundStage = new RoundStage(2);
        Labels labels = ByteLabels.fromMap(Map.of("test", "value"));
        TimeSeries inputSeries = new TimeSeries(Arrays.asList(new FloatSample(1000L, 3.14159)), labels, 1000L, 1000L, 1000L, "test");

        // Apply rounding once
        List<TimeSeries> firstRound = roundStage.process(Arrays.asList(inputSeries));

        // Apply rounding again to the result
        List<TimeSeries> secondRound = roundStage.process(firstRound);

        // Results should be identical
        assertEquals(firstRound.get(0).getSamples().getValue(0), secondRound.get(0).getSamples().getValue(0), 0.001);
    }

    // ========== Null Input Tests ==========

    public void testNullInputThrowsException() {
        RoundStage stage = new RoundStage(1);
        TestUtils.assertNullInputThrowsException(stage, "round");
    }

    // ========== getPrecision() Tests ==========

    public void testGetPrecision() {
        // Test default precision
        RoundStage defaultStage = new RoundStage();
        assertEquals(0, defaultStage.getPrecision());

        // Test positive precision
        RoundStage positiveStage = new RoundStage(3);
        assertEquals(3, positiveStage.getPrecision());

        // Test negative precision
        RoundStage negativeStage = new RoundStage(-2);
        assertEquals(-2, negativeStage.getPrecision());

        // Test large precision
        RoundStage largeStage = new RoundStage(10);
        assertEquals(10, largeStage.getPrecision());
    }

    // ========== getFactor() Tests ==========

    public void testGetFactorForSmallPrecisions() {
        RoundStage stage = new RoundStage();

        // Test precisions within POW10 array bounds (0-6)
        assertEquals(1.0, stage.getFactor(0), 0.0001);    // 10^0 = 1
        assertEquals(10.0, stage.getFactor(1), 0.0001);   // 10^1 = 10
        assertEquals(100.0, stage.getFactor(2), 0.0001);  // 10^2 = 100
        assertEquals(1000.0, stage.getFactor(3), 0.0001); // 10^3 = 1000
        assertEquals(10000.0, stage.getFactor(4), 0.0001); // 10^4 = 10000
        assertEquals(100000.0, stage.getFactor(5), 0.0001); // 10^5 = 100000
        assertEquals(1000000.0, stage.getFactor(6), 0.0001); // 10^6 = 1000000
    }

    public void testGetFactorForLargePrecisions() {
        RoundStage stage = new RoundStage();

        // Test precisions beyond POW10 array bounds (uses Math.pow)
        assertEquals(10000000.0, stage.getFactor(7), 0.0001); // 10^7 = 10000000
        assertEquals(100000000.0, stage.getFactor(8), 0.0001); // 10^8 = 100000000
        assertEquals(1000000000.0, stage.getFactor(9), 0.0001); // 10^9 = 1000000000
        assertEquals(1e10, stage.getFactor(10), 0.0001); // 10^10
    }

    // ========== toXContent() Tests ==========

    public void testToXContentWithDefaultPrecision() throws Exception {
        RoundStage stage = new RoundStage();
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"precision\":0}", json);
    }

    public void testToXContentWithPositivePrecision() throws Exception {
        RoundStage stage = new RoundStage(3);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"precision\":3}", json);
    }

    public void testToXContentWithNegativePrecision() throws Exception {
        RoundStage stage = new RoundStage(-2);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{\"precision\":-2}", json);
    }

    // ========== fromArgs() Tests ==========

    public void testFromArgsWithEmptyMap() {
        // Test with empty args map - should use default precision
        Map<String, Object> args = new HashMap<>();
        RoundStage stage = RoundStage.fromArgs(args);

        assertEquals(0, stage.getPrecision());
        assertEquals("round", stage.getName());
    }

    public void testFromArgsWithValidPositivePrecision() {
        // Test with valid positive precision
        Map<String, Object> args = new HashMap<>();
        args.put("precision", 3);
        RoundStage stage = RoundStage.fromArgs(args);

        assertEquals(3, stage.getPrecision());
        assertEquals("round", stage.getName());
    }

    public void testFromArgsWithValidNegativePrecision() {
        // Test with valid negative precision
        Map<String, Object> args = new HashMap<>();
        args.put("precision", -2);
        RoundStage stage = RoundStage.fromArgs(args);

        assertEquals(-2, stage.getPrecision());
        assertEquals("round", stage.getName());
    }

    public void testFromArgsWithZeroPrecision() {
        // Test with explicit zero precision
        Map<String, Object> args = new HashMap<>();
        args.put("precision", 0);
        RoundStage stage = RoundStage.fromArgs(args);

        assertEquals(0, stage.getPrecision());
        assertEquals("round", stage.getName());
    }

    public void testFromArgsWithDifferentNumberTypes() {
        // Test with Integer
        Map<String, Object> args1 = new HashMap<>();
        args1.put("precision", Integer.valueOf(5));
        RoundStage stage1 = RoundStage.fromArgs(args1);
        assertEquals(5, stage1.getPrecision());

        // Test with Long
        Map<String, Object> args2 = new HashMap<>();
        args2.put("precision", Long.valueOf(7));
        RoundStage stage2 = RoundStage.fromArgs(args2);
        assertEquals(7, stage2.getPrecision());

        // Test with Double (should convert to int)
        Map<String, Object> args3 = new HashMap<>();
        args3.put("precision", Double.valueOf(3.7));
        RoundStage stage3 = RoundStage.fromArgs(args3);
        assertEquals(3, stage3.getPrecision());

        // Test with Float (should convert to int)
        Map<String, Object> args4 = new HashMap<>();
        args4.put("precision", Float.valueOf(2.9f));
        RoundStage stage4 = RoundStage.fromArgs(args4);
        assertEquals(2, stage4.getPrecision());
    }

    // ========== fromArgs() Error Cases ==========

    public void testFromArgsWithNullMap() {
        // Test with null args map
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> RoundStage.fromArgs(null));

        assertEquals("Args cannot be null", exception.getMessage());
    }

    public void testFromArgsWithNullPrecision() {
        // Test with null precision value
        Map<String, Object> args = new HashMap<>();
        args.put("precision", null);

        assertEquals(0, RoundStage.fromArgs(args).getPrecision());
    }

    public void testFromArgsWithInvalidPrecisionType() {
        // Test with non-numeric precision value
        Map<String, Object> args = new HashMap<>();
        args.put("precision", "invalid");

        assertThrows(ClassCastException.class, () -> RoundStage.fromArgs(args));
    }

    // ========== RoundStage Creation in PipelineStageFactory ==========

    public void testCreateWithArgsRoundStageValid() {
        // Test creating RoundStage with valid precision
        Map<String, Object> args = Map.of("precision", 3);
        PipelineStage stage = PipelineStageFactory.createWithArgs("round", args);

        assertNotNull(stage);
        assertTrue(stage instanceof RoundStage);
        assertEquals("round", stage.getName());
        // Test that functional behavior is correct (indirect precision validation)
        RoundStage expectedStage = new RoundStage(3);
        assertEquals(expectedStage, stage);
    }

    public void testCreateWithArgsRoundStageDefaultPrecision() {
        // Test creating RoundStage with no precision argument (should use default)
        Map<String, Object> args = Map.of();
        PipelineStage stage = PipelineStageFactory.createWithArgs("round", args);

        assertNotNull(stage);
        assertTrue(stage instanceof RoundStage);
        assertEquals("round", stage.getName());
        // Test that functional behavior is correct (indirect precision validation)
        RoundStage expectedStage = new RoundStage(); // Default precision is 0
        assertEquals(expectedStage, stage);
    }

    // ========== Helper Methods ==========

    private TimeSeriesProvider createMockTimeSeriesProvider(String name) {
        return new TimeSeriesProvider() {
            @Override
            public List<TimeSeries> getTimeSeries() {
                return Collections.emptyList();
            }

            @Override
            public TimeSeriesProvider createReduced(List<TimeSeries> reducedTimeSeries) {
                return this;
            }
        };
    }

    @Override
    protected Writeable.Reader instanceReader() {
        return RoundStage::readFrom;
    }

    @Override
    protected RoundStage createTestInstance() {
        return new RoundStage(randomIntBetween(0, 6));
    }

}
