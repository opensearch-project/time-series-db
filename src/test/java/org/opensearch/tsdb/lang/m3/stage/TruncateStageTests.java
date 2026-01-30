/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.List;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import static org.opensearch.tsdb.TestUtils.findSeriesByLabel;
import static org.opensearch.tsdb.TestUtils.assertNullInputThrowsException;

/**
 * Unit tests for TruncateStage
 */
public class TruncateStageTests extends AbstractWireSerializingTestCase<TruncateStage> {

    @Override
    protected TruncateStage createTestInstance() {
        long min = randomLongBetween(0, 1000);
        long max = randomLongBetween(min + 1, 2000);
        return new TruncateStage(min, max);
    }

    @Override
    protected TruncateStage mutateInstance(TruncateStage instance) {
        if (instance.equals(new TruncateStage(100L, 200L))) {
            return new TruncateStage(150L, 250L);
        }
        return new TruncateStage(100L, 200L);
    }

    @Override
    protected Writeable.Reader<TruncateStage> instanceReader() {
        return TruncateStage::readFrom;
    }

    /**
     * Test truncating with both minTimestamp and maxTimestamp.
     * Tests dense, sparse (missing data at boundaries), and empty series.
     * Note: maxTimestamp is exclusive, so [20, 50) excludes 50.
     */
    public void testTruncate() {
        TruncateStage stage = new TruncateStage(20L, 50L);

        // Dense series: all data points present
        List<Sample> denseSamples = List.of(
            new FloatSample(0L, 1.0),
            new FloatSample(10L, 2.0),
            new FloatSample(20L, 3.0),
            new FloatSample(30L, 4.0),
            new FloatSample(40L, 5.0),
            new FloatSample(50L, 6.0),
            new FloatSample(60L, 7.0)
        );

        // Sparse series: missing data at both boundaries (20L and 50L missing)
        List<Sample> sparseSamples = List.of(
            new FloatSample(0L, 1.0),
            new FloatSample(10L, 2.0),
            new FloatSample(30L, 4.0),
            new FloatSample(40L, 5.0),
            new FloatSample(60L, 7.0)
        );

        // Empty series
        List<Sample> emptySamples = List.of();

        ByteLabels labels1 = ByteLabels.fromStrings("type", "dense");
        ByteLabels labels2 = ByteLabels.fromStrings("type", "sparse");
        ByteLabels labels3 = ByteLabels.fromStrings("type", "empty");

        TimeSeries denseSeries = new TimeSeries(denseSamples, labels1, 0L, 60L, 10L, null);
        TimeSeries sparseSeries = new TimeSeries(sparseSamples, labels2, 0L, 60L, 10L, null);
        TimeSeries emptySeries = new TimeSeries(emptySamples, labels3, 0L, 60L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(denseSeries, sparseSeries, emptySeries));

        assertEquals(3, result.size());

        // Dense: keep samples in [20, 50) - excludes 50
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(new FloatSample(20L, 3.0), new FloatSample(30L, 4.0), new FloatSample(40L, 5.0));
        assertSamplesEqual("Both Dense", expectedDense, denseResult.getSamples().toList());
        assertEquals("Truncated minTimestamp should be 20", 20L, denseResult.getMinTimestamp());
        assertEquals("Truncated maxTimestamp should be 40 (largest aligned < 50)", 40L, denseResult.getMaxTimestamp());

        // Sparse: keep samples in [20, 50) (20 and 50 missing, only 30 and 40)
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(new FloatSample(30L, 4.0), new FloatSample(40L, 5.0));
        assertSamplesEqual("Both Sparse", expectedSparse, sparseResult.getSamples().toList());
        assertEquals("Truncated minTimestamp should be 20", 20L, sparseResult.getMinTimestamp());
        assertEquals("Truncated maxTimestamp should be 40 (largest aligned < 50)", 40L, sparseResult.getMaxTimestamp());

        // Empty: no samples
        TimeSeries emptyResult = findSeriesByLabel(result, "type", "empty");
        assertEquals(0, emptyResult.getSamples().size());
        assertEquals("Truncated minTimestamp should be 20", 20L, emptyResult.getMinTimestamp());
        assertEquals("Truncated maxTimestamp should be 40 (largest aligned < 50)", 40L, emptyResult.getMaxTimestamp());
    }

    /**
     * Test truncating when no samples fall within the range (startIndex > endIndex).
     * This happens when all samples are outside the truncation bounds.
     */
    public void testTruncateNoSamplesInRange() {
        TruncateStage stage = new TruncateStage(100L, 200L);

        // All samples are before the min timestamp
        List<Sample> samples = List.of(
            new FloatSample(0L, 1.0),
            new FloatSample(10L, 2.0),
            new FloatSample(20L, 3.0),
            new FloatSample(30L, 4.0)
        );

        ByteLabels labels = ByteLabels.fromStrings("host", "server1");
        TimeSeries ts = new TimeSeries(samples, labels, 0L, 30L, 10L, null);

        List<TimeSeries> result = stage.process(List.of(ts));

        assertEquals(1, result.size());
        TimeSeries resultTs = result.get(0);

        // Should return empty time series with truncated time bounds
        assertEquals(0, resultTs.getSamples().size());
        assertEquals(labels, resultTs.getLabels());
        assertEquals(100L, resultTs.getMinTimestamp()); // Truncation start
        assertEquals(190L, resultTs.getMaxTimestamp()); // Largest aligned < 200 with step=10
        assertEquals(10L, resultTs.getStep());
    }

    /**
     * Test that constructor throws when minTimestamp >= maxTimestamp.
     */
    public void testTruncateWithMinGreaterThanOrEqualToMax() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> { new TruncateStage(50L, 20L); });
        assertTrue(ex.getMessage().contains("must be <"));

        // Also test when min equals max
        IllegalArgumentException ex2 = expectThrows(IllegalArgumentException.class, () -> { new TruncateStage(50L, 50L); });
        assertTrue(ex2.getMessage().contains("must be <"));
    }

    /**
     * Test getName method.
     */
    public void testGetName() {
        TruncateStage stage = new TruncateStage(10L, 50L);
        assertEquals("truncate", stage.getName());
    }

    /**
     * Test XContent serialization.
     */
    public void testToXContent() throws IOException {
        TruncateStage stage = new TruncateStage(10L, 50L);

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            stage.toXContent(builder, EMPTY_PARAMS);
            builder.endObject();

            String json = builder.toString();
            assertEquals("{\"truncate_start\":10,\"truncate_end\":50}", json);
        }
    }

    /**
     * Test fromArgs method.
     */
    public void testFromArgs() {
        TruncateStage stage = TruncateStage.fromArgs(java.util.Map.of("truncate_start", 10L, "truncate_end", 50L));
        assertEquals(new TruncateStage(10L, 50L), stage);
    }

    /**
     * Test fromArgs throws when truncate_start is missing.
     */
    public void testFromArgsMissingStart() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> TruncateStage.fromArgs(java.util.Map.of("truncate_end", 50L))
        );
        assertTrue(ex.getMessage().contains("requires both"));
    }

    /**
     * Test fromArgs throws when truncate_end is missing.
     */
    public void testFromArgsMissingEnd() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> TruncateStage.fromArgs(java.util.Map.of("truncate_start", 10L))
        );
        assertTrue(ex.getMessage().contains("requires both"));
    }

    public void testNullInputThrowsException() {
        TruncateStage stage = new TruncateStage(100L, 200L);
        assertNullInputThrowsException(stage, "truncate");
    }

    /**
     * Test that TruncateStage supports concurrent segment search.
     */
    public void testSupportConcurrentSegmentSearch() {
        TruncateStage stage = new TruncateStage(10L, 50L);
        assertTrue("TruncateStage should support concurrent segment search", stage.supportConcurrentSegmentSearch());
    }
}
