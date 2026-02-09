/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DerivativeStageTests extends AbstractWireSerializingTestCase<DerivativeStage> {

    public void testProcessWithEmptyInput() {
        DerivativeStage stage = new DerivativeStage();
        List<TimeSeries> input = new ArrayList<>();
        List<TimeSeries> result = stage.process(input);
        assertTrue(result.isEmpty());
    }

    public void testProcessWithNullInput() {
        DerivativeStage stage = new DerivativeStage();
        TestUtils.assertNullInputThrowsException(stage, "derivative");
    }

    public void testProcessBasicDerivative() {
        DerivativeStage stage = new DerivativeStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "actions", "city", "atlanta");
        List<Sample> samples = List.of(
            new FloatSample(1000L, 10.0),
            new FloatSample(2000L, 30.0),
            new FloatSample(3000L, 20.0),
            new FloatSample(4000L, 50.0)
        );
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 4000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        List<Sample> expectedSamples = List.of(
            new FloatSample(2000L, 20.0), // 30 - 10
            new FloatSample(3000L, -10.0), // 20 - 30
            new FloatSample(4000L, 30.0) // 50 - 20
        );
        assertSamplesEqual("Basic derivative", expectedSamples, result.get(0).getSamples().toList());
    }

    public void testProcessWithNaNValues() {
        DerivativeStage stage = new DerivativeStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "bookings", "dc", "dca8");
        List<Sample> samples = List.of(
            new FloatSample(1000L, Double.NaN),
            new FloatSample(2000L, 20.0),
            new FloatSample(3000L, Double.NaN),
            new FloatSample(4000L, 30.0)
        );
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 4000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        assertEquals(0, result.get(0).getSamples().size()); // NaN values not inserted
    }

    public void testProcessWithSingleSample() {
        DerivativeStage stage = new DerivativeStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries series = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);
        List<TimeSeries> result = stage.process(List.of(series));

        assertEquals(1, result.size());
        assertEquals(0, result.get(0).getSamples().size()); // No derivative for single sample
    }

    public void testProcessWithEmptySamples() {
        DerivativeStage stage = new DerivativeStage();
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        TimeSeries series = new TimeSeries(new ArrayList<>(), labels, 1000L, 1000L, 1000L, null);
        List<TimeSeries> input = List.of(series);

        List<TimeSeries> result = stage.process(input);

        assertEquals(1, result.size());
        assertTrue(result.get(0).getSamples().isEmpty());
    }

    public void testFromArgs() {
        DerivativeStage stage = DerivativeStage.fromArgs(Map.of());
        assertEquals("derivative", stage.getName());
    }

    public void testEstimateMemoryOverhead() {
        DerivativeStage stage = new DerivativeStage();

        // Null and empty input should return 0
        assertEquals(0L, stage.estimateMemoryOverhead(null));
        assertEquals(0L, stage.estimateMemoryOverhead(List.of()));

        // Create test series with samples
        ByteLabels labels = ByteLabels.fromStrings("name", "test");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0), new FloatSample(3000L, 30.0));
        TimeSeries ts = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, null);

        // Single series should return positive overhead
        long overhead = stage.estimateMemoryOverhead(List.of(ts));
        assertTrue("Overhead should be positive for non-empty input", overhead > 0);

        // Multiple series should scale
        long doubleOverhead = stage.estimateMemoryOverhead(List.of(ts, ts));
        assertTrue("Overhead should scale with number of series", doubleOverhead > overhead);
    }

    @Override
    protected DerivativeStage createTestInstance() {
        return new DerivativeStage();
    }

    @Override
    protected Writeable.Reader<DerivativeStage> instanceReader() {
        return DerivativeStage::readFrom;
    }
}
