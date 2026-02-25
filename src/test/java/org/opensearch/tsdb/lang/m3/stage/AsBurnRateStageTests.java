/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

public class AsBurnRateStageTests extends AbstractWireSerializingTestCase<AsBurnRateStage> {

    private static final long STEP = 10_000L; // 10 seconds
    private static final long ONE_MINUTE = 60_000L;

    /**
     * Burn rate = 1 when error rate exactly equals error budget.
     * SLO = 99.9% → error budget = 0.1%.
     * If unexpected/total = 0.001 for every sample, after moving sum the ratio stays 0.001.
     * burn_rate = 0.001 / 0.001 = 1.0
     */
    public void testBurnRateEqualsOneAtBudget() {
        AsBurnRateStage stage = new AsBurnRateStage("right", ONE_MINUTE, 99.9);

        // 7 samples covering 60s window (step=10s, 0s to 60s)
        List<Sample> unexpectedSamples = new ArrayList<>();
        List<Sample> totalSamples = new ArrayList<>();
        for (int i = 0; i <= 6; i++) {
            long ts = i * STEP;
            unexpectedSamples.add(new FloatSample(ts, 1.0));   // 1 error per step
            totalSamples.add(new FloatSample(ts, 1000.0));      // 1000 total per step
        }

        ByteLabels labels = ByteLabels.fromStrings("service", "api");
        TimeSeries unexpected = new TimeSeries(unexpectedSamples, labels, 0L, 60_000L, STEP, null);
        TimeSeries total = new TimeSeries(totalSamples, labels, 0L, 60_000L, STEP, null);

        List<TimeSeries> result = stage.process(List.of(unexpected), List.of(total));
        assertFalse("Should produce results", result.isEmpty());

        assertFalse("Should have samples", result.get(0).getSamples().toList().isEmpty());

        // After moving sum: each sample represents the sum of values in the 60s window.
        // Ratio stays 1/1000 = 0.001. burn_rate = 0.001 / (1 - 0.999) = 0.001 / 0.001 = 1.0
        double burnRate = getLastNonZeroValue(result);
        assertFalse("Expected at least one non-zero, non-NaN sample", Double.isNaN(burnRate));
        assertEquals("Burn rate should be 1.0 at budget", 1.0, burnRate, 0.01);
    }

    /**
     * Burn rate = 7 when consuming 7x the error budget.
     * SLO = 99.9% → error budget = 0.1%.
     * If error rate = 0.7%, burn_rate = 0.007 / 0.001 = 7.0
     */
    public void testBurnRateSevenTimesOverBudget() {
        AsBurnRateStage stage = new AsBurnRateStage("right", ONE_MINUTE, 99.9);

        List<Sample> unexpectedSamples = new ArrayList<>();
        List<Sample> totalSamples = new ArrayList<>();
        for (int i = 0; i <= 6; i++) {
            long ts = i * STEP;
            unexpectedSamples.add(new FloatSample(ts, 7.0));   // 7 errors per step
            totalSamples.add(new FloatSample(ts, 1000.0));
        }

        ByteLabels labels = ByteLabels.fromStrings("service", "api");
        TimeSeries unexpected = new TimeSeries(unexpectedSamples, labels, 0L, 60_000L, STEP, null);
        TimeSeries total = new TimeSeries(totalSamples, labels, 0L, 60_000L, STEP, null);

        List<TimeSeries> result = stage.process(List.of(unexpected), List.of(total));
        assertFalse(result.isEmpty());

        double burnRate = getLastNonZeroValue(result);
        assertFalse("Expected at least one non-zero, non-NaN sample", Double.isNaN(burnRate));
        assertEquals("Burn rate should be 7.0", 7.0, burnRate, 0.01);
    }

    /**
     * Burn rate < 1 when errors are below budget.
     * SLO = 99.9%, errors = 0.05% → burn_rate = 0.0005 / 0.001 = 0.5
     */
    public void testBurnRateBelowBudget() {
        AsBurnRateStage stage = new AsBurnRateStage("right", ONE_MINUTE, 99.9);

        List<Sample> unexpectedSamples = new ArrayList<>();
        List<Sample> totalSamples = new ArrayList<>();
        for (int i = 0; i <= 6; i++) {
            long ts = i * STEP;
            unexpectedSamples.add(new FloatSample(ts, 0.5));
            totalSamples.add(new FloatSample(ts, 1000.0));
        }

        ByteLabels labels = ByteLabels.fromStrings("service", "api");
        TimeSeries unexpected = new TimeSeries(unexpectedSamples, labels, 0L, 60_000L, STEP, null);
        TimeSeries total = new TimeSeries(totalSamples, labels, 0L, 60_000L, STEP, null);

        List<TimeSeries> result = stage.process(List.of(unexpected), List.of(total));
        assertFalse(result.isEmpty());

        double burnRate = getLastNonZeroValue(result);
        assertFalse("Expected at least one non-zero, non-NaN sample", Double.isNaN(burnRate));
        assertEquals("Burn rate should be 0.5", 0.5, burnRate, 0.01);
    }

    /**
     * Zero total should produce 0 (NaN replaced by transformNull).
     */
    public void testZeroTotalProducesZero() {
        AsBurnRateStage stage = new AsBurnRateStage("right", ONE_MINUTE, 99.9);

        List<Sample> unexpectedSamples = new ArrayList<>();
        List<Sample> totalSamples = new ArrayList<>();
        for (int i = 0; i <= 6; i++) {
            long ts = i * STEP;
            unexpectedSamples.add(new FloatSample(ts, 1.0));
            totalSamples.add(new FloatSample(ts, 0.0));
        }

        ByteLabels labels = ByteLabels.fromStrings("service", "api");
        TimeSeries unexpected = new TimeSeries(unexpectedSamples, labels, 0L, 60_000L, STEP, null);
        TimeSeries total = new TimeSeries(totalSamples, labels, 0L, 60_000L, STEP, null);

        List<TimeSeries> result = stage.process(List.of(unexpected), List.of(total));
        // Should still produce output, with transformNull converting NaN to 0
        assertFalse(result.isEmpty());
        for (Sample s : result.get(0).getSamples().toList()) {
            assertEquals("NaN should be transformed to 0", 0.0, s.getValue(), 0.0001);
        }
    }

    /**
     * No errors → burn rate = 0.
     */
    public void testZeroErrors() {
        AsBurnRateStage stage = new AsBurnRateStage("right", ONE_MINUTE, 99.9);

        List<Sample> unexpectedSamples = new ArrayList<>();
        List<Sample> totalSamples = new ArrayList<>();
        for (int i = 0; i <= 6; i++) {
            long ts = i * STEP;
            unexpectedSamples.add(new FloatSample(ts, 0.0));
            totalSamples.add(new FloatSample(ts, 1000.0));
        }

        ByteLabels labels = ByteLabels.fromStrings("service", "api");
        TimeSeries unexpected = new TimeSeries(unexpectedSamples, labels, 0L, 60_000L, STEP, null);
        TimeSeries total = new TimeSeries(totalSamples, labels, 0L, 60_000L, STEP, null);

        List<TimeSeries> result = stage.process(List.of(unexpected), List.of(total));
        assertFalse(result.isEmpty());

        for (Sample s : result.get(0).getSamples().toList()) {
            assertEquals("Burn rate should be 0 with no errors", 0.0, s.getValue(), 0.0001);
        }
    }

    /**
     * Empty input lists produce empty output.
     */
    public void testEmptyInput() {
        AsBurnRateStage stage = new AsBurnRateStage("right", ONE_MINUTE, 99.9);
        List<TimeSeries> result = stage.process(List.of(), List.of());
        assertTrue(result.isEmpty());
    }

    /**
     * Null left input throws NPE.
     */
    public void testNullLeftInput() {
        AsBurnRateStage stage = new AsBurnRateStage("right", ONE_MINUTE, 99.9);
        expectThrows(NullPointerException.class, () -> stage.process(null, List.of()));
    }

    /**
     * Null right input throws NPE.
     */
    public void testNullRightInput() {
        AsBurnRateStage stage = new AsBurnRateStage("right", ONE_MINUTE, 99.9);
        expectThrows(NullPointerException.class, () -> stage.process(List.of(), null));
    }

    /**
     * Invalid SLO values are rejected.
     */
    public void testInvalidSlo() {
        expectThrows(IllegalArgumentException.class, () -> new AsBurnRateStage("right", ONE_MINUTE, 0));
        expectThrows(IllegalArgumentException.class, () -> new AsBurnRateStage("right", ONE_MINUTE, 100));
        expectThrows(IllegalArgumentException.class, () -> new AsBurnRateStage("right", ONE_MINUTE, -1));
        expectThrows(IllegalArgumentException.class, () -> new AsBurnRateStage("right", ONE_MINUTE, 150));
    }

    /**
     * Zero or negative interval values are rejected.
     */
    public void testInvalidInterval() {
        expectThrows(IllegalArgumentException.class, () -> new AsBurnRateStage("right", 0, 99.9));
        expectThrows(IllegalArgumentException.class, () -> new AsBurnRateStage("right", -1000, 99.9));
    }

    /**
     * Different SLO percentages produce different burn rates for the same error rate.
     */
    public void testDifferentSloValues() {
        // Error rate = 0.001 (0.1% = 1/1000)
        // SLO 99.9% → budget 0.001 → burn_rate = 0.001/0.001 = 1
        // SLO 99.0% → budget 0.01 → burn_rate = 0.001/0.01 = 0.1
        // SLO 99.99% → budget 0.0001 → burn_rate = 0.001/0.0001 = 10

        List<Sample> unexpectedSamples = new ArrayList<>();
        List<Sample> totalSamples = new ArrayList<>();
        for (int i = 0; i <= 6; i++) {
            long ts = i * STEP;
            unexpectedSamples.add(new FloatSample(ts, 1.0));
            totalSamples.add(new FloatSample(ts, 1000.0));
        }

        ByteLabels labels = ByteLabels.fromStrings("service", "api");
        TimeSeries unexpected = new TimeSeries(unexpectedSamples, labels, 0L, 60_000L, STEP, null);
        TimeSeries total = new TimeSeries(totalSamples, labels, 0L, 60_000L, STEP, null);

        double lastNonZeroValue999 = getLastNonZeroValue(
            new AsBurnRateStage("right", ONE_MINUTE, 99.9).process(List.of(unexpected), List.of(total))
        );
        double lastNonZeroValue99 = getLastNonZeroValue(
            new AsBurnRateStage("right", ONE_MINUTE, 99.0).process(List.of(unexpected), List.of(total))
        );
        double lastNonZeroValue9999 = getLastNonZeroValue(
            new AsBurnRateStage("right", ONE_MINUTE, 99.99).process(List.of(unexpected), List.of(total))
        );

        assertEquals(1.0, lastNonZeroValue999, 0.01);
        assertEquals(0.1, lastNonZeroValue99, 0.01);
        assertEquals(10.0, lastNonZeroValue9999, 0.1);
    }

    /**
     * Multiple series with different labels are matched correctly.
     */
    public void testMultipleSeriesLabelMatching() {
        AsBurnRateStage stage = new AsBurnRateStage("right", ONE_MINUTE, 99.9);

        ByteLabels apiLabels = ByteLabels.fromStrings("service", "api");
        ByteLabels webLabels = ByteLabels.fromStrings("service", "web");

        List<Sample> apiUnexpected = new ArrayList<>();
        List<Sample> apiTotal = new ArrayList<>();
        List<Sample> webUnexpected = new ArrayList<>();
        List<Sample> webTotal = new ArrayList<>();

        for (int i = 0; i <= 6; i++) {
            long ts = i * STEP;
            apiUnexpected.add(new FloatSample(ts, 1.0));
            apiTotal.add(new FloatSample(ts, 1000.0));
            webUnexpected.add(new FloatSample(ts, 5.0));
            webTotal.add(new FloatSample(ts, 1000.0));
        }

        List<TimeSeries> left = List.of(
            new TimeSeries(apiUnexpected, apiLabels, 0L, 60_000L, STEP, null),
            new TimeSeries(webUnexpected, webLabels, 0L, 60_000L, STEP, null)
        );
        List<TimeSeries> right = List.of(
            new TimeSeries(apiTotal, apiLabels, 0L, 60_000L, STEP, null),
            new TimeSeries(webTotal, webLabels, 0L, 60_000L, STEP, null)
        );

        List<TimeSeries> result = stage.process(left, right);
        assertEquals(2, result.size());
    }

    public void testGetName() {
        AsBurnRateStage stage = new AsBurnRateStage("right", ONE_MINUTE, 99.9);
        assertEquals("burn_rate", stage.getName());
    }

    public void testEqualsAndHashCode() {
        AsBurnRateStage stage1 = new AsBurnRateStage("right", ONE_MINUTE, 99.9);
        AsBurnRateStage stage2 = new AsBurnRateStage("right", ONE_MINUTE, 99.9);
        AsBurnRateStage stage3 = new AsBurnRateStage("other", ONE_MINUTE, 99.9);
        AsBurnRateStage stage4 = new AsBurnRateStage("right", 120_000L, 99.9);
        AsBurnRateStage stage5 = new AsBurnRateStage("right", ONE_MINUTE, 99.0);

        assertEquals(stage1, stage2);
        assertEquals(stage1.hashCode(), stage2.hashCode());
        assertNotEquals(stage1, stage3);
        assertNotEquals(stage1, stage4);
        assertNotEquals(stage1, stage5);
    }

    public void testSerializationCustom() throws IOException {
        AsBurnRateStage original = new AsBurnRateStage("right_ref", 86400000L, 99.9);

        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        AsBurnRateStage deserialized = AsBurnRateStage.readFrom(input);

        assertEquals(original, deserialized);
    }

    public void testToXContent() throws IOException {
        AsBurnRateStage stage = new AsBurnRateStage("ref", 86400000L, 99.9);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stage.toXContent(builder, EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue(json.contains("\"right_op_reference\":\"ref\""));
        assertTrue(json.contains("\"interval\":86400000"));
        assertTrue(json.contains("\"slo\":99.9"));
    }

    public void testFromArgs() {
        Map<String, Object> args = Map.of("right_op_reference", "ref", "interval", 86400000L, "slo", 99.9);
        AsBurnRateStage stage = AsBurnRateStage.fromArgs(args);
        assertEquals("ref", stage.getRightOpReferenceName());
        assertEquals(86400000L, stage.getInterval());
        assertEquals(99.9, stage.getSlo(), 0.0001);
    }

    /**
     * fromArgs throws when required keys are missing.
     */
    public void testFromArgsMissingKeys() {
        expectThrows(IllegalArgumentException.class, () -> AsBurnRateStage.fromArgs(null));
        expectThrows(IllegalArgumentException.class, () -> AsBurnRateStage.fromArgs(Map.of("interval", 60_000L, "slo", 99.9)));
        expectThrows(IllegalArgumentException.class, () -> AsBurnRateStage.fromArgs(Map.of("right_op_reference", "ref", "slo", 99.9)));
        expectThrows(
            IllegalArgumentException.class,
            () -> AsBurnRateStage.fromArgs(Map.of("right_op_reference", "ref", "interval", 60_000L))
        );
    }

    /**
     * A left series with no matching right-side label is silently dropped when there are
     * multiple right-side groups (so label matching is required to disambiguate).
     */
    public void testMismatchedLabelsDropsUnmatchedSeries() {
        AsBurnRateStage stage = new AsBurnRateStage("right", ONE_MINUTE, 99.9);

        ByteLabels apiLabels = ByteLabels.fromStrings("service", "api");
        ByteLabels dbLabels = ByteLabels.fromStrings("service", "db");
        ByteLabels webLabels = ByteLabels.fromStrings("service", "web");

        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i <= 6; i++) {
            samples.add(new FloatSample(i * STEP, 1.0));
        }

        // Left has "api"; right has "db" and "web" — two groups, neither matches "api"
        TimeSeries leftSeries = new TimeSeries(samples, apiLabels, 0L, 60_000L, STEP, null);
        TimeSeries rightDb = new TimeSeries(samples, dbLabels, 0L, 60_000L, STEP, null);
        TimeSeries rightWeb = new TimeSeries(samples, webLabels, 0L, 60_000L, STEP, null);

        List<TimeSeries> result = stage.process(List.of(leftSeries), List.of(rightDb, rightWeb));
        assertTrue("Unmatched left series should be silently dropped", result.isEmpty());
    }

    public void testReadFromThroughFactory() throws IOException {
        AsBurnRateStage original = new AsBurnRateStage("ref", ONE_MINUTE, 99.9);

        BytesStreamOutput output = new BytesStreamOutput();
        output.writeString(original.getName());
        original.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        PipelineStage deserialized = PipelineStageFactory.readFrom(input);

        assertTrue(deserialized instanceof AsBurnRateStage);
        assertEquals(original, deserialized);
    }

    private double getLastNonZeroValue(List<TimeSeries> result) {
        if (result.isEmpty()) return Double.NaN;
        List<Sample> samples = result.get(0).getSamples().toList();
        for (int i = samples.size() - 1; i >= 0; i--) {
            double v = samples.get(i).getValue();
            if (v != 0.0 && !Double.isNaN(v)) {
                return v;
            }
        }
        return 0.0;
    }

    @Override
    protected AsBurnRateStage createTestInstance() {
        return new AsBurnRateStage(randomAlphaOfLength(5), randomLongBetween(1000, 86400000), randomDoubleBetween(0.01, 99.99, true));
    }

    @Override
    protected Writeable.Reader<AsBurnRateStage> instanceReader() {
        return AsBurnRateStage::readFrom;
    }
}
