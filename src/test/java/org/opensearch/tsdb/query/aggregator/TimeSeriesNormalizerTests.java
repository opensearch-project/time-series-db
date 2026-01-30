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

import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;

public class TimeSeriesNormalizerTests extends OpenSearchTestCase {

    public void testNormalize_SingleSeries() {
        // Single series should be returned as-is without normalization
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0f), new FloatSample(2000L, 20.0f));
        TimeSeries series = new TimeSeries(samples, ByteLabels.emptyLabels(), 1000L, 2000L, 1000L, null);

        List<TimeSeries> result = TimeSeriesNormalizer.normalize(List.of(series));

        assertEquals(1, result.size());
        assertEquals(series, result.get(0));
    }

    public void testNormalize_ComprehensiveConsolidationWithMisalignedSeries() {
        // Comprehensive test with misaligned timestamps, different step sizes, and all consolidation methods

        // Series A: [0, 40000] with 8s steps - creates complex misalignment scenario
        List<Sample> samplesA = List.of(
            new FloatSample(0L, 100.0f),       // Bucket 1: [0, 24000)
            new FloatSample(8000L, 200.0f),    // Bucket 1: [0, 24000)
            new FloatSample(16000L, 300.0f),   // Bucket 1: [0, 24000)
            new FloatSample(24000L, 400.0f),   // Bucket 2: [24000, 48000)
            new FloatSample(32000L, 500.0f),   // Bucket 2: [24000, 48000)
            new FloatSample(40000L, 600.0f)    // Bucket 2: [24000, 48000)
        );
        Labels labelsA = ByteLabels.fromMap(Map.of("host", "server1", "env", "prod"));
        TimeSeries seriesA = new TimeSeries(samplesA, labelsA, 0L, 40000L, 8000L, "series-A");

        // Series B: [3000, 51000] with 12s steps - different step and offset
        List<Sample> samplesB = List.of(
            new FloatSample(3000L, 50.0f),     // Bucket 1: [0, 24000)
            new FloatSample(15000L, 75.0f),    // Bucket 1: [0, 24000)
            new FloatSample(27000L, 100.0f),   // Bucket 2: [24000, 48000)
            new FloatSample(39000L, 125.0f),   // Bucket 2: [24000, 48000)
            new FloatSample(51000L, 150.0f)    // Bucket 3: [48000, 72000)
        );
        Labels labelsB = ByteLabels.fromMap(Map.of("host", "server2", "env", "dev"));
        TimeSeries seriesB = new TimeSeries(samplesB, labelsB, 3000L, 51000L, 12000L, "series-B");

        // Series C: [6000, 30000] with 6s steps - forces LCM calculation
        List<Sample> samplesC = List.of(
            new FloatSample(6000L, 10.0f),     // Bucket 1: [0, 24000)
            new FloatSample(12000L, 20.0f),    // Bucket 1: [0, 24000)
            new FloatSample(18000L, 30.0f),    // Bucket 1: [0, 24000)
            new FloatSample(24000L, 40.0f),    // Bucket 2: [24000, 48000)
            new FloatSample(30000L, 50.0f)     // Bucket 2: [24000, 48000)
        );
        Labels labelsC = ByteLabels.fromMap(Map.of("host", "server3", "env", "test"));
        TimeSeries seriesC = new TimeSeries(samplesC, labelsC, 6000L, 30000L, 6000L, "series-C");

        List<TimeSeries> inputSeries = List.of(seriesA, seriesB, seriesC);

        // LCM(8000, 12000, 6000) = 24000
        // Min start = 0, Max end = 51000
        // Adjusted end to align with step: 48000 (51000 - (51000 % 24000) = 51000 - 3000 = 48000)
        // Expected normalized range: [0, 48000] with 24s steps
        // Buckets: [0, 24000), [24000, 48000), and [48000, 72000) for samples >= 48000
        // Note: numSteps = (48000 - 0) / 24000 + 1 = 3, creating 3 buckets

        // Test SUM consolidation
        List<TimeSeries> resultSum = TimeSeriesNormalizer.normalize(
            inputSeries,
            TimeSeriesNormalizer.StepSizeStrategy.LCM,
            TimeSeriesNormalizer.ConsolidationStrategy.SUM
        );
        assertEquals(3, resultSum.size());

        // Verify metadata preservation
        for (int i = 0; i < resultSum.size(); i++) {
            TimeSeries normalized = resultSum.get(i);
            TimeSeries original = inputSeries.get(i);
            assertEquals("Labels should be preserved", original.getLabels(), normalized.getLabels());
            assertEquals("Alias should be preserved", original.getAlias(), normalized.getAlias());
            assertEquals("Step should be normalized", 24000L, normalized.getStep());
            assertEquals("Start should be aligned", 0L, normalized.getMinTimestamp());
            assertEquals("End should be aligned", 48000L, normalized.getMaxTimestamp());
        }

        // === EXPECTED OUTPUTS FOR ALL CONSOLIDATION METHODS ===

        // Series A Expected Results:
        // Bucket [0, 24000): samples 100, 200, 300
        // Bucket [24000, 48000): samples 400, 500, 600
        List<Sample> expectedA_SUM = List.of(
            new FloatSample(0L, 600.0f),      // sum(100,200,300) = 600
            new FloatSample(24000L, 1500.0f)  // sum(400,500,600) = 1500
        );
        List<Sample> expectedA_AVG = List.of(
            new FloatSample(0L, 200.0f),      // avg(100,200,300) = 200
            new FloatSample(24000L, 500.0f)   // avg(400,500,600) = 500
        );
        List<Sample> expectedA_MAX = List.of(
            new FloatSample(0L, 300.0f),      // max(100,200,300) = 300
            new FloatSample(24000L, 600.0f)   // max(400,500,600) = 600
        );
        List<Sample> expectedA_MIN = List.of(
            new FloatSample(0L, 100.0f),      // min(100,200,300) = 100
            new FloatSample(24000L, 400.0f)   // min(400,500,600) = 400
        );
        List<Sample> expectedA_LAST = List.of(
            new FloatSample(0L, 300.0f),      // last of (100@0, 200@8000, 300@16000) = 300
            new FloatSample(24000L, 600.0f)   // last of (400@24000, 500@32000, 600@40000) = 600
        );

        // Series B Expected Results:
        // Bucket [0, 24000): samples 50, 75
        // Bucket [24000, 48000): samples 100, 125
        // Bucket [48000+): sample 150 (from 51000L)
        List<Sample> expectedB_SUM = List.of(
            new FloatSample(0L, 125.0f),      // sum(50,75) = 125
            new FloatSample(24000L, 225.0f),  // sum(100,125) = 225
            new FloatSample(48000L, 150.0f)   // sum(150) = 150
        );
        List<Sample> expectedB_AVG = List.of(
            new FloatSample(0L, 62.5f),       // avg(50,75) = 62.5
            new FloatSample(24000L, 112.5f),  // avg(100,125) = 112.5
            new FloatSample(48000L, 150.0f)   // avg(150) = 150
        );
        List<Sample> expectedB_MAX = List.of(
            new FloatSample(0L, 75.0f),       // max(50,75) = 75
            new FloatSample(24000L, 125.0f),  // max(100,125) = 125
            new FloatSample(48000L, 150.0f)   // max(150) = 150
        );
        List<Sample> expectedB_MIN = List.of(
            new FloatSample(0L, 50.0f),       // min(50,75) = 50
            new FloatSample(24000L, 100.0f),  // min(100,125) = 100
            new FloatSample(48000L, 150.0f)   // min(150) = 150
        );
        List<Sample> expectedB_LAST = List.of(
            new FloatSample(0L, 75.0f),       // last of (50@3000, 75@15000) = 75
            new FloatSample(24000L, 125.0f),  // last of (100@27000, 125@39000) = 125
            new FloatSample(48000L, 150.0f)   // last of (150@51000) = 150
        );

        // Series C Expected Results:
        // Bucket [0, 24000): samples 10, 20, 30
        // Bucket [24000, 48000): samples 40, 50
        List<Sample> expectedC_SUM = List.of(
            new FloatSample(0L, 60.0f),       // sum(10,20,30) = 60
            new FloatSample(24000L, 90.0f)    // sum(40,50) = 90
        );
        List<Sample> expectedC_AVG = List.of(
            new FloatSample(0L, 20.0f),       // avg(10,20,30) = 20
            new FloatSample(24000L, 45.0f)    // avg(40,50) = 45
        );
        List<Sample> expectedC_MAX = List.of(
            new FloatSample(0L, 30.0f),       // max(10,20,30) = 30
            new FloatSample(24000L, 50.0f)    // max(40,50) = 50
        );
        List<Sample> expectedC_MIN = List.of(
            new FloatSample(0L, 10.0f),       // min(10,20,30) = 10
            new FloatSample(24000L, 40.0f)    // min(40,50) = 40
        );
        List<Sample> expectedC_LAST = List.of(
            new FloatSample(0L, 30.0f),       // last of (10@6000, 20@12000, 30@18000) = 30
            new FloatSample(24000L, 50.0f)    // last of (40@24000, 50@30000) = 50
        );

        // === EXECUTE TESTS FOR ALL CONSOLIDATION METHODS ===

        assertSamplesEqual("Series A SUM consolidation", expectedA_SUM, resultSum.get(0).getSamples().toList());
        assertSamplesEqual("Series B SUM consolidation", expectedB_SUM, resultSum.get(1).getSamples().toList());
        assertSamplesEqual("Series C SUM consolidation", expectedC_SUM, resultSum.get(2).getSamples().toList());

        // Test AVG consolidation
        List<TimeSeries> resultAvg = TimeSeriesNormalizer.normalize(
            inputSeries,
            TimeSeriesNormalizer.StepSizeStrategy.LCM,
            TimeSeriesNormalizer.ConsolidationStrategy.AVG
        );
        assertSamplesEqual("Series A AVG consolidation", expectedA_AVG, resultAvg.get(0).getSamples().toList());
        assertSamplesEqual("Series B AVG consolidation", expectedB_AVG, resultAvg.get(1).getSamples().toList());
        assertSamplesEqual("Series C AVG consolidation", expectedC_AVG, resultAvg.get(2).getSamples().toList());

        // Test MAX consolidation
        List<TimeSeries> resultMax = TimeSeriesNormalizer.normalize(
            inputSeries,
            TimeSeriesNormalizer.StepSizeStrategy.LCM,
            TimeSeriesNormalizer.ConsolidationStrategy.MAX
        );
        assertSamplesEqual("Series A MAX consolidation", expectedA_MAX, resultMax.get(0).getSamples().toList());
        assertSamplesEqual("Series B MAX consolidation", expectedB_MAX, resultMax.get(1).getSamples().toList());
        assertSamplesEqual("Series C MAX consolidation", expectedC_MAX, resultMax.get(2).getSamples().toList());

        // Test MIN consolidation
        List<TimeSeries> resultMin = TimeSeriesNormalizer.normalize(
            inputSeries,
            TimeSeriesNormalizer.StepSizeStrategy.LCM,
            TimeSeriesNormalizer.ConsolidationStrategy.MIN
        );
        assertSamplesEqual("Series A MIN consolidation", expectedA_MIN, resultMin.get(0).getSamples().toList());
        assertSamplesEqual("Series B MIN consolidation", expectedB_MIN, resultMin.get(1).getSamples().toList());
        assertSamplesEqual("Series C MIN consolidation", expectedC_MIN, resultMin.get(2).getSamples().toList());

        // Test LAST consolidation
        List<TimeSeries> resultLast = TimeSeriesNormalizer.normalize(
            inputSeries,
            TimeSeriesNormalizer.StepSizeStrategy.LCM,
            TimeSeriesNormalizer.ConsolidationStrategy.LAST
        );
        assertSamplesEqual("Series A LAST consolidation", expectedA_LAST, resultLast.get(0).getSamples().toList());
        assertSamplesEqual("Series B LAST consolidation", expectedB_LAST, resultLast.get(1).getSamples().toList());
        assertSamplesEqual("Series C LAST consolidation", expectedC_LAST, resultLast.get(2).getSamples().toList());
    }

    public void testNormalize_SkipsNaNValues() {
        // NaN values should be skipped in consolidation - create two series with LCM(5000, 10000) = 10000
        List<Sample> samples = List.of(
            new FloatSample(0L, Float.NaN),
            new FloatSample(5000L, 20.0f),
            new FloatSample(10000L, 30.0f),
            new FloatSample(15000L, Float.NaN)
        );
        TimeSeries series = new TimeSeries(samples, ByteLabels.emptyLabels(), 0L, 15000L, 5000L, null);

        // Add a second series with 10s steps to force LCM to be 10s
        TimeSeries series2 = new TimeSeries(
            List.of(new FloatSample(0L, 1.0f), new FloatSample(10000L, 2.0f)),
            ByteLabels.emptyLabels(),
            0L,
            10000L,
            10000L,
            null
        );

        List<TimeSeries> result = TimeSeriesNormalizer.normalize(List.of(series, series2));

        TimeSeries normalized = result.get(0);
        // Bucket [0, 10000): samples at 0(NaN) and 5000(20), only 20 counts = 20
        // Bucket [10000, 20000): samples at 10000(30), only 30 counts = 30
        assertEquals(2, normalized.getSamples().size());
        assertEquals(20.0f, normalized.getSamples().getValue(0), 0.001f);
        assertEquals(30.0f, normalized.getSamples().getValue(1), 0.001f);
    }

    public void testNormalize_EmptyBucketSkipped() {
        // Buckets with no values should be skipped (no sample added)
        List<Sample> samples = List.of(new FloatSample(0L, 10.0f), new FloatSample(30000L, 30.0f));
        TimeSeries series = new TimeSeries(samples, ByteLabels.emptyLabels(), 0L, 30000L, 10000L, null);

        // Normalize to 15s steps - LCM(10000, 15000) = 30000
        TimeSeries otherSeries = new TimeSeries(List.of(new FloatSample(0L, 1.0f)), ByteLabels.emptyLabels(), 0L, 30000L, 15000L, null);

        List<TimeSeries> result = TimeSeriesNormalizer.normalize(List.of(series, otherSeries));

        TimeSeries normalized = result.get(0);
        // LCM is 30000, creates samples at timestamp 0 and 30000
        // Bucket [0, 30000): contains sample at 0L (10.0)
        // Bucket [30000, 60000): contains sample at 30000L (30.0)
        assertEquals(2, normalized.getSamples().size());
        assertEquals(0L, normalized.getSamples().getTimestamp(0));
        assertEquals(10.0f, normalized.getSamples().getValue(0), 0.001f);
        assertEquals(30000L, normalized.getSamples().getTimestamp(1));
        assertEquals(30.0f, normalized.getSamples().getValue(1), 0.001f);
    }

    public void testNormalize_AcceptsEmptyList() {
        List<TimeSeries> result = TimeSeriesNormalizer.normalize(List.of());

        assertNotNull(result);
        assertTrue(result.isEmpty());
    }

    public void testNormalize_ThrowsOnNullList() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> TimeSeriesNormalizer.normalize(null));
        assertTrue(exception.getMessage().contains("Cannot normalize null series list"));
    }

    public void testNormalize_PreservesLabelsAndAlias() {
        // Verify that labels and alias are preserved after normalization
        Labels labels = ByteLabels.fromMap(Map.of("host", "server1", "region", "us-east"));
        List<Sample> samples = List.of(new FloatSample(0L, 10.0f), new FloatSample(10000L, 20.0f));
        TimeSeries series = new TimeSeries(samples, labels, 0L, 10000L, 10000L, "my-alias");

        List<TimeSeries> result = TimeSeriesNormalizer.normalize(List.of(series));

        TimeSeries normalized = result.get(0);
        assertEquals(labels, normalized.getLabels());
        assertEquals("my-alias", normalized.getAlias());
    }

    public void testConsolidationFunction_GetDefault() {
        assertEquals(ConsolidationFunction.AVG, ConsolidationFunction.getDefault());
    }

    public void testNormalize_TypeAware_CounterType() {
        // Test TYPE_AWARE strategy with all types: counter, counts, and no type label
        // Counter series: type="counter" - should use SUM
        List<Sample> counterSamples = List.of(new FloatSample(0L, 10.0f), new FloatSample(5000L, 20.0f), new FloatSample(10000L, 30.0f));
        Labels counterLabels = ByteLabels.fromMap(Map.of("type", "counter", "metric", "requests"));
        TimeSeries counterSeries = new TimeSeries(counterSamples, counterLabels, 0L, 10000L, 5000L, null);

        // Counts series: type="counts" - should use SUM
        List<Sample> countsSamples = List.of(new FloatSample(0L, 15.0f), new FloatSample(5000L, 25.0f), new FloatSample(10000L, 35.0f));
        Labels countsLabels = ByteLabels.fromMap(Map.of("type", "counts", "metric", "events"));
        TimeSeries countsSeries = new TimeSeries(countsSamples, countsLabels, 0L, 10000L, 5000L, null);

        // No type series: no type label - should use AVG
        List<Sample> noTypeSamples = List.of(new FloatSample(0L, 5.0f), new FloatSample(5000L, 10.0f), new FloatSample(10000L, 15.0f));
        Labels noTypeLabels = ByteLabels.fromMap(Map.of("metric", "cpu"));
        TimeSeries noTypeSeries = new TimeSeries(noTypeSamples, noTypeLabels, 0L, 10000L, 5000L, null);

        // Add a series with larger step size to trigger rebucketing
        // This series has step 10000L, so MAX(5000, 5000, 5000, 10000) = 10000L
        List<Sample> triggerSamples = List.of(new FloatSample(0L, 100.0f), new FloatSample(10000L, 200.0f));
        Labels triggerLabels = ByteLabels.fromMap(Map.of("metric", "memory"));
        TimeSeries triggerSeries = new TimeSeries(triggerSamples, triggerLabels, 0L, 10000L, 10000L, null);

        List<TimeSeries> result = TimeSeriesNormalizer.normalize(
            List.of(counterSeries, countsSeries, noTypeSeries, triggerSeries),
            TimeSeriesNormalizer.StepSizeStrategy.MAX,
            TimeSeriesNormalizer.ConsolidationStrategy.TYPE_AWARE
        );

        // With MAX step size (10000L), samples are rebucketed:
        // Bucket [0, 10000): samples at 0L and 5000L
        // Bucket [10000, 20000): sample at 10000L
        // Counter series should use SUM: bucket [0, 10000) contains samples 10, 20 → sum = 30
        List<Sample> expectedCounter = List.of(
            new FloatSample(0L, 30.0f),      // sum(10, 20) = 30
            new FloatSample(10000L, 30.0f)   // sum(30) = 30
        );
        assertSamplesEqual("Counter series TYPE_AWARE", expectedCounter, result.get(0).getSamples().toList(), 0.001f);

        // Counts series should use SUM: bucket [0, 10000) contains samples 15, 25 → sum = 40
        List<Sample> expectedCounts = List.of(
            new FloatSample(0L, 40.0f),      // sum(15, 25) = 40
            new FloatSample(10000L, 35.0f)   // sum(35) = 35
        );
        assertSamplesEqual("Counts series TYPE_AWARE", expectedCounts, result.get(1).getSamples().toList(), 0.001f);

        // No type series should use AVG: bucket [0, 10000) contains samples 5, 10 → avg = 7.5
        List<Sample> expectedNoType = List.of(
            new FloatSample(0L, 7.5f),      // avg(5, 10) = 7.5
            new FloatSample(10000L, 15.0f)   // avg(15) = 15
        );
        assertSamplesEqual("No type series TYPE_AWARE", expectedNoType, result.get(2).getSamples().toList(), 0.001f);

        // Trigger series (no type label) should use AVG: bucket [0, 10000) contains sample 100 → avg = 100
        List<Sample> expectedTrigger = List.of(
            new FloatSample(0L, 100.0f),    // avg(100) = 100
            new FloatSample(10000L, 200.0f) // avg(200) = 200
        );
        assertSamplesEqual("Trigger series TYPE_AWARE", expectedTrigger, result.get(3).getSamples().toList(), 0.001f);
    }
}
