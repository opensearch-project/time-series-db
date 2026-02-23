/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.List;

/**
 * Tests for AbstractMapperStage functionality that cannot be tested through
 * concrete implementations (e.g., null filtering, default methods).
 */
public class AbstractMapperStageTests extends OpenSearchTestCase {

    /**
     * Test mapper that filters out samples by returning null.
     * Tests AbstractMapperStage's null handling logic and default metadata preservation.
     */
    private static class FilteringTestMapper extends AbstractMapperStage {
        @Override
        protected Sample mapSample(Sample sample) {
            // Filter out samples with value > 15
            return sample.getValue() > 15 ? null : sample.deepCopy();
        }

        @Override
        public String getName() {
            return "test_filter";
        }

        @Override
        public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            // No-op for test
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            // No-op for test
        }
    }

    // ========== Null Filtering Tests ==========

    public void testMapSampleReturningNull() {
        // Test that AbstractMapperStage correctly filters out null samples
        // and preserves metadata
        FilteringTestMapper stage = new FilteringTestMapper();
        Labels labels = ByteLabels.fromStrings("service", "api", "region", "us-west");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(2000L, 20.0), new FloatSample(3000L, 30.0));
        long minTimestamp = 1000L;
        long maxTimestamp = 3000L;
        long step = 1000L;
        String alias = "test-series";
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, minTimestamp, maxTimestamp, step, alias);

        // Act
        List<TimeSeries> result = stage.process(List.of(inputTimeSeries));

        // Assert - samples with value > 15 should be filtered out
        assertEquals(1, result.size());
        TimeSeries resultSeries = result.get(0);
        assertEquals(1, resultSeries.getSamples().size()); // Only sample with value 10.0 remains
        assertEquals(10.0, resultSeries.getSamples().getValue(0), 0.001);

        // Assert - metadata should be preserved by default implementation
        assertEquals(labels, resultSeries.getLabels());
        assertEquals(minTimestamp, resultSeries.getMinTimestamp());
        assertEquals(maxTimestamp, resultSeries.getMaxTimestamp());
        assertEquals(step, resultSeries.getStep());
        assertEquals(alias, resultSeries.getAlias());
    }

    public void testMapSampleReturningNullForAllSamples() {
        // Test filtering out all samples
        FilteringTestMapper stage = new FilteringTestMapper();
        Labels labels = ByteLabels.fromStrings("service", "api");
        List<Sample> samples = List.of(new FloatSample(1000L, 20.0), new FloatSample(2000L, 30.0));
        TimeSeries inputTimeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "test-series");

        // Act
        List<TimeSeries> result = stage.process(List.of(inputTimeSeries));

        // Assert - all samples filtered out
        assertEquals(1, result.size());
        assertTrue(result.get(0).getSamples().isEmpty());
    }
}
