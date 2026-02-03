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
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.tsdb.TestUtils.assertSamplesEqual;
import static org.opensearch.tsdb.TestUtils.findSeriesByLabel;

public class IsNonNullStageTests extends AbstractWireSerializingTestCase<IsNonNullStage> {

    /**
     * Test isNonNull with dense, sparse, and empty time series.
     * Tests that existing samples get 1.0 and missing timestamps get 0.0.
     */
    public void testIsNonNull() {
        IsNonNullStage stage = new IsNonNullStage();

        // Dense series: has samples at all timestamps [1000, 2000, 3000, 4000]
        List<Sample> denseSamples = List.of(
            new FloatSample(1000L, 40.0),
            new FloatSample(2000L, 10.0),
            new FloatSample(3000L, 22.0),
            new FloatSample(4000L, 15.0)
        );

        // Sparse series: only has samples at 1000 and 3000 (missing 2000, 4000)
        List<Sample> sparseSamples = List.of(new FloatSample(1000L, 40.0), new FloatSample(3000L, 22.0));

        // Empty series: no samples at all
        List<Sample> emptySamples = List.of();

        ByteLabels labels1 = ByteLabels.fromStrings("type", "dense");
        ByteLabels labels2 = ByteLabels.fromStrings("type", "sparse");
        ByteLabels labels3 = ByteLabels.fromStrings("type", "empty");

        TimeSeries denseSeries = new TimeSeries(denseSamples, labels1, 1000L, 4000L, 1000L, null);
        TimeSeries sparseSeries = new TimeSeries(sparseSamples, labels2, 1000L, 4000L, 1000L, null);
        TimeSeries emptySeries = new TimeSeries(emptySamples, labels3, 1000L, 4000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(denseSeries, sparseSeries, emptySeries));

        assertEquals(3, result.size());

        // Dense result: all timestamps present -> all 1.0
        TimeSeries denseResult = findSeriesByLabel(result, "type", "dense");
        List<Sample> expectedDense = List.of(
            new FloatSample(1000L, 1.0),  // present
            new FloatSample(2000L, 1.0),  // present
            new FloatSample(3000L, 1.0),  // present
            new FloatSample(4000L, 1.0)   // present
        );
        assertSamplesEqual("IsNonNull Dense", expectedDense, denseResult.getSamples().toList());

        // Sparse result: mix of present and missing
        TimeSeries sparseResult = findSeriesByLabel(result, "type", "sparse");
        List<Sample> expectedSparse = List.of(
            new FloatSample(1000L, 1.0),  // present
            new FloatSample(2000L, 0.0),  // missing
            new FloatSample(3000L, 1.0),  // present
            new FloatSample(4000L, 0.0)   // missing
        );
        assertSamplesEqual("IsNonNull Sparse", expectedSparse, sparseResult.getSamples().toList());

        // Empty result: all timestamps missing -> all 0.0
        TimeSeries emptyResult = findSeriesByLabel(result, "type", "empty");
        List<Sample> expectedEmpty = List.of(
            new FloatSample(1000L, 0.0),  // missing
            new FloatSample(2000L, 0.0),  // missing
            new FloatSample(3000L, 0.0),  // missing
            new FloatSample(4000L, 0.0)   // missing
        );
        assertSamplesEqual("IsNonNull Empty", expectedEmpty, emptyResult.getSamples().toList());
    }

    /**
     * Test that isNonNull handles special values (infinities, zeros, min/max) correctly.
     * All existing samples should result in 1.0 regardless of their values.
     */
    public void testIsNonNullWithSpecialValues() {
        IsNonNullStage stage = new IsNonNullStage();

        // Series with special numeric values
        List<Sample> specialSamples = List.of(
            new FloatSample(1000L, Double.POSITIVE_INFINITY),
            new FloatSample(2000L, Double.NEGATIVE_INFINITY),
            new FloatSample(3000L, 0.0),
            new FloatSample(4000L, -0.0),
            new FloatSample(5000L, Double.MIN_VALUE),
            new FloatSample(6000L, Double.MAX_VALUE)
        );

        ByteLabels labels = ByteLabels.fromStrings("type", "special");
        TimeSeries specialSeries = new TimeSeries(specialSamples, labels, 1000L, 6000L, 1000L, null);

        List<TimeSeries> result = stage.process(List.of(specialSeries));

        assertEquals(1, result.size());

        // All special values should be treated as present -> 1.0
        List<Sample> expected = List.of(
            new FloatSample(1000L, 1.0),  // +Inf present
            new FloatSample(2000L, 1.0),  // -Inf present
            new FloatSample(3000L, 1.0),  // 0.0 present
            new FloatSample(4000L, 1.0),  // -0.0 present
            new FloatSample(5000L, 1.0),  // MIN present
            new FloatSample(6000L, 1.0)   // MAX present
        );
        assertSamplesEqual("IsNonNull Special Values", expected, result.get(0).getSamples().toList());
    }

    /**
     * Test with empty input list.
     */
    public void testEmptyInput() {
        IsNonNullStage stage = new IsNonNullStage();
        List<TimeSeries> result = stage.process(List.of());
        assertTrue(result.isEmpty());
    }

    /**
     * Test that null input throws exception.
     */
    public void testNullInput() {
        IsNonNullStage stage = new IsNonNullStage();
        TestUtils.assertNullInputThrowsException(stage, "is_non_null");
    }

    /**
     * Test stage name.
     */
    public void testGetName() {
        IsNonNullStage stage = new IsNonNullStage();
        assertEquals("is_non_null", stage.getName());
    }

    /**
     * Test XContent serialization.
     */
    public void testToXContent() throws Exception {
        IsNonNullStage stage = new IsNonNullStage();
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertEquals("{}", json); // No parameters
    }

    /**
     * Test fromArgs with empty map.
     */
    public void testFromArgs() {
        Map<String, Object> args = new HashMap<>();
        IsNonNullStage stage = IsNonNullStage.fromArgs(args);
        assertEquals("is_non_null", stage.getName());
    }

    /**
     * Test fromArgs with null throws exception.
     */
    public void testFromArgsNull() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> IsNonNullStage.fromArgs(null));
        assertEquals("Args cannot be null", exception.getMessage());
    }

    /**
     * Test creation via PipelineStageFactory.
     */
    public void testCreateWithFactory() {
        Map<String, Object> args = Map.of();
        PipelineStage stage = PipelineStageFactory.createWithArgs("is_non_null", args);

        assertNotNull(stage);
        assertTrue(stage instanceof IsNonNullStage);
        assertEquals("is_non_null", stage.getName());
    }

    /**
     * Test stage does not support concurrent segment search (needs complete time series).
     */
    public void testSupportConcurrentSegmentSearch() {
        IsNonNullStage stage = new IsNonNullStage();
        assertFalse(stage.supportConcurrentSegmentSearch());
    }

    /**
     * Test equals and hashCode.
     */
    public void testEqualsAndHashCode() {
        IsNonNullStage stage1 = new IsNonNullStage();
        IsNonNullStage stage2 = new IsNonNullStage();

        assertEquals(stage1, stage1);
        assertEquals(stage1, stage2);
        assertEquals(stage2, stage1);
        assertEquals(stage1.hashCode(), stage2.hashCode());

        assertNotEquals(stage1, null);
        assertNotEquals(stage1, new Object());
    }

    @Override
    protected Writeable.Reader<IsNonNullStage> instanceReader() {
        return IsNonNullStage::readFrom;
    }

    @Override
    protected IsNonNullStage createTestInstance() {
        return new IsNonNullStage();
    }
}
