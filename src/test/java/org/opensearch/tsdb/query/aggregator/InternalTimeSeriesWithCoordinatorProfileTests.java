/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link InternalTimeSeriesWithCoordinatorProfile}.
 *
 * <p>This test class focuses on the profiling-specific functionality added by
 * InternalTimeSeriesWithCoordinatorProfile. Tests for inherited behavior from
 * InternalTimeSeries are covered in InternalTimeSeriesTests.</p>
 */
public class InternalTimeSeriesWithCoordinatorProfileTests extends OpenSearchTestCase {

    private static final String TEST_NAME = "test-profiled-time-series";

    // ========== Constructor Tests (Profiling-Specific) ==========

    public void testConstructorWithProfileInfo() {

        CoordinatorProfileInfo profileInfo = createProfileInfo("test_stages", 1000L);

        InternalTimeSeriesWithCoordinatorProfile result = new InternalTimeSeriesWithCoordinatorProfile(
            TEST_NAME,
            List.of(),
            Map.of(),
            null,
            profileInfo
        );

        assertEquals(profileInfo, result.getCoordinatorProfileInfo());
        assertEquals(0L, result.getSerializeTimeNanos());
        assertEquals(0L, result.getDeserializeTimeNanos());
    }

    public void testConstructorWithNullProfileInfo() {

        InternalTimeSeriesWithCoordinatorProfile result = new InternalTimeSeriesWithCoordinatorProfile(
            TEST_NAME,
            List.of(),
            Map.of(),
            null,
            null
        );

        assertNull(result.getCoordinatorProfileInfo());
    }

    public void testConstructorWith7ParametersIncludesExecStatsAndDataSource() {

        AggregationExecStats execStats = new AggregationExecStats(1L, 2L, 3L, 4L, 5L, 6L, 7L);
        AggregationDataSource dataSource = AggregationDataSource.EMPTY;
        CoordinatorProfileInfo profileInfo = createProfileInfo("stages", 1000L);

        InternalTimeSeriesWithCoordinatorProfile result = new InternalTimeSeriesWithCoordinatorProfile(
            TEST_NAME,
            List.of(),
            Map.of(),
            null,
            execStats,
            dataSource,
            profileInfo
        );

        assertEquals(profileInfo, result.getCoordinatorProfileInfo());
        assertEquals(execStats, result.getExecStats());
        assertEquals(dataSource, result.getDataSource());
    }

    // ========== Writeable Name Test (Override Verification) ==========

    public void testGetWriteableNameDiffersFromParent() {

        InternalTimeSeriesWithCoordinatorProfile profiled = createResult(TEST_NAME);
        InternalTimeSeries standard = new InternalTimeSeries(TEST_NAME, List.of(), Map.of());

        assertEquals("time_series_with_coordinator_profile", profiled.getWriteableName());
        assertEquals("time_series", standard.getWriteableName());
        assertNotEquals(standard.getWriteableName(), profiled.getWriteableName());
    }

    // ========== Serialization Tests (Profiling-Specific) ==========

    public void testSerializationWithProfileInfo() throws IOException {

        CoordinatorProfileInfo profileInfo = createProfileInfo("mock: 500ns;scale: 300ns", 1500L);
        InternalTimeSeriesWithCoordinatorProfile original = new InternalTimeSeriesWithCoordinatorProfile(
            "test_ser",
            List.of(),
            Map.of("key", "value"),
            null,
            profileInfo
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTimeSeriesWithCoordinatorProfile deserialized = new InternalTimeSeriesWithCoordinatorProfile(in);

        assertEquals(original.getName(), deserialized.getName());
        assertNotNull(deserialized.getCoordinatorProfileInfo());
        assertEquals(original.getCoordinatorProfileInfo().getStages(), deserialized.getCoordinatorProfileInfo().getStages());
    }

    public void testSerializationWithNullProfileInfo() throws IOException {

        InternalTimeSeriesWithCoordinatorProfile original = new InternalTimeSeriesWithCoordinatorProfile(
            "test_null",
            List.of(),
            Map.of(),
            null,
            null
        );

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        InternalTimeSeriesWithCoordinatorProfile deserialized = new InternalTimeSeriesWithCoordinatorProfile(in);

        assertNull(deserialized.getCoordinatorProfileInfo());
    }

    public void testSerializationCapturesTimings() throws IOException {

        InternalTimeSeriesWithCoordinatorProfile original = createResult("timing_test");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        InternalTimeSeriesWithCoordinatorProfile deserialized = new InternalTimeSeriesWithCoordinatorProfile(in);

        // Note: serialize time from original is not preserved, timing happens per serialization
        assertTrue("Deserialize time should be captured", deserialized.getDeserializeTimeNanos() >= 0);
    }

    // ========== XContent Tests (Override Verification) ==========

    public void testDoXContentBodyIncludesCoordinatorProfile() throws IOException {

        CoordinatorProfileInfo profileInfo = new CoordinatorProfileInfo("mockFetch: 1000ns;scale: 500ns", 2000L, 1500L, 500L, 300L, 200L);

        InternalTimeSeriesWithCoordinatorProfile result = new InternalTimeSeriesWithCoordinatorProfile(
            "test",
            List.of(),
            Map.of(),
            null,
            profileInfo
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        result.doXContentBody(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertTrue("Should contain coordinator_profile", json.contains("coordinator_profile"));
        assertTrue("Should contain stages", json.contains("mockFetch: 1000ns"));
        assertTrue("Should contain reduce_time_ns", json.contains("reduce_time_ns"));
    }

    public void testDoXContentBodyWithoutProfileExcludesCoordinatorProfile() throws IOException {

        InternalTimeSeriesWithCoordinatorProfile result = new InternalTimeSeriesWithCoordinatorProfile(
            "test",
            List.of(),
            Map.of(),
            null,
            null
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        result.doXContentBody(builder, null);
        builder.endObject();

        String json = builder.toString();
        assertFalse("Should not contain coordinator_profile when null", json.contains("coordinator_profile"));
    }

    // ========== Reduce Tests (Override Verification) ==========

    public void testReducePartialDelegatesToParent() {

        List<InternalAggregation> aggregations = List.of(createResult("test1"), createResult("test2"));
        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forPartialReduction(null, null, () -> emptyTree);

        InternalTimeSeriesWithCoordinatorProfile first = (InternalTimeSeriesWithCoordinatorProfile) aggregations.get(0);
        InternalAggregation result = first.reduce(aggregations, context);

        assertNotNull(result);
    }

    public void testReduceFinalCreatesProfilingResult() {

        CoordinatorProfileInfo profile1 = createProfileInfo("stage1", 1000L);
        CoordinatorProfileInfo profile2 = createProfileInfo("stage2", 2000L);

        List<InternalAggregation> aggregations = List.of(
            new InternalTimeSeriesWithCoordinatorProfile("test1", List.of(), Map.of(), null, profile1),
            new InternalTimeSeriesWithCoordinatorProfile("test2", List.of(), Map.of(), null, profile2)
        );

        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);

        InternalTimeSeriesWithCoordinatorProfile first = (InternalTimeSeriesWithCoordinatorProfile) aggregations.get(0);
        InternalAggregation result = first.reduce(aggregations, context);

        assertTrue("Should return InternalTimeSeriesWithCoordinatorProfile", result instanceof InternalTimeSeriesWithCoordinatorProfile);
        InternalTimeSeriesWithCoordinatorProfile profiledResult = (InternalTimeSeriesWithCoordinatorProfile) result;
        assertNotNull("Should have coordinator profile info", profiledResult.getCoordinatorProfileInfo());
    }

    public void testReduceFinalAggregatesSerializationTimings() {

        // Note: In real usage, these times are captured during actual serialization,
        // but for unit testing we verify the aggregation logic
        CoordinatorProfileInfo profile1 = createProfileInfo("stage1", 1000L);
        CoordinatorProfileInfo profile2 = createProfileInfo("stage2", 2000L);

        List<InternalAggregation> aggregations = List.of(
            new InternalTimeSeriesWithCoordinatorProfile("test1", List.of(), Map.of(), null, profile1),
            new InternalTimeSeriesWithCoordinatorProfile("test2", List.of(), Map.of(), null, profile2)
        );

        PipelineAggregator.PipelineTree emptyTree = new PipelineAggregator.PipelineTree(Collections.emptyMap(), Collections.emptyList());
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(null, null, s -> {}, emptyTree);

        InternalTimeSeriesWithCoordinatorProfile first = (InternalTimeSeriesWithCoordinatorProfile) aggregations.get(0);
        InternalAggregation result = first.reduce(aggregations, context);

        assertTrue(result instanceof InternalTimeSeriesWithCoordinatorProfile);
        InternalTimeSeriesWithCoordinatorProfile profiledResult = (InternalTimeSeriesWithCoordinatorProfile) result;
        assertNotNull("Should have aggregated profile info", profiledResult.getCoordinatorProfileInfo());
        assertTrue("Should have internal reduce time", profiledResult.getCoordinatorProfileInfo().getInternalReduceTimeNanos() >= 0);
    }

    // ========== Inheritance Verification ==========

    public void testInstanceOfParentClass() {

        InternalTimeSeriesWithCoordinatorProfile result = createResult("test");

        assertTrue("Should be instance of InternalTimeSeries", result instanceof InternalTimeSeries);
    }

    public void testInheritsParentBehavior() {

        InternalTimeSeriesWithCoordinatorProfile result = createResult("inherited_test");

        assertEquals("inherited_test", result.getName());
        assertNotNull(result.getTimeSeries());
        assertEquals(AggregationExecStats.EMPTY, result.getExecStats());
        assertEquals(AggregationDataSource.EMPTY, result.getDataSource());
    }

    // ========== Helper Methods ==========

    private InternalTimeSeriesWithCoordinatorProfile createResult(String name) {
        return new InternalTimeSeriesWithCoordinatorProfile(name, List.of(), Map.of(), null, createProfileInfo("test_stages", 1000L));
    }

    private CoordinatorProfileInfo createProfileInfo(String stages, long reduceTimeNanos) {
        return new CoordinatorProfileInfo(stages, reduceTimeNanos, 500L, 200L, 0L, 0L);
    }
}
