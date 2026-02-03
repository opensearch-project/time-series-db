/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Tests for AliasStage functionality.
 */
public class AliasStageTests extends AbstractWireSerializingTestCase<AliasStage> {

    /**
     * Test AliasStage with simple string constant.
     */
    public void testAliasStageWithStringConstant() {
        AliasStage stage = new AliasStage("new_series_name");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0), new FloatSample(1100L, 20.0));

        ByteLabels labels = ByteLabels.fromStrings("instance", "server1", "job", "api");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1100L, 10000, null);

        List<TimeSeries> input = List.of(timeSeries);
        List<TimeSeries> result = stage.process(input);

        assertEquals(1, result.size());
        assertEquals("new_series_name", result.getFirst().getAlias());
        assertEquals(samples, result.getFirst().getSamples().toList());
        assertEquals(labels, result.getFirst().getLabels());
    }

    /**
     * Test AliasStage with tag interpolation using {{.tag}} syntax.
     */
    public void testAliasStageWithTagInterpolation() {
        AliasStage stage = new AliasStage("server_{{.instance}}_job_{{.job}}");

        List<Sample> samples = List.of(new FloatSample(1000L, 15.0), new FloatSample(1100L, 25.0));

        ByteLabels labels = ByteLabels.fromStrings("instance", "web01", "job", "nginx");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1100L, 1000L, null);

        List<TimeSeries> input = List.of(timeSeries);
        List<TimeSeries> result = stage.process(input);

        assertEquals(1, result.size());
        assertEquals("server_web01_job_nginx", result.getFirst().getAlias());
        assertEquals(samples, result.getFirst().getSamples().toList());
        assertEquals(labels, result.getFirst().getLabels());
    }

    /**
     * Test AliasStage with missing tag values (should be replaced with tag name).
     */
    public void testAliasStageWithMissingTags() {
        AliasStage stage = new AliasStage("server_{{.instance}}_{{.missing_tag}}");

        List<Sample> samples = List.of(new FloatSample(1000L, 30.0));

        ByteLabels labels = ByteLabels.fromStrings("instance", "db01");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> input = List.of(timeSeries);
        List<TimeSeries> result = stage.process(input);

        assertEquals(1, result.size());
        assertEquals("server_db01_missing_tag", result.getFirst().getAlias());
    }

    /**
     * Test AliasStage with multiple time series having different labels.
     */
    public void testAliasStageWithMultipleTimeSeries() {
        AliasStage stage = new AliasStage("{{.service}}_metrics");

        List<Sample> samples1 = List.of(new FloatSample(1000L, 10.0));
        List<Sample> samples2 = List.of(new FloatSample(1000L, 20.0));

        ByteLabels labels1 = ByteLabels.fromStrings("service", "api", "instance", "server1");
        ByteLabels labels2 = ByteLabels.fromStrings("service", "database", "instance", "server2");

        TimeSeries ts1 = new TimeSeries(samples1, labels1, 1000L, 1000L, 1000L, null);
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 1000L, 1000L, 1000L, null);

        List<TimeSeries> input = List.of(ts1, ts2);
        List<TimeSeries> result = stage.process(input);

        assertEquals(2, result.size());
        assertEquals("api_metrics", result.getFirst().getAlias());
        assertEquals("database_metrics", result.get(1).getAlias());
    }

    /**
     * Test AliasStage fromArgs method with string constant.
     */
    public void testAliasStageFromArgsStringConstant() {
        Map<String, Object> args = Map.of("pattern", "simple_name");
        AliasStage stage = AliasStage.fromArgs(args);
        assertEquals("simple_name", stage.getAliasPattern());

        // Test with quoted string
        Map<String, Object> argsQuoted = Map.of("pattern", "\"quoted_name\"");
        AliasStage stageQuoted = AliasStage.fromArgs(argsQuoted);
        assertEquals("\"quoted_name\"", stageQuoted.getAliasPattern());
    }

    /**
     * Test AliasStage fromArgs method with tag interpolation.
     */
    public void testAliasStageFromArgsWithTagInterpolation() {
        Map<String, Object> args = Map.of("pattern", "prefix_{{.tag}}_suffix");
        AliasStage stage = AliasStage.fromArgs(args);
        assertEquals("prefix_{{.tag}}_suffix", stage.getAliasPattern());
    }

    /**
     * Test AliasStage fromArgs method with null and empty args.
     */
    public void testAliasStageFromArgsUnsetAlias() {
        // Test with null pattern
        Map<String, Object> argsNull = Map.of();
        AliasStage nullStage = AliasStage.fromArgs(argsNull);
        assertNull(nullStage.getAliasPattern());

        // Test with empty pattern
        Map<String, Object> argsEmpty = Map.of("pattern", "");
        AliasStage emptyStage = AliasStage.fromArgs(argsEmpty);
        assertEquals("", emptyStage.getAliasPattern());
    }

    /**
     * Test AliasStage getName method.
     */
    public void testAliasStageGetName() {
        AliasStage stage = new AliasStage("test_alias");
        assertEquals("alias", stage.getName());
    }

    /**
     * Test AliasStage with empty input list.
     */
    public void testAliasStageWithEmptyInput() {
        AliasStage stage = new AliasStage("empty_test");
        List<TimeSeries> input = new ArrayList<>();
        List<TimeSeries> result = stage.process(input);

        assertTrue(result.isEmpty());
    }

    /**
     * Test AliasStage with null input throws exception.
     */
    public void testAliasStageWithNullInput() {
        AliasStage stage = new AliasStage("test");
        TestUtils.assertNullInputThrowsException(stage, "alias");
    }

    /**
     * Test AliasStage with null alias pattern.
     */
    public void testAliasStageWithNullPattern() {
        AliasStage stage = new AliasStage(null);

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        ByteLabels labels = ByteLabels.fromStrings("instance", "test");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> input = List.of(timeSeries);
        List<TimeSeries> result = stage.process(input);

        assertEquals(1, result.size());
        assertNull(result.getFirst().getAlias());
    }

    /**
     * Test AliasStage with complex tag interpolation pattern.
     */
    public void testAliasStageComplexPattern() {
        AliasStage stage = new AliasStage("{{.env}}/{{.service}}:{{.port}}_{{.version}}");

        List<Sample> samples = List.of(new FloatSample(2000L, 42.0));

        ByteLabels labels = ByteLabels.fromStrings("env", "production", "service", "auth-service", "port", "8080", "version", "v1.2.3");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 2000L, 2000L, 2000L, null);

        List<TimeSeries> input = List.of(timeSeries);
        List<TimeSeries> result = stage.process(input);

        assertEquals(1, result.size());
        assertEquals("production/auth-service:8080_v1.2.3", result.get(0).getAlias());
    }

    /**
     * Test example from requirements: alias with existing and missing tags.
     * alias "my-series {{.existing_tag}} {{.missing_tag}}" on series {existing_tag=abc}
     * should produce "my-series abc missing_tag"
     */
    public void testAliasStageExampleFromRequirements() {
        AliasStage stage = new AliasStage("my-series {{.existing_tag}} {{.missing_tag}}");

        ByteLabels labels = ByteLabels.fromStrings("existing_tag", "abc");
        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> input = List.of(timeSeries);
        List<TimeSeries> result = stage.process(input);

        assertEquals(1, result.size());
        assertEquals("my-series abc missing_tag", result.get(0).getAlias());
    }

    /**
     * Test multiple series with different tag values to demonstrate
     * we can correctly set aliases of multiple series separately.
     */
    public void testAliasStageWithMultipleSeriesDifferentTags() {
        AliasStage stage = new AliasStage("{{.service}}_on_{{.instance}}_{{.missing}}");

        List<Sample> samples1 = List.of(new FloatSample(1000L, 10.0));
        List<Sample> samples2 = List.of(new FloatSample(1000L, 20.0));

        ByteLabels labels1 = ByteLabels.fromStrings("service", "api", "instance", "server1");
        ByteLabels labels2 = ByteLabels.fromStrings("service", "database", "instance", "server2", "extra", "value");

        TimeSeries ts1 = new TimeSeries(samples1, labels1, 1000L, 1000L, 1000L, null);
        TimeSeries ts2 = new TimeSeries(samples2, labels2, 1000L, 1000L, 1000L, null);

        List<TimeSeries> input = List.of(ts1, ts2);
        List<TimeSeries> result = stage.process(input);

        assertEquals(2, result.size());
        assertEquals("api_on_server1_missing", result.get(0).getAlias());
        assertEquals("database_on_server2_missing", result.get(1).getAlias());

        // Verify original data is preserved
        assertEquals(labels1, result.get(0).getLabels());
        assertEquals(labels2, result.get(1).getLabels());
        assertEquals(samples1, result.get(0).getSamples().toList());
        assertEquals(samples2, result.get(1).getSamples().toList());
    }

    /**
     * Test that original TimeSeries data is preserved (labels, hash, samples).
     */
    public void testAliasStagePreservesOriginalData() {
        AliasStage stage = new AliasStage("renamed_series");

        List<Sample> originalSamples = List.of(new FloatSample(1000L, 1.0), new FloatSample(2000L, 2.0), new FloatSample(3000L, 3.0));

        ByteLabels originalLabels = ByteLabels.fromStrings("job", "prometheus", "instance", "localhost:9090");

        TimeSeries originalTs = new TimeSeries(originalSamples, originalLabels, 1000L, 3000L, 1000L, null);

        List<TimeSeries> input = List.of(originalTs);
        List<TimeSeries> result = stage.process(input);

        assertEquals(1, result.size());
        TimeSeries resultTs = result.get(0);

        // Check alias was set
        assertEquals("renamed_series", resultTs.getAlias());

        // Check original data is preserved
        assertEquals(originalTs.getLabels(), resultTs.getLabels());
        assertEquals(originalTs.getSamples().toList(), resultTs.getSamples().toList());
        assertEquals(originalTs.getMinTimestamp(), resultTs.getMinTimestamp());
        assertEquals(originalTs.getMaxTimestamp(), resultTs.getMaxTimestamp());
        assertEquals(originalTs.getStep(), resultTs.getStep());
        assertEquals("renamed_series", resultTs.getAlias());
    }

    /**
     * Test AliasStage with pattern that simulates multiple concatenated arguments.
     * This verifies the multiple argument functionality works correctly in practice.
     */
    public void testAliasStageWithMultipleWordPattern() {
        AliasStage stage = new AliasStage("prefix {{.service}} middle {{.env}} suffix");

        List<Sample> samples = List.of(new FloatSample(1000L, 10.0));

        ByteLabels labels = ByteLabels.fromStrings("service", "api-gateway", "env", "production", "version", "v2.1");
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 1000L, 1000L, null);

        List<TimeSeries> input = List.of(timeSeries);
        List<TimeSeries> result = stage.process(input);

        assertEquals(1, result.size());
        assertEquals("prefix api-gateway middle production suffix", result.get(0).getAlias());
    }

    /**
     * Test PipelineStageFactory
     */
    public void testPipelineStageFactoryCreateWithArgs() {
        assertTrue(PipelineStageFactory.isStageTypeSupported(AliasStage.NAME));
        PipelineStage aliasStage = PipelineStageFactory.createWithArgs(AliasStage.NAME, Map.of("pattern", "test_alias"));
        assertTrue(aliasStage instanceof AliasStage);
        assertEquals("test_alias", ((AliasStage) aliasStage).getAliasPattern());
    }

    /**
     * Basic test for PipelineStageFactory.readFrom(StreamInput) with AliasStage.
     */
    public void testPipelineStageFactoryReadFrom_StreamInput() throws Exception {
        AliasStage original = new AliasStage("alias_{{.instance}}");

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Factory variant that reads stage name first
            out.writeString(AliasStage.NAME);
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                PipelineStage restored = PipelineStageFactory.readFrom(in);
                assertNotNull(restored);
                assertTrue(restored instanceof AliasStage);
                assertEquals("alias_{{.instance}}", ((AliasStage) restored).getAliasPattern());
                assertEquals(AliasStage.NAME, restored.getName());
            }
        }
    }

    /**
     * Basic test for PipelineStageFactory.readFrom(StreamInput, String) with AliasStage.
     */
    public void testPipelineStageFactoryReadFrom_WithStageName() throws Exception {
        AliasStage original = new AliasStage("svc_{{.service}}");

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            // Overload that takes stage name separately only needs stage payload in the stream
            original.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                PipelineStage restored = PipelineStageFactory.readFrom(in, AliasStage.NAME);
                assertNotNull(restored);
                assertTrue(restored instanceof AliasStage);
                assertEquals("svc_{{.service}}", ((AliasStage) restored).getAliasPattern());
                assertEquals(AliasStage.NAME, restored.getName());
            }
        }
    }

    /**
     * Test equals method for AliasStage.
     */
    public void testEquals() {
        AliasStage stage1 = new AliasStage("test_pattern");
        AliasStage stage2 = new AliasStage("test_pattern");

        assertEquals("Equal AliasStages should be equal", stage1, stage2);

        AliasStage stageDifferent = new AliasStage("different_pattern");
        assertNotEquals("Different alias patterns should not be equal", stage1, stageDifferent);

        AliasStage stageNull1 = new AliasStage(null);
        AliasStage stageNull2 = new AliasStage(null);
        assertEquals("Null alias patterns should be equal", stageNull1, stageNull2);

        assertNotEquals("Null vs non-null alias patterns should not be equal", stage1, stageNull1);
        assertNotEquals("Non-null vs null alias patterns should not be equal", stageNull1, stage1);

        assertEquals("Stage should equal itself", stage1, stage1);

        assertNotEquals("Stage should not equal null", null, stage1);

        assertNotEquals("Stage should not equal different class", "string", stage1);
    }

    @Override
    protected Writeable.Reader<AliasStage> instanceReader() {
        return AliasStage::readFrom;
    }

    @Override
    protected AliasStage createTestInstance() {
        return new AliasStage(randomAlphaOfLengthBetween(0, 20));
    }
}
