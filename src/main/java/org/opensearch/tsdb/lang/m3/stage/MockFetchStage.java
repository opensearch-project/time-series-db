/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A pipeline stage that generates mock time series data based on provided values and tags.
 *
 * MockFetchStage generates synthetic time series data on the coordinator node for testing purposes.
 * Unlike stages that transform existing time series data, MockFetchStage ignores its input and creates
 * new data from scratch based on the provided values.

 * @see AbstractMockFetchStage
 */
@PipelineStageAnnotation(name = MockFetchStage.NAME)
public class MockFetchStage extends AbstractMockFetchStage {

    public static final String NAME = "mockFetch";

    private final List<Double> values;

    /**
     * Constructor for MockFetchStage.
     *
     * @param values List of values to generate
     * @param tags Map of tag key-value pairs for the series
     * @param startTime Start timestamp in milliseconds
     * @param endTime End timestamp in milliseconds
     * @param step Step size in milliseconds
     */
    public MockFetchStage(List<Double> values, Map<String, String> tags, long startTime, long endTime, long step) {
        super(tags, startTime, endTime, step);
        if (values == null || values.isEmpty()) {
            throw new IllegalArgumentException("MockFetch requires at least one value");
        }
        this.values = new ArrayList<>(values);
    }

    @Override
    protected List<Double> generateValues() {
        // Calculate maximum samples (exclusive endTime)
        int maxSamples = (int) ((endTime - startTime) / step);

        // Limit values to fit within the time range
        if (values.size() > maxSamples) {
            return values.subList(0, maxSamples);
        }

        return values;
    }

    @Override
    protected String getDefaultTagName() {
        return NAME;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("values", values);
        writeCommonFieldsToXContent(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(values.size());
        for (Double value : values) {
            out.writeDouble(value);
        }
        writeCommonFields(out);
    }

    /**
     * Create a MockFetchStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new MockFetchStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static MockFetchStage readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<Double> values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            values.add(in.readDouble());
        }
        Object[] commonFields = readCommonFields(in);
        @SuppressWarnings("unchecked")
        Map<String, String> tags = (Map<String, String>) commonFields[0];
        long startTime = (long) commonFields[1];
        long endTime = (long) commonFields[2];
        long step = (long) commonFields[3];
        return new MockFetchStage(values, tags, startTime, endTime, step);
    }

    /**
     * Create a MockFetchStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return MockFetchStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    @SuppressWarnings("unchecked")
    public static MockFetchStage fromArgs(Map<String, Object> args) {
        if (!args.containsKey("values")) {
            throw new IllegalArgumentException("MockFetch requires 'values' argument");
        }

        Object valuesObj = args.get("values");
        List<Double> values = new ArrayList<>();

        if (valuesObj instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof Number num) {
                    values.add(num.doubleValue());
                } else if (item instanceof String str) {
                    try {
                        values.add(Double.parseDouble(str));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid numeric value: " + str, e);
                    }
                } else {
                    throw new IllegalArgumentException("Invalid value type: " + item.getClass());
                }
            }
        } else if (valuesObj instanceof Number num) {
            values.add(num.doubleValue());
        } else if (valuesObj instanceof String str) {
            try {
                values.add(Double.parseDouble(str));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid numeric value: " + str, e);
            }
        } else {
            throw new IllegalArgumentException("Invalid values argument type: " + valuesObj.getClass());
        }

        Map<String, String> tags = parseTagsFromArgs(args, NAME);
        long startTime = parseStartTimeFromArgs(args);
        long endTime = parseEndTimeFromArgs(args);
        long step = parseStepFromArgs(args);

        return new MockFetchStage(values, tags, startTime, endTime, step);
    }

    /**
     * Returns the values for testing purposes.
     * @return list of values
     */
    public List<Double> getValues() {
        return new ArrayList<>(values);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        MockFetchStage that = (MockFetchStage) obj;
        return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), values);
    }
}
