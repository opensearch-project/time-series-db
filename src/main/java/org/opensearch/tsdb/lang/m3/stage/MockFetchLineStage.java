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
 * Pipeline stage that generates a synthetic time series with a constant value (horizontal line).

 * This stage is primarily used for testing and mocking purposes, producing a series where each data point
 * has the same value over the specified time range and step interval. The generated series includes optional tags
 * and can be customized via arguments for value, start time, end time, and step size.

 */
@PipelineStageAnnotation(name = MockFetchLineStage.NAME)
public class MockFetchLineStage extends AbstractMockFetchStage {

    public static final String NAME = "mockFetchLine";

    private final double value;

    /**
     * Constructor for MockFetchLineStage.
     *
     * @param value Constant value for the horizontal line
     * @param tags Map of tag key-value pairs for the series
     * @param startTime Start timestamp in milliseconds
     * @param endTime End timestamp in milliseconds
     * @param step Step size in milliseconds
     */
    public MockFetchLineStage(double value, Map<String, String> tags, long startTime, long endTime, long step) {
        super(tags, startTime, endTime, step);
        this.value = value;
    }

    @Override
    protected List<Double> generateValues() {
        // Calculate number of data points based on time range (exclusive endTime)
        int numPoints = (int) ((endTime - startTime) / step);
        List<Double> values = new ArrayList<>(numPoints);
        for (int i = 0; i < numPoints; i++) {
            values.add(value);
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
        builder.field("value", value);
        writeCommonFieldsToXContent(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(value);
        writeCommonFields(out);
    }

    /**
     * Create a MockFetchLineStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new MockFetchLineStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static MockFetchLineStage readFrom(StreamInput in) throws IOException {
        double value = in.readDouble();
        Object[] commonFields = readCommonFields(in);
        @SuppressWarnings("unchecked")
        Map<String, String> tags = (Map<String, String>) commonFields[0];
        long startTime = (long) commonFields[1];
        long endTime = (long) commonFields[2];
        long step = (long) commonFields[3];
        return new MockFetchLineStage(value, tags, startTime, endTime, step);
    }

    /**
     * Create a MockFetchLineStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return MockFetchLineStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static MockFetchLineStage fromArgs(Map<String, Object> args) {
        if (!args.containsKey("value")) {
            throw new IllegalArgumentException("MockFetchLine requires 'value' argument");
        }
        if (!args.containsKey("endTime")) {
            throw new IllegalArgumentException("MockFetchLine requires 'endTime' argument");
        }

        double value;

        try {
            Object valueObj = args.get("value");
            if (valueObj instanceof Number) {
                value = ((Number) valueObj).doubleValue();
            } else if (valueObj instanceof String) {
                value = Double.parseDouble((String) valueObj);
            } else {
                throw new IllegalArgumentException("MockFetchLine 'value' must be a number, got: " + valueObj.getClass().getSimpleName());
            }
        } catch (NumberFormatException | ClassCastException e) {
            throw new IllegalArgumentException("Invalid value for MockFetchLine 'value' argument: " + args.get("value"), e);
        }

        Map<String, String> tags = parseTagsFromArgs(args, NAME);
        long startTime = parseStartTimeFromArgs(args);
        long endTime = parseEndTimeFromArgs(args);
        long step = parseStepFromArgs(args);

        return new MockFetchLineStage(value, tags, startTime, endTime, step);
    }

    /**
     * Returns the constant value for testing purposes.
     * @return constant value
     */
    public double getValue() {
        return value;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        MockFetchLineStage that = (MockFetchLineStage) obj;
        return Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }
}
