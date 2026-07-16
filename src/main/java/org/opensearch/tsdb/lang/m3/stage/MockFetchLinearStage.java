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
 * A pipeline stage that generates mock time series data with linear progression.
 *
 * MockFetchLinearStage generates synthetic time series data on the coordinator node. The values
 * follow a linear progression when given a start value, stop value, and step size
 *
 * @see AbstractMockFetchStage
 * @see MockFetchLineStage
 */
@PipelineStageAnnotation(name = MockFetchLinearStage.NAME)
public class MockFetchLinearStage extends AbstractMockFetchStage {

    public static final String NAME = "mockFetchLinear";

    private final double start;
    private final double stop;
    private final double slope;

    /**
     * Constructor for MockFetchLinearStage.
     *
     * @param start Starting value for the linear progression
     * @param stop Ending value for the linear progression (inclusive)
     * @param slope Increment for each step
     * @param tags Map of tag key-value pairs for the series
     * @param startTime Start timestamp in milliseconds
     * @param step Step size in milliseconds (time interval)
     */
    public MockFetchLinearStage(
        double start,
        double stop,
        double slope,
        Map<String, String> tags,
        long startTime,
        long endTime,
        long step
    ) {
        super(tags, startTime, endTime, step);
        if (slope == 0) {
            throw new IllegalArgumentException("MockFetchLinear requires non-zero slope");
        }
        // Validate step direction matches start/stop range
        if (stop > start && slope < 0) {
            throw new IllegalArgumentException("Step size must be positive if end is greater than start");
        }
        if (start > stop && slope > 0) {
            throw new IllegalArgumentException("Step size must be negative if start is greater than end");
        }
        this.start = start;
        this.stop = stop;
        this.slope = slope;
    }

    @Override
    protected List<Double> generateValues() {
        // Cap number of points by the time window (exclusive endTime)
        int maxSamples = (int) ((endTime - startTime) / step);
        List<Double> values = new ArrayList<>();

        if (slope > 0) {
            // Ascending
            for (double value = start; value <= stop && values.size() < maxSamples; value += slope) {
                values.add(value);
            }
        } else {
            // Descending
            for (double value = start; value >= stop && values.size() < maxSamples; value += slope) {
                values.add(value);
            }
        }

        // Ensure we have at least one value
        if (values.isEmpty()) {
            values.add(start);
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
        builder.field("start", start);
        builder.field("stop", stop);
        builder.field("slope", slope);
        writeCommonFieldsToXContent(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(start);
        out.writeDouble(stop);
        out.writeDouble(slope);
        writeCommonFields(out);
    }

    /**
     * Create a MockFetchLinearStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new MockFetchLinearStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static MockFetchLinearStage readFrom(StreamInput in) throws IOException {
        double start = in.readDouble();
        double stop = in.readDouble();
        double slope = in.readDouble();
        Object[] commonFields = readCommonFields(in);
        @SuppressWarnings("unchecked")
        Map<String, String> tags = (Map<String, String>) commonFields[0];
        long startTime = (long) commonFields[1];
        long endTime = (long) commonFields[2];
        long step = (long) commonFields[3];
        return new MockFetchLinearStage(start, stop, slope, tags, startTime, endTime, step);
    }

    /**
     * Create a MockFetchLinearStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return MockFetchLinearStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static MockFetchLinearStage fromArgs(Map<String, Object> args) {
        if (!args.containsKey("start")) {
            throw new IllegalArgumentException("MockFetchLinear requires 'start' argument");
        }
        if (!args.containsKey("stop")) {
            throw new IllegalArgumentException("MockFetchLinear requires 'stop' argument");
        }
        if (!args.containsKey("slope")) {
            throw new IllegalArgumentException("MockFetchLinear requires 'slope' argument");
        }
        if (!args.containsKey("endTime")) {
            throw new IllegalArgumentException("MockFetchLinear requires 'endTime' argument");
        }

        double start = ((Number) args.get("start")).doubleValue();
        double stop = ((Number) args.get("stop")).doubleValue();
        double slope = ((Number) args.get("slope")).doubleValue();

        Map<String, String> tags = parseTagsFromArgs(args, NAME);
        long startTime = parseStartTimeFromArgs(args);
        long endTime = parseEndTimeFromArgs(args);
        long step = parseStepFromArgs(args);

        return new MockFetchLinearStage(start, stop, slope, tags, startTime, endTime, step);
    }

    /**
     * Returns the start value for testing purposes.
     * @return start value
     */
    public double getStart() {
        return start;
    }

    /**
     * Returns the stop value for testing purposes.
     * @return stop value
     */
    public double getStop() {
        return stop;
    }

    /**
     * Returns the slope for testing purposes.
     * @return slope value
     */
    public double getSlope() {
        return slope;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        MockFetchLinearStage that = (MockFetchLinearStage) obj;
        return Double.compare(that.start, start) == 0 && Double.compare(that.stop, stop) == 0 && Double.compare(that.slope, slope) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), start, stop, slope);
    }
}
