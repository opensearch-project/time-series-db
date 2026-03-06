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
import java.util.function.DoubleUnaryOperator;

/**
 * Pipeline stage that generates a synthetic time series with following common periodic functions.

 * This stage is primarily used for testing and mocking purposes and produces a periodic series where the min, max,
 * and period are specified inputs. The generated series includes optional tags.

 */
@PipelineStageAnnotation(name = MockFetchPeriodicStage.NAME)
public class MockFetchPeriodicStage extends AbstractMockFetchStage {

    public static final String NAME = "mockFetchPeriodic";

    private static final List<String> SUPPORTED_PERIODIC_FUNCTIONS = List.of("sine", "cosine", "triangle");

    private final String periodicFunction;
    private final double min;
    private final double max;
    private final double period;
    private final long endTime;

    /**
     * Constructor for MockFetchPeriodicStage.
     *
     * @param periodicFunction Periodic pattern that the series will follow.
     * @param min The lowest point of the periodic graph
     * @param max The highest point of the periodic graph
     * @param period Determines how many points fall within one iteration of a periodic function
     * @param tags Map of tag key-value pairs for the series
     * @param startTime Start timestamp in milliseconds
     * @param endTime End timestamp in milliseconds
     * @param step Step size in milliseconds
     */
    public MockFetchPeriodicStage(
        String periodicFunction,
        double min,
        double max,
        double period,
        Map<String, String> tags,
        long startTime,
        long endTime,
        long step
    ) {
        super(tags, startTime, step);

        if (!SUPPORTED_PERIODIC_FUNCTIONS.contains(periodicFunction)) {
            throw new IllegalArgumentException("Unknown periodic function: " + periodicFunction);
        }

        this.periodicFunction = periodicFunction;
        this.min = min;
        this.max = max;
        this.period = period;
        this.endTime = endTime;
    }

    @Override
    protected List<Double> generateValues() {
        // Calculate number of data points based on time range
        int numPoints = (int) ((endTime - startTime) / step);
        List<Double> values = new ArrayList<>(numPoints);

        double amplitude = (max - min) / 2.0;
        double mid = (max + min) / 2.0;

        DoubleUnaryOperator generateValueFn;
        switch (periodicFunction) {
            case "sine":
                generateValueFn = (t) -> mid + amplitude * Math.sin(2 * Math.PI * t / period);
                break;
            case "cosine":
                generateValueFn = (t) -> mid + amplitude * Math.cos(2 * Math.PI * t / period);
                break;
            case "triangle":
                generateValueFn = (t) -> {
                    double phase = ((t + 3 * period / 4) % period) / period;
                    double triangleWave = 4 * Math.abs(phase - 0.5) - 1;
                    return mid + amplitude * triangleWave;
                };
                break;
            default:
                throw new IllegalArgumentException("Unknown periodic function: " + periodicFunction);
        }

        for (int i = 0; i < numPoints; i++) {
            double value = generateValueFn.applyAsDouble(i);
            // Round to 10 decimal places to eliminate floating-point artifacts from trig functions
            // (e.g., sin(π) returns ~1e-16 instead of exactly 0)
            value = Math.round(value * 1e10) / 1e10;
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
        builder.field("periodicFunction", periodicFunction);
        builder.field("min", min);
        builder.field("max", max);
        builder.field("period", period);
        builder.field("endTime", endTime);
        writeCommonFieldsToXContent(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(periodicFunction);
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeDouble(period);
        out.writeLong(endTime);
        writeCommonFields(out);
    }

    /**
     * Create a MockFetchPeriodic instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new MockFetchPeriodicStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static MockFetchPeriodicStage readFrom(StreamInput in) throws IOException {
        String periodicFunction = in.readString();
        double min = in.readDouble();
        double max = in.readDouble();
        double period = in.readDouble();
        long endTime = in.readLong();
        Object[] commonFields = readCommonFields(in);
        @SuppressWarnings("unchecked")
        Map<String, String> tags = (Map<String, String>) commonFields[0];
        long startTime = (long) commonFields[1];
        long step = (long) commonFields[2];
        return new MockFetchPeriodicStage(periodicFunction, min, max, period, tags, startTime, endTime, step);
    }

    /**
     * Create a MockFetchPeriodicStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return MockFetchPeriodicStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static MockFetchPeriodicStage fromArgs(Map<String, Object> args) {
        if (!args.containsKey("periodicFunction")) {
            throw new IllegalArgumentException("MockFetchPeriodic requires 'periodicFunction' argument");
        }
        if (!args.containsKey("min")) {
            throw new IllegalArgumentException("MockFetchPeriodic requires 'min' argument");
        }
        if (!args.containsKey("max")) {
            throw new IllegalArgumentException("MockFetchPeriodic requires 'max' argument");
        }
        if (!args.containsKey("period")) {
            throw new IllegalArgumentException("MockFetchPeriodic requires 'period' argument");
        }

        String periodicFunction = (String) args.get("periodicFunction");
        double min = ((Number) args.get("min")).doubleValue();
        double max = ((Number) args.get("max")).doubleValue();
        double period = ((Number) args.get("period")).doubleValue();

        long endTime = ((Number) args.get("endTime")).longValue();
        Map<String, String> tags = parseTagsFromArgs(args, NAME);
        long startTime = parseStartTimeFromArgs(args);
        long step = parseStepFromArgs(args);

        return new MockFetchPeriodicStage(periodicFunction, min, max, period, tags, startTime, endTime, step);
    }

    /**
     * Returns the periodic function type.
     * @return periodic function name
     */
    public String getPeriodicFunction() {
        return periodicFunction;
    }

    /**
     * Returns the minimum value.
     * @return minimum value
     */
    public double getMin() {
        return min;
    }

    /**
     * Returns the maximum value.
     * @return maximum value
     */
    public double getMax() {
        return max;
    }

    /**
     * Returns the period.
     * @return period value
     */
    public double getPeriod() {
        return period;
    }

    /**
     * Returns the end time for testing purposes.
     * @return end time in milliseconds
     */
    public long getEndTime() {
        return endTime;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!super.equals(obj)) return false;
        if (!(obj instanceof MockFetchPeriodicStage)) return false;
        MockFetchPeriodicStage that = (MockFetchPeriodicStage) obj;
        return Double.compare(that.min, min) == 0
            && Double.compare(that.max, max) == 0
            && Double.compare(that.period, period) == 0
            && endTime == that.endTime
            && Objects.equals(periodicFunction, that.periodicFunction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), periodicFunction, min, max, period, endTime);
    }

}
