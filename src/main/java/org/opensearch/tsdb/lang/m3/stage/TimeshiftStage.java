/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.Map;

/**
 * A pipeline stage that shifts all timestamps in a time series by a specified amount.
 * This implements the 'timeshift amount+unit' command from M3QL.
 *
 * <p>This stage shifts all timestamps forward in the time series by the specified amount (in milliseconds)
 * while preserving the original values. It extends {@link AbstractMapperStage} to
 * process each data point independently.</p>
 *
 * <h2>Time Shifting Operations:</h2>
 * <ul>
 *   <li><strong>Forward Shifting:</strong> Always moves timestamps forward, regardless of sign, used to move old data into the query window</li>
 *   <li><strong>Absolute Value:</strong> Uses absolute value of shift amount to ensure forward movement</li>
 *   <li><strong>Millisecond Precision:</strong> Accepts shift amount directly in milliseconds for precise control</li>
 *   <li><strong>Value Preservation:</strong> All sample values remain unchanged</li>
 *   <li><strong>Consistent Behavior:</strong> Both positive and negative values result in forward shifting</li>
 * </ul>
 *
 * <h2>Example Usage:</h2>
 * <pre>{@code
 * // Create a TimeshiftStage to shift by 1 hour
 * TimeshiftStage timeshiftStage = new TimeshiftStage(TimeValue.timeValueHours(1).getMillis());
 *
 * // Process a list of time series
 * List<TimeSeries> inputTimeSeries = ...;
 * List<TimeSeries> result = timeshiftStage.process(inputTimeSeries);
 *
 * // Even if a negative shift amount is provided, it will shift forward
 * TimeshiftStage alsoForward = new TimeshiftStage(TimeValue.timeValueMinutes(-30).getMillis());
 * List<TimeSeries> result = alsoForward.process(inputTimeSeries);
 * }</pre>
 *
 * <h2>Performance Considerations:</h2>
 * <p>This stage is optimized for timestamp manipulation and supports concurrent
 * segment search since time shifting operations can be performed independently
 * on different segments of the time series data.</p>
 *
 * @since 0.0.1
 */
@PipelineStageAnnotation(name = "timeshift")
public class TimeshiftStage extends AbstractMapperStage {
    /** The name identifier for this pipeline stage type. */
    public static final String NAME = "timeshift";

    /** The argument name for the shift amount parameter. */
    public static final String SHIFT_AMOUNT_ARG = "shift_amount";

    /** The amount to shift timestamps by in milliseconds. */
    private final long shiftMillis;

    /**
     * Constructs a new TimeshiftStage with the specified shift amount in milliseconds.
     * FIXME: remove milliseconds refs and update the serialized representation for consistency with other stages
     *
     * @param shiftMillis the amount to shift timestamps by in milliseconds (can be positive or negative)
     * @throws IllegalArgumentException if the shift amount is too large (Long.MIN_VALUE)
     */
    public TimeshiftStage(long shiftMillis) {
        // Validate that the shift amount can be safely converted to absolute value
        try {
            this.shiftMillis = Math.absExact(shiftMillis);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Time shift value too large: " + shiftMillis, e);
        }
    }

    /**
     * Map a single sample by shifting its timestamp forward by the specified amount.
     */
    @Override
    protected void mapSample(long timestamp, double value, UpdateConsumer updateConsumer) {
        long shiftedTimestamp = timestamp + shiftMillis;

        // Create new sample with shifted timestamp but same value
        updateConsumer.update(shiftedTimestamp, value);
    }

    /**
     * Create a new time series with shifted samples and updated timestamp metadata.
     * This method overrides the default implementation to update min/max timestamps.
     *
     * @param mappedSamples The list of samples with shifted timestamps
     * @param originalSeries The original time series
     * @return A new time series with shifted samples and updated metadata
     */
    @Override
    protected TimeSeries createMappedTimeSeries(SampleList mappedSamples, TimeSeries originalSeries) {
        // Create new time series with shifted samples, preserving all metadata except timestamps
        return new TimeSeries(
            mappedSamples,
            originalSeries.getLabels(),
            originalSeries.getMinTimestamp() + shiftMillis,
            originalSeries.getMaxTimestamp() + shiftMillis,
            originalSeries.getStep(),
            originalSeries.getAlias()
        );
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(SHIFT_AMOUNT_ARG, TimeValue.timeValueMillis(getShiftMillis()).getStringRep());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(shiftMillis);
    }

    /**
     * Create a TimeshiftStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new TimeshiftStage instance with the deserialized shift amount
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static TimeshiftStage readFrom(StreamInput in) throws IOException {
        long millis = in.readLong();
        return new TimeshiftStage(millis);
    }

    /**
     * Get the shift amount in milliseconds.
     *
     * @return the shift amount in milliseconds
     */
    public long getShiftMillis() {
        return shiftMillis;
    }

    /**
     * Create a TimeshiftStage from arguments map.
     *
     * @param args Map of argument names to values
     * @return TimeshiftStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static TimeshiftStage fromArgs(Map<String, Object> args) {
        if (args == null || !args.containsKey(SHIFT_AMOUNT_ARG)) {
            throw new IllegalArgumentException("Timeshift stage requires '" + SHIFT_AMOUNT_ARG + "' argument");
        }

        Object shiftAmountObj = args.get(SHIFT_AMOUNT_ARG);
        if (shiftAmountObj == null) {
            throw new IllegalArgumentException("Shift amount cannot be null");
        }

        long shiftMillis;
        if (shiftAmountObj instanceof String shiftAmountStr) {
            try {
                TimeValue shiftAmount = TimeValue.parseTimeValue(shiftAmountStr, null, "timeshift");
                shiftMillis = shiftAmount.getMillis();
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid shift amount format: " + shiftAmountObj, e);
            }
        } else if (shiftAmountObj instanceof Number shiftAmountNum) {
            // Convert to milliseconds
            shiftMillis = shiftAmountNum.longValue();
        } else {
            throw new IllegalArgumentException(
                "Shift amount must be a string or number, got: " + shiftAmountObj.getClass().getSimpleName()
            );
        }

        return new TimeshiftStage(shiftMillis);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(shiftMillis);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TimeshiftStage that = (TimeshiftStage) obj;
        return shiftMillis == that.shiftMillis;
    }
}
