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
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

/**
 * FallbackSeriesUnaryStage returns its input series if it is not empty,
 * otherwise it returns a constant series with the specified value.
 *
 * <p>This is the unary variant of fallbackSeries that takes a constant value as replacement.
 * Example: {@code fetch a | fallbackSeries 1.0}
 *
 * <p>This stage is useful in auto-generated alerts where the input series may not always exist.
 * When the input is empty, it creates a constant series using the query metadata (min/max timestamp, step).
 */
@PipelineStageAnnotation(name = FallbackSeriesUnaryStage.NAME)
public class FallbackSeriesUnaryStage implements UnaryPipelineStage {
    /** The name of this pipeline stage. */
    public static final String NAME = "fallback_series_unary";

    private final double fallbackValue;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long step;

    // ThreadLocal to ensure thread-safety for DecimalFormat in query hotpath
    private static final ThreadLocal<DecimalFormat> FALLBACK_ALIAS_FORMAT = ThreadLocal.withInitial(
        () -> new DecimalFormat("0.000", DecimalFormatSymbols.getInstance(Locale.ROOT))
    );

    /**
     * Constructor for FallbackSeriesUnaryStage.
     *
     * @param fallbackValue the constant value to use as fallback
     * @param minTimestamp the minimum timestamp for the constant series (inclusive)
     * @param maxTimestamp the maximum timestamp for the constant series (exclusive)
     * @param step the step size for the constant series
     */
    public FallbackSeriesUnaryStage(double fallbackValue, long minTimestamp, long maxTimestamp, long step) {
        this.fallbackValue = fallbackValue;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
    }

    /**
     * Constructor for FallbackSeriesUnaryStage with stream input.
     *
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public FallbackSeriesUnaryStage(StreamInput in) throws IOException {
        this.fallbackValue = in.readDouble();
        this.minTimestamp = in.readLong();
        this.maxTimestamp = in.readLong();
        this.step = in.readLong();
    }

    /**
     * Process the input time series. If empty, return a constant series with fallback value.
     *
     * @param input the input time series list
     * @return input series if non-empty, otherwise a constant series with fallback value
     */
    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        // If input is empty, create a constant series
        if (input.isEmpty()) {
            return createConstantSeries();
        }
        return input;
    }

    /**
     * Create a constant series with the fallback value.
     * Samples are generated at regular intervals from minTimestamp (inclusive) to maxTimestamp (exclusive).
     *
     * @return a list containing a single constant time series
     */
    private List<TimeSeries> createConstantSeries() {
        // Create labels for the constant series
        ByteLabels labels = ByteLabels.emptyLabels();

        // Calculate the actual last sample timestamp (inclusive) for TimeSeries
        // Since maxTimestamp is exclusive, the last sample is the largest minTimestamp + N*step < maxTimestamp
        long lastSampleTimestamp = TimeSeries.calculateAlignedMaxTimestamp(minTimestamp, maxTimestamp, step);

        // Format alias as 3 decimal places (e.g., 0 -> "0.000", 0.5135 -> "0.514")
        String alias = FALLBACK_ALIAS_FORMAT.get().format(fallbackValue);
        return List.of(
            new TimeSeries(
                new FloatSampleList.ConstantList(minTimestamp, lastSampleTimestamp, step, fallbackValue),
                labels,
                minTimestamp,
                lastSampleTimestamp,
                step,
                alias
            )
        );
    }

    /**
     * Get the name of this pipeline stage.
     *
     * @return the stage name "fallbackSeriesUnary"
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Serialize this stage to XContent format.
     *
     * @param builder the XContent builder to write to
     * @param params the XContent parameters
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("fallbackValue", fallbackValue);
        builder.field("minTimestamp", minTimestamp);
        builder.field("maxTimestamp", maxTimestamp);
        builder.field("step", step);
    }

    /**
     * Write this stage to a stream for serialization.
     *
     * @param out the stream output to write to
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(fallbackValue);
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
        out.writeLong(step);
    }

    /**
     * Creates a FallbackSeriesUnaryStage from DSL arguments.
     *
     * @param args the arguments map containing fallbackValue, minTimestamp, maxTimestamp, step
     * @return a new FallbackSeriesUnaryStage instance
     */
    public static FallbackSeriesUnaryStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("FallbackSeriesUnaryStage requires arguments");
        }

        // Extract arguments, handling different Number types (Integer, Long, Double)
        Object fallbackValueObj = args.get("fallbackValue");
        Object minTimestampObj = args.get("minTimestamp");
        Object maxTimestampObj = args.get("maxTimestamp");
        Object stepObj = args.get("step");

        Double fallbackValue = fallbackValueObj instanceof Number ? ((Number) fallbackValueObj).doubleValue() : null;
        Long minTimestamp = minTimestampObj instanceof Number ? ((Number) minTimestampObj).longValue() : null;
        Long maxTimestamp = maxTimestampObj instanceof Number ? ((Number) maxTimestampObj).longValue() : null;
        Long step = stepObj instanceof Number ? ((Number) stepObj).longValue() : null;

        if (fallbackValue == null || minTimestamp == null || maxTimestamp == null || step == null) {
            throw new IllegalArgumentException(
                "FallbackSeriesUnaryStage requires fallbackValue, minTimestamp, maxTimestamp, and step arguments"
            );
        }

        return new FallbackSeriesUnaryStage(fallbackValue, minTimestamp, maxTimestamp, step);
    }

    /**
     * Reads a FallbackSeriesUnaryStage from a stream.
     *
     * @param in the stream input
     * @return a new FallbackSeriesUnaryStage instance
     * @throws IOException if an I/O error occurs
     */
    public static FallbackSeriesUnaryStage readFrom(StreamInput in) throws IOException {
        return new FallbackSeriesUnaryStage(in);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FallbackSeriesUnaryStage that = (FallbackSeriesUnaryStage) obj;
        return Double.compare(that.fallbackValue, fallbackValue) == 0
            && minTimestamp == that.minTimestamp
            && maxTimestamp == that.maxTimestamp
            && step == that.step;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(Double.doubleToLongBits(fallbackValue)) ^ Long.hashCode(minTimestamp) ^ Long.hashCode(maxTimestamp) ^ Long
            .hashCode(step);
    }

    /**
     * This stage must be executed only at the coordinator level.
     * It cannot be executed in the UnfoldAggregator because it needs to see the complete
     * aggregated results from all shards to determine if the input is truly empty
     * before applying the fallback logic.
     *
     * @return true to indicate this is a coordinator-only stage
     */
    @Override
    public boolean isCoordinatorOnly() {
        return true;
    }
}
