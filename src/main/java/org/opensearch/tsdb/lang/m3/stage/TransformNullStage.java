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
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Pipeline stage that fills missing data points with a default value.
 * Creates a dense time series using the metadata from the input TimeSeries.
 * This implements the transformNull function from M3QL.
 */
@PipelineStageAnnotation(name = "transform_null")
public class TransformNullStage implements UnaryPipelineStage {
    /** The name of this pipeline stage. */
    public static final String NAME = "transform_null";
    private final double fillValue;

    /**
     * Constructor that reads time parameters from TimeSeries metadata.
     *
     * @param fillValue The value to use for filling missing/null data points
     */
    public TransformNullStage(double fillValue) {
        this.fillValue = fillValue;
    }

    /**
     * Constructor with default fill value of 0.
     */
    public TransformNullStage() {
        this(0.0);
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }
        List<TimeSeries> result = new ArrayList<>();

        for (TimeSeries series : input) {
            long seriesMinTimestamp = series.getMinTimestamp();
            long seriesMaxTimestamp = series.getMaxTimestamp();
            long seriesStep = series.getStep();

            // Calculate size and pre-create dense samples list
            int arraySize = (int) ((seriesMaxTimestamp - seriesMinTimestamp) / seriesStep) + 1;
            List<Sample> denseSamples = new ArrayList<>(arraySize);

            // Build dense samples in one pass using a pointer into existing samples
            SampleList existingSamples = series.getSamples();
            int sampleIndex = 0;
            long timestamp = seriesMinTimestamp;

            for (int i = 0; i < arraySize; i++) {
                // Check if current existing sample matches this timestamp
                if (sampleIndex < existingSamples.size() && existingSamples.getTimestamp(sampleIndex) == timestamp) {
                    double value = existingSamples.getValue(sampleIndex);
                    // Treat NaN as null/missing
                    if (Double.isNaN(value)) {
                        denseSamples.add(new FloatSample(timestamp, fillValue));
                    } else {
                        denseSamples.add(new FloatSample(existingSamples.getTimestamp(sampleIndex), existingSamples.getValue(sampleIndex)));
                    }
                    sampleIndex++;
                } else {
                    // Missing timestamp, use fill value
                    denseSamples.add(new FloatSample(timestamp, fillValue));
                }
                timestamp += seriesStep;
            }

            result.add(
                new TimeSeries(
                    denseSamples,
                    series.getLabels(),
                    series.getMinTimestamp(),
                    series.getMaxTimestamp(),
                    series.getStep(),
                    series.getAlias()
                )
            );
        }

        return result;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("fill_value", fillValue);
    }

    /**
     * Get the display name including the fill value (e.g., "transformNull 0", "transformNull 1").
     *
     * @return The display name string
     */
    public String getDisplayName() {
        return "transformNull " + (fillValue == (long) fillValue ? String.valueOf((long) fillValue) : String.valueOf(fillValue));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(fillValue);
        // Note: minTimestamp, maxTimestamp, and step are now read from TimeSeries metadata
    }

    /**
     * Create a TransformNullStage instance from the input stream for deserialization.
     *
     * @param in The stream input to read from
     * @return A new TransformNullStage instance
     * @throws IOException if an I/O error occurs during deserialization
     */
    public static TransformNullStage readFrom(StreamInput in) throws IOException {
        double fillValue = in.readDouble();
        return new TransformNullStage(fillValue);
    }

    /**
     * Create a TransformNullStage from arguments map.
     *
     * @param args Map of argument names to values. Expects "fill_value" key with a numeric value.
     * @return TransformNullStage instance
     * @throws IllegalArgumentException if arguments are invalid
     */
    public static TransformNullStage fromArgs(Map<String, Object> args) {
        double fillValue = 0.0;
        if (args.containsKey("fill_value")) {
            fillValue = ((Number) args.get("fill_value")).doubleValue();
        }
        return new TransformNullStage(fillValue);
    }

    @Override
    public boolean supportConcurrentSegmentSearch() {
        return false; // TransformNull requires complete time series, not suitable for CSS
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TransformNullStage that = (TransformNullStage) obj;
        return Double.compare(that.fillValue, fillValue) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(fillValue);
    }

    /**
     * Estimate temporary memory overhead for transform null operations.
     * TransformNullStage creates new TimeSeries with new sample lists (reusing labels).
     *
     * <p>Delegates to {@link SampleList#ramBytesUsed()} for sample estimation, ensuring
     * the calculation stays accurate as underlying implementations change.</p>
     *
     * @param input The input time series
     * @return Estimated temporary memory overhead in bytes
     */
    @Override
    public long estimateMemoryOverhead(List<TimeSeries> input) {
        return UnaryPipelineStage.estimateSampleReuseOverhead(input);
    }
}
