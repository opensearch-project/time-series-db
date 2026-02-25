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
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.lang.m3.common.WindowAggregationType;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Binary pipeline stage that computes the burn rate for SLO monitoring.
 *
 * <p>Burn rate measures how fast an error budget is being consumed relative to the
 * allowed error rate defined by an SLO. A burn rate of 1 means the error budget
 * will be consumed exactly over the reporting period; values greater than 1 indicate
 * faster consumption.</p>
 *
 * <p>Mathematically: {@code burn_rate = error_rate / error_budget} where
 * {@code error_rate = moving_sum(unexpected) / moving_sum(total)} and
 * {@code error_budget = 1 - slo/100}.</p>
 *
 * <p>The left operand is the "unexpected" (error) series, and the right operand is
 * the "total" (traffic) series. Both are subjected to a moving sum over the
 * specified interval before the burn rate is computed.</p>
 *
 * <p>Equivalent M3QL semantics:
 * {@code unexpected | moving <interval> sum | asPercent (total | moving <interval> sum) | scale 1/(1-slo) | transformNull}
 * Note: this implementation computes the mathematical burn rate directly rather than
 * chaining asPercent + scale, so the result matches the standard SRE definition
 * (burn_rate = 1 when error rate equals error budget).</p>
 */
@PipelineStageAnnotation(name = AsBurnRateStage.NAME)
public class AsBurnRateStage extends AbstractBinaryProjectionStage {
    public static final String NAME = "burn_rate";
    private static final String INTERVAL_ARG = "interval";
    private static final String SLO_ARG = "slo";

    private final String rightOperandReferenceName;
    private final long interval;
    private final double slo;
    private final MovingStage movingSum;
    private final TransformNullStage transformNull;

    /**
     * Constructs a new AsBurnRateStage with the specified parameters.
     *
     * @param rightOperandReferenceName the reference for the right (total) operand
     * @param interval moving sum window in milliseconds; must be positive
     * @param slo SLO target as a percentage (e.g. 99.9 for 99.9%)
     */
    public AsBurnRateStage(String rightOperandReferenceName, long interval, double slo) {
        if (interval <= 0) {
            throw new IllegalArgumentException("Interval must be positive, got: " + interval);
        }
        if (slo <= 0 || slo >= 100) {
            throw new IllegalArgumentException("SLO must be between 0 and 100 (exclusive), got: " + slo);
        }
        this.rightOperandReferenceName = rightOperandReferenceName;
        this.interval = interval;
        this.slo = slo;
        this.movingSum = new MovingStage(interval, WindowAggregationType.SUM);
        this.transformNull = new TransformNullStage();
    }

    /**
     * Applies moving sum to both operands, computes burn rate via the parent's
     * tag-based matching, then replaces NaN with 0 via transformNull.
     */
    @Override
    public List<TimeSeries> process(List<TimeSeries> left, List<TimeSeries> right) {
        if (left == null) {
            throw new NullPointerException(getName() + " stage received null left input");
        }
        if (right == null) {
            throw new NullPointerException(getName() + " stage received null right input");
        }

        List<TimeSeries> leftMoving = movingSum.process(left);
        List<TimeSeries> rightMoving = movingSum.process(right);

        List<TimeSeries> result = super.process(leftMoving, rightMoving);

        return transformNull.process(result);
    }

    /**
     * Process sample values to calculate burn rate.
     *
     * @param leftValue The left value (error count, may be null)
     * @param rightValue The right value (total count, may be null)
     * @return burn rate value, or NaN if right value is zero/negative/null or left value is null
     */
    @Override
    protected Double processSampleValues(Double leftValue, Double rightValue) {
        if (rightValue == null || rightValue <= 0.0) {
            return Double.NaN;
        }
        if (leftValue == null) {
            return Double.NaN;
        }
        double errorRate = leftValue / rightValue;
        double errorBudget = 1.0 - slo / 100.0;
        return errorRate / errorBudget;
    }

    @Override
    protected Labels transformLabels(Labels originalLabels) {
        return originalLabels;
    }

    @Override
    protected TimeSeries mergeMatchingSeries(List<TimeSeries> rightTimeSeries) {
        // AsBurnRate expects only one time series for matched group
        if (rightTimeSeries.isEmpty()) {
            return null;
        } else if (rightTimeSeries.size() == 1) {
            return rightTimeSeries.get(0);
        } else {
            throw new IllegalArgumentException("bucket for burnRate must have exactly one total series, got " + rightTimeSeries.size());
        }
    }

    @Override
    protected boolean hasKeepNansOption() {
        return false;
    }

    @Override
    protected NormalizationStrategy getNormalizationStrategy() {
        return NormalizationStrategy.BATCH;
    }

    @Override
    protected boolean shouldExtractCommonTagKeys() {
        return true;
    }

    @Override
    protected List<String> getLabelKeys() {
        return null;
    }

    @Override
    public String getRightOpReferenceName() {
        return rightOperandReferenceName;
    }

    @Override
    public String getName() {
        return NAME;
    }

    public long getInterval() {
        return interval;
    }

    public double getSlo() {
        return slo;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(RIGHT_OP_REFERENCE_PARAM_KEY, rightOperandReferenceName);
        builder.field(INTERVAL_ARG, interval);
        builder.field(SLO_ARG, slo);
    }

    /**
     * Write stage-specific data to the output stream for serialization.
     */
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(rightOperandReferenceName);
        out.writeLong(interval);
        out.writeDouble(slo);
    }

    /**
     * Create an AsBurnRateStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new AsBurnRateStage instance
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public static AsBurnRateStage readFrom(StreamInput in) throws IOException {
        String referenceName = in.readString();
        long interval = in.readLong();
        double slo = in.readDouble();
        return new AsBurnRateStage(referenceName, interval, slo);
    }

    /**
     * Creates a new instance of AsBurnRateStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct an AsBurnRateStage instance.
     *             The map must include keys for right operand reference, interval, and slo.
     * @return a new AsBurnRateStage instance initialized with the provided arguments.
     */
    public static AsBurnRateStage fromArgs(Map<String, Object> args) {
        if (args == null || !args.containsKey(RIGHT_OP_REFERENCE_PARAM_KEY)) {
            throw new IllegalArgumentException(NAME + " stage requires " + RIGHT_OP_REFERENCE_PARAM_KEY + " argument");
        }
        if (!args.containsKey(INTERVAL_ARG)) {
            throw new IllegalArgumentException(NAME + " stage requires " + INTERVAL_ARG + " argument");
        }
        if (!args.containsKey(SLO_ARG)) {
            throw new IllegalArgumentException(NAME + " stage requires " + SLO_ARG + " argument");
        }
        String rightOpReference = (String) args.get(RIGHT_OP_REFERENCE_PARAM_KEY);
        long interval = ((Number) args.get(INTERVAL_ARG)).longValue();
        double slo = ((Number) args.get(SLO_ARG)).doubleValue();
        return new AsBurnRateStage(rightOpReference, interval, slo);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (!super.equals(obj)) return false;
        AsBurnRateStage that = (AsBurnRateStage) obj;
        return interval == that.interval
            && Double.compare(that.slo, slo) == 0
            && Objects.equals(rightOperandReferenceName, that.rightOperandReferenceName);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + Objects.hash(rightOperandReferenceName, interval, slo);
        return result;
    }
}
