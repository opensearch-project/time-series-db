/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.support.ValuesSourceRegistry;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregation builder for time series unfold pipeline aggregations.
 *
 * <p>This builder creates {@link TimeSeriesUnfoldAggregator} instances that unfold
 * time series data from chunks and apply unary pipeline stages. It supports
 * configuring the time range and step size for data processing.</p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Pipeline Stages:</strong> Supports a list of unary pipeline stages
 *       to be applied to the unfolded time series data</li>
 *   <li><strong>Time Range Configuration:</strong> Allows setting minimum and maximum
 *       timestamps for data filtering</li>
 *   <li><strong>Step Size Control:</strong> Configurable step size for timestamp
 *       alignment and data aggregation</li>
 *   <li><strong>Serialization Support:</strong> Full support for streaming
 *       serialization/deserialization</li>
 * </ul>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * TimeSeriesUnfoldAggregationBuilder builder = new TimeSeriesUnfoldAggregationBuilder("my_aggregation")
 *     .stages(List.of(new ScaleStage(2.0), new RoundStage(2)))
 *     .minTimestamp(1000000L)
 *     .maxTimestamp(2000000L)
 *     .step(1000L);
 * }</pre>
 */
public class TimeSeriesUnfoldAggregationBuilder extends AbstractAggregationBuilder<TimeSeriesUnfoldAggregationBuilder> {
    /** The name of the aggregation type */
    public static final String NAME = "time_series_unfold";

    private List<UnaryPipelineStage> stages;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final long step;

    /**
     * Create a time series unfold aggregation builder.
     *
     * @param name The name of the aggregation
     * @param stages The list of unary pipeline stages (null will be converted to empty list)
     * @param minTimestamp The minimum timestamp for filtering
     * @param maxTimestamp The maximum timestamp for filtering
     * @param step The step size for timestamp alignment
     */
    public TimeSeriesUnfoldAggregationBuilder(
        String name,
        List<UnaryPipelineStage> stages,
        long minTimestamp,
        long maxTimestamp,
        long step
    ) {
        super(name);
        this.stages = (stages == null || stages.isEmpty()) ? null : stages;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.step = step;
    }

    /**
     * Read from a stream.
     *
     * @param in The stream input to read from
     * @throws IOException If an error occurs during reading
     */
    public TimeSeriesUnfoldAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.minTimestamp = in.readLong();
        this.maxTimestamp = in.readLong();
        this.step = in.readLong();

        int stageCount = in.readInt();
        if (stageCount == 0) {
            this.stages = null;
        } else {
            this.stages = new ArrayList<>(stageCount);
            for (int i = 0; i < stageCount; i++) {
                this.stages.add((UnaryPipelineStage) PipelineStageFactory.readFrom(in));
            }
        }
    }

    /**
     * Protected copy constructor.
     *
     * @param clone The builder to clone from
     * @param factoriesBuilder The sub-aggregations builder
     * @param metadata The aggregation metadata
     */
    protected TimeSeriesUnfoldAggregationBuilder(
        TimeSeriesUnfoldAggregationBuilder clone,
        Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.stages = clone.stages;
        this.minTimestamp = clone.minTimestamp;
        this.maxTimestamp = clone.maxTimestamp;
        this.step = clone.step;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeLong(minTimestamp);
        out.writeLong(maxTimestamp);
        out.writeLong(step);

        if (stages == null) {
            out.writeInt(0);
        } else {
            out.writeInt(stages.size());
            for (PipelineStage stage : stages) {
                // Write stage name first, then stage data
                out.writeString(stage.getName());
                stage.writeTo(out);
            }
        }
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("min_timestamp", minTimestamp);
        builder.field("max_timestamp", maxTimestamp);
        builder.field("step", step);
        if (stages != null) {
            builder.startArray("stages");
            for (PipelineStage stage : stages) {
                builder.startObject();
                builder.field("type", stage.getName());
                stage.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    /**
     * Parse from XContent.
     *
     * @param aggregationName The name of the aggregation
     * @param parser The XContent parser to read from
     * @return The parsed aggregation builder
     * @throws IOException If an error occurs during parsing
     */
    public static TimeSeriesUnfoldAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        Long minTimestamp = null;
        Long maxTimestamp = null;
        Long step = null;
        List<UnaryPipelineStage> stages = null;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("min_timestamp".equals(currentFieldName)) {
                    minTimestamp = parser.longValue();
                } else if ("max_timestamp".equals(currentFieldName)) {
                    maxTimestamp = parser.longValue();
                } else if ("step".equals(currentFieldName)) {
                    step = parser.longValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == XContentParser.Token.START_ARRAY && "stages".equals(currentFieldName)) {
                // Parse stages array
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token == XContentParser.Token.START_OBJECT) {
                        // Parse stage object with type and arguments
                        String stageType = null;
                        Map<String, Object> stageArgs = new HashMap<>();

                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                String fieldName = parser.currentName();
                                token = parser.nextToken();
                                if ("type".equals(fieldName)) {
                                    stageType = parser.text();
                                } else {
                                    // Parse stage-specific arguments
                                    if (token == XContentParser.Token.VALUE_STRING) {
                                        stageArgs.put(fieldName, parser.text());
                                    } else if (token == XContentParser.Token.VALUE_NUMBER) {
                                        if (parser.numberType() == XContentParser.NumberType.INT) {
                                            stageArgs.put(fieldName, parser.intValue());
                                        } else {
                                            stageArgs.put(fieldName, parser.doubleValue());
                                        }
                                    } else if (token == XContentParser.Token.START_ARRAY) {
                                        List<String> arrayValues = new ArrayList<>();
                                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                            if (token == XContentParser.Token.VALUE_STRING) {
                                                arrayValues.add(parser.text());
                                            }
                                        }
                                        stageArgs.put(fieldName, arrayValues);
                                    }
                                }
                            }
                        }

                        // Create stage with arguments
                        if (stageType != null) {
                            PipelineStage stage = PipelineStageFactory.createWithArgs(stageType, stageArgs);
                            if (stage instanceof UnaryPipelineStage) {
                                if (stages == null) {
                                    stages = new ArrayList<>();
                                }
                                stages.add((UnaryPipelineStage) stage);
                            } else {
                                throw new IllegalArgumentException("Stage type '" + stageType + "' is not a UnaryPipelineStage");
                            }
                        }
                    }
                }
            } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            }
        }

        // Validate required parameters
        if (minTimestamp == null) {
            throw new IllegalArgumentException("Required parameter 'min_timestamp' is missing for aggregation '" + aggregationName + "'");
        }
        if (maxTimestamp == null) {
            throw new IllegalArgumentException("Required parameter 'max_timestamp' is missing for aggregation '" + aggregationName + "'");
        }
        if (step == null) {
            throw new IllegalArgumentException("Required parameter 'step' is missing for aggregation '" + aggregationName + "'");
        }

        return new TimeSeriesUnfoldAggregationBuilder(aggregationName, stages, minTimestamp, maxTimestamp, step);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new TimeSeriesUnfoldAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent, Builder subFactoriesBuilder)
        throws IOException {
        return new TimeSeriesUnfoldAggregatorFactory(
            name,
            queryShardContext,
            parent,
            subFactoriesBuilder,
            metadata,
            stages,
            minTimestamp,
            maxTimestamp,
            step
        );
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }

        TimeSeriesUnfoldAggregationBuilder that = (TimeSeriesUnfoldAggregationBuilder) obj;
        if (minTimestamp != that.minTimestamp) {
            return false;
        }
        if (maxTimestamp != that.maxTimestamp) {
            return false;
        }
        if (step != that.step) {
            return false;
        }
        return Objects.equals(stages, that.stages);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (stages != null) {
            for (PipelineStage stage : stages) {
                result = 31 * result + stage.hashCode();
            }
        }
        result = 31 * result + Long.hashCode(minTimestamp);
        result = 31 * result + Long.hashCode(maxTimestamp);
        result = 31 * result + Long.hashCode(step);
        return result;
    }

    /**
     * Set the pipeline stages.
     *
     * @param stages The list of unary pipeline stages (can be null or empty, both treated as null)
     */
    public void setStages(List<UnaryPipelineStage> stages) {
        this.stages = (stages == null || stages.isEmpty()) ? null : stages;
    }

    /**
     * Get the configured stages.
     *
     * @return The list of unary pipeline stages
     */
    public List<UnaryPipelineStage> getStages() {
        return stages;
    }

    /**
     * Get the minimum timestamp.
     *
     * @return The minimum timestamp
     */
    public long getMinTimestamp() {
        return minTimestamp;
    }

    /**
     * Get the maximum timestamp.
     *
     * @return The maximum timestamp
     */
    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    /**
     * Get the step size.
     *
     * @return The step size
     */
    public long getStep() {
        return step;
    }

    /**
     * Register aggregators with the values source registry.
     *
     * @param builder The values source registry builder
     */
    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        // Register usage for the aggregation since we don't use ValuesSourceRegistry
        // but still need to be registered for usage tracking
        builder.registerUsage(NAME);
    }
}
