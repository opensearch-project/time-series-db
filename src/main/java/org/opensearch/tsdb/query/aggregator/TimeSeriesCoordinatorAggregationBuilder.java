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
import org.opensearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.PipelineAggregator;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Aggregation builder for time series coordinator pipeline aggregations.
 *
 * <p>This builder creates {@link TimeSeriesCoordinatorAggregator} instances that handle
 * pipeline stages at the coordinator level. It supports complex pipeline configurations
 * including named references, macro definitions, and multi-input operations.</p>
 *
 * <h2>Key Features:</h2>
 * <ul>
 *   <li><strong>Named References:</strong> Support for named references to bucket paths,
 *       enabling complex multi-input operations</li>
 *   <li><strong>Macro Definitions:</strong> Define reusable pipeline segments that can
 *       be referenced by the main pipeline</li>
 *   <li><strong>Multi-input Operations:</strong> Support for binary operations that
 *       combine data from multiple aggregation results</li>
 *   <li><strong>Reference Resolution:</strong> Automatic resolution of named references
 *       to their corresponding bucket paths</li>
 * </ul>
 *
 * <h2>Usage Example:</h2>
 * <pre>{@code
 * Map<String, String> references = Map.of(
 *     "a", "agg1",
 *     "b", "agg2"
 * );
 * Map<String, MacroDefinition> macros = Map.of(
 *     "e", new MacroDefinition("a | asPercent(b)")
 * );
 * List<PipelineStage> stages = List.of(
 *     new AsPercentStage("e")
 * );
 *
 * TimeSeriesCoordinatorAggregationBuilder builder = new TimeSeriesCoordinatorAggregationBuilder(
 *     "coordinator_agg", stages, references, "a", macros
 * );
 * }</pre>
 */
public class TimeSeriesCoordinatorAggregationBuilder extends AbstractPipelineAggregationBuilder<TimeSeriesCoordinatorAggregationBuilder> {
    /** The name of the aggregation type */
    public static final String NAME = "coordinator_pipeline";

    private final List<PipelineStage> stages; // Main pipeline stages to execute
    private final Map<String, String> references; // Named references to bucket paths
    private final String inputReference; // Which reference to use as primary input (required)
    private final LinkedHashMap<String, TimeSeriesCoordinatorAggregator.MacroDefinition> macroDefinitions; // Macro definitions
                                                                                                           // (order-preserving)

    /**
     * Create a new coordinator pipeline aggregation builder with macro support.
     *
     * @param name The name of the aggregation
     * @param stages List of main pipeline stages to execute
     * @param macroDefinitions LinkedHashMap of macro definitions (order-preserving for dependency resolution)
     * @param references Map of reference names to buckets_path strings
     * @param inputReference Which reference to use as input (required)
     */
    public TimeSeriesCoordinatorAggregationBuilder(
        String name,
        List<PipelineStage> stages,
        LinkedHashMap<String, TimeSeriesCoordinatorAggregator.MacroDefinition> macroDefinitions,
        Map<String, String> references,
        String inputReference
    ) {
        super(name, NAME, new String[0]);
        this.stages = stages;
        this.references = references != null ? references : Map.of();
        this.inputReference = inputReference;
        // Preserve insertion order for macros - they are evaluated in dependency order
        this.macroDefinitions = macroDefinitions != null ? macroDefinitions : new LinkedHashMap<>();
    }

    /**
     * Read from a stream.
     *
     * @param in The stream input to read from
     * @throws IOException If an error occurs during reading
     */
    public TimeSeriesCoordinatorAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        // Read references into LinkedHashMap to preserve order
        Map<String, String> refs = in.readMap(StreamInput::readString, StreamInput::readString);
        this.references = new LinkedHashMap<>(refs);
        this.inputReference = in.readOptionalString();

        // Read macro definitions into LinkedHashMap to preserve order
        int macroCount = in.readVInt();
        this.macroDefinitions = new LinkedHashMap<>(macroCount);
        for (int i = 0; i < macroCount; i++) {
            String macroName = in.readString();
            TimeSeriesCoordinatorAggregator.MacroDefinition macroDef = TimeSeriesCoordinatorAggregator.MacroDefinition.readFrom(
                in,
                macroName
            );
            this.macroDefinitions.put(macroName, macroDef);
        }

        int stagesCount = in.readVInt();
        this.stages = new ArrayList<>(stagesCount);
        for (int i = 0; i < stagesCount; i++) {
            String stageName = in.readString();
            PipelineStage stage = PipelineStageFactory.readFrom(in, stageName);
            this.stages.add(stage);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeMap(references, StreamOutput::writeString, StreamOutput::writeString);
        out.writeOptionalString(inputReference);

        // Write macro definitions
        out.writeVInt(macroDefinitions.size());
        for (Map.Entry<String, TimeSeriesCoordinatorAggregator.MacroDefinition> entry : macroDefinitions.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeToWithoutName(out);
        }

        out.writeVInt(stages.size());
        for (PipelineStage stage : stages) {
            out.writeString(stage.getName());
            stage.writeTo(out);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Get the macro definitions.
     *
     * @return Map of macro definitions
     */
    public LinkedHashMap<String, TimeSeriesCoordinatorAggregator.MacroDefinition> getMacroDefinitions() {
        return macroDefinitions;
    }

    @Override
    public TimeSeriesCoordinatorAggregationBuilder setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("stages");
        for (PipelineStage stage : stages) {
            builder.startObject();
            builder.field("type", stage.getName());
            stage.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();

        builder.startObject("references");
        for (Map.Entry<String, String> entry : references.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();

        if (inputReference != null) {
            builder.field("inputReference", inputReference);
        }

        return builder;
    }

    /**
     * Collect validation errors for the builder configuration.
     *
     * @param errors List to collect validation error messages
     */
    private void collectValidationErrors(List<String> errors) {
        // Validate stages
        if (stages == null || stages.isEmpty()) {
            errors.add("stages must not be null or empty");
        } else {
            // Check for null stages
            for (int i = 0; i < stages.size(); i++) {
                if (stages.get(i) == null) {
                    errors.add("Stage at index " + i + " is null");
                }
            }
        }

        // Validate references
        if (references == null || references.isEmpty()) {
            errors.add("references must not be null or empty");
        }

        // Validate macro definitions (if present)
        // Note: macroDefinitions can be null or empty - it's valid to have a coordinator without macros
        if (macroDefinitions != null && !macroDefinitions.isEmpty()) {
            // Validate each macro definition has valid stages
            for (Map.Entry<String, TimeSeriesCoordinatorAggregator.MacroDefinition> entry : macroDefinitions.entrySet()) {
                TimeSeriesCoordinatorAggregator.MacroDefinition macro = entry.getValue();
                if (macro.getStages() == null || macro.getStages().isEmpty()) {
                    errors.add("Macro " + entry.getKey() + " must have non-empty stages");
                } else {
                    // Check for null stages in the macro
                    List<PipelineStage> macroStages = macro.getStages();
                    for (int i = 0; i < macroStages.size(); i++) {
                        if (macroStages.get(i) == null) {
                            errors.add("Macro " + entry.getKey() + " stage at index " + i + " is null");
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void validate(ValidationContext context) {
        // TODO: Investigate when OpenSearch framework calls this validate() method for pipeline aggregations.
        // Currently, validation is performed in createInternal() via validateAndThrow() to ensure it happens
        // before the aggregator is created. If the framework does call this method, we should enable it to
        // avoid duplicate validation logic.
        //
        // Keeping this empty for now to support the builder pattern where fields are set incrementally.
    }

    /**
     * Validate the builder configuration and throw an exception if invalid.
     * This method is called from the constructors to ensure validation occurs.
     *
     * @throws IllegalArgumentException if validation fails
     */
    public void validateAndThrow() {
        List<String> errors = new ArrayList<>();
        collectValidationErrors(errors);
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException("Validation failed: " + String.join(", ", errors));
        }
    }

    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metadata) {
        // Validate before creating the aggregator
        validateAndThrow();
        return new TimeSeriesCoordinatorAggregator(name, bucketsPaths, stages, macroDefinitions, references, inputReference, metadata);
    }

    @Override
    public String getType() {
        return NAME;
    }

    /**
     * Get the pipeline stages.
     *
     * @return The pipeline stages
     */
    public List<PipelineStage> getStages() {
        return stages;
    }

    /**
     * Get the references map.
     *
     * @return The references map
     */
    public Map<String, String> getReferences() {
        return references;
    }

    /**
     * Get the input reference.
     *
     * @return The input reference
     */
    public String getInputReference() {
        return inputReference;
    }

    /**
     * Parse from XContent.
     *
     * @param aggregationName The name of the aggregation
     * @param parser The XContent parser to read from
     * @return The parsed aggregation builder
     * @throws IOException If an error occurs during parsing
     */
    public static TimeSeriesCoordinatorAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        List<PipelineStage> stages = new ArrayList<>();
        Map<String, String> references = new LinkedHashMap<>();
        String inputReference = null;

        XContentParser.Token token;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
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
                                    } else {
                                        throw new IllegalArgumentException(
                                            "Unsupported token type for stage argument '"
                                                + fieldName
                                                + "': "
                                                + token
                                                + ". Aggregation: "
                                                + aggregationName
                                                + ", Stage type: "
                                                + (stageType != null ? stageType : "unknown")
                                                + ". Supported types: string, number, or array of strings."
                                        );
                                    }
                                }
                            }
                        }

                        // Create stage with arguments
                        if (stageType != null) {
                            stages.add(PipelineStageFactory.createWithArgs(stageType, stageArgs));
                        }
                    }
                }
            } else if (token == XContentParser.Token.START_OBJECT && "references".equals(currentFieldName)) {
                // Parse references
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String refName = parser.currentName();
                        token = parser.nextToken();
                        if (token == XContentParser.Token.VALUE_STRING) {
                            references.put(refName, parser.text());
                        }
                    }
                }
            } else if (token == XContentParser.Token.VALUE_STRING && "inputReference".equals(currentFieldName)) {
                inputReference = parser.text();
            } else if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                parser.skipChildren();
            }
        }

        return new TimeSeriesCoordinatorAggregationBuilder(aggregationName, stages, new LinkedHashMap<>(), references, inputReference);
    }

}
