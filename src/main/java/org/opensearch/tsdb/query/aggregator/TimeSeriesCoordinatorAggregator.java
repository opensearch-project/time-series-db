/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.aggregator;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.opensearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.opensearch.tsdb.query.stage.BinaryPipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStage;
import org.opensearch.tsdb.query.stage.PipelineStageExecutor;
import org.opensearch.tsdb.query.stage.PipelineStageFactory;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;
import org.opensearch.tsdb.query.utils.AggregationConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Coordinator pipeline aggregator that handles a list of pipeline stages at the coordinator.
 *
 * This aggregator supports:
 * 1. Single bucket path: Extract one time series input, apply all stages sequentially
 * 2. Multiple bucket paths: Extract multiple time series inputs, support binary operations
 *    with named references and complex reference resolution
 * 3. Macro definitions: Define named macros that can be referenced by the main pipeline
 *    Example: macros: {e = a | asPercent(b)}, main pipeline: c | asPercent(e)
 *
 * Following M3QL semantics:
 * - Macros are definitions that can be referenced
 * - The main pipeline (stages) is what gets executed
 *
 * This aggregator references other pipeline aggregations through buckets_path and applies
 * the specified stages to their results at coordination node level.
 */
public class TimeSeriesCoordinatorAggregator extends SiblingPipelineAggregator {

    private final List<PipelineStage> stages; // Main pipeline stages to execute
    private final Map<String, String> references; // Named references to bucket paths
    private final String inputReference; // Which reference to use as primary input
    private final LinkedHashMap<String, MacroDefinition> macroDefinitions; // Macro definitions (order-preserving for evaluation order)

    /**
     * Represents a macro definition with a name, pipeline stages, and input reference.
     * Macros use the same reference mapping as the main pipeline.
     */
    public static class MacroDefinition {
        private final String name;
        private final List<PipelineStage> stages;
        private final String inputReference; // Which reference to use as input for this macro

        /**
         * Create a macro definition.
         *
         * @param name The name of the macro
         * @param stages The pipeline stages for this macro
         * @param inputReference The input reference name for this macro
         */
        public MacroDefinition(String name, List<PipelineStage> stages, String inputReference) {
            this.name = name;
            this.stages = stages;
            this.inputReference = inputReference;
        }

        /**
         * Get the name of the macro.
         *
         * @return The macro name
         */
        public String getName() {
            return name;
        }

        /**
         * Get the pipeline stages for this macro.
         *
         * @return The list of pipeline stages
         */
        public List<PipelineStage> getStages() {
            return stages;
        }

        /**
         * Get the input reference for this macro.
         *
         * @return The input reference name
         */
        public String getInputReference() {
            return inputReference;
        }

        /**
         * Read macro definition from stream.
         *
         * @param in The stream input to read from
         * @return The parsed macro definition
         * @throws IOException If an error occurs during reading
         */
        public static MacroDefinition readFrom(StreamInput in) throws IOException {
            String name = in.readString();
            return readFrom(in, name);
        }

        /**
         * Read macro definition from stream with name already read.
         *
         * @param in The stream input to read from
         * @param name The name of the macro (already read from stream)
         * @return The parsed macro definition
         * @throws IOException If an error occurs during reading
         */
        public static MacroDefinition readFrom(StreamInput in, String name) throws IOException {
            int stagesCount = in.readVInt();
            List<PipelineStage> stages = new ArrayList<>(stagesCount);
            for (int i = 0; i < stagesCount; i++) {
                String stageName = in.readString();
                stages.add(PipelineStageFactory.readFrom(in, stageName));
            }
            String inputReference = in.readOptionalString();
            return new MacroDefinition(name, stages, inputReference);
        }

        /**
         * Write macro definition to stream.
         *
         * @param out The stream output to write to
         * @throws IOException If an error occurs during writing
         */
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            writeToWithoutName(out);
        }

        /**
         * Write macro definition to stream without the name (for use in builder).
         *
         * @param out The stream output to write to
         * @throws IOException If an error occurs during writing
         */
        public void writeToWithoutName(StreamOutput out) throws IOException {
            out.writeVInt(stages.size());
            for (PipelineStage stage : stages) {
                out.writeString(stage.getName());
                stage.writeTo(out);
            }
            out.writeOptionalString(inputReference);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MacroDefinition that = (MacroDefinition) o;
            return Objects.equals(name, that.name)
                && Objects.equals(inputReference, that.inputReference)
                && stagesEqual(stages, that.stages);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, inputReference, stagesHashCode(stages));
        }

        /**
         * Compare two stage lists for equality by comparing their serialized form.
         * This ensures that stages with the same name but different parameters are not considered equal.
         */
        private boolean stagesEqual(List<PipelineStage> stages1, List<PipelineStage> stages2) {
            if (stages1.size() != stages2.size()) return false;

            // Serialize both lists and compare the bytes
            BytesStreamOutput out1 = new BytesStreamOutput();
            BytesStreamOutput out2 = new BytesStreamOutput();

            try {
                for (PipelineStage stage : stages1) {
                    out1.writeString(stage.getName());
                    stage.writeTo(out1);
                }

                for (PipelineStage stage : stages2) {
                    out2.writeString(stage.getName());
                    stage.writeTo(out2);
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize pipeline stages for comparison", e);
            }

            return out1.bytes().equals(out2.bytes());
        }

        /**
         * Compute hash code for stage list based on stage names.
         */
        private int stagesHashCode(List<PipelineStage> stages) {
            if (stages == null) return 0;
            int result = 1;
            for (PipelineStage stage : stages) {
                result = 31 * result + (stage == null ? 0 : stage.getName().hashCode());
            }
            return result;
        }
    }

    /**
     * Constructor with macro support.
     * Macros are definitions that can be referenced by the main pipeline stages.
     *
     * @param name The name of the aggregator
     * @param bucketsPaths The buckets paths for pipeline aggregation
     * @param stages The main pipeline stages to execute
     * @param macroDefinitions Map of macro definitions (converted to LinkedHashMap to preserve evaluation order)
     * @param references Map of reference names to bucket paths
     * @param inputReference The input reference to use (required)
     * @param metadata The aggregation metadata
     */
    public TimeSeriesCoordinatorAggregator(
        String name,
        String[] bucketsPaths,
        List<PipelineStage> stages,
        Map<String, MacroDefinition> macroDefinitions,
        Map<String, String> references,
        String inputReference,
        Map<String, Object> metadata
    ) {
        super(name, bucketsPaths, metadata);
        this.stages = stages;
        this.references = references != null ? references : Map.of();
        this.inputReference = inputReference;
        // Preserve insertion order for macros - they are evaluated in definition order
        this.macroDefinitions = macroDefinitions != null
            ? (macroDefinitions instanceof LinkedHashMap
                ? (LinkedHashMap<String, MacroDefinition>) macroDefinitions
                : new LinkedHashMap<>(macroDefinitions))
            : new LinkedHashMap<>();
        // Validation is now performed in TimeSeriesCoordinatorAggregationBuilder.createInternal()
        // before this constructor is called
    }

    @Override
    public InternalAggregation doReduce(Aggregations aggregations, ReduceContext context) {
        // Execute the main pipeline stages, with macro support if macros are defined
        List<TimeSeries> result = processMainPipeline(aggregations);
        return new InternalTimeSeries(name(), result, metadata());
    }

    /**
     * Process the main pipeline stages, with support for macro references.
     * Macros are definitions that can be referenced by stages in the main pipeline.
     */
    private List<TimeSeries> processMainPipeline(Aggregations aggregations) {
        // Extract referenced pipeline results using buckets_path
        Map<String, List<TimeSeries>> availableReferences = extractReferenceResults(aggregations);

        // If we have macro definitions, evaluate them and append results to available references
        if (!macroDefinitions.isEmpty()) {
            evaluateAndAppendMacros(availableReferences);
        }

        // Execute the main pipeline stages
        List<TimeSeries> resultTimeSeries = null;
        if (!availableReferences.isEmpty()) {
            // inputReference must be explicitly specified and must exist
            if (inputReference == null) {
                throw new IllegalArgumentException("inputReference must be explicitly specified for coordinator aggregation");
            }
            if (!availableReferences.containsKey(inputReference)) {
                throw new IllegalArgumentException(
                    "inputReference '" + inputReference + "' not found in available references: " + availableReferences.keySet()
                );
            }
            resultTimeSeries = availableReferences.get(inputReference);

            // Apply main pipeline stages sequentially
            resultTimeSeries = executeStages(stages, resultTimeSeries, availableReferences);
        }
        return resultTimeSeries != null ? resultTimeSeries : List.of();
    }

    /**
     * Evaluate macro definitions and append their results to the provided reference map.
     * Macros can reference other macros, so they are evaluated in dependency order.
     * Modifies the availableReferences map in place.
     */
    private void evaluateAndAppendMacros(Map<String, List<TimeSeries>> availableReferences) {
        // Evaluate macros in definition order as provided by the parser/planner
        // TODO: Support topological sort for automatic dependency resolution in the future
        for (MacroDefinition macro : macroDefinitions.values()) {
            // Execute the macro stages using all references available so far
            List<TimeSeries> macroInput = getMacroInput(availableReferences, macro);
            List<TimeSeries> macroOutput = executeStages(macro.getStages(), macroInput, availableReferences);

            // Add the macro output to available references for subsequent macros
            availableReferences.put(macro.getName(), macroOutput);
        }
    }

    /**
     * Extract reference results from aggregations using bucket paths.
     * Only extracts results for actual aggregations, not macro references.
     */
    private Map<String, List<TimeSeries>> extractReferenceResults(Aggregations aggregations) {
        Map<String, List<TimeSeries>> referenceResults = new LinkedHashMap<>(references.size());
        for (Map.Entry<String, String> entry : references.entrySet()) {
            String refName = entry.getKey();
            String bucketsPath = entry.getValue();

            // Skip macro result references (like "e", "f") - they will be computed later
            // But allow macro input references (like "a", "b", "c") - they have actual aggregation paths
            // The references map contains input references (a->unfold_a, b->unfold_b, c->unfold_c)
            // The macroDefinitions map contains macro names (e, f)
            // So we should extract results for all references that have actual aggregation paths

            // Extract time series from the specific aggregation path
            // bucketsPath is the actual aggregation name (e.g., "unfold_a", "unfold_b", "unfold_c")
            String[] pathElements = bucketsPath.split(AggregationConstants.BUCKETS_PATH_SEPARATOR);

            // Find the root aggregation by name
            // aggregations.get(name) internally builds and caches a map for O(1) lookups
            Aggregation currentAgg = aggregations.get(pathElements[0]);
            if (currentAgg == null) {
                throw new IllegalArgumentException("Referenced aggregation not found: " + pathElements[0]);
            }

            // Navigate through the path elements
            for (int i = 1; i < pathElements.length; i++) {
                String pathElement = pathElements[i];
                if (currentAgg instanceof InternalSingleBucketAggregation aggregation) {
                    Aggregation result = aggregation.getAggregations().get(pathElement);
                    if (result == null) {
                        throw new IllegalArgumentException("Path element not found: " + pathElement + " in " + aggregation.getName());
                    }
                    currentAgg = result;
                } else {
                    throw new IllegalArgumentException(
                        "Cannot navigate through aggregation: "
                            + currentAgg.getName()
                            + " of type "
                            + currentAgg.getClass().getSimpleName()
                            + ". Only single-bucket aggregations are supported in paths."
                    );
                }
            }

            // Extract time series from the final pipeline aggregation
            if (currentAgg instanceof TimeSeriesProvider provider) {
                List<TimeSeries> timeSeries = provider.getTimeSeries();
                referenceResults.put(refName, timeSeries);
            } else {
                throw new IllegalArgumentException(
                    "Referenced aggregation '"
                        + currentAgg.getName()
                        + "' does not implement TimeSeriesProvider, got: "
                        + currentAgg.getClass().getSimpleName()
                );
            }
        }
        return referenceResults;
    }

    /**
     * Get the input for a macro by resolving its references.
     * For a macro like "e = a | asPercent(b)", the input should be "a".
     * The macro must explicitly define its input reference.
     */
    private List<TimeSeries> getMacroInput(Map<String, List<TimeSeries>> availableReferences, MacroDefinition macro) {

        // Macro must explicitly define its input reference
        String inputRef = macro.getInputReference();
        if (inputRef == null) {
            throw new IllegalArgumentException("Macro '" + macro.getName() + "' must explicitly define an input reference, but it is null");
        }

        // The input reference must be available
        if (!availableReferences.containsKey(inputRef)) {
            throw new IllegalArgumentException(
                "Macro '"
                    + macro.getName()
                    + "' references input '"
                    + inputRef
                    + "' which is not available. Available references: "
                    + availableReferences.keySet()
            );
        }

        return availableReferences.get(inputRef);
    }

    /**
     * Execute pipeline stages on time series data.
     */
    private List<TimeSeries> executeStages(
        List<PipelineStage> stages,
        List<TimeSeries> input,
        Map<String, List<TimeSeries>> availableReferences
    ) {
        List<TimeSeries> resultTimeSeries = input;

        for (PipelineStage stage : stages) {
            if (stage instanceof BinaryPipelineStage binaryStage) {
                // For binary stages, determine the left and right operands
                List<TimeSeries> left = resultTimeSeries;
                String rightRefName = binaryStage.getRightOpReferenceName();

                // The availableReferences map contains logical names (a, b, c)
                // So we should look up the logical name directly, not the mapped name
                List<TimeSeries> right = availableReferences.get(rightRefName);
                if (right == null) {
                    throw new IllegalArgumentException(
                        "Referenced aggregation not found for " + binaryStage.getClass().getSimpleName() + ": " + rightRefName
                    );
                }

                resultTimeSeries = PipelineStageExecutor.executeBinaryStage(
                    binaryStage,
                    left,
                    right,
                    true // coordinator-level execution
                );
            } else {
                // Must be UnaryPipelineStage - all stages are validated in constructor
                UnaryPipelineStage unaryStage = (UnaryPipelineStage) stage;
                resultTimeSeries = PipelineStageExecutor.executeUnaryStage(
                    unaryStage,
                    resultTimeSeries,
                    true // coordinator-level execution
                );
            }
        }

        return resultTimeSeries != null ? resultTimeSeries : List.of();
    }

}
