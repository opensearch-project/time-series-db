/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.stage;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.tsdb.lang.m3.stage.AbsStage;
import org.opensearch.tsdb.lang.m3.stage.AliasByTagsStage;
import org.opensearch.tsdb.lang.m3.stage.AliasStage;
import org.opensearch.tsdb.lang.m3.stage.AsPercentStage;
import org.opensearch.tsdb.lang.m3.stage.AvgStage;
import org.opensearch.tsdb.lang.m3.stage.CopyStage;
import org.opensearch.tsdb.lang.m3.stage.CountStage;
import org.opensearch.tsdb.lang.m3.stage.DerivativeStage;
import org.opensearch.tsdb.lang.m3.stage.DivideScalarStage;
import org.opensearch.tsdb.lang.m3.stage.DivideStage;
import org.opensearch.tsdb.lang.m3.stage.ExcludeByTagStage;
import org.opensearch.tsdb.lang.m3.stage.TagSubStage;
import org.opensearch.tsdb.lang.m3.stage.FallbackSeriesBinaryStage;
import org.opensearch.tsdb.lang.m3.stage.FallbackSeriesUnaryStage;
import org.opensearch.tsdb.lang.m3.stage.HeadStage;
import org.opensearch.tsdb.lang.m3.stage.HistogramPercentileStage;
import org.opensearch.tsdb.lang.m3.stage.IntegralStage;
import org.opensearch.tsdb.lang.m3.stage.IntersectStage;
import org.opensearch.tsdb.lang.m3.stage.IsNonNullStage;
import org.opensearch.tsdb.lang.m3.stage.KeepLastValueStage;
import org.opensearch.tsdb.lang.m3.stage.LogarithmStage;
import org.opensearch.tsdb.lang.m3.stage.MaxStage;
import org.opensearch.tsdb.lang.m3.stage.MinStage;
import org.opensearch.tsdb.lang.m3.stage.MovingStage;
import org.opensearch.tsdb.lang.m3.stage.MultiplyStage;
import org.opensearch.tsdb.lang.m3.stage.OffsetStage;
import org.opensearch.tsdb.lang.m3.stage.PerSecondRateStage;
import org.opensearch.tsdb.lang.m3.stage.PerSecondStage;
import org.opensearch.tsdb.lang.m3.stage.PercentileOfSeriesStage;
import org.opensearch.tsdb.lang.m3.stage.RemoveEmptyStage;
import org.opensearch.tsdb.lang.m3.stage.RoundStage;
import org.opensearch.tsdb.lang.m3.stage.ScaleStage;
import org.opensearch.tsdb.lang.m3.stage.ScaleToSecondsStage;
import org.opensearch.tsdb.lang.m3.stage.ShowTagsStage;
import org.opensearch.tsdb.lang.m3.stage.SortStage;
import org.opensearch.tsdb.lang.m3.stage.SqrtStage;
import org.opensearch.tsdb.lang.m3.stage.SubtractStage;
import org.opensearch.tsdb.lang.m3.stage.SumStage;
import org.opensearch.tsdb.lang.m3.stage.SummarizeStage;
import org.opensearch.tsdb.lang.m3.stage.SustainStage;
import org.opensearch.tsdb.lang.m3.stage.TimeshiftStage;
import org.opensearch.tsdb.lang.m3.stage.TransformNullStage;
import org.opensearch.tsdb.lang.m3.stage.TruncateStage;
import org.opensearch.tsdb.lang.m3.stage.UnionStage;
import org.opensearch.tsdb.lang.m3.stage.ValueFilterStage;

/**
 * Factory class for creating pipeline stage instances from string definitions.
 *
 * <p>This factory provides a centralized way to create and register pipeline stage types.
 * It supports both unary and binary pipeline stages, allowing for flexible pipeline
 * configuration through string-based definitions.</p>
 *
 * <h2>Supported Stage Types:</h2>
 * <ul>
 *   <li><strong>Unary Stages:</strong> alias, avg, max, min, scale, sort, sum, timeshift</li>
 *   <li><strong>Unary Stages:</strong> scale, valueFilter</li>
 * </ul>
     * <h2>Supported Stage Types:</h2>
     * <ul>
     *   <li><strong>Unary Stages:</strong> scale, timeshift</li>
     * </ul>
 *
 * <h2>Usage Examples:</h2>
 * <pre>{@code
 * // Create a scale stage from arguments
 * Map<String, Object> args = Map.of("factor", 2.0);
 * PipelineStage scaleStage = PipelineStageFactory.createWithArgs("scale", args);
 * }</pre>
 *
 * <h2>Registration:</h2>
 * <p>New stage types can be registered by adding entries to the {@code STAGE_ARGS_CREATORS} map
 * in the static initializer block. Each entry maps a stage name to a factory function
 * that creates the appropriate stage instance.</p>
 *
 */
public class PipelineStageFactory {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private PipelineStageFactory() {
        // Utility class
    }

    private static final Map<String, Function<Map<String, Object>, PipelineStage>> STAGE_ARGS_CREATORS = new HashMap<>();
    private static final Map<String, Function<StreamInput, PipelineStage>> STAGE_READERS = new HashMap<>();

    static {
        // Auto-register all annotated stage classes
        // TODO: Replace manual registration with automatic classpath scanning to discover
        // all classes annotated with @PipelineStageAnnotation
        autoRegisterStages();
    }

    /**
     * Automatically discover and register all classes annotated with @PipelineStageAnnotation.
     */
    private static void autoRegisterStages() {
        try {
            // For now, manually register known stages. In a full implementation,
            // this would use classpath scanning to find all annotated classes.
            // Registering in alphabetical order for consistency
            registerStage(AbsStage.class);
            registerStage(AliasByTagsStage.class);
            registerStage(AliasStage.class);
            registerStage(AsPercentStage.class);
            registerStage(AvgStage.class);
            registerStage(CountStage.class);
            registerStage(DerivativeStage.class);
            registerStage(DivideScalarStage.class);
            registerStage(DivideStage.class);
            registerStage(ExcludeByTagStage.class);
            registerStage(TagSubStage.class);
            registerStage(FallbackSeriesBinaryStage.class);
            registerStage(FallbackSeriesUnaryStage.class);
            registerStage(HeadStage.class);
            registerStage(HistogramPercentileStage.class);
            registerStage(IntegralStage.class);
            registerStage(IntersectStage.class);
            registerStage(IsNonNullStage.class);
            registerStage(KeepLastValueStage.class);
            registerStage(LogarithmStage.class);
            registerStage(MaxStage.class);
            registerStage(MinStage.class);
            registerStage(MovingStage.class);
            registerStage(MultiplyStage.class);
            registerStage(OffsetStage.class);
            registerStage(PercentileOfSeriesStage.class);
            registerStage(PerSecondStage.class);
            registerStage(PerSecondRateStage.class);
            registerStage(RemoveEmptyStage.class);
            registerStage(RoundStage.class);
            registerStage(ScaleStage.class);
            registerStage(ScaleToSecondsStage.class);
            registerStage(ShowTagsStage.class);
            registerStage(SortStage.class);
            registerStage(SqrtStage.class);
            registerStage(SustainStage.class);
            registerStage(SubtractStage.class);
            registerStage(SumStage.class);
            registerStage(SummarizeStage.class);
            registerStage(TimeshiftStage.class);
            registerStage(TransformNullStage.class);
            registerStage(TruncateStage.class);
            registerStage(UnionStage.class);
            registerStage(ValueFilterStage.class);
            registerStage(CopyStage.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to auto-register pipeline stages", e);
        }
    }

    /**
     * Register a stage class that has the @PipelineStageAnnotation.
     */
    private static void registerStage(Class<? extends PipelineStage> stageClass) {
        PipelineStageAnnotation annotation = stageClass.getAnnotation(PipelineStageAnnotation.class);
        if (annotation == null) {
            throw new IllegalArgumentException("Stage class " + stageClass.getName() + " must be annotated with @PipelineStageAnnotation");
        }

        String stageName = annotation.name();

        try {
            // Register fromArgs method (required)
            Method fromArgsMethod = stageClass.getMethod("fromArgs", Map.class);
            assert !STAGE_ARGS_CREATORS.containsKey(stageName) : "Stage type '"
                + stageName
                + "' is already registered with "
                + STAGE_ARGS_CREATORS.get(stageName);
            STAGE_ARGS_CREATORS.put(stageName, args -> {
                try {
                    return (PipelineStage) fromArgsMethod.invoke(null, args);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to create stage from args: " + args, e);
                }
            });

            // Register readFrom method (required)
            Method readFromMethod = stageClass.getMethod("readFrom", StreamInput.class);
            assert !STAGE_READERS.containsKey(stageName) : "Stage type '"
                + stageName
                + "' is already registered with "
                + STAGE_READERS.get(stageName);
            STAGE_READERS.put(stageName, in -> {
                try {
                    return (PipelineStage) readFromMethod.invoke(null, in);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to read stage from stream", e);
                }
            });
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                "Stage class " + stageClass.getName() + " must have static fromArgs(Map<String, Object>) and readFrom(StreamInput) methods",
                e
            );
        }
    }

    /**
     * Create a PipelineStage or BinaryPipelineStage from arguments map.
     *
     * @param stageType The type of stage to create
     * @param args Map of argument names to values
     * @return A new stage instance (PipelineStage or BinaryPipelineStage)
     * @throws IllegalArgumentException if the stage type is unknown or arguments are invalid
     */
    public static PipelineStage createWithArgs(String stageType, Map<String, Object> args) {
        if (stageType == null || stageType.trim().isEmpty()) {
            throw new IllegalArgumentException("Stage type cannot be null or empty");
        }

        Function<Map<String, Object>, PipelineStage> creator = STAGE_ARGS_CREATORS.get(stageType);
        if (creator == null) {
            throw new IllegalArgumentException("Unknown stage type: " + stageType);
        }

        try {
            return creator.apply(args);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create stage of type '" + stageType + "' from args: " + args, e);
        }
    }

    /**
     * Check if a stage type is supported by the factory.
     *
     * @param stageType The stage type to check
     * @return true if the stage type is supported, false otherwise
     */
    public static boolean isStageTypeSupported(String stageType) {
        return STAGE_ARGS_CREATORS.containsKey(stageType);
    }

    /**
     * Get all supported stage types.
     *
     * @return Set of supported stage type identifiers
     */
    public static java.util.Set<String> getSupportedStageTypes() {
        return new HashSet<>(STAGE_ARGS_CREATORS.keySet());
    }

    /**
     * Create a stage instance from the input stream for deserialization.
     *
     * @param in the input stream
     * @return a new stage instance
     * @throws IOException if an I/O error occurs
     */
    public static PipelineStage readFrom(StreamInput in) throws IOException {
        String stageName = in.readString();
        return readFrom(in, stageName);
    }

    /**
     * Create a stage instance from the input stream for deserialization for the given stage name.
     *
     * @param in the input stream
     * @param stageName the stage name
     * @return a new stage instance
     * @throws IOException if an I/O error occurs
     */
    public static PipelineStage readFrom(StreamInput in, String stageName) throws IOException {
        Function<StreamInput, PipelineStage> reader = STAGE_READERS.get(stageName);
        if (reader == null) {
            throw new IllegalArgumentException("Unknown stage name: " + stageName);
        }

        try {
            return reader.apply(in);
        } catch (Exception e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new IOException("Failed to read stage '" + stageName + "' from stream", e);
        }
    }
}
