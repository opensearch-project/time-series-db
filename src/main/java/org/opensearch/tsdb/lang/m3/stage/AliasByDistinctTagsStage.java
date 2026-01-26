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
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A coordinator-only pipeline stage that creates aliases based on tag values that vary across the series set.
 * This stage analyzes all series to identify which tags have different values across different series,
 * and builds aliases from those varying tag values.
 *
 * <p>Key Features:</p>
 * <ul>
 *   <li><strong>Coordinator-only:</strong> Requires global view of all series to identify varying tags</li>
 *   <li><strong>Automatic Detection:</strong> Can automatically find all varying tags if no tag list provided</li>
 *   <li><strong>Format Control:</strong> Boolean parameter controls "key:value" vs "value" only format</li>
 *   <li><strong>Tag Filtering:</strong> Optional tag list to filter which tags to consider</li>
 * </ul>
 */
@PipelineStageAnnotation(name = AliasByDistinctTagsStage.NAME)
public class AliasByDistinctTagsStage implements UnaryPipelineStage {

    /** The name identifier for this pipeline stage. */
    public static final String NAME = "aliasByDistinctTags";

    private static final String INCLUDE_KEYS_ARG = "include_keys";
    private static final String TAG_NAMES_ARG = "tag_names";

    private final boolean includeKeys; // true = "key:value" format, false = "value" only
    private final List<String> tagNames; // null means auto-detect varying tags

    /**
     * Creates a new AliasByDistinctTagsStage.
     *
     * @param includeKeys true for "key:value" format, false for "value" only format
     * @param tagNames list of tag names to consider (null for auto-detection)
     */
    public AliasByDistinctTagsStage(boolean includeKeys, List<String> tagNames) {
        this.includeKeys = includeKeys;
        this.tagNames = tagNames != null ? new ArrayList<>(tagNames) : null;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean isCoordinatorOnly() {
        return true;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }

        if (input.isEmpty()) {
            return input;
        }

        // Step 1: Analyze all series to find varying tags
        Set<String> varyingTags = findVaryingTags(input);

        // Step 2: Filter to specified tags if provided
        if (tagNames != null && !tagNames.isEmpty()) {
            varyingTags.retainAll(tagNames);
        }

        // Step 3: Build aliases for each series based on varying tags
        List<TimeSeries> result = new ArrayList<>(input.size());
        for (TimeSeries ts : input) {
            String alias = buildAliasFromVaryingTags(ts.getLabels(), varyingTags);

            // Create new TimeSeries with distinct tags alias
            TimeSeries newTs = new TimeSeries(
                ts.getSamples(),
                ts.getLabels(),
                ts.getMinTimestamp(),
                ts.getMaxTimestamp(),
                ts.getStep(),
                alias
            );
            result.add(newTs);
        }

        return result;
    }

    /**
     * Analyze all series to find tags that vary across the series set.
     *
     * @param input list of time series to analyze
     * @return set of tag names that have different values across series
     */
    private Set<String> findVaryingTags(List<TimeSeries> input) {
        if (input.size() <= 1) {
            return new HashSet<>(); // No variation possible with 0 or 1 series
        }

        // Collect all tag names across all series
        Set<String> allTagNames = new HashSet<>();
        for (TimeSeries ts : input) {
            Labels labels = ts.getLabels();
            if (labels != null) {
                Map<String, String> labelsMap = labels.toMapView();
                allTagNames.addAll(labelsMap.keySet());
            }
        }

        // For each tag name, check if values vary across series
        Set<String> varyingTags = new HashSet<>();
        for (String tagName : allTagNames) {
            if (isTagVarying(input, tagName)) {
                varyingTags.add(tagName);
            }
        }

        return varyingTags;
    }

    /**
     * Check if a specific tag has varying values across the series set.
     *
     * @param input list of time series to analyze
     * @param tagName the tag name to check
     * @return true if the tag has different values across different series
     */
    private boolean isTagVarying(List<TimeSeries> input, String tagName) {
        Set<String> uniqueValues = new HashSet<>();
        boolean hasNullValue = false;

        for (TimeSeries ts : input) {
            Labels labels = ts.getLabels();
            if (labels != null) {
                String value = labels.get(tagName);
                if (value != null) {
                    uniqueValues.add(value);
                } else {
                    hasNullValue = true;
                }
            } else {
                hasNullValue = true;
            }

            // If we find more than one unique value, or both null and non-null values, it's varying
            if (uniqueValues.size() > 1 || (uniqueValues.size() >= 1 && hasNullValue)) {
                return true;
            }
        }

        // Tag varies if we have more than one unique value or both present and missing values
        return uniqueValues.size() > 1 || (uniqueValues.size() >= 1 && hasNullValue);
    }

    /**
     * Build alias from varying tag values.
     *
     * @param labels the labels for this series
     * @param varyingTags set of tag names that vary across the series set
     * @return the constructed alias string
     */
    private String buildAliasFromVaryingTags(Labels labels, Set<String> varyingTags) {
        if (varyingTags.isEmpty() || labels == null) {
            return null;
        }

        List<String> aliasParts = new ArrayList<>();
        Map<String, String> labelsMap = labels.toMapView();

        // Process tags in sorted order for consistent output
        List<String> sortedTags = new ArrayList<>(varyingTags);
        sortedTags.sort(String::compareTo);

        for (String tagName : sortedTags) {
            String tagValue = labelsMap.get(tagName);
            if (tagValue != null && !tagValue.isEmpty()) {
                if (includeKeys) {
                    aliasParts.add(tagName + ":" + tagValue);
                } else {
                    aliasParts.add(tagValue);
                }
            }
            // If tag value is null/empty, skip it (don't add anything to aliasParts)
        }

        if (aliasParts.isEmpty()) {
            return null;
        }

        return String.join(" ", aliasParts);
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(INCLUDE_KEYS_ARG, includeKeys);
        if (tagNames != null) {
            builder.field(TAG_NAMES_ARG, tagNames);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(includeKeys);
        out.writeOptionalStringCollection(tagNames);
    }

    /**
     * Create an AliasByDistinctTagsStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new AliasByDistinctTagsStage instance
     * @throws IOException if an I/O error occurs while reading
     */
    public static AliasByDistinctTagsStage readFrom(StreamInput in) throws IOException {
        boolean includeKeys = in.readBoolean();
        List<String> tagNames = in.readOptionalStringList();
        return new AliasByDistinctTagsStage(includeKeys, tagNames);
    }

    /**
     * Creates a new instance of AliasByDistinctTagsStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct an AliasByDistinctTagsStage instance
     * @return a new AliasByDistinctTagsStage instance initialized with the provided parameters
     */
    @SuppressWarnings("unchecked")
    public static AliasByDistinctTagsStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            return new AliasByDistinctTagsStage(false, null); // Default: values only, auto-detect tags
        }

        // Parse includeKeys parameter (default to false)
        boolean includeKeys = false;
        Object includeKeysObj = args.get(INCLUDE_KEYS_ARG);
        if (includeKeysObj instanceof Boolean) {
            includeKeys = (Boolean) includeKeysObj;
        }

        // Parse tagNames parameter (optional)
        List<String> tagNames = null;
        Object tagNamesObj = args.get(TAG_NAMES_ARG);
        if (tagNamesObj instanceof List) {
            try {
                tagNames = (List<String>) tagNamesObj;
            } catch (ClassCastException e) {
                throw new IllegalArgumentException("tag_names must be a list of strings", e);
            }
        }

        return new AliasByDistinctTagsStage(includeKeys, tagNames);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AliasByDistinctTagsStage that = (AliasByDistinctTagsStage) obj;
        return includeKeys == that.includeKeys && Objects.equals(tagNames, that.tagNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(includeKeys, tagNames);
    }

    /**
     * Gets whether keys are included in the alias format.
     *
     * @return true for "key:value" format, false for "value" only format
     */
    public boolean isIncludeKeys() {
        return includeKeys;
    }

    /**
     * Gets the list of tag names to consider.
     *
     * @return the list of tag names, or null for auto-detection
     */
    public List<String> getTagNames() {
        return tagNames != null ? new ArrayList<>(tagNames) : null;
    }
}
