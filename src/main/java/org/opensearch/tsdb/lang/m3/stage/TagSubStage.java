/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A pipeline stage that performs regex substitution on tag values.
 * Takes a metric or wildcard seriesList, followed by a tag name, search pattern, and replacement.
 * The replacement pattern supports backreferences (e.g., $1, $2).
 */
@PipelineStageAnnotation(name = TagSubStage.NAME)
public class TagSubStage implements UnaryPipelineStage {

    /** The name identifier for this pipeline stage. */
    public static final String NAME = "tag_sub";
    private static final String TAG_NAME_ARG = "tag_name";
    private static final String SEARCH_PATTERN_ARG = "search_pattern";
    private static final String REPLACEMENT_ARG = "replacement";

    private final String tagName;
    private final String searchPattern;
    private final String replacement;
    private final Pattern compiledPattern;

    /**
     * Constructs a new TagSubStage.
     *
     * @param tagName the tag name to modify
     * @param searchPattern the regex pattern to search
     * @param replacement the replacement string (supports backreferences)
     */
    public TagSubStage(String tagName, String searchPattern, String replacement) {
        this.tagName = tagName;
        this.searchPattern = searchPattern;
        this.replacement = replacement;
        this.compiledPattern = Pattern.compile(searchPattern);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }

        List<TimeSeries> result = new ArrayList<>(input.size());
        for (TimeSeries ts : input) {
            Labels seriesLabels = ts.getLabels();

            // If tag exists, apply regex replacement
            if (seriesLabels != null && seriesLabels.has(tagName)) {
                String originalValue = seriesLabels.get(tagName);
                Matcher matcher = compiledPattern.matcher(originalValue);

                // Apply regex replacement with backreference support
                String newValue = RegexReplacementUtil.replaceAll(originalValue, matcher, replacement);

                // Create new Labels with updated tag value
                Labels newLabels = seriesLabels.withLabel(tagName, newValue);

                // Create new TimeSeries with updated labels
                TimeSeries newTs = new TimeSeries(
                    ts.getSamples(),
                    newLabels,
                    ts.getMinTimestamp(),
                    ts.getMaxTimestamp(),
                    ts.getStep(),
                    ts.getAlias()
                );
                result.add(newTs);
            } else {
                // Tag doesn't exist, pass through unchanged
                result.add(ts);
            }
        }
        return result;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(TAG_NAME_ARG, tagName);
        builder.field(SEARCH_PATTERN_ARG, searchPattern);
        builder.field(REPLACEMENT_ARG, replacement);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tagName);
        out.writeString(searchPattern);
        out.writeString(replacement);
    }

    /**
     * Create a TagSubStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new TagSubStage instance
     * @throws IOException if an I/O error occurs while reading
     */
    public static TagSubStage readFrom(StreamInput in) throws IOException {
        String tagName = in.readString();
        String searchPattern = in.readString();
        String replacement = in.readString();
        return new TagSubStage(tagName, searchPattern, replacement);
    }

    /**
     * Creates a new instance of TagSubStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct a TagSubStage instance
     * @return a new TagSubStage instance initialized with the provided parameters
     */
    public static TagSubStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }

        if (!args.containsKey(TAG_NAME_ARG)) {
            throw new IllegalArgumentException("TagSub stage requires '" + TAG_NAME_ARG + "' argument");
        }
        String tagName = (String) args.get(TAG_NAME_ARG);
        if (tagName == null || tagName.isEmpty()) {
            throw new IllegalArgumentException("Tag name cannot be null or empty");
        }

        if (!args.containsKey(SEARCH_PATTERN_ARG)) {
            throw new IllegalArgumentException("TagSub stage requires '" + SEARCH_PATTERN_ARG + "' argument");
        }
        String searchPattern = (String) args.get(SEARCH_PATTERN_ARG);
        if (searchPattern == null || searchPattern.isEmpty()) {
            throw new IllegalArgumentException("Search pattern cannot be null or empty");
        }

        if (!args.containsKey(REPLACEMENT_ARG)) {
            throw new IllegalArgumentException("TagSub stage requires '" + REPLACEMENT_ARG + "' argument");
        }
        String replacement = (String) args.get(REPLACEMENT_ARG);
        if (replacement == null) {
            throw new IllegalArgumentException("Replacement cannot be null");
        }

        return new TagSubStage(tagName, searchPattern, replacement);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        TagSubStage that = (TagSubStage) obj;
        return Objects.equals(tagName, that.tagName)
            && Objects.equals(searchPattern, that.searchPattern)
            && Objects.equals(replacement, that.replacement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tagName, searchPattern, replacement);
    }
}
