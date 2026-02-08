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
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.PipelineStageAnnotation;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A pipeline stage that performs regex substitution on series names (aliases).
 * Takes a metric or wildcard seriesList, followed by a search pattern and replacement.
 * The replacement pattern supports backreferences (e.g., \1, \2).
 */
@PipelineStageAnnotation(name = AliasSubStage.NAME)
public class AliasSubStage implements UnaryPipelineStage {

    /** The name identifier for this pipeline stage. */
    public static final String NAME = "aliasSub";
    private static final String SEARCH_PATTERN_ARG = "search_pattern";
    private static final String REPLACEMENT_ARG = "replacement";

    private final String searchPattern;
    private final String replacement;
    private final Pattern compiledPattern;

    /**
     * Constructs a new AliasSubStage.
     *
     * @param searchPattern the regex pattern to search
     * @param replacement the replacement string (supports backreferences)
     */
    public AliasSubStage(String searchPattern, String replacement) {
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
            String seriesName = ts.getAlias();

            if (seriesName != null && !seriesName.isEmpty()) {
                Matcher matcher = compiledPattern.matcher(seriesName);

                // Apply regex replacement with backreference support
                String newAlias = RegexReplacementUtil.replaceAll(seriesName, matcher, replacement);

                // Create new TimeSeries with updated alias
                TimeSeries newTs = new TimeSeries(
                    ts.getSamples(),
                    ts.getLabels(),
                    ts.getMinTimestamp(),
                    ts.getMaxTimestamp(),
                    ts.getStep(),
                    newAlias
                );
                result.add(newTs);
            } else {
                // No series name, pass through unchanged
                result.add(ts);
            }
        }
        return result;
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(SEARCH_PATTERN_ARG, searchPattern);
        builder.field(REPLACEMENT_ARG, replacement);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(searchPattern);
        out.writeString(replacement);
    }

    /**
     * Create an AliasSubStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new AliasSubStage instance
     * @throws IOException if an I/O error occurs while reading
     */
    public static AliasSubStage readFrom(StreamInput in) throws IOException {
        String searchPattern = in.readString();
        String replacement = in.readString();
        return new AliasSubStage(searchPattern, replacement);
    }

    /**
     * Creates a new instance of AliasSubStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct an AliasSubStage instance
     * @return a new AliasSubStage instance initialized with the provided parameters
     */
    public static AliasSubStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }

        if (!args.containsKey(SEARCH_PATTERN_ARG)) {
            throw new IllegalArgumentException("AliasSub stage requires '" + SEARCH_PATTERN_ARG + "' argument");
        }
        String searchPattern = (String) args.get(SEARCH_PATTERN_ARG);
        if (searchPattern == null || searchPattern.isEmpty()) {
            throw new IllegalArgumentException("Search pattern cannot be null or empty");
        }

        if (!args.containsKey(REPLACEMENT_ARG)) {
            throw new IllegalArgumentException("AliasSub stage requires '" + REPLACEMENT_ARG + "' argument");
        }
        String replacement = (String) args.get(REPLACEMENT_ARG);
        if (replacement == null) {
            throw new IllegalArgumentException("Replacement cannot be null");
        }

        return new AliasSubStage(searchPattern, replacement);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AliasSubStage that = (AliasSubStage) obj;
        return Objects.equals(searchPattern, that.searchPattern) && Objects.equals(replacement, that.replacement);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchPattern, replacement);
    }

    /**
     * Gets the search pattern used by this stage.
     *
     * @return the search pattern
     */
    public String getSearchPattern() {
        return searchPattern;
    }

    /**
     * Gets the replacement string used by this stage.
     *
     * @return the replacement string
     */
    public String getReplacement() {
        return replacement;
    }
}
