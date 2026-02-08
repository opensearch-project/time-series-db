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
 * A pipeline stage that renames tag keys in time series.
 * Will replace the tag with key matching the argument key with the replacement key in all the input series.
 * If the old key doesn't exist, the series passes through unchanged.
 */
@PipelineStageAnnotation(name = MapKeyStage.NAME)
public class MapKeyStage implements UnaryPipelineStage {

    /** The name identifier for this pipeline stage. */
    public static final String NAME = "map_key";
    private static final String OLD_KEY_ARG = "old_key";
    private static final String NEW_KEY_ARG = "new_key";

    private final String oldKey;
    private final String newKey;

    /**
     * Constructs a new MapKeyStage.
     *
     * @param oldKey the existing tag key to rename
     * @param newKey the new tag key name
     */
    public MapKeyStage(String oldKey, String newKey) {
        this.oldKey = oldKey;
        this.newKey = newKey;
    }

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Get the old key name.
     * @return the old key
     */
    public String getOldKey() {
        return oldKey;
    }

    /**
     * Get the new key name.
     * @return the new key
     */
    public String getNewKey() {
        return newKey;
    }

    @Override
    public List<TimeSeries> process(List<TimeSeries> input) {
        if (input == null) {
            throw new NullPointerException(getName() + " stage received null input");
        }

        List<TimeSeries> result = new ArrayList<>(input.size());
        for (TimeSeries ts : input) {
            result.add(processTimeSeries(ts));
        }
        return result;
    }

    /**
     * Processes a single time series, renaming the tag key if it exists.
     *
     * @param ts the time series to process
     * @return a new TimeSeries with renamed tag key, or the original if key doesn't exist
     */
    private TimeSeries processTimeSeries(TimeSeries ts) {
        Labels seriesLabels = ts.getLabels();

        // If series has no labels or the old key doesn't exist, pass through unchanged
        if (seriesLabels == null || !seriesLabels.has(oldKey)) {
            return ts;
        }

        // Get the value of the old key
        String value = seriesLabels.get(oldKey);

        // Create new labels with the key renamed
        // Build map with all labels except the old key, plus the new key
        Map<String, String> labelMap = new java.util.HashMap<>(seriesLabels.toMapView());
        labelMap.remove(oldKey);
        labelMap.put(newKey, value);

        // Create new Labels from the modified map
        Labels newLabels = ByteLabels.fromMap(labelMap);

        // Create new TimeSeries with updated labels
        return new TimeSeries(ts.getSamples(), newLabels, ts.getMinTimestamp(), ts.getMaxTimestamp(), ts.getStep(), ts.getAlias());
    }

    @Override
    public void toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field(OLD_KEY_ARG, oldKey);
        builder.field(NEW_KEY_ARG, newKey);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(oldKey);
        out.writeString(newKey);
    }

    /**
     * Create a MapKeyStage instance from the input stream for deserialization.
     *
     * @param in the stream input to read from
     * @return a new MapKeyStage instance
     * @throws IOException if an I/O error occurs while reading
     */
    public static MapKeyStage readFrom(StreamInput in) throws IOException {
        String oldKey = in.readString();
        String newKey = in.readString();
        return new MapKeyStage(oldKey, newKey);
    }

    /**
     * Creates a new instance of MapKeyStage using the provided arguments.
     *
     * @param args a map containing the arguments required to construct a MapKeyStage instance
     * @return a new MapKeyStage instance initialized with the provided parameters
     * @throws IllegalArgumentException if required arguments are missing or invalid
     */
    public static MapKeyStage fromArgs(Map<String, Object> args) {
        if (args == null) {
            throw new IllegalArgumentException("Args cannot be null");
        }

        if (!args.containsKey(OLD_KEY_ARG)) {
            throw new IllegalArgumentException("MapKey stage requires '" + OLD_KEY_ARG + "' argument");
        }
        String oldKey = (String) args.get(OLD_KEY_ARG);
        if (oldKey == null || oldKey.isEmpty()) {
            throw new IllegalArgumentException("Old key cannot be null or empty");
        }

        if (!args.containsKey(NEW_KEY_ARG)) {
            throw new IllegalArgumentException("MapKey stage requires '" + NEW_KEY_ARG + "' argument");
        }
        String newKey = (String) args.get(NEW_KEY_ARG);
        if (newKey == null || newKey.isEmpty()) {
            throw new IllegalArgumentException("New key cannot be null or empty");
        }

        return new MapKeyStage(oldKey, newKey);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        MapKeyStage that = (MapKeyStage) obj;
        return Objects.equals(oldKey, that.oldKey) && Objects.equals(newKey, that.newKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(oldKey, newKey);
    }
}
