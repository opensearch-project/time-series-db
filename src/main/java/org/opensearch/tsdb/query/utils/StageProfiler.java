/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.query.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * A lightweight class to format and emit the stats per stage
 */
public class StageProfiler {

    private final List<String> profileResults = new ArrayList<>();

    /**
     * Record the stats of one stage
     * @param stageName name of the stage
     * @param latencyNs latency in nanoseconds
     * @param inputSampleCount number of input sample count in total
     * @param memoryOverhead estimated memory overhead for this stage, in bytes
     */
    public void record(String stageName, long latencyNs, long inputSampleCount, long memoryOverhead) {
        String result = stageName + "(" + inputSampleCount + "): " + latencyNs + " ns, " + memoryOverhead + " bytes";
        profileResults.add(result);
    }

    /**
     * Get a aggregated report for profile per stage
     * @return a whole string, with one stage's stats per line
     */
    public String getResults() {
        return String.join(";", profileResults);
    }
}
