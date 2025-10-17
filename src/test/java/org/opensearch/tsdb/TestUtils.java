/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

/**
 * Utility functions for tests.
 */
public class TestUtils {

    /**
     * Get all files with the specified extension from the given subdirectory of the resources directory.
     * @param subDirectory the subdirectory within the resources directory
     * @param extension the file extension to filter by (e.g., ".txt")
     * @return
     * @throws IOException if an I/O error occurs
     * @throws URISyntaxException if the resource URL is not formatted correctly
     */
    public static NavigableMap<String, String> getResourceFilesWithExtension(String subDirectory, String extension) throws IOException,
        URISyntaxException {
        URL resourceUrl = TestUtils.class.getResource(subDirectory);
        Path path = Path.of(resourceUrl.toURI());

        // Ensure ordering by numeric value rather than lexical order (i.e., case 3 should be 3.txt, not 11.txt)
        NavigableMap<String, String> result = new TreeMap<>((a, b) -> {
            int intA = Integer.parseInt(a.substring(0, a.lastIndexOf('.')));
            int intB = Integer.parseInt(b.substring(0, b.lastIndexOf('.')));
            return Integer.compare(intA, intB);
        });

        try (Stream<Path> walk = Files.walk(path)) {
            List<String> files = walk.filter(Files::isRegularFile)
                .map(Path::toString)
                .filter(string -> string.endsWith(extension))
                .sorted()
                .toList();
            for (String filename : files) {
                Path filePath = Path.of(filename);
                String mapKey = filePath.getFileName().toString();
                String fileContent = Files.readString(filePath, StandardCharsets.UTF_8);
                result.put(mapKey, fileContent);
            }
        }
        return result;
    }

    /**
     * Default delta value for sample value comparison.
     */
    private static final double DEFAULT_SAMPLE_DELTA = 0.0001;

    /**
     * Assert that two lists of samples are equal within a tolerance for float values.
     * Timestamps must match exactly, but float values are compared with a delta.
     *
     * @param message Description of what is being compared
     * @param expected Expected list of samples
     * @param actual Actual list of samples
     * @param delta Maximum allowed difference for float value comparison
     */
    public static void assertSamplesEqual(String message, List<Sample> expected, List<Sample> actual, double delta) {
        assertEquals(message + " - sample count", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Sample expectedSample = expected.get(i);
            Sample actualSample = actual.get(i);
            assertEquals(message + " - timestamp at index " + i, expectedSample.getTimestamp(), actualSample.getTimestamp());
            assertEquals(message + " - value at index " + i, expectedSample.getValue(), actualSample.getValue(), delta);
        }
    }

    /**
     * Assert that two lists of samples are equal within the default tolerance for float values.
     * Timestamps must match exactly, but float values are compared with a default delta of 0.0001.
     *
     * @param message Description of what is being compared
     * @param expected Expected list of samples
     * @param actual Actual list of samples
     */
    public static void assertSamplesEqual(String message, List<Sample> expected, List<Sample> actual) {
        assertSamplesEqual(message, expected, actual, DEFAULT_SAMPLE_DELTA);
    }

    /**
     * Find a time series in a list by matching a specific label value.
     *
     * @param series List of time series to search
     * @param labelKey The label key to match
     * @param labelValue The expected label value
     * @return The first time series with the matching label
     * @throws AssertionError if no time series with the specified label is found
     */
    public static TimeSeries findSeriesByLabel(List<TimeSeries> series, String labelKey, String labelValue) {
        for (TimeSeries ts : series) {
            String value = ts.getLabels().get(labelKey);
            if (labelValue.equals(value)) {
                return ts;
            }
        }
        throw new AssertionError("Time series with label " + labelKey + "=" + labelValue + " not found");
    }
}
