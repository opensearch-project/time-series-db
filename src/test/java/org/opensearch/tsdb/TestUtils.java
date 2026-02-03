/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.opensearch.tsdb.core.chunk.ChunkIterator;
import org.opensearch.tsdb.core.chunk.Encoding;
import org.opensearch.tsdb.core.head.MemChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunk;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndex;
import org.opensearch.tsdb.core.index.closed.ClosedChunkIndexIO;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.core.model.SampleList;
import org.opensearch.tsdb.query.aggregator.TimeSeries;
import org.opensearch.tsdb.query.stage.BinaryPipelineStage;
import org.opensearch.tsdb.query.stage.UnaryPipelineStage;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Utility functions for tests.
 */
public class TestUtils {

    /**
     * Get all files with the specified extension from the given subdirectory of the resources directory.
     *
     * @param subDirectory the subdirectory within the resources directory
     * @param extension    the file extension to filter by (e.g., ".txt")
     * @return
     * @throws IOException        if an I/O error occurs
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
     * @param message  Description of what is being compared
     * @param expected Expected list of samples
     * @param actual   Actual list of samples
     * @param delta    Maximum allowed difference for float value comparison
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
     * @param message  Description of what is being compared
     * @param expected Expected list of samples
     * @param actual   Actual list of samples
     */
    public static void assertSamplesEqual(String message, List<Sample> expected, List<Sample> actual) {
        assertSamplesEqual(message, expected, actual, DEFAULT_SAMPLE_DELTA);
    }

    /**
     * Compares samples in two lists for equality with default delta tolerance.
     *
     * @param message  Description of what is being compared
     * @param expected Expected list of samples
     * @param actual   Actual sample list
     * @param delta    Maximum allowed difference for float value comparison
     */
    public static void assertSamplesEqual(String message, List<Sample> expected, SampleList actual, double delta) {
        assertSamplesEqual(message, expected, actual.toList(), delta);
    }

    /**
     * Compares samples in two lists for equality with default delta tolerance.
     *
     * @param message  Description of what is being compared
     * @param expected Expected list of samples
     * @param actual   Actual sample list
     */
    public static void assertSamplesEqual(String message, List<Sample> expected, SampleList actual) {
        assertSamplesEqual(message, expected, actual, DEFAULT_SAMPLE_DELTA);
    }

    /**
     * Find a time series in a list by matching a specific label value.
     *
     * @param series     List of time series to search
     * @param labelKey   The label key to match
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

    /**
     * Creates a {@link MemChunk} with specified number of samples within given range.
     *
     * @param numSamples   Number of samples to ingest
     * @param minTimestamp min timestamp of the sample
     * @param maxTimestamp max timestamp of the sample
     * @return MemChunk with numSamples samples spanning a range [minTimestamp, maxTimestamp].
     */
    public static MemChunk getMemChunk(int numSamples, long minTimestamp, long maxTimestamp) {
        long interval = (maxTimestamp - minTimestamp) / (numSamples - 1);

        MemChunk chunk = new MemChunk(0, minTimestamp, maxTimestamp, null, Encoding.XOR);
        for (int i = 0; i < numSamples; i++) {
            long timestamp = minTimestamp + (i * interval);
            chunk.append(timestamp, i, i);
        }
        return chunk;
    }

    /**
     * Retrieves the chunks from the given index.
     *
     * @param closedChunkIndex an instance of {@link ClosedChunkIndex}
     * @return List of all {@link  ClosedChunk} found in the given index.
     * @throws IOException
     */
    public static List<ClosedChunk> getChunks(ClosedChunkIndex closedChunkIndex) throws IOException {
        List<ClosedChunk> chunks = new ArrayList<>();

        ReaderManager closedReaderManager = closedChunkIndex.getDirectoryReaderManager();
        DirectoryReader closedReader = null;
        try {
            closedReader = closedReaderManager.acquire();
            IndexSearcher closedSearcher = new IndexSearcher(closedReader);
            TopDocs topDocs = closedSearcher.search(new MatchAllDocsQuery(), Integer.MAX_VALUE);

            for (LeafReaderContext leaf : closedReader.leaves()) {
                BinaryDocValues chunkDocValues = leaf.reader()
                    .getBinaryDocValues(org.opensearch.tsdb.core.mapping.Constants.IndexSchema.CHUNK);
                if (chunkDocValues == null) {
                    continue;
                }
                int docBase = leaf.docBase;
                for (ScoreDoc sd : topDocs.scoreDocs) {
                    int docId = sd.doc;
                    if (docId >= docBase && docId < docBase + leaf.reader().maxDoc()) {
                        int localDocId = docId - docBase;
                        if (chunkDocValues.advanceExact(localDocId)) {
                            chunks.add(ClosedChunkIndexIO.getClosedChunkFromSerialized(chunkDocValues.binaryValue()));
                        }
                    }
                }
            }
        } finally {
            if (closedReader != null) {
                closedReaderManager.release(closedReader);
            }
        }
        return chunks;
    }

    /**
     * Helper method to assert that a ChunkIterator contains exactly the expected timestamp-value pairs.
     * @param it the iterator to check
     * @param expectedTimestamps list of expected timestamps in order
     * @param expectedValues list of expected values in order
     */
    public static void assertIteratorEquals(ChunkIterator it, List<Long> expectedTimestamps, List<Double> expectedValues) {
        assertEquals("Timestamps and values must have same length", expectedTimestamps.size(), expectedValues.size());

        List<Long> actualTimestamps = new ArrayList<>();
        List<Double> actualValues = new ArrayList<>();
        while (it.next() != ChunkIterator.ValueType.NONE) {
            ChunkIterator.TimestampValue tv = it.at();
            actualTimestamps.add(tv.timestamp());
            actualValues.add(tv.value());
        }

        assertEquals("Iterator should have same number of elements", expectedTimestamps.size(), actualTimestamps.size());
        for (int i = 0; i < expectedTimestamps.size(); i++) {
            assertEquals("Timestamp at index " + i, expectedTimestamps.get(i).longValue(), actualTimestamps.get(i).longValue());
            assertEquals("Value at index " + i, expectedValues.get(i), actualValues.get(i), 0.0001);
        }
    }

    /**
     * Test that a unary pipeline stage throws NullPointerException when given null input.
     * This utility method verifies that the stage properly validates its input and throws
     * an exception with the expected message format: "{stage_name} stage received null input".
     *
     * @param stage             The unary pipeline stage to test
     * @param expectedStageName The expected stage name for the error message
     * @throws AssertionError if the exception is not thrown or the message doesn't match
     */

    public static void assertNullInputThrowsException(UnaryPipelineStage stage, String expectedStageName) {
        NullPointerException exception = assertThrows(
            "Stage should throw NullPointerException for null input",
            NullPointerException.class,
            () -> stage.process(null)
        );
        assertEquals(
            "Exception message should indicate null input with stage name",
            expectedStageName + " stage received null input",
            exception.getMessage()
        );
    }

    /**
     * Test that a binary pipeline stage throws NullPointerException when given null left input.
     * This utility method verifies that the stage properly validates its left input and throws
     * an exception with the expected message format: "{stage_name} stage received null left input".
     *
     * @param stage             The binary pipeline stage to test
     * @param rightInput        The non-null right input to provide
     * @param expectedStageName The expected stage name for the error message
     * @throws AssertionError if the exception is not thrown or the message doesn't match
     */
    public static void assertNullLeftInputThrowsException(
        BinaryPipelineStage stage,
        List<TimeSeries> rightInput,
        String expectedStageName
    ) {
        NullPointerException exception = assertThrows(
            "Stage should throw NullPointerException for null left input",
            NullPointerException.class,
            () -> stage.process(null, rightInput)
        );
        assertEquals(
            "Exception message should indicate null left input with stage name",
            expectedStageName + " stage received null left input",
            exception.getMessage()
        );
    }

    /**
     * Test that a binary pipeline stage throws NullPointerException when given null right input.
     * This utility method verifies that the stage properly validates its right input and throws
     * an exception with the expected message format: "{stage_name} stage received null right input".
     *
     * @param stage             The binary pipeline stage to test
     * @param leftInput         The non-null left input to provide
     * @param expectedStageName The expected stage name for the error message
     * @throws AssertionError if the exception is not thrown or the message doesn't match
     */
    public static void assertNullRightInputThrowsException(
        BinaryPipelineStage stage,
        List<TimeSeries> leftInput,
        String expectedStageName
    ) {
        NullPointerException exception = assertThrows(
            "Stage should throw NullPointerException for null right input",
            NullPointerException.class,
            () -> stage.process(leftInput, null)
        );
        assertEquals(
            "Exception message should indicate null right input with stage name",
            expectedStageName + " stage received null right input",
            exception.getMessage()
        );
    }

    /**
     * Test that a binary pipeline stage throws NullPointerException for both null left and null right inputs.
     * This is a convenience method that calls both assertNullLeftInputThrowsException and assertNullRightInputThrowsException.
     *
     * @param stage             The binary pipeline stage to test
     * @param nonNullInput      A non-null input list to use when testing the other input
     * @param expectedStageName The expected stage name for the error messages
     * @throws AssertionError if any of the exceptions are not thrown or messages don't match
     */
    public static void assertBinaryNullInputsThrowExceptions(
        BinaryPipelineStage stage,
        List<TimeSeries> nonNullInput,
        String expectedStageName
    ) {
        assertNullLeftInputThrowsException(stage, nonNullInput, expectedStageName);
        assertNullRightInputThrowsException(stage, nonNullInput, expectedStageName);
    }

    /**
     * Finds all time series that match all the specified label criteria.
     *
     * @param seriesList The list of time series to search
     * @param labelCriteria Map of label key-value pairs that must all match
     * @return List of matching time series (may be empty)
     */
    public static List<TimeSeries> findSeriesWithLabels(List<TimeSeries> seriesList, Map<String, String> labelCriteria) {
        List<TimeSeries> matchingSeries = new ArrayList<>();

        for (TimeSeries series : seriesList) {
            Labels labels = series.getLabels();
            if (labels == null) {
                continue;
            }

            Map<String, String> labelMap = labels.toMapView();

            // Check if all label criteria match
            boolean allMatch = true;
            for (Map.Entry<String, String> criterion : labelCriteria.entrySet()) {
                String expectedValue = criterion.getValue();
                String actualValue = labelMap.get(criterion.getKey());
                if (!expectedValue.equals(actualValue)) {
                    allMatch = false;
                    break;
                }
            }

            if (allMatch) {
                matchingSeries.add(series);
            }
        }

        return matchingSeries;
    }
}
