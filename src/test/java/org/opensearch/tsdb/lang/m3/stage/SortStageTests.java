/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.stage;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.core.model.ByteLabels;
import org.opensearch.tsdb.core.model.FloatSample;
import org.opensearch.tsdb.core.model.Labels;
import org.opensearch.tsdb.core.model.Sample;
import org.opensearch.tsdb.lang.m3.common.SortByType;
import org.opensearch.tsdb.lang.m3.common.SortOrderType;
import org.opensearch.tsdb.query.aggregator.TimeSeries;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SortStageTests extends AbstractWireSerializingTestCase<SortStage> {

    // ========== Constructor Tests ==========

    public void testConstructorWithBothParameters() {
        // Arrange & Act
        SortStage sortStage = new SortStage(SortByType.AVG, SortOrderType.ASC);

        // Assert
        assertEquals(SortByType.AVG, sortStage.getSortBy());
        assertEquals(SortOrderType.ASC, sortStage.getSortOrder());
        assertEquals("sort", sortStage.getName());
    }

    public void testConstructorWithSortByOnly() {
        // Arrange & Act
        SortStage sortStage = new SortStage(SortByType.MAX);

        // Assert
        assertEquals(SortByType.MAX, sortStage.getSortBy());
        assertEquals(SortOrderType.DESC, sortStage.getSortOrder()); // Default
        assertEquals("sort", sortStage.getName());
    }

    // ========== Process Method Tests ==========

    public void testProcessWithEmptyInput() {
        // Arrange
        SortStage sortStage = new SortStage(SortByType.AVG);
        List<TimeSeries> input = new ArrayList<>();

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertSame(input, result); // Should return the same empty list
    }

    public void testProcessWithNullInput() {
        SortStage sortStage = new SortStage(SortByType.AVG);
        TestUtils.assertNullInputThrowsException(sortStage, "sort");
    }

    public void testProcessSortByAvgDesc() {
        // Arrange
        List<TimeSeries> input = createTestSeriesABC();
        SortStage sortStage = new SortStage(SortByType.AVG, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Verify sort order by checking labels (B > C > A)
        assertEquals(3, result.size());
        assertEquals("B", getLabel(result.get(0))); // highest avg
        assertEquals("C", getLabel(result.get(1))); // middle avg
        assertEquals("A", getLabel(result.get(2))); // lowest avg
    }

    public void testProcessSortByAvgAsc() {
        // Arrange
        List<TimeSeries> input = createTestSeriesABC();
        SortStage sortStage = new SortStage(SortByType.AVG, SortOrderType.ASC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Verify sort order (A < C < B)
        assertEquals(3, result.size());
        assertEquals("A", getLabel(result.get(0))); // lowest avg
        assertEquals("C", getLabel(result.get(1))); // middle avg
        assertEquals("B", getLabel(result.get(2))); // highest avg
    }

    public void testProcessSortByMaxDesc() {
        // Arrange
        List<TimeSeries> input = createTestSeriesABC();
        SortStage sortStage = new SortStage(SortByType.MAX, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Verify sort order (B > C > A)
        assertEquals(3, result.size());
        assertEquals("B", getLabel(result.get(0))); // max=3.0
        assertEquals("C", getLabel(result.get(1))); // max=2.0
        assertEquals("A", getLabel(result.get(2))); // max=1.0
    }

    public void testProcessSortBySumDesc() {
        // Arrange
        List<TimeSeries> input = createTestSeriesABC();
        SortStage sortStage = new SortStage(SortByType.SUM, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Verify sort order (B > C > A)
        assertEquals(3, result.size());
        assertEquals("B", getLabel(result.get(0))); // sum=6.0
        assertEquals("C", getLabel(result.get(1))); // sum=3.0
        assertEquals("A", getLabel(result.get(2))); // sum=2.0
    }

    public void testProcessSortByCurrentDesc() {
        // Arrange
        List<TimeSeries> input = createTestSeriesABC();
        SortStage sortStage = new SortStage(SortByType.CURRENT, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Verify sort order (B > C > A)
        assertEquals(3, result.size());
        assertEquals("B", getLabel(result.get(0))); // current=3.0
        assertEquals("C", getLabel(result.get(1))); // current=2.0
        assertEquals("A", getLabel(result.get(2))); // current=1.0
    }

    public void testProcessSortByCurrentAsc() {
        // Arrange
        List<TimeSeries> input = createTestSeriesABC();
        SortStage sortStage = new SortStage(SortByType.CURRENT, SortOrderType.ASC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Verify sort order (A < C < B)
        assertEquals(3, result.size());
        assertEquals("A", getLabel(result.get(0))); // current=1.0
        assertEquals("C", getLabel(result.get(1))); // current=2.0
        assertEquals("B", getLabel(result.get(2))); // current=3.0
    }

    public void testProcessSortByMinDesc() {
        // Arrange: Create series with DIFFERENT minimum values
        // Series A: [5.0, 10.0] -> min=5.0
        // Series B: [1.0, 20.0] -> min=1.0
        // Series C: [10.0, 15.0] -> min=10.0
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(5.0, 10.0));
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(1.0, 20.0));
        TimeSeries seriesC = createLabeledTimeSeries("C", Arrays.asList(10.0, 15.0));
        List<TimeSeries> input = Arrays.asList(seriesA, seriesB, seriesC);

        SortStage sortStage = new SortStage(SortByType.MIN, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Verify sort order (C > A > B)
        assertEquals(3, result.size());
        assertEquals("C", getLabel(result.get(0))); // min=10.0
        assertEquals("A", getLabel(result.get(1))); // min=5.0
        assertEquals("B", getLabel(result.get(2))); // min=1.0
    }

    public void testProcessSortByMinAsc() {
        // Arrange
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(5.0, 10.0));
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(1.0, 20.0));
        TimeSeries seriesC = createLabeledTimeSeries("C", Arrays.asList(10.0, 15.0));
        List<TimeSeries> input = Arrays.asList(seriesA, seriesB, seriesC);

        SortStage sortStage = new SortStage(SortByType.MIN, SortOrderType.ASC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Verify sort order (B < A < C)
        assertEquals(3, result.size());
        assertEquals("B", getLabel(result.get(0))); // min=1.0
        assertEquals("A", getLabel(result.get(1))); // min=5.0
        assertEquals("C", getLabel(result.get(2))); // min=10.0
    }

    public void testProcessSortByStddevDesc() {
        // Arrange: Create series with different variations
        // Series A: [5.0, 5.0] -> stddev=0.0 (no variation)
        // Series B: [1.0, 10.0] -> stddev=high (large variation)
        // Series C: [4.0, 6.0] -> stddev=medium (small variation)
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(5.0, 5.0));
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(1.0, 10.0));
        TimeSeries seriesC = createLabeledTimeSeries("C", Arrays.asList(4.0, 6.0));
        List<TimeSeries> input = Arrays.asList(seriesA, seriesB, seriesC);

        SortStage sortStage = new SortStage(SortByType.STDDEV, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Verify sort order (B > C > A)
        assertEquals(3, result.size());
        assertEquals("B", getLabel(result.get(0))); // highest variation
        assertEquals("C", getLabel(result.get(1))); // medium variation
        assertEquals("A", getLabel(result.get(2))); // no variation
    }

    public void testProcessSortByStddevAsc() {
        // Arrange
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(5.0, 5.0));
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(1.0, 10.0));
        TimeSeries seriesC = createLabeledTimeSeries("C", Arrays.asList(4.0, 6.0));
        List<TimeSeries> input = Arrays.asList(seriesA, seriesB, seriesC);

        SortStage sortStage = new SortStage(SortByType.STDDEV, SortOrderType.ASC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Verify sort order (A < C < B)
        assertEquals(3, result.size());
        assertEquals("A", getLabel(result.get(0))); // no variation
        assertEquals("C", getLabel(result.get(1))); // medium variation
        assertEquals("B", getLabel(result.get(2))); // highest variation
    }

    public void testProcessSortByNameDesc() {
        // Arrange
        TimeSeries seriesA = createTimeSeriesWithAlias("alpha", Arrays.asList(1.0, 2.0));
        TimeSeries seriesB = createTimeSeriesWithAlias("charlie", Arrays.asList(3.0, 4.0));
        TimeSeries seriesC = createTimeSeriesWithAlias("bravo", Arrays.asList(5.0, 6.0));
        List<TimeSeries> input = Arrays.asList(seriesA, seriesB, seriesC);

        SortStage sortStage = new SortStage(SortByType.NAME, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Verify sort order (charlie > bravo > alpha)
        assertEquals(3, result.size());
        assertEquals("charlie", getAlias(result.get(0))); // highest alphabetically
        assertEquals("bravo", getAlias(result.get(1))); // middle alphabetically
        assertEquals("alpha", getAlias(result.get(2))); // lowest alphabetically
    }

    public void testProcessSortByNameAsc() {
        // Arrange
        TimeSeries seriesA = createTimeSeriesWithAlias("zulu", Arrays.asList(1.0, 2.0));
        TimeSeries seriesB = createTimeSeriesWithAlias("alpha", Arrays.asList(3.0, 4.0));
        TimeSeries seriesC = createTimeSeriesWithAlias("mike", Arrays.asList(5.0, 6.0));
        List<TimeSeries> input = Arrays.asList(seriesA, seriesB, seriesC);

        SortStage sortStage = new SortStage(SortByType.NAME, SortOrderType.ASC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Verify sort order (alpha < mike < zulu)
        assertEquals(3, result.size());
        assertEquals("alpha", getAlias(result.get(0))); // lowest alphabetically
        assertEquals("mike", getAlias(result.get(1))); // middle alphabetically
        assertEquals("zulu", getAlias(result.get(2))); // highest alphabetically
    }

    public void testProcessSortByNameWithMissingAlias() {
        // Arrange: Time series without alias (treated as empty string) sorts first in ASC
        TimeSeries seriesWithAlias = createTimeSeriesWithAlias("zebra", Arrays.asList(1.0, 2.0));
        TimeSeries seriesWithoutAlias = createTimeSeriesWithoutAlias(Arrays.asList(3.0, 4.0)); // No alias (null -> "")
        List<TimeSeries> input = Arrays.asList(seriesWithAlias, seriesWithoutAlias);

        SortStage sortStage = new SortStage(SortByType.NAME, SortOrderType.ASC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Series without alias (empty string) comes first in ASC order
        assertEquals(2, result.size());
        assertNull(getAlias(result.get(0))); // No alias (null -> empty string sorts first)
        assertEquals("zebra", getAlias(result.get(1))); // Has alias
    }

    public void testProcessSortByNameWithNulls() {
        // Arrange: Null aliases are treated as empty strings in sorting
        TimeSeries seriesA = createTimeSeriesWithAlias("alpha", Arrays.asList(1.0, 2.0));
        TimeSeries seriesB = createTimeSeriesWithAlias("charlie", Arrays.asList(3.0, 4.0));
        TimeSeries seriesWithoutAlias = createTimeSeriesWithoutAlias(Arrays.asList(5.0, 6.0)); // No alias (null -> "")
        List<TimeSeries> input = Arrays.asList(seriesA, seriesWithoutAlias, seriesB);

        SortStage sortStage = new SortStage(SortByType.NAME, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: DESC order - charlie, alpha, "" (null)
        assertEquals(3, result.size());
        assertEquals("charlie", getAlias(result.get(0))); // Highest alphabetically
        assertEquals("alpha", getAlias(result.get(1))); // Lowest alphabetically
        assertNull(getAlias(result.get(2))); // No alias (empty string sorts last in DESC)

        sortStage = new SortStage(SortByType.NAME, SortOrderType.ASC);

        // Act
        result = sortStage.process(input);

        // Assert: ASC order - "" (null), alpha, charlie
        assertEquals(3, result.size());
        assertNull(getAlias(result.get(0))); // No alias (empty string sorts first in ASC)
        assertEquals("alpha", getAlias(result.get(1))); // Lowest alphabetically
        assertEquals("charlie", getAlias(result.get(2))); // Highest alphabetically
    }

    public void testProcessSortByNameWithBothNulls() {
        // Arrange: Test when both time series have null aliases (both treated as empty strings)
        TimeSeries series1 = createTimeSeriesWithoutAlias(Arrays.asList(1.0, 2.0)); // No alias (null -> "")
        TimeSeries series2 = createTimeSeriesWithoutAlias(Arrays.asList(3.0, 4.0)); // No alias (null -> "")
        List<TimeSeries> input = Arrays.asList(series1, series2);

        SortStage sortStage = new SortStage(SortByType.NAME, SortOrderType.ASC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: When both are null (empty strings), they should be equal - stable sort preserves order
        assertEquals(2, result.size());
        // Both have null aliases (empty strings), so order should be stable (same as input)
        assertTrue(result.contains(series1));
        assertTrue(result.contains(series2));
        assertNull(getAlias(result.get(0)));
        assertEquals(1.0, result.get(0).getSamples().getValue(0), 0.0);
        assertNull(getAlias(result.get(1)));
        assertEquals(3.0, result.get(1).getSamples().getValue(0), 0.0);
    }

    public void testProcessSortByNameWithEmptyAlias() {
        // Arrange: Test when alias is empty string (sorts first in ASC, last in DESC)
        TimeSeries seriesWithEmptyAlias = createTimeSeriesWithAlias("", Arrays.asList(1.0, 2.0));
        TimeSeries seriesWithAlias = createTimeSeriesWithAlias("alpha", Arrays.asList(3.0, 4.0));
        List<TimeSeries> input = Arrays.asList(seriesWithAlias, seriesWithEmptyAlias);

        SortStage sortStage = new SortStage(SortByType.NAME, SortOrderType.ASC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Empty string alias sorts before other strings in ASC
        assertEquals(2, result.size());
        assertEquals("", getAlias(result.get(0))); // Empty alias comes first in ASC
        assertEquals("alpha", getAlias(result.get(1))); // Non-empty alias comes after
    }

    public void testProcessSortByNameWithMixedAliasesDescending() {
        // Arrange: Test mixed aliases in descending order (null treated as empty string)
        TimeSeries series1 = createTimeSeriesWithAlias("delta", Arrays.asList(1.0));
        TimeSeries series2 = createTimeSeriesWithoutAlias(Arrays.asList(2.0)); // null alias -> ""
        TimeSeries series3 = createTimeSeriesWithAlias("beta", Arrays.asList(3.0));
        TimeSeries series4 = createTimeSeriesWithAlias("", Arrays.asList(4.0)); // empty alias
        List<TimeSeries> input = Arrays.asList(series1, series2, series3, series4);

        SortStage sortStage = new SortStage(SortByType.NAME, SortOrderType.DESC);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: DESC order - delta > beta > "" (empty, both null and empty string)
        assertEquals(4, result.size());
        assertEquals("delta", getAlias(result.get(0))); // Highest
        assertEquals("beta", getAlias(result.get(1))); // Middle
        // Both series2 and series4 have empty string aliases, order between them is stable
        String alias2 = getAlias(result.get(2));
        String alias3 = getAlias(result.get(3));
        assertTrue((alias2 == null || alias2.equals("")) && (alias3 == null || alias3.equals("")));
    }

    public void testProcessWithSingleTimeSeries() {
        // Arrange
        SortStage sortStage = new SortStage(SortByType.AVG);
        List<TimeSeries> input = Arrays.asList(createLabeledTimeSeries("single", Arrays.asList(1.0, 2.0, 3.0)));

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert
        assertEquals(1, result.size());
        assertSame(input.get(0), result.get(0)); // Should return the same time series
    }

    public void testProcessWithEmptyTimeSeries() {
        // Arrange: Empty time series should not cause errors
        SortStage sortStage = new SortStage(SortByType.AVG);
        TimeSeries emptyTimeSeries = createLabeledTimeSeries("empty", new ArrayList<>());
        List<TimeSeries> input = Arrays.asList(emptyTimeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should return the series as-is
        assertEquals(1, result.size());
        assertEquals("empty", getLabel(result.get(0)));
    }

    public void testProcessSortByAvgComparingEmptyAndNonEmpty() {
        // Test AVG comparing empty vs non-empty time series - triggers calculation with empty samples
        SortStage sortStage = new SortStage(SortByType.AVG);
        TimeSeries emptyTimeSeries = createLabeledTimeSeries("empty", new ArrayList<>());
        TimeSeries nonEmptyTimeSeries = createLabeledTimeSeries("nonempty", Arrays.asList(5.0));
        List<TimeSeries> input = Arrays.asList(emptyTimeSeries, nonEmptyTimeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(2, result.size());
    }

    public void testProcessSortByCurrentComparingEmptyAndNonEmpty() {
        // Test CURRENT comparing empty vs non-empty - should return 0.0 for empty
        SortStage sortStage = new SortStage(SortByType.CURRENT);
        TimeSeries emptyTimeSeries = createLabeledTimeSeries("empty", new ArrayList<>());
        TimeSeries nonEmptyTimeSeries = createLabeledTimeSeries("nonempty", Arrays.asList(5.0));
        List<TimeSeries> input = Arrays.asList(emptyTimeSeries, nonEmptyTimeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(2, result.size());
    }

    public void testProcessSortByMaxComparingEmptyAndNonEmpty() {
        // Test MAX comparing empty vs non-empty - should return 0.0 for empty
        SortStage sortStage = new SortStage(SortByType.MAX);
        TimeSeries emptyTimeSeries = createLabeledTimeSeries("empty", new ArrayList<>());
        TimeSeries nonEmptyTimeSeries = createLabeledTimeSeries("nonempty", Arrays.asList(5.0));
        List<TimeSeries> input = Arrays.asList(emptyTimeSeries, nonEmptyTimeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(2, result.size());
    }

    public void testProcessSortByMinComparingEmptyAndNonEmpty() {
        // Test MIN comparing empty vs non-empty - should return 0.0 for empty
        SortStage sortStage = new SortStage(SortByType.MIN);
        TimeSeries emptyTimeSeries = createLabeledTimeSeries("empty", new ArrayList<>());
        TimeSeries nonEmptyTimeSeries = createLabeledTimeSeries("nonempty", Arrays.asList(5.0));
        List<TimeSeries> input = Arrays.asList(emptyTimeSeries, nonEmptyTimeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(2, result.size());
    }

    public void testProcessSortBySumComparingEmptyAndNonEmpty() {
        // Test SUM comparing empty vs non-empty - should return 0.0 for empty
        SortStage sortStage = new SortStage(SortByType.SUM);
        TimeSeries emptyTimeSeries = createLabeledTimeSeries("empty", new ArrayList<>());
        TimeSeries nonEmptyTimeSeries = createLabeledTimeSeries("nonempty", Arrays.asList(5.0));
        List<TimeSeries> input = Arrays.asList(emptyTimeSeries, nonEmptyTimeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(2, result.size());
    }

    public void testProcessSortByStddevComparingEmptyAndNonEmpty() {
        // Test STDDEV comparing empty vs non-empty - should return 0.0 for empty
        SortStage sortStage = new SortStage(SortByType.STDDEV);
        TimeSeries emptyTimeSeries = createLabeledTimeSeries("empty", new ArrayList<>());
        TimeSeries nonEmptyTimeSeries = createLabeledTimeSeries("nonempty", Arrays.asList(5.0, 10.0));
        List<TimeSeries> input = Arrays.asList(emptyTimeSeries, nonEmptyTimeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(2, result.size());
    }

    public void testProcessWithNaNValues() {
        // Arrange: NaN values should be handled correctly during sorting
        SortStage sortStage = new SortStage(SortByType.AVG);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, Double.NaN), new FloatSample(3000L, 3.0));
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(1, result.size());
        assertEquals("test", getLabel(result.get(0)));
    }

    public void testProcessWithNullSamples() {
        // Arrange: Null samples should be handled correctly during sorting
        SortStage sortStage = new SortStage(SortByType.AVG);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, 1.0), null, new FloatSample(3000L, 3.0));
        Labels labels = ByteLabels.fromMap(Map.of("label", "test"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "test");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(1, result.size());
        assertEquals("test", getLabel(result.get(0)));
    }

    // ========== SortBy Enum Tests ==========

    public void testSortByFromString() {
        assertEquals(SortByType.AVG, SortByType.fromString("avg"));
        assertEquals(SortByType.CURRENT, SortByType.fromString("current"));
        assertEquals(SortByType.MAX, SortByType.fromString("max"));
        assertEquals(SortByType.MIN, SortByType.fromString("min"));
        assertEquals(SortByType.SUM, SortByType.fromString("sum"));
        assertEquals(SortByType.STDDEV, SortByType.fromString("stddev"));
        assertEquals(SortByType.NAME, SortByType.fromString("name"));
    }

    public void testSortByFromStringInvalid() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortByType.fromString("invalid"));
        assertTrue(exception.getMessage().contains("Invalid sortby type"));
        assertTrue(exception.getMessage().contains("avg, current, max, min, name, stddev, sum"));
    }

    public void testSortByGetValue() {
        assertEquals("avg", SortByType.AVG.getValue());
        assertEquals("current", SortByType.CURRENT.getValue());
        assertEquals("max", SortByType.MAX.getValue());
        assertEquals("min", SortByType.MIN.getValue());
        assertEquals("sum", SortByType.SUM.getValue());
        assertEquals("stddev", SortByType.STDDEV.getValue());
        assertEquals("name", SortByType.NAME.getValue());
    }

    // ========== SortOrder Enum Tests ==========

    public void testSortOrderFromString() {
        assertEquals(SortOrderType.ASC, SortOrderType.fromString("asc"));
        assertEquals(SortOrderType.DESC, SortOrderType.fromString("desc"));
    }

    public void testSortOrderFromStringInvalid() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortOrderType.fromString("invalid"));
        assertTrue(exception.getMessage().contains("Invalid sort order type"));
        assertTrue(exception.getMessage().contains("asc, ascending, desc, descending"));
    }

    public void testSortOrderGetValue() {
        assertEquals("asc", SortOrderType.ASC.getValue());
        assertEquals("desc", SortOrderType.DESC.getValue());
    }

    // ========== Serialization Tests ==========

    public void testWriteToAndReadFrom() throws IOException {
        // Arrange
        SortStage original = new SortStage(SortByType.MAX, SortOrderType.ASC);
        BytesStreamOutput output = new BytesStreamOutput();

        // Act
        original.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        SortStage deserialized = SortStage.readFrom(input);

        // Assert
        assertEquals(original.getSortBy(), deserialized.getSortBy());
        assertEquals(original.getSortOrder(), deserialized.getSortOrder());
    }

    public void testToXContent() throws IOException {
        // Test XContent serialization
        SortStage stage = new SortStage(SortByType.SUM, SortOrderType.DESC);
        XContentBuilder builder = XContentFactory.jsonBuilder();

        builder.startObject();
        stage.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
        assertTrue("JSON should contain sortBy field", json.contains(SortStage.SORT_BY_ARG));
        assertTrue("JSON should contain sortOrder field", json.contains(SortStage.SORT_ORDER_ARG));
        assertTrue("JSON should contain sum", json.contains("sum"));
        assertTrue("JSON should contain desc", json.contains("desc"));
    }

    // ========== FromArgs Tests ==========

    public void testFromArgsValid() {
        // Arrange
        Map<String, Object> args = Map.of(SortStage.SORT_BY_ARG, "avg", SortStage.SORT_ORDER_ARG, "asc");

        // Act
        SortStage sortStage = SortStage.fromArgs(args);

        // Assert
        assertEquals(SortByType.AVG, sortStage.getSortBy());
        assertEquals(SortOrderType.ASC, sortStage.getSortOrder());
    }

    public void testFromArgsWithDefaultOrder() {
        // Arrange
        Map<String, Object> args = Map.of(SortStage.SORT_BY_ARG, "max");

        // Act
        SortStage sortStage = SortStage.fromArgs(args);

        // Assert
        assertEquals(SortByType.MAX, sortStage.getSortBy());
        assertEquals(SortOrderType.DESC, sortStage.getSortOrder()); // Default
    }

    public void testFromArgsMissingSortBy() {
        // Arrange
        Map<String, Object> args = Map.of(SortStage.SORT_ORDER_ARG, "asc");

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Sort stage requires '" + SortStage.SORT_BY_ARG + "' argument"));
    }

    public void testFromArgsNullArgs() {
        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortStage.fromArgs(null));
        assertTrue(exception.getMessage().contains("Sort stage requires '" + SortStage.SORT_BY_ARG + "' argument"));
    }

    public void testFromArgsNullSortBy() {
        // Arrange
        Map<String, Object> args = new HashMap<>();
        args.put(SortStage.SORT_BY_ARG, null);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortStage.fromArgs(args));
        assertEquals("SortBy cannot be null", exception.getMessage());
    }

    public void testFromArgsInvalidSortByType() {
        // Arrange
        Map<String, Object> args = Map.of(SortStage.SORT_BY_ARG, 123);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid type for '" + SortStage.SORT_BY_ARG + "' argument"));
    }

    public void testFromArgsInvalidSortOrderType() {
        // Arrange
        Map<String, Object> args = Map.of(SortStage.SORT_BY_ARG, "avg", SortStage.SORT_ORDER_ARG, 123);

        // Act & Assert
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> SortStage.fromArgs(args));
        assertTrue(exception.getMessage().contains("Invalid type for '" + SortStage.SORT_ORDER_ARG + "' argument"));
    }

    public void testFromArgsNullSortOrder() {
        // Arrange
        Map<String, Object> args = new HashMap<>();
        args.put(SortStage.SORT_BY_ARG, "avg");
        args.put(SortStage.SORT_ORDER_ARG, null);

        // Act
        SortStage sortStage = SortStage.fromArgs(args);

        // Assert - should use default order when null
        assertEquals(SortByType.AVG, sortStage.getSortBy());
        assertEquals(SortOrderType.DESC, sortStage.getSortOrder());
    }

    // ========== Interface Compliance Tests ==========

    public void testIsGlobalAggregation() {
        SortStage sortStage = new SortStage(SortByType.AVG);
        assertTrue(sortStage.isGlobalAggregation());
    }

    public void testIsCoordinatorOnly() {
        SortStage sortStage = new SortStage(SortByType.AVG);
        assertTrue(sortStage.isCoordinatorOnly());
    }

    public void testSupportConcurrentSegmentSearch() {
        SortStage sortStage = new SortStage(SortByType.AVG);
        assertFalse(sortStage.supportConcurrentSegmentSearch());
    }

    // ========== Edge Cases and Error Handling ==========

    public void testProcessWithVeryLargeValues() {
        // Test with very large values - should not overflow
        SortStage sortStage = new SortStage(SortByType.MAX);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.MAX_VALUE), new FloatSample(2000L, Double.MIN_VALUE));
        Labels labels = ByteLabels.fromMap(Map.of("label", "large"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "large");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(1, result.size());
        assertEquals("large", getLabel(result.get(0)));
    }

    public void testProcessWithAllNaNValues() {
        // Test with all NaN values - should handle gracefully
        SortStage sortStage = new SortStage(SortByType.AVG);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.NaN), new FloatSample(2000L, Double.NaN));
        Labels labels = ByteLabels.fromMap(Map.of("label", "allnan"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "allnan");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(1, result.size());
        assertEquals("allnan", getLabel(result.get(0)));
    }

    public void testProcessWithAllNullSamples() {
        // Test with all null samples - should handle gracefully
        SortStage sortStage = new SortStage(SortByType.AVG);
        List<Sample> samples = Arrays.asList(null, null);
        Labels labels = ByteLabels.fromMap(Map.of("label", "allnull"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "allnull");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(1, result.size());
        assertEquals("allnull", getLabel(result.get(0)));
    }

    public void testProcessSortByCurrentWithTrailingNaN() {
        // Test CURRENT with NaN at the end - verify it sorts correctly
        SortStage sortStage = new SortStage(SortByType.CURRENT);
        // Series A: last value is 5.0 (has trailing NaN)
        // Series B: last value is 10.0 (no NaN)
        TimeSeries seriesA = createTimeSeriesWithSamples(
            "A",
            Arrays.asList(new FloatSample(1000L, 1.0), new FloatSample(2000L, 5.0), new FloatSample(3000L, Double.NaN))
        );
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(10.0));
        List<TimeSeries> input = Arrays.asList(seriesA, seriesB);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: B should be first (current=10.0), then A (current=5.0)
        assertEquals(2, result.size());
        assertEquals("B", getLabel(result.get(0)));
        assertEquals("A", getLabel(result.get(1)));
    }

    public void testProcessSortByCurrentWithAllNaN() {
        // Test CURRENT with all NaN values - should handle gracefully
        SortStage sortStage = new SortStage(SortByType.CURRENT);
        TimeSeries series = createTimeSeriesWithSamples(
            "nanonly",
            Arrays.asList(new FloatSample(1000L, Double.NaN), new FloatSample(2000L, Double.NaN))
        );
        List<TimeSeries> input = Arrays.asList(series);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(1, result.size());
        assertEquals("nanonly", getLabel(result.get(0)));
    }

    public void testProcessSortByMinWithEmpty() {
        // Test MIN with empty time series - should handle gracefully
        SortStage sortStage = new SortStage(SortByType.MIN);
        TimeSeries emptyTimeSeries = createLabeledTimeSeries("empty", new ArrayList<>());
        List<TimeSeries> input = Arrays.asList(emptyTimeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(1, result.size());
        assertEquals("empty", getLabel(result.get(0)));
    }

    public void testProcessSortByStddevWithSingleValue() {
        // Test STDDEV with single value - should sort correctly
        SortStage sortStage = new SortStage(SortByType.STDDEV);
        // Single value series should have stddev=0, multi-value series should have higher stddev
        TimeSeries singleValueSeries = createLabeledTimeSeries("single", Arrays.asList(5.0));
        TimeSeries multiValueSeries = createLabeledTimeSeries("multi", Arrays.asList(1.0, 10.0));
        List<TimeSeries> input = Arrays.asList(singleValueSeries, multiValueSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: multi should be first (higher stddev), single should be last (stddev=0)
        assertEquals(2, result.size());
        assertEquals("multi", getLabel(result.get(0)));
        assertEquals("single", getLabel(result.get(1)));
    }

    public void testProcessSortByStddevWithIdenticalValues() {
        // Test STDDEV with identical values - should sort correctly
        SortStage sortStage = new SortStage(SortByType.STDDEV);
        // Identical values should have stddev=0, varying values should have higher stddev
        TimeSeries identicalSeries = createLabeledTimeSeries("identical", Arrays.asList(5.0, 5.0, 5.0));
        TimeSeries varyingSeries = createLabeledTimeSeries("varying", Arrays.asList(1.0, 5.0, 10.0));
        List<TimeSeries> input = Arrays.asList(identicalSeries, varyingSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: varying should be first (higher stddev), identical should be last (stddev=0)
        assertEquals(2, result.size());
        assertEquals("varying", getLabel(result.get(0)));
        assertEquals("identical", getLabel(result.get(1)));
    }

    public void testProcessSortByAvgWithAllNaNAndNullSamples() {
        // Test AVG with all NaN/null values - should handle count=0 case
        SortStage sortStage = new SortStage(SortByType.AVG);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.NaN), null, new FloatSample(3000L, Double.NaN));
        Labels labels = ByteLabels.fromMap(Map.of("label", "allnanandnull"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "allnanandnull");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors and return 0.0 for count=0
        assertEquals(1, result.size());
        assertEquals("allnanandnull", getLabel(result.get(0)));
    }

    public void testProcessSortByMaxWithAllNaNAndNullSamples() {
        // Test MAX when all samples are NaN/null - should return 0.0
        SortStage sortStage = new SortStage(SortByType.MAX);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.NaN), null);
        Labels labels = ByteLabels.fromMap(Map.of("label", "maxnanonly"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "maxnanonly");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(1, result.size());
        assertEquals("maxnanonly", getLabel(result.get(0)));
    }

    public void testProcessSortByMinWithAllNaNAndNullSamples() {
        // Test MIN when all samples are NaN/null - should return 0.0
        SortStage sortStage = new SortStage(SortByType.MIN);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.NaN), null);
        Labels labels = ByteLabels.fromMap(Map.of("label", "minnanonly"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "minnanonly");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(1, result.size());
        assertEquals("minnanonly", getLabel(result.get(0)));
    }

    public void testProcessSortBySumWithAllNaNAndNullSamples() {
        // Test SUM with all NaN/null values
        SortStage sortStage = new SortStage(SortByType.SUM);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.NaN), null);
        Labels labels = ByteLabels.fromMap(Map.of("label", "sumnanonly"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 2000L, 1000L, "sumnanonly");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(1, result.size());
        assertEquals("sumnanonly", getLabel(result.get(0)));
    }

    public void testProcessSortByStddevWithAllNaNAndNullSamples() {
        // Test STDDEV with all NaN/null values
        SortStage sortStage = new SortStage(SortByType.STDDEV);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.NaN), null, new FloatSample(3000L, Double.NaN));
        Labels labels = ByteLabels.fromMap(Map.of("label", "stddevnanonly"));
        TimeSeries timeSeries = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "stddevnanonly");
        List<TimeSeries> input = Arrays.asList(timeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(1, result.size());
        assertEquals("stddevnanonly", getLabel(result.get(0)));
    }

    public void testProcessSortByCurrentWithAllNaNAndNullSamples() {
        // Test CURRENT when all samples are NaN/null - should return 0.0
        // Need 2 series to trigger comparison
        SortStage sortStage = new SortStage(SortByType.CURRENT);
        List<Sample> samples = Arrays.asList(new FloatSample(1000L, Double.NaN), new FloatSample(3000L, Double.NaN));
        Labels labels = ByteLabels.fromMap(Map.of("label", "currentnanonly"));
        TimeSeries timeSeriesNanOnly = new TimeSeries(samples, labels, 1000L, 3000L, 1000L, "currentnanonly");
        TimeSeries normalTimeSeries = createLabeledTimeSeries("normal", Arrays.asList(5.0));
        List<TimeSeries> input = Arrays.asList(timeSeriesNanOnly, normalTimeSeries);

        // Act
        List<TimeSeries> result = sortStage.process(input);

        // Assert: Should process without errors
        assertEquals(2, result.size());
    }

    // ========== Helper Methods ==========

    /**
     * Creates a standard set of three test time series (A, B, C) for sorting tests.
     * Series A: [1.0, 1.0] -> avg=1.0, max=1.0, sum=2.0, current=1.0
     * Series B: [1.0, 2.0, 3.0] -> avg=2.0, max=3.0, sum=6.0, current=3.0
     * Series C: [1.0, 2.0] -> avg=1.5, max=2.0, sum=3.0, current=2.0
     */
    private List<TimeSeries> createTestSeriesABC() {
        TimeSeries seriesA = createLabeledTimeSeries("A", Arrays.asList(1.0, 1.0));
        TimeSeries seriesB = createLabeledTimeSeries("B", Arrays.asList(1.0, 2.0, 3.0));
        TimeSeries seriesC = createLabeledTimeSeries("C", Arrays.asList(1.0, 2.0));
        return Arrays.asList(seriesA, seriesB, seriesC);
    }

    /**
     * Creates a time series with a label identifier for testing.
     * This allows tests to verify sorting behavior by checking the order of labels
     * instead of recalculating expected values.
     */
    private TimeSeries createLabeledTimeSeries(String label, List<Double> values) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            samples.add(new FloatSample(1000L + i * 1000L, values.get(i)));
        }
        Labels labels = ByteLabels.fromMap(Map.of("label", label));
        long endTime = values.isEmpty() ? 1000L : 1000L + (values.size() - 1) * 1000L;
        return new TimeSeries(samples, labels, 1000L, endTime, 1000L, label);
    }

    /**
     * Creates a time series with custom samples and a label identifier.
     */
    private TimeSeries createTimeSeriesWithSamples(String label, List<Sample> samples) {
        Labels labels = ByteLabels.fromMap(Map.of("label", label));
        long endTime = samples.isEmpty() ? 1000L : samples.get(samples.size() - 1).getTimestamp();
        return new TimeSeries(samples, labels, 1000L, endTime, 1000L, label);
    }

    /**
     * Extracts the label from a time series for verification purposes.
     */
    private String getLabel(TimeSeries timeSeries) {
        return timeSeries.getLabels().get("label");
    }

    /**
     * Creates a time series with an alias for testing NAME sorting.
     * The alias is set as the TimeSeries alias field (extractAlias uses getAlias() directly).
     */
    private TimeSeries createTimeSeriesWithAlias(String alias, List<Double> values) {
        List<Sample> samples = new ArrayList<>();
        for (int i = 0; i < values.size(); i++) {
            samples.add(new FloatSample(1000L + i * 1000L, values.get(i)));
        }
        Labels labels = ByteLabels.emptyLabels();
        long endTime = values.isEmpty() ? 1000L : 1000L + (values.size() - 1) * 1000L;
        return new TimeSeries(samples, labels, 1000L, endTime, 1000L, alias);
    }

    /**
     * Extracts the "name" tag from a time series for verification purposes.
     */
    private String getNameLabel(TimeSeries timeSeries) {
        return timeSeries.getLabels().get("name");
    }

    /**
     * Extracts the alias from a time series for verification purposes.
     */
    private String getAlias(TimeSeries timeSeries) {
        return timeSeries.getAlias();
    }

    /**
     * Creates a time series without an alias (null) for testing NAME sorting with nulls.
     */
    private TimeSeries createTimeSeriesWithoutAlias(List<Double> values) {
        return createTimeSeriesWithAlias(null, values);
    }

    /**
     * Test equals method for SortStage.
     */
    public void testEquals() {
        SortStage stage1 = new SortStage(SortByType.AVG, SortOrderType.DESC);
        SortStage stage2 = new SortStage(SortByType.AVG, SortOrderType.DESC);

        assertEquals("Equal SortStages should be equal", stage1, stage2);

        SortStage stageDiffSortBy = new SortStage(SortByType.MAX, SortOrderType.DESC);
        assertNotEquals("Different sortBy should not be equal", stage1, stageDiffSortBy);

        SortStage stageDiffSortOrder = new SortStage(SortByType.AVG, SortOrderType.ASC);
        assertNotEquals("Different sortOrder should not be equal", stage1, stageDiffSortOrder);

        assertEquals("Stage should equal itself", stage1, stage1);

        assertNotEquals("Stage should not equal null", null, stage1);

        assertNotEquals("Stage should not equal different class", "string", stage1);

        for (SortByType sortBy : SortByType.values()) {
            for (SortOrderType sortOrder : SortOrderType.values()) {
                SortStage stage3 = new SortStage(sortBy, sortOrder);
                SortStage stage4 = new SortStage(sortBy, sortOrder);
                assertEquals("Stages with same sortBy " + sortBy + " and sortOrder " + sortOrder + " should be equal", stage3, stage4);
            }
        }

        SortStage stageDefault1 = new SortStage(SortByType.SUM);
        SortStage stageDefault2 = new SortStage(SortByType.SUM);
        assertEquals("Stages with default sort order should be equal", stageDefault1, stageDefault2);
    }

    @Override
    protected Writeable.Reader<SortStage> instanceReader() {
        return SortStage::readFrom;
    }

    @Override
    protected SortStage createTestInstance() {
        return new SortStage(randomFrom(SortByType.values()), randomFrom(SortOrderType.values()));
    }
}
