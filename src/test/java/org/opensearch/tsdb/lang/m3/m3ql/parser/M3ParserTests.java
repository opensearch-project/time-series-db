/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.parser;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.tsdb.TestUtils;
import org.opensearch.tsdb.lang.m3.M3TestUtils;
import org.opensearch.tsdb.lang.m3.m3ql.parser.generated.M3QLParser;
import org.opensearch.tsdb.lang.m3.m3ql.parser.nodes.RootNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NavigableMap;

public class M3ParserTests extends OpenSearchTestCase {

    /**
     * Tests the AST generation by comparing the generated AST with expected results for a set of m3ql queries.
     */
    public void testASTGeneration() {
        try {
            NavigableMap<String, String> queries = TestUtils.getResourceFilesWithExtension("lang/m3/queries", ".m3ql");
            NavigableMap<String, String> expectedResults = TestUtils.getResourceFilesWithExtension("lang/m3/ast", ".txt");

            assertEquals("Number of query files does not match result files", queries.size(), expectedResults.size());

            Iterator<String> queryIterator = queries.values().iterator();
            Iterator<String> resultIterator = expectedResults.values().iterator();
            int count = 0;
            while (queryIterator.hasNext()) {
                String query = queryIterator.next();
                String expected = resultIterator.next();
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                    PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8);
                    M3TestUtils.printAST(M3QLParser.parse(query, true), 0, ps);
                    assertEquals("AST does not match for test case: " + count++, expected, baos.toString(StandardCharsets.UTF_8));
                } catch (Exception e) {
                    fail("Failed to parse query: " + query + " with error: " + e.getMessage());
                }
            }
        } catch (IOException | URISyntaxException e) {
            fail("Failed to run parser tests: " + e.getMessage());
        }
    }

    public void testRoundTrip() {
        String[] queries = {
            "fetch city_id:1 | transformNull | moving 1m sum | sum region | avg",
            "fetch city_name:\"San Francisco\" host:{\"host1\", \"host2\"} | sum merchantID | transformNull | moving 1m sum",
            "a = fetch city_id:1 | transformNull; b = fetch city_id:2 | transformNull; a | asPercent(b)" };

        for (String query : queries) {
            try {
                RootNode astRoot = M3QLParser.parse(query, false);
                M3QLExpressionPrinter printer = new M3QLExpressionPrinter();
                String outputExpression = astRoot.accept(printer);
                String expectedQuery = query.replaceAll("\\s+", " ").trim();
                assertEquals("Output m3ql expression does not match", expectedQuery, outputExpression);
            } catch (Exception e) {
                fail("Failed to parse and round trip query: " + query + " with error: " + e.getMessage());
            }
        }
    }
}
