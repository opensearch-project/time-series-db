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
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link RegexReplacementUtil}.
 * Covers backreference functionality shared by aliasSub and tagSub stages.
 */
public class RegexReplacementUtilTests extends OpenSearchTestCase {

    public void testNoMatch() {
        Pattern pattern = Pattern.compile("nomatch");
        Matcher matcher = pattern.matcher("original");
        String result = RegexReplacementUtil.replaceAll("original", matcher, "replacement");
        assertEquals("original", result);
    }

    public void testSimpleReplacementWithoutBackreference() {
        Pattern pattern = Pattern.compile("prod");
        Matcher matcher = pattern.matcher("prod-env");
        String result = RegexReplacementUtil.replaceAll("prod-env", matcher, "production");
        assertEquals("production-env", result);
    }

    public void testBackreferenceWithBackslashSyntax() {
        Pattern pattern = Pattern.compile("^prod-(.*)$");
        Matcher matcher = pattern.matcher("prod-east");
        String result = RegexReplacementUtil.replaceAll("prod-east", matcher, "production-\\1");
        assertEquals("production-east", result);
    }

    public void testMultipleBackreferences() {
        Pattern pattern = Pattern.compile("^(\\w+)-(\\w+)-(\\w+)$");
        Matcher matcher = pattern.matcher("prod-us-east");
        String result = RegexReplacementUtil.replaceAll("prod-us-east", matcher, "\\1_\\2_\\3");
        assertEquals("prod_us_east", result);
    }

    public void testBackreferenceZeroForFullMatch() {
        Pattern pattern = Pattern.compile("prod");
        Matcher matcher = pattern.matcher("prod");
        String result = RegexReplacementUtil.replaceAll("prod", matcher, "prefix-\\0-suffix");
        assertEquals("prefix-prod-suffix", result);
    }

    public void testInvalidBackreferenceGroupNumber() {
        Pattern pattern = Pattern.compile("^(\\w+)-(\\w+)$");
        Matcher matcher = pattern.matcher("prod-east");
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> RegexReplacementUtil.replaceAll("prod-east", matcher, "\\9")
        );
        assertTrue(exception.getMessage().contains("Invalid group reference"));
        assertTrue(exception.getMessage().contains("\\9"));
    }

    public void testBackreferenceToEmptyGroup() {
        Pattern pattern = Pattern.compile("^prod(-(.*))?$");
        Matcher matcher = pattern.matcher("prod");
        String result = RegexReplacementUtil.replaceAll("prod", matcher, "production\\2");
        assertEquals("production", result);
    }

    public void testBackreferenceReordering() {
        Pattern pattern = Pattern.compile("^(\\w+)-(\\w+)$");
        Matcher matcher = pattern.matcher("us-east");
        String result = RegexReplacementUtil.replaceAll("us-east", matcher, "\\2-\\1");
        assertEquals("east-us", result);
    }

    public void testDuplicateBackreferences() {
        Pattern pattern = Pattern.compile("^(\\w+)$");
        Matcher matcher = pattern.matcher("prod");
        String result = RegexReplacementUtil.replaceAll("prod", matcher, "\\1-\\1-\\1");
        assertEquals("prod-prod-prod", result);
    }

    public void testBackreferenceComplexPattern() {
        Pattern pattern = Pattern.compile("^v([0-9]+)\\.([0-9]+)\\.([0-9]+)$");
        Matcher matcher = pattern.matcher("v1.2.3");
        String result = RegexReplacementUtil.replaceAll("v1.2.3", matcher, "version-\\1-\\2-\\3");
        assertEquals("version-1-2-3", result);
    }

    public void testBackreferenceWithSpecialCharsInReplacement() {
        Pattern pattern = Pattern.compile("^(\\w+)$");
        Matcher matcher = pattern.matcher("prod");
        String result = RegexReplacementUtil.replaceAll("prod", matcher, "[\\1]");
        assertEquals("[prod]", result);
    }

    public void testRemoveSubstring() {
        Pattern pattern = Pattern.compile("-v[0-9]+$");
        Matcher matcher = pattern.matcher("api-v123");
        String result = RegexReplacementUtil.replaceAll("api-v123", matcher, "");
        assertEquals("api", result);
    }

    public void testExtractSubstring() {
        Pattern pattern = Pattern.compile("^([a-z]+-[a-z]+-[0-9]+)-.*$");
        Matcher matcher = pattern.matcher("us-east-1-host123");
        String result = RegexReplacementUtil.replaceAll("us-east-1-host123", matcher, "\\1");
        assertEquals("us-east-1", result);
    }

    public void testDollarStyleBackreference() {
        Pattern pattern = Pattern.compile("^(\\w+)-(\\w+)$");
        Matcher matcher = pattern.matcher("us-east");
        String result = RegexReplacementUtil.replaceAll("us-east", matcher, "$2-$1");
        assertEquals("east-us", result);
    }

    public void testEscapedDollarWithDoubleDollar() {
        // $$ in replacement should become literal $
        Pattern pattern = Pattern.compile("price");
        Matcher matcher = pattern.matcher("price100");
        String result = RegexReplacementUtil.replaceAll("price100", matcher, "$$");
        assertEquals("$100", result);
    }

    public void testEscapedDollarWithBackslashDollar() {
        // \$ in replacement should become literal $
        Pattern pattern = Pattern.compile("price");
        Matcher matcher = pattern.matcher("price100");
        String result = RegexReplacementUtil.replaceAll("price100", matcher, "\\$");
        assertEquals("$100", result);
    }

    public void testEscapedDollarInMiddleOfReplacement() {
        // $$ in the middle of replacement should become literal $
        Pattern pattern = Pattern.compile("price");
        Matcher matcher = pattern.matcher("price");
        String result = RegexReplacementUtil.replaceAll("price", matcher, "cost:$$");
        assertEquals("cost:$", result);
    }

    public void testMixedEscapedDollarAndBackreference() {
        // \$ should become literal $, and $1 should be a backreference
        Pattern pattern = Pattern.compile("(\\w+)");
        Matcher matcher = pattern.matcher("value");
        String result = RegexReplacementUtil.replaceAll("value", matcher, "\\$$1");
        assertEquals("$value", result);
    }
}
