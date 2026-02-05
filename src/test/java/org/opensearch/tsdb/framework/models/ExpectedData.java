/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.framework.models;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Expected data configuration for time series testing
 */
public record ExpectedData(@JsonProperty("metric") Map<String, String> metric, @JsonProperty("values") Double[] values,
    @JsonProperty("alias") String alias) {
}
