/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3;

import org.opensearch.tsdb.lang.m3.dsl.SourceBuilderVisitor;
import org.opensearch.tsdb.metrics.TSDBMetrics;
import org.opensearch.tsdb.query.rest.RemoteIndexSettingsCache;
import org.opensearch.tsdb.query.rest.RestM3QLAction;

import java.util.List;

/** M3QL-specific TSDB metrics initializers. */
public class M3QLMetrics {
    public static List<TSDBMetrics.MetricsInitializer> getMetricsInitializers() {
        return List.of(
            RestM3QLAction.getMetricsInitializer(),
            SourceBuilderVisitor.getMetricsInitializer(),
            RemoteIndexSettingsCache.getMetricsInitializer()
        );
    }
}
