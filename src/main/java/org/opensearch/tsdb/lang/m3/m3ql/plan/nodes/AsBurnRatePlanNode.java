/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.tsdb.lang.m3.m3ql.plan.nodes;

import org.opensearch.tsdb.lang.m3.common.M3Duration;

import java.time.Duration;
import java.util.Locale;

/**
 * Plan node for the asBurnRate / burnRate binary function.
 *
 * <p>Syntax: {@code unexpected | asBurnRate (total) <interval> <slo>}
 * where interval is a duration (e.g. "1d", "24h") and slo is a percentage (e.g. 99.9).</p>
 *
 * <p>This is a binary plan node: child 0 is the "unexpected" (error) pipeline,
 * child 1 is the "total" (traffic) pipeline.</p>
 */
public class AsBurnRatePlanNode extends BinaryPlanNode {

    private final String interval;
    private final double slo;
    private final Duration intervalDuration;

    public AsBurnRatePlanNode(int id, String interval, double slo) {
        super(id);
        this.interval = interval;
        this.slo = slo;
        Duration window = M3Duration.valueOf(interval);
        if (window.isNegative() || window.isZero()) {
            throw new IllegalArgumentException("Interval must be positive: " + interval);
        }
        this.intervalDuration = window;
    }

    public Duration getIntervalDuration() {
        return intervalDuration;
    }

    public long getIntervalMillis() {
        return intervalDuration.toMillis();
    }

    public String getInterval() {
        return interval;
    }

    public double getSlo() {
        return slo;
    }

    @Override
    public String getExplainName() {
        return String.format(Locale.ROOT, "AS_BURN_RATE(interval=%s,slo=%s)", interval, slo);
    }
}
