# Guide: Applying Metric 1 Improvements

This guide helps you apply the shared improvements (trace logging removal, parsing latency metric, metric renaming) to the Metric 1 branch.

## Step 1: Stash Current Metric 2 Changes

```bash
cd opensearch-tsdb-internal-fork
git stash push -m "Metric 2: searchable latency listener + all improvements"
```

## Step 2: Switch to Metric 1 Branch

```bash
git checkout sakrah/ingestion-lag-metrics
```

## Step 3: Apply Metric 1-Specific Changes

The following files need to be updated for Metric 1:

### Files to Update:

1. **`src/main/java/org/opensearch/tsdb/metrics/TSDBIngestionLagMetrics.java`**
   - Use `METRIC1_TSDBIngestionLagMetrics.java` (only has `lagUntilCoordinator` and `parsingLatency`, NO `lagUntilSearchable`)

2. **`src/test/java/org/opensearch/tsdb/metrics/TSDBIngestionLagMetricsTests.java`**
   - Use `METRIC1_TSDBIngestionLagMetricsTests.java` (only tests 2 metrics, not 3)

3. **`src/main/java/org/opensearch/tsdb/action/TSDBIngestionLagActionFilter.java`**
   - Copy from current branch (already correct - uses `lagUntilCoordinator` and `parsingLatency`)

4. **`src/test/java/org/opensearch/tsdb/action/TSDBIngestionLagActionFilterTests.java`**
   - Copy from current branch (already correct - tests `lagUntilCoordinator` and `parsingLatency`)

5. **`src/main/java/org/opensearch/tsdb/metrics/TSDBMetricsConstants.java`**
   - Add parsing latency constant (shared between both metrics)

## Step 4: Verify Changes

```bash
./gradlew :compileJava :compileTestJava
./gradlew :test --tests TSDBIngestionLagActionFilterTests --tests TSDBIngestionLagMetricsTests -x jacocoTestCoverageVerification
```

## Step 5: Commit and Push Metric 1

```bash
git add .
git commit -S -m "Remove trace logging, add parsing latency metric, rename metrics

- Remove trace logging from TSDBIngestionLagActionFilter (aligns with TSDB plugin pattern)
- Add parsing latency metric to track time taken to parse bulk requests
- Rename lagReachesOs to lagUntilCoordinator for clarity
- Add test coverage for previously uncovered code paths"
git push origin sakrah/ingestion-lag-metrics
```

## Step 6: Switch Back to Metric 2 and Rebase

```bash
git checkout sakrah/ingestion-lag-searchable-metric
git stash pop
git rebase sakrah/ingestion-lag-metrics
```

## Step 7: Resolve Conflicts (if any)

After rebase, you may need to:
- Ensure `TSDBIngestionLagMetrics.java` has all 3 metrics (`lagUntilCoordinator`, `lagUntilSearchable`, `parsingLatency`)
- Ensure `TSDBIngestionLagMetricsTests.java` tests all 3 metrics
- Verify `TSDBIngestionLagIndexingListener.java` uses `lagUntilSearchable`

## Step 8: Verify Metric 2 Still Works

```bash
./gradlew :compileJava :compileTestJava
./gradlew :test --tests TSDBIngestionLagActionFilterTests --tests TSDBIngestionLagIndexingListenerTests --tests TSDBIngestionLagMetricsTests -x jacocoTestCoverageVerification
```
