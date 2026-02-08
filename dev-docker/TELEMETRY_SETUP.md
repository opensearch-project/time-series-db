# OpenTelemetry Telemetry + Prometheus Setup

This guide shows how to export TSDB plugin metrics from OpenSearch to Prometheus using OpenTelemetry.

## Architecture

```
OpenSearch TSDB Plugin (with OTEL Telemetry)
    ↓ (OTLP gRPC on port 4317)
OpenTelemetry Collector
    ↓ (Prometheus exposition format on port 9464)
Prometheus (scrapes every 10s)
    ↓ (Prometheus datasource)
Grafana Dashboards (port 3000)
    ↓
View beautiful TSDB metrics visualizations
```

## What You Get

The TSDB plugin exposes comprehensive metrics via OpenTelemetry, some examples:

**Engine Metrics:**
- `tsdb.samples.ingested.total` - Total samples ingested
- `tsdb.series.created.total` - Total time series created
- `tsdb.series.open` - Current open series count
- `tsdb.memchunks.created.total` - Memory chunks created
- `tsdb.flush.latency` - Flush operation latency

**Aggregation Metrics:**
- `tsdb.aggregation.collect.latency` - Collection latency
- `tsdb.aggregation.docs.total` - Documents processed
- `tsdb.aggregation.circuit_breaker.bytes` - Circuit breaker memory usage
- `tsdb.aggregation.circuit_breaker.trips.total` - Circuit breaker trips

**Query Metrics:**
- `tsdb.action.rest.queries.execution.latency` - Query execution time
- `tsdb.action.rest.queries.collect_phase.latency.max` - Max collect phase latency
- `tsdb.action.rest.queries.reduce_phase.latency.max` - Max reduce phase latency

## Setup Steps

### 1. Build OpenSearch with telemetry-otel Plugin

The telemetry-otel plugin is bundled with OpenSearch core but needs to be built and installed.

```bash
# Build custom OpenSearch, telemetry-otel, and TSDB plugins
OPENSEARCH_HOME=~/OpenSearch BUILD_METHOD=tar ./build-custom.sh
```

### 2. Build and Start Docker Stack

**IMPORTANT:** You must use the same docker-compose file for both build and run commands.

**Option A: Single Node (Recommended for Development)**

```bash
# Build single node with telemetry-otel plugin
docker-compose -f docker-compose.single.yml build --no-cache

# Start single node
docker-compose -f docker-compose.single.yml up -d

# Watch logs
docker-compose -f docker-compose.single.yml logs -f
```

**Option B: 2-Node Cluster**

```bash
# Build 2-node cluster with telemetry-otel plugin
docker-compose build --no-cache

# Start 2-node cluster
docker-compose up -d

# Watch logs
docker-compose logs -f
```

This starts:
- `opensearch-node1` (port 9200) with OTEL telemetry enabled
- `opensearch-node2` (port 9201) with OTEL telemetry enabled
- `otel-collector` (port 4317 for OTLP, 9464 for Prometheus)
- `prometheus` (port 9090)
- `grafana` (port 3000) with pre-configured TSDB dashboard

### 3. Verify OpenSearch Started

**Check that telemetry-otel plugin is installed:**
```bash
curl localhost:9200/_cat/plugins?v
```

Expected output should include both plugins:
```
opensearch-node1 tsdb          3.5.0.0-SNAPSHOT
opensearch-node1 telemetry-otel 3.5.0-SNAPSHOT
opensearch-node2 tsdb          3.5.0.0-SNAPSHOT
opensearch-node2 telemetry-otel 3.5.0-SNAPSHOT
```

**Check cluster health:**

```bash
curl localhost:9200/_cluster/health?pretty
```

Expected: `"status": "green"` and `"number_of_nodes": 2`

### 4. Create TSDB Index and Generate Metrics

```bash
# Create TSDB-enabled index
curl -X PUT "localhost:9200/metrics-test" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index": {
      "tsdb_engine": {
        "enabled": true
      }
    }
  }
}'

# Index some sample data to generate metrics
curl -X POST "localhost:9200/metrics-test/_doc" -H 'Content-Type: application/json' -d'
{
  "timestamp": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'",
  "cpu_usage": 45.2,
  "memory_usage": 78.5
}'
```

### 5. Access Grafana Dashboard

**Open Grafana:** http://localhost:3000

**Login credentials:**
- Username: `admin`
- Password: `admin`

**View TSDB Dashboard:**
1. Click on "Dashboards" in the left menu
2. Select "OpenSearch TSDB Metrics"
3. Dashboard auto-refreshes every 10 seconds

**What you'll see:**
- Sample Ingestion Rate
- Open Time Series (current count)
- Time Series Creation Rate
- Memory Chunk Creation Rate
- Out-of-Order Samples Rejected
- Flush Latency (with percentiles)
- Aggregation Collect Latency
- Aggregation Documents Processed
- Circuit Breaker Memory Usage
- Circuit Breaker Trips
- Query Execution Latency (p50, p95, p99)
- Query Collect Phase Latency
- Query Reduce Phase Latency

### 6. (Optional) Check Prometheus

Open browser: http://localhost:9090

**Verify targets are UP:**
- Go to Status → Targets
- Check `opensearch-tsdb-otel` is in state `UP`

**Query TSDB metrics:**
```promql
# See all OpenSearch metrics
{namespace="opensearch"}

# Query specific TSDB metrics
opensearch_tsdb_samples_ingested_total
opensearch_tsdb_series_created_total
opensearch_tsdb_aggregation_collect_latency
```

### 6. Check OTEL Collector Logs

```bash
# Verify OTLP metrics are being received
docker logs otel-collector

# Should see lines like:
# Metric #0: tsdb.samples.ingested.total
# Metric #1: tsdb.series.created.total
```

## Configuration Files

### otel-collector-config.yml
- Receives OTLP metrics from OpenSearch on port 4317
- Exports to Prometheus format on port 9464
- Adds namespace prefix: `opensearch_`

### prometheus.yml
- Scrapes OTEL collector every 10 seconds
- Stores metrics in `/prometheus` volume
- Web UI available at http://localhost:9090

### docker-compose.yml
Environment variables set on OpenSearch nodes:
```yaml
- telemetry.otel.metrics.exporter.class=io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
- OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
- OTEL_METRICS_EXPORTER=otlp
- OTEL_SERVICE_NAME=opensearch-node1
```

## Troubleshooting

### Error: "unknown setting [telemetry.otel.metrics.exporter.class]"

This means the telemetry-otel plugin is not installed. Common causes:

**1. Built with wrong docker-compose file:**
You must use the SAME file for build and run:
```bash
# If running single node - use single file for both
docker-compose -f docker-compose.single.yml build --no-cache
docker-compose -f docker-compose.single.yml up -d

# If running cluster - use default file for both
docker-compose build --no-cache
docker-compose up -d
```

**2. Need to rebuild with telemetry plugin:**
```bash
# Rebuild everything with telemetry plugin
./build-custom.sh

# Single node
docker-compose -f docker-compose.single.yml build --no-cache
docker-compose -f docker-compose.single.yml up -d

# OR cluster
docker-compose build --no-cache
docker-compose up -d

# Verify plugin is installed
curl localhost:9200/_cat/plugins?v | grep telemetry
```

### No metrics in Prometheus

1. **Check OTEL collector is receiving data:**
   ```bash
   docker logs otel-collector | grep -i metric
   ```

2. **Check Prometheus target status:**
   - Open http://localhost:9090/targets
   - Should see `opensearch-tsdb-otel (1/1 up)`

3. **Check OpenSearch logs for OTEL errors:**
   ```bash
   docker-compose logs opensearch-node1 | grep -i telemetry
   ```

### OTEL collector not starting

```bash
# Check config syntax
docker logs otel-collector

# Common issue: YAML indentation errors in otel-collector-config.yml
```

### Metrics have wrong namespace

The OTEL collector config adds `namespace: opensearch` prefix.
Metrics appear as: `opensearch_tsdb_samples_ingested_total`

## Port Reference

| Service | Port | Purpose |
|---------|------|---------|
| OpenSearch Node 1 | 9200 | HTTP API |
| OpenSearch Node 2 | 9201 | HTTP API |
| OTEL Collector | 4317 | OTLP gRPC receiver |
| OTEL Collector | 4318 | OTLP HTTP receiver |
| OTEL Collector | 9464 | Prometheus exporter |
| Prometheus | 9090 | Web UI |
| Grafana | 3000 | Dashboard UI |

## Customizing the Dashboard

The dashboard is fully editable in Grafana:

1. Click "Settings" (gear icon) in the top right
2. Edit panels by clicking the panel title → Edit
3. Add new panels with "Add panel" button
4. Save changes with "Save dashboard" button

**Available TSDB metrics to add:**
```promql
opensearch_tsdb_samples_ingested_total
opensearch_tsdb_series_created_total
opensearch_tsdb_series_open
opensearch_tsdb_memchunks_created_total
opensearch_tsdb_ooo_samples_rejected_total
opensearch_tsdb_flush_latency
opensearch_tsdb_commit_total
opensearch_tsdb_aggregation_collect_latency
opensearch_tsdb_aggregation_docs_total
opensearch_tsdb_aggregation_chunks_total
opensearch_tsdb_aggregation_samples_total
opensearch_tsdb_aggregation_circuit_breaker_bytes
opensearch_tsdb_aggregation_circuit_breaker_trips_total
opensearch_tsdb_action_rest_queries_execution_latency
opensearch_tsdb_action_rest_queries_collect_phase_latency_max
opensearch_tsdb_action_rest_queries_reduce_phase_latency_max
```

## Clean Up

**Single Node:**
```bash
# Stop all services
docker-compose -f docker-compose.single.yml down

# Remove all data including Prometheus and Grafana
docker-compose -f docker-compose.single.yml down -v
```

**2-Node Cluster:**
```bash
# Stop all services
docker-compose down

# Remove all data including Prometheus and Grafana
docker-compose down -v
```
