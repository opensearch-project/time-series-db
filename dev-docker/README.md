# OpenSearch TSDB Docker Development Environment

This directory contains Docker configurations for developing and testing the OpenSearch TSDB plugin in containerized environments.

**All commands run from `dev-docker/` directory.**

## Prerequisites

- Docker Desktop or OrbStack running
- Java 21 installed
- Minimum RAM: 4GB (single node) or 8GB (2-node cluster)

Verify:
```bash
docker ps
java -version  # Should show version 21
```

## Quick Start

The TSDB plugin requires a **custom OpenSearch build** with the `index.translog.read_forward` setting (not in official releases).

### Step 1: Get Custom OpenSearch

Clone OpenSearch with TSDB patches:

```bash
# Clone to any location (example uses ~/opensearch/)
cd ~/opensearch
git clone https://github.com/opensearch-project/OpenSearch.git

# Verify custom setting exists
grep -r "read_forward" ~/opensearch/OpenSearch/server/src/main/java/org/opensearch/index/IndexSettings.java
# Should output: "index.translog.read_forward",
```

**If setting not found**: Contact TSDB team for the correct branch/fork with TSDB patches.

### Step 2: Build Everything

The `build-custom.sh` script automates:
- Building custom OpenSearch TAR distribution
- Building TSDB plugin
- Copying artifacts to correct locations

```bash
cd dev-docker/

# Build custom OpenSearch and TSDB plugin (20-40 min first time)
OPENSEARCH_HOME=~/opensearch/OpenSearch BUILD_METHOD=tar ./build-custom.sh

# The script will:
# 1. Build OpenSearch TAR from OPENSEARCH_HOME
# 2. Copy TAR to build/distributions/
# 3. Build TSDB plugin
# 4. Verify everything is ready
```

**Environment variables**:
- `OPENSEARCH_HOME` - Path to custom OpenSearch repo (required)
- `BUILD_METHOD` - `tar` (recommended) or `docker`

### Step 3: Start OpenSearch

Choose your deployment:

#### Single Node (Development)

**Requirements**: 2 CPUs, 4GB RAM

```bash
# Build Docker image
docker-compose -f docker-compose.single.yml build --no-cache

# Start single node
docker-compose -f docker-compose.single.yml up -d

# Watch logs (Ctrl+C to exit)
docker-compose -f docker-compose.single.yml logs -f opensearch
```

Wait ~30 seconds. Look for:
```
[INFO] loaded plugin [tsdb]
[INFO] started
```

#### 2-Node Cluster (Production-like)

**Requirements**: 4 CPUs, 8GB RAM

```bash
# Build Docker images
docker-compose build --no-cache

# Start cluster
docker-compose up -d

# Watch logs
docker-compose logs -f
```

Wait ~45 seconds. Look for:
```
[INFO] detected_master {opensearch-node1}
```

## Verify Installation

### Check Cluster Health

```bash
curl "localhost:9200/_cluster/health?pretty"
```

Expected:
- Single: `"number_of_nodes": 1`, `"status": "green"`
- Cluster: `"number_of_nodes": 2`, `"status": "green"`

### Verify Plugin

```bash
curl "localhost:9200/_cat/plugins?v"
```

Expected:
```
name              component version
opensearch-single tsdb      X.Y.Z.0-SNAPSHOT
```

### Test TSDB Index

```bash
curl -X PUT "localhost:9200/metrics-test" -H 'Content-Type: application/json' -d'{
  "settings": {
    "index": {
      "tsdb_engine": {
        "enabled": true
      },
      "translog": {
        "read_forward": true
      }
    }
  }
}'
```

Expected: `{"acknowledged":true,"shards_acknowledged":true,"index":"metrics-test"}`

**Error `"unknown setting [index.translog.read_forward]"`?**
Your OpenSearch lacks TSDB patches. Return to Step 1.

## Configuration

### Service Ports

**Single Node:**
- `9200` - OpenSearch HTTP API
- `9300` - Transport (node-to-node)
- `9600` - Performance Analyzer
- `9010` - JMX (profiling)
- `4317/4318` - OTLP receivers (telemetry)
- `9090` - Prometheus
- `3000` - Grafana (admin/admin)

**2-Node Cluster:**
- Node 1: `9200`, `9300`, `9600`, `9010`, `4317`, `4318`, `9464`
- Node 2: `9201`, `9301`, `9601`, `4319`, `4320`, `9465`
- Prometheus: `9090`
- Grafana: `3000`

### Resource Limits

Default per node: 2 CPUs, 4GB RAM (2GB heap)

To change this, edit `docker-compose.yml` or `docker-compose.single.yml`:

```yaml
environment:
  - "OPENSEARCH_JAVA_OPTS=-Xms4g -Xmx4g"  # Change heap
deploy:
  resources:
    limits:
      cpus: '4'      # Change CPU
      memory: 8g     # Change memory
```

### OpenSearch Settings

Edit `config/opensearch.yml`:

```yaml
# Write thread pool
thread_pool.write.size: 8
thread_pool.write.queue_size: 10000

# Telemetry
opensearch.experimental.feature.telemetry.enabled: true
telemetry.feature.metrics.enabled: true
```

Or set dynamically:

```bash
curl -X PUT "localhost:9200/_cluster/settings" -H 'Content-Type: application/json' -d'{
  "persistent": {
    "thread_pool.write.size": 8
  }
}'
```

### Environment Variables

Create `.env` in `dev-docker/`:

```bash
DISABLE_SECURITY_PLUGIN=true  # For development
```

For production, set to `false` and configure security.

## Rebuilding After Changes

### Plugin Changes Only

```bash
# From repo root
# Note that if you use ./build-custom.sh, it copies the OpenSearch core tar into this repos build dir, so if you 
# run `gradlew clean` it will also delete the core tar. If you do that, re-run the build-custom.sh script which should
# handle re-copying the files for you.
./gradlew clean assemble

# From dev-docker/
docker-compose -f docker-compose.single.yml down
docker-compose -f docker-compose.single.yml build --no-cache
docker-compose -f docker-compose.single.yml up -d
```

### Rebuild Everything (OpenSearch + Plugin)

```bash
cd dev-docker/
OPENSEARCH_HOME=~/opensearch/OpenSearch BUILD_METHOD=tar ./build-custom.sh
docker-compose build --no-cache
docker-compose up -d
```

## Troubleshooting

### Build Script Issues

**Error**: `OpenSearch directory not found`

**Fix**: Specify correct path:
```bash
OPENSEARCH_HOME=~/opensearch/OpenSearch BUILD_METHOD=tar ./build-custom.sh
```

**Error**: `Custom setting not found`

**Fix**: Your OpenSearch lacks TSDB patches. Check with team for correct branch.

### Version Mismatch

**Error**: `Plugin [tsdb] was built for OpenSearch version X but version Y is running`

**Fix**: Rebuild with correct version:
```bash
./gradlew clean assemble -Dopensearch.version=3.5.0
```

### Plugin Not Found

**Error**: `FileNotFoundException: /tmp/tsdb-*.zip`

**Fix**: Ensure plugin built successfully:
```bash
ls -lh build/distributions/tsdb-*.zip
```

### Unknown Setting Error

**Error**: `unknown setting [index.translog.read_forward]`

**Fix**: Using official OpenSearch instead of custom build. Return to Step 1.

### Container Won't Start

Check logs:
```bash
docker-compose -f docker-compose.single.yml logs opensearch
```

Common fixes:
```bash
# Kill existing containers
docker-compose down -v

# Check port conflicts
lsof -i :9200

# Increase Docker Desktop memory
# Preferences → Resources → Memory (min 4GB single, 8GB cluster)
```

### Memory Issues

**Error**: `OutOfMemoryError: Java heap space`

**Fixes**:
1. Increase Docker Desktop memory allocation
2. On Linux:
   ```bash
   sudo sysctl -w vm.max_map_count=262144
   ```

### Cluster Not Forming (2-Node)

```bash
# Check both running
docker ps | grep opensearch

# Test connectivity
docker exec opensearch-tsdb-node1 ping opensearch-node2

# Restart
docker-compose down && docker-compose up -d
```

## Data Management

### Persistent Data

Data stored in Docker volumes:
- Single: `dev-docker_opensearch-data`
- Cluster: `dev-docker_opensearch-data1`, `dev-docker_opensearch-data2`
- Monitoring: `dev-docker_prometheus-data`, `dev-docker_grafana-data`

### Clean Slate

```bash
# Delete all data and volumes
docker-compose down -v

# Rebuild fresh
docker-compose build --no-cache
docker-compose up -d
```

## Profiling

Containers include profiling capabilities (SYS_PTRACE, SYS_ADMIN).

### async-profiler (CPU/Wall-clock Flamegraphs)

For some reason, this might not be able to generate the output file, so it may not work.
If it doesn't work, you can try the JMX approach below instead.
```bash
# Shell into container
docker exec -it opensearch-tsdb-single bash

# Download profiler
cd /tmp
curl -L https://github.com/async-profiler/async-profiler/releases/download/v3.0/async-profiler-3.0-linux-x64.tar.gz | tar xz

# Find Java PID
/usr/share/opensearch/jdk/bin/jcmd -l
# Example output: 7 org.opensearch.bootstrap.OpenSearch ...

# Profile CPU (replace 7 with your PID)
cd /tmp/async-profiler-3.0-linux-x64/bin
./asprof -d 30 -e cpu -o flamegraph -f /tmp/cpu.html 7

# Profile wall-clock
./asprof -d 30 -e wall -o flamegraph -f /tmp/wall.html 7

# Exit container and copy flamegraphs
exit
docker cp opensearch-tsdb-single:/tmp/cpu.html .
docker cp opensearch-tsdb-single:/tmp/wall.html .
```

Open `.html` files in browser for interactive flamegraphs.

### JMX/VisualVM

JMX enabled on port 9010. Connect VisualVM to `localhost:9010`.
You can connect to it using (VisualVM)[https://visualvm.github.io/], and use (File > Add JMX Connection) to connect to it via: localhost:9010

Visualizing flame graphs:
* relevant blog post: https://softwaredoug.com/blog/2023/10/15/visualvm-flamegraphs
  * script to parse VisualVM call graph export into text file: https://gist.github.com/softwaredoug/b0ddee0941c9dd5b3922a68f64622f32
  * flamegraphs repo to convert above text file into flame graph: https://github.com/brendangregg/FlameGraph 
  * Example:
      * VisuaVM > sampler
        * select CPU, let it run for the duration you want to sample
        * "snapshot"
        * "export forward call graphs"
      * generate flame graph
      ```shell
      python parse_stack.py ~/stacks/stack.csv > intellij_stack.txt
      ./path/to/flamegraph.pl intellij_stack.txt > intellij_graph.svg
      ```

## Useful Commands

```bash
# View logs
docker-compose logs -f
docker-compose logs --tail=100 opensearch

# Shell access
docker exec -it opensearch-tsdb-single bash

# Resource monitoring
docker stats

# Stop (keep data)
docker-compose down

# Stop (delete data)
docker-compose down -v

# Cluster health
curl "localhost:9200/_cluster/health?pretty"

# Node stats
curl "localhost:9200/_cat/nodes?v"

# Heap usage
curl "localhost:9200/_cat/nodes?v&h=name,heap.percent,heap.max,ram.percent"

# Indices
curl "localhost:9200/_cat/indices?v"

# Plugins
curl "localhost:9200/_cat/plugins?v"

# Cluster settings
curl "localhost:9200/_cluster/settings?pretty"
```

## Quick Reference

| Task | Command |
|------|---------|
| Build everything | `OPENSEARCH_HOME=~/opensearch/OpenSearch BUILD_METHOD=tar ./build-custom.sh` |
| Build plugin only | `./gradlew clean assemble` |
| Build Docker images | `docker-compose build --no-cache` |
| Start single node | `docker-compose -f docker-compose.single.yml up -d` |
| Start cluster | `docker-compose up -d` |
| View logs | `docker-compose logs -f` |
| Stop (keep data) | `docker-compose down` |
| Stop (delete all) | `docker-compose down -v` |
| Shell access | `docker exec -it opensearch-tsdb-single bash` |
| Check health | `curl localhost:9200/_cluster/health?pretty` |

## Directory Contents

### Docker Compose Files
- `docker-compose.yml` - 2-node cluster with monitoring stack
- `docker-compose.single.yml` - Single node for development

### Build Files
- `Dockerfile.custom` - Builds from OpenSearch TAR + TSDB plugin
- `build-custom.sh` - Automated build script (TAR or Docker methods)
- `.dockerignore` - Build context exclusions

### Configuration Files
- `.env` / `.env.example` - Environment variables
- `config/opensearch.yml` - OpenSearch settings
- `prometheus.yml` / `prometheus-cluster.yml` - Metrics scraping
- `otel-collector-config.yml` - Telemetry pipeline
- `grafana/` - Dashboards and provisioning

## Production Considerations
This is NOT for production usage, you should use this for TESTING LOCALLY ONLY.

## Additional Documentation
- Telemetry setup: `TELEMETRY_SETUP.md`
