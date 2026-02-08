# Docker Deployment Guide for OpenSearch TSDB Plugin

This guide provides complete instructions for building and running OpenSearch with the TSDB plugin using Docker. All steps have been tested and verified.

**Note:** All Docker-related files are located in the `dev-docker/` directory. Unless otherwise specified, all commands in this guide should be run from the `dev-docker/` directory:
```bash
cd dev-docker/
```

## ⚠️ Important: Custom OpenSearch Required

**The TSDB plugin requires a custom OpenSearch build** with patches that are not available in official releases. The official Docker images (`opensearch:3.4.0`, `opensearch:latest`) will **NOT work** with full TSDB functionality.

**Required custom setting**: `index.translog.read_forward`

### Quick Check

If you see this error when creating a TSDB index:
```
"unknown setting [index.translog.read_forward] please check that any required plugins are installed"
```

You need to build custom OpenSearch. **See [CUSTOM_OPENSEARCH.md](CUSTOM_OPENSEARCH.md) for complete instructions.**

### Quick Start with Custom OpenSearch

```bash
# 1. Clone OpenSearch (with TSDB patches)
export OPENSEARCH_HOME=/path/to/your/OpenSearch

# 2. Build custom OpenSearch and TSDB plugin
cd dev-docker/
./build-custom.sh

# 3. Update Dockerfile to use: FROM opensearch-custom:latest

# 4. Build and run (from dev-docker/)
docker-compose build --no-cache
docker-compose up -d
```

---

## Table of Contents
- [Prerequisites](#prerequisites)
- [Resource Configuration](#resource-configuration)
- [Step-by-Step Guide](#step-by-step-guide)
- [Testing and Verification](#testing-and-verification)
- [Configuration Reference](#configuration-reference)
- [Troubleshooting](#troubleshooting)
- [Production Considerations](#production-considerations)

## Prerequisites

- **Docker**: Docker Desktop or OrbStack installed and running
- **Docker Compose**: v2.0 or later
- **Java 21**: For building the plugin (Eclipse Temurin recommended)
- **RAM**: Minimum 4GB for single node, 8GB for 2-node cluster
- **CPU**: Minimum 2 cores per node

### Verify Prerequisites

```bash
# Check Docker is running
docker ps

# Check Docker Compose version
docker-compose version

# Check Java version
java -version  # Should show version 21
```

## Resource Configuration

Each OpenSearch node is configured with:
- **CPU**: 2 cores (hard limit)
- **Memory**: 4GB total (2GB Java heap + 2GB system)
- **Disk**: Unlimited (managed by Docker volumes)

## Step-by-Step Guide

### Step 1: Build the Plugin for OpenSearch 3.4.0

**Important**: The plugin version must match the OpenSearch Docker image version. The Dockerfile uses OpenSearch 3.4.0.

```bash
# Build plugin for OpenSearch 3.4.0 (from repo root)
cd /path/to/opensearch-tsdb-internal
./gradlew clean assemble -Dopensearch.version=3.4.0

# Verify the build
ls -lh build/distributions/
```

**Expected Output:**
```
-rw-r--r--  1 user  staff   856K Jan 27 13:55 tsdb-3.4.0.0-SNAPSHOT.zip
```

If you see `tsdb-3.5.0.0-SNAPSHOT.zip` or another version, you need to rebuild with the correct version flag.

### Step 2: Create Environment File

```bash
# Copy the example environment file
cp .env.example .env

# View the contents
cat .env
```

**Expected Contents:**
```
DISABLE_SECURITY_PLUGIN=true
```

For local development, security is disabled. For production, set this to `false`.

### Step 3: Choose Your Deployment

#### Option A: Single Node (Recommended for Development)

**System Requirements:** 2 CPUs, 4GB RAM

**IMPORTANT:** Always use the same docker-compose file for both build and run commands.

```bash
# Build single node
docker-compose -f docker-compose.single.yml build --no-cache

# Start single node
docker-compose -f docker-compose.single.yml up -d

# Watch startup logs (Ctrl+C to exit)
docker-compose -f docker-compose.single.yml logs -f opensearch
```

**Expected Log Output:**
```
opensearch-tsdb-single  | [INFO ][o.o.p.PluginsService] loaded plugin [tsdb]
opensearch-tsdb-single  | [INFO ][o.o.n.Node] started
```

Wait approximately 30 seconds for OpenSearch to fully start.

#### Option B: 2-Node Cluster (Production-like)

**System Requirements:** 4 CPUs, 8GB RAM

**IMPORTANT:** Always use the same docker-compose file for both build and run commands.

```bash
# Build 2-node cluster
docker-compose build --no-cache

# Start 2-node cluster
docker-compose up -d

# Watch startup logs for both nodes
docker-compose logs -f
```

**Expected Log Output:**
```
opensearch-tsdb-node1  | [INFO ][o.o.p.PluginsService] loaded plugin [tsdb]
opensearch-tsdb-node2  | [INFO ][o.o.p.PluginsService] loaded plugin [tsdb]
opensearch-tsdb-node1  | [INFO ][o.o.c.s.ClusterApplierService] detected_master {opensearch-node1}
```

Wait approximately 30-45 seconds for the cluster to form.

## Testing and Verification

### Test 1: Check Cluster Health

**Single Node:**
```bash
curl -X GET "localhost:9200/_cluster/health?pretty"
```

**Expected Output:**
```json
{
  "cluster_name" : "opensearch-tsdb-single",
  "status" : "green",
  "number_of_nodes" : 1,
  "number_of_data_nodes" : 1,
  "active_primary_shards" : 2,
  "active_shards" : 2
}
```

**2-Node Cluster:**
```bash
curl -X GET "localhost:9200/_cluster/health?pretty"
```

**Expected Output:**
```json
{
  "cluster_name" : "opensearch-tsdb-cluster",
  "status" : "green",
  "number_of_nodes" : 2,
  "number_of_data_nodes" : 2,
  "active_primary_shards" : 2,
  "active_shards" : 4
}
```

### Test 2: Verify TSDB Plugin Installation

**Single Node:**
```bash
curl -X GET "localhost:9200/_cat/plugins?v"
```

**Expected Output:**
```
name              component version
opensearch-single tsdb      3.4.0.0-SNAPSHOT
```

**2-Node Cluster:**
```bash
curl -X GET "localhost:9200/_cat/plugins?v" | grep tsdb
```

**Expected Output:**
```
opensearch-node1 tsdb 3.4.0.0-SNAPSHOT
opensearch-node2 tsdb 3.4.0.0-SNAPSHOT
```

### Test 3: List Cluster Nodes

```bash
curl -X GET "localhost:9200/_cat/nodes?v"
```

**Expected Output (Single Node):**
```
ip        heap.percent ram.percent cpu load_1m node.role cluster_manager name
127.0.0.1           59          61  -1    0.53 dimr      *               opensearch-single
```

**Expected Output (2-Node Cluster):**
```
ip            heap.percent ram.percent cpu node.role cluster_manager name
192.168.107.3           59          61  -1 dimr      *               opensearch-node1
192.168.107.2           59          61  -1 dimr      -               opensearch-node2
```

### Test 4: Create TSDB-Enabled Index

**Single Node:**
```bash
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
```

**Expected Output:**
```json
{"acknowledged":true,"shards_acknowledged":true,"index":"metrics-test"}
```

**2-Node Cluster with Replication:**
```bash
curl -X PUT "localhost:9200/metrics-cluster-test" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index": {
      "number_of_replicas": 1,
      "tsdb_engine": {
        "enabled": true
      }
    }
  }
}'
```

### Test 5: Verify TSDB Settings

```bash
curl -X GET "localhost:9200/metrics-test/_settings?pretty" | grep -A 3 tsdb_engine
```

**Expected Output:**
```json
"tsdb_engine" : {
  "enabled" : "true"
},
```

### Test 6: Verify Index Replication (Cluster Only)

```bash
curl -X GET "localhost:9200/_cat/indices/metrics-cluster-test?v"
```

**Expected Output:**
```
health status index                pri rep docs.count store.size
green  open   metrics-cluster-test   1   1          0       416b
```

The `rep 1` indicates one replica, and `green` status means all shards are allocated.

## Configuration Reference

### Environment Variables

Edit `.env` to customize your deployment:

| Variable | Default | Description |
|----------|---------|-------------|
| `DISABLE_SECURITY_PLUGIN` | `true` | Disable security for local development |

### Resource Limits

Defined in `docker-compose.yml` and `docker-compose.single.yml`:

```yaml
deploy:
  resources:
    limits:
      cpus: '2'        # Maximum CPU cores
      memory: 4g       # Maximum memory
    reservations:
      memory: 2g       # Minimum guaranteed memory
```

**Java Heap Size Rule**: Set to 50% of total memory
- 4GB total → 2GB heap: `-Xms2g -Xmx2g`
- 8GB total → 4GB heap: `-Xms4g -Xmx4g`

### Port Mappings

**Single Node:**

| Port | Service |
|------|---------|
| 9200 | HTTP API |
| 9300 | Transport (node-to-node) |
| 9600 | Performance Analyzer |

**2-Node Cluster:**

| Port | Node | Service |
|------|------|---------|
| 9200 | Node 1 | HTTP API |
| 9300 | Node 1 | Transport |
| 9600 | Node 1 | Performance Analyzer |
| 9201 | Node 2 | HTTP API |
| 9301 | Node 2 | Transport |
| 9601 | Node 2 | Performance Analyzer |

### Data Persistence

Data is stored in Docker volumes:

**Single Node:**
- Volume: `opensearch-tsdb-internal_opensearch-data`

**2-Node Cluster:**
- Volume 1: `opensearch-tsdb-internal_opensearch-data1`
- Volume 2: `opensearch-tsdb-internal_opensearch-data2`

**List Volumes:**
```bash
docker volume ls | grep opensearch
```

**Delete All Data (Clean Slate):**

```bash
# Single node
docker-compose -f docker-compose.single.yml down -v

# Cluster
docker-compose down -v
```

## Rebuilding After Plugin Changes

When you modify the plugin source code:

**IMPORTANT:** Use the same docker-compose file throughout (either default or `-f docker-compose.single.yml`).

```bash
# Step 1: Stop running containers
docker-compose down  # for cluster
# OR
docker-compose -f docker-compose.single.yml down  # for single node

# Step 2: Rebuild plugin with correct version
./gradlew clean assemble -Dopensearch.version=3.5.0

# Step 3: Rebuild Docker images (no cache) - USE SAME FILE AS STEP 1
docker-compose build --no-cache  # for cluster
# OR
docker-compose -f docker-compose.single.yml build --no-cache  # for single node

# Step 4: Start containers - USE SAME FILE AS STEP 1
docker-compose up -d  # for cluster
# OR
docker-compose -f docker-compose.single.yml up -d  # for single node

# Step 5: Verify plugin version
curl -X GET "localhost:9200/_cat/plugins?v" | grep tsdb
```

## Troubleshooting

### Issue: Docker Daemon Not Running

**Error:**
```
Cannot connect to the Docker daemon at unix:///var/run/docker.sock.
Is the docker daemon running?
```

**Solution:**
1. Start Docker Desktop or OrbStack
2. Wait for Docker to fully start (check menu bar icon)
3. Verify: `docker ps`

### Issue: Version Mismatch

**Error in Docker build logs:**
```
Exception in thread "main" java.lang.IllegalArgumentException:
Plugin [tsdb] was built for OpenSearch version 3.5.0 but version 3.4.0 is running
```

**Solution:**
```bash
# Rebuild plugin for correct OpenSearch version
./gradlew clean assemble -Dopensearch.version=3.4.0

# Verify correct version was built
ls -lh build/distributions/
# Should show: tsdb-3.4.0.0-SNAPSHOT.zip (not 3.5.0)

# Rebuild Docker images
docker-compose build --no-cache
docker-compose up -d
```

### Issue: Plugin File Not Found

**Error in Docker build:**
```
java.io.FileNotFoundException: /tmp/tsdb-*.zip (No such file or directory)
```

**Solution:**

The wildcard wasn't expanded properly. This is already fixed in the Dockerfile, but if you see this:

1. Check that the plugin zip exists:
   ```bash
   ls -lh build/distributions/tsdb-*.zip
   ```

2. Verify the Dockerfile uses the exact filename:
   ```dockerfile
   RUN /usr/share/opensearch/bin/opensearch-plugin install --batch file:///tmp/tsdb-3.4.0.0-SNAPSHOT.zip
   ```

### Issue: Container Won't Start

**Check logs:**
```bash
# Single node
docker-compose -f docker-compose.single.yml logs opensearch

# Cluster
docker-compose logs opensearch-node1
docker-compose logs opensearch-node2
```

**Common causes:**
- Insufficient memory allocated to Docker
- Port conflicts (another service using 9200)
- Previous container not fully stopped

**Solutions:**
```bash
# Kill any existing containers
docker-compose down -v

# Check port availability
lsof -i :9200  # On macOS/Linux
netstat -ano | findstr :9200  # On Windows

# Verify Docker memory settings (Docker Desktop)
# Preferences → Resources → Memory (set to at least 4GB for single node, 8GB for cluster)
```

### Issue: Memory Issues

**Error in logs:**
```
OutOfMemoryError: Java heap space
```

**Solutions:**

1. **Increase Docker Desktop Memory:**
   - Docker Desktop → Preferences → Resources → Memory
   - Single node: Minimum 4GB
   - Cluster: Minimum 8GB

2. **On Linux, set vm.max_map_count:**
   ```bash
   # Check current value
   sysctl vm.max_map_count

   # Set to required value (at least 262144)
   sudo sysctl -w vm.max_map_count=262144

   # Make permanent
   echo 'vm.max_map_count=262144' | sudo tee -a /etc/sysctl.conf
   ```

3. **Adjust heap size** in docker-compose files (if you have more RAM):
   ```yaml
   environment:
     - "OPENSEARCH_JAVA_OPTS=-Xms4g -Xmx4g"
   deploy:
     resources:
       limits:
         memory: 8g
   ```

### Issue: Plugin Not Loading

**Symptoms:**
- Plugin doesn't appear in `curl localhost:9200/_cat/plugins?v`
- TSDB engine settings don't work

**Debug Steps:**

1. **Check if plugin file exists in container:**
   ```bash
   docker exec -it opensearch-tsdb-single ls -la /usr/share/opensearch/plugins/
   ```
   Expected: Directory named `tsdb` should exist

2. **Check plugin installation logs:**
   ```bash
   docker-compose logs | grep -i "plugin"
   ```
   Expected: `loaded plugin [tsdb]`

3. **Verify plugin descriptor:**
   ```bash
   docker exec -it opensearch-tsdb-single cat /usr/share/opensearch/plugins/tsdb/plugin-descriptor.properties
   ```
   Expected: `version=3.4.0.0-SNAPSHOT`

4. **Rebuild from scratch:**
   ```bash
   docker-compose down -v
   ./gradlew clean assemble -Dopensearch.version=3.4.0
   docker-compose build --no-cache
   docker-compose up -d
   ```

### Issue: Cluster Not Forming (2-Node Setup)

**Symptoms:**
- Only 1 node appears in `/_cat/nodes`
- Cluster health shows `number_of_nodes: 1`

**Debug Steps:**

1. **Check both containers are running:**
   ```bash
   docker ps | grep opensearch
   ```
   Expected: Two containers running

2. **Check network connectivity:**
   ```bash
   docker exec -it opensearch-tsdb-node1 ping opensearch-node2
   ```

3. **Check discovery settings in logs:**
   ```bash
   docker-compose logs | grep -i "discovery"
   ```

4. **Restart cluster:**
   ```bash
   docker-compose down
   docker-compose up -d
   ```

### Issue: Permission Denied Errors

**Error:**
```
rm: cannot remove '/tmp/tsdb-3.4.0.0-SNAPSHOT.zip': Operation not permitted
```

**Solution:**
This error appears during cleanup but doesn't affect functionality. It's already handled in the Dockerfile with `|| true`. If you see this error but the container starts successfully, it can be safely ignored.

## Useful Commands

### Container Management

```bash
# View running containers
docker ps

# View all containers (including stopped)
docker ps -a

# Stop containers
docker-compose down  # Cluster
docker-compose -f docker-compose.single.yml down  # Single node

# Stop and remove volumes (clean slate)
docker-compose down -v

# Restart containers
docker-compose restart

# View container logs
docker-compose logs -f  # Follow mode
docker-compose logs --tail=100  # Last 100 lines
docker-compose logs opensearch-node1  # Specific service
```

### OpenSearch Health Checks

```bash
# Cluster health
curl -X GET "localhost:9200/_cluster/health?pretty"

# Node statistics
curl -X GET "localhost:9200/_cat/nodes?v"

# Heap usage
curl -X GET "localhost:9200/_cat/nodes?v&h=name,heap.percent,heap.current,heap.max,ram.percent,ram.current,ram.max"

# All indices
curl -X GET "localhost:9200/_cat/indices?v"

# Installed plugins
curl -X GET "localhost:9200/_cat/plugins?v"

# Cluster settings
curl -X GET "localhost:9200/_cluster/settings?pretty"
```

### Container Inspection

```bash
# Execute commands in container
docker exec -it opensearch-tsdb-single bash

# View container resource usage (live)
docker stats

# Inspect container configuration
docker inspect opensearch-tsdb-single

# View container processes
docker top opensearch-tsdb-single
```

### Cleanup Commands

```bash
# Remove stopped containers
docker container prune

# Remove unused volumes
docker volume prune

# Remove unused images
docker image prune -a

# Full cleanup (careful!)
docker system prune -a --volumes
```

## Production Considerations

This setup is optimized for development and testing. For production deployments:

### 1. Enable Security

```bash
# Edit .env
DISABLE_SECURITY_PLUGIN=false
```

Then configure certificates and authentication:
- Generate SSL/TLS certificates
- Configure `opensearch.yml` with security settings
- Set up users and roles
- Enable audit logging

### 2. Increase Resources

Adjust based on workload:

```yaml
environment:
  - "OPENSEARCH_JAVA_OPTS=-Xms8g -Xmx8g"
deploy:
  resources:
    limits:
      cpus: '4'
      memory: 16g
    reservations:
      memory: 8g
```

### 3. Add More Nodes

Extend `docker-compose.yml` to add a third node:

```yaml
opensearch-node3:
  build:
    context: .
    dockerfile: Dockerfile
  container_name: opensearch-tsdb-node3
  environment:
    - cluster.name=opensearch-tsdb-cluster
    - node.name=opensearch-node3
    - discovery.seed_hosts=opensearch-node1,opensearch-node2,opensearch-node3
    - cluster.initial_cluster_manager_nodes=opensearch-node1,opensearch-node2,opensearch-node3
    # ... rest of config
```

### 4. Use External Volumes

For data persistence outside Docker:

```yaml
volumes:
  opensearch-data1:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /path/to/data/node1
```

### 5. Configure Backups

Set up snapshot repository for backups:

```bash
curl -X PUT "localhost:9200/_snapshot/backup_repository" -H 'Content-Type: application/json' -d'
{
  "type": "fs",
  "settings": {
    "location": "/mnt/backups"
  }
}'
```

### 6. Add Monitoring

Consider adding:
- OpenSearch Dashboards for visualization
- Prometheus + Grafana for metrics
- Alerting rules for critical conditions

### 7. Set System Limits (Linux)

```bash
# Set vm.max_map_count permanently
sudo sysctl -w vm.max_map_count=262144
echo 'vm.max_map_count=262144' | sudo tee -a /etc/sysctl.conf

# Verify
sysctl vm.max_map_count
```

### 8. Use Docker Swarm or Kubernetes

For orchestration in production:
- Docker Swarm for simpler deployments
- Kubernetes with Helm charts for enterprise scale

## Additional Resources

- [OpenSearch Docker Documentation](https://opensearch.org/docs/latest/install-and-configure/install-opensearch/docker/)
- [OpenSearch Configuration](https://opensearch.org/docs/latest/install-and-configure/configuration/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [OpenSearch Docker Hub](https://hub.docker.com/r/opensearchproject/opensearch)
- [OpenSearch Security Configuration](https://opensearch.org/docs/latest/security/configuration/)
- [OpenSearch Performance Tuning](https://opensearch.org/docs/latest/tuning-your-cluster/)

## Quick Reference Card

| Task | Command |
|------|---------|
| Build plugin | `./gradlew clean assemble -Dopensearch.version=3.4.0` |
| Start single node | `docker-compose -f docker-compose.single.yml up -d` |
| Start cluster | `docker-compose up -d` |
| View logs | `docker-compose logs -f` |
| Check health | `curl localhost:9200/_cluster/health?pretty` |
| List plugins | `curl localhost:9200/_cat/plugins?v` |
| List nodes | `curl localhost:9200/_cat/nodes?v` |
| Stop containers | `docker-compose -f docker-compose.yml down` |
| Clean everything | `docker-compose -f docker-compose.yml down -v` |
| Rebuild images | `docker-compose -f docker-compose.yml build --no-cache` |
| Container shell | `docker exec -it opensearch-tsdb-single bash` |
