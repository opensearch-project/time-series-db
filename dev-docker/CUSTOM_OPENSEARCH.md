# Building Custom OpenSearch for TSDB Plugin

**Note:** This guide references Docker files located in the `dev-docker/` directory. When running docker-compose commands, ensure you're in the `dev-docker/` directory.

## Why Custom OpenSearch is Required

The TSDB plugin requires OpenSearch core changes that may **NOT available in the latest official docker releases**:

- **Custom Setting**: `index.translog.read_forward`
- **Purpose**: Enables forward translog reading for TSDB engine
- **Status**: Not merged into upstream OpenSearch (as of Jan 2026)

**Official Docker images will fail** with:
```
"unknown setting [index.translog.read_forward] please check that any required plugins are installed"
```

Once the latest main branch is released to dockerhub, we should be able to directly install the tsdb plugin without recompiling OpenSearch core.

## Prerequisites

1. **Clone OpenSearch Core** (with custom TSDB patches):
   ```bash
   # Clone to any directory (we'll use OPENSEARCH_HOME to refer to it)
   git clone https://github.com/opensearch-project/OpenSearch.git
   cd OpenSearch

   # Set environment variable for easy reference
   export OPENSEARCH_HOME=$(pwd)
   ```

2. **Verify Custom Patches**:
   ```bash
   cd $OPENSEARCH_HOME

   # Check if translog.read_forward setting exists
   grep -r "read_forward" server/src/main/java/org/opensearch/index/IndexSettings.java

   # Should output:
   # "index.translog.read_forward",
   ```

   If this setting doesn't exist, you need to apply custom patches or use a fork with TSDB support.

3. **Java 21**:
   ```bash
   java -version  # Should show version 21
   ```

## Build Options

### Option 1: Build TAR Distribution

**Steps:**

```bash
cd $OPENSEARCH_HOME

# Build TAR distribution
./gradlew :distribution:archives:linux-tar:assemble

# Find the distribution
ls -lh distribution/archives/linux-tar/build/distributions/

# Output: opensearch-X.Y.Z-SNAPSHOT-linux-x64.tar.gz
```

### Option 2: Build Custom OpenSearch Docker Image 
Note: this may not work out of the box due to SSL issues, that may end up requiring code changes in core

**Steps:**

```bash
# Navigate to OpenSearch core
cd $OPENSEARCH_HOME

# Build Docker image (x64 architecture)
./gradlew :distribution:docker:docker-export:assemble

# Find the built image
docker images | grep opensearch

# Tag it for easier reference
docker tag opensearch:latest opensearch-custom:latest
```

## Integration with TSDB Plugin

### Method 1: Using TAR Distribution

If you built the TAR (Option 1):

## Automated Build Script
execute `build-custom.sh` 

### Explaination
1. **Copy TAR to TSDB plugin directory**:

```bash
# From your TSDB plugin directory
export TSDB_HOME=$(pwd)

# Copy OpenSearch TAR
cp $OPENSEARCH_HOME/distribution/archives/linux-tar/build/distributions/opensearch-*.tar.gz \
   $TSDB_HOME/
```

2. **Create Dockerfile.custom**:

```dockerfile
FROM ubuntu:22.04

# Install dependencies
RUN apt-get update && \
    apt-get install -y curl tar gzip openjdk-21-jdk && \
    rm -rf /var/lib/apt/lists/*

# Create opensearch user
RUN groupadd -g 1000 opensearch && \
    useradd -u 1000 -g opensearch -s /bin/bash -m opensearch

# Copy and extract custom OpenSearch
COPY opensearch-*.tar.gz /tmp/
RUN tar -xzf /tmp/opensearch-*.tar.gz -C /usr/share/ && \
    mv /usr/share/opensearch-* /usr/share/opensearch && \
    rm /tmp/opensearch-*.tar.gz

# Copy TSDB plugin
COPY build/distributions/tsdb-*.zip /tmp/

# Install TSDB plugin
RUN /usr/share/opensearch/bin/opensearch-plugin install --batch file:///tmp/tsdb-*.zip && \
    rm -f /tmp/tsdb-*.zip || true

# Set ownership
RUN chown -R opensearch:opensearch /usr/share/opensearch

USER opensearch
WORKDIR /usr/share/opensearch

ENV OPENSEARCH_JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64

EXPOSE 9200 9300 9600

CMD ["./bin/opensearch"]
```

3. **Update docker-compose.yml** to use `Dockerfile.custom`:

```yaml
services:
  opensearch-node1:
    build:
      context: .
      dockerfile: Dockerfile.custom  # Changed from Dockerfile
    # ... rest of config
```

4. **Build and Run**:

```bash
./gradlew clean assemble
docker-compose build --no-cache
docker-compose up -d
```

### Method 2: Using Custom OpenSearch Docker Image

If you built the Docker image (Option 2):

1. **Update Dockerfile**:

```dockerfile
# Use your custom OpenSearch image instead of official
FROM opensearch-custom:latest

# Switch to root to install plugin
USER root

# Copy TSDB plugin
COPY build/distributions/tsdb-*.zip /tmp/

# Install plugin
RUN /usr/share/opensearch/bin/opensearch-plugin install --batch file:///tmp/tsdb-*.zip && \
    rm -f /tmp/tsdb-*.zip || true

# Set ownership
RUN chown -R opensearch:opensearch /usr/share/opensearch

# Switch back to opensearch user
USER opensearch

EXPOSE 9200 9300 9600
```

2. **Build and Run**:

```bash
# Build TSDB plugin
./gradlew clean assemble

# Build Docker image with TSDB plugin
docker-compose build --no-cache

# Start cluster
docker-compose up -d
```

## Usage

### Quick Start (Docker Method)

```bash
# 1. Set OpenSearch location
export OPENSEARCH_HOME=/path/to/your/OpenSearch

# 2. Build everything
./build-custom.sh

# 3. Update Dockerfile to use custom image
# Change FROM line to: FROM opensearch-custom:latest

# 4. Build and run
docker-compose build --no-cache
docker-compose up -d

# 5. Verify
curl "localhost:9200/_cat/plugins?v" | grep tsdb
```

### Using TAR Method

```bash
# 1. Set OpenSearch location and build method
export OPENSEARCH_HOME=/path/to/your/OpenSearch
export BUILD_METHOD=tar

# 2. Build everything
./build-custom.sh

# 3. Update docker-compose.yml
# Change dockerfile to: Dockerfile.custom

# 4. Build and run
docker-compose build --no-cache
docker-compose up -d
```

## Testing Custom TSDB Features

Once running with custom OpenSearch, you can use all README.md features:

```bash
# Create TSDB index with custom settings
curl -X PUT -H 'Content-Type: application/json' http://localhost:9200/my-index --data '{
  "settings": {
    "index.tsdb_engine.enabled": true,
    "index.tsdb_engine.labels.storage_type": "binary",
    "index.translog.read_forward": true,
    "index.number_of_shards": 1,
    "index.number_of_replicas": 1
  }
}'

# Should succeed with custom OpenSearch
# Should fail with official OpenSearch (unknown setting error)
```

## Troubleshooting

### Error: OpenSearch directory not found

```bash
# Check your OPENSEARCH_HOME
echo $OPENSEARCH_HOME

# Clone OpenSearch if needed
git clone https://github.com/opensearch-project/OpenSearch.git
export OPENSEARCH_HOME=$(pwd)/OpenSearch
```

### Error: Custom setting not found

Your OpenSearch doesn't have TSDB patches. You need:
- A fork with TSDB support, or
- Apply patches manually, or
- Contact the TSDB team for the correct OpenSearch branch

### Build fails with "OutOfMemoryError"

```bash
# Increase Gradle memory
export GRADLE_OPTS="-Xmx4g"

# Then rebuild
cd $OPENSEARCH_HOME
./gradlew clean
./gradlew :distribution:docker:docker-export:assemble
```

### Docker image not found after build

```bash
# List all OpenSearch images
docker images | grep opensearch

# Tag it explicitly
docker tag opensearch:<version> opensearch-custom:latest
```

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENSEARCH_HOME` | `../OpenSearch` | Path to custom OpenSearch repository |
| `BUILD_METHOD` | `docker` | Build method: `docker` or `tar` |

## Directory Structure

```
your-workspace/
├── OpenSearch/                    # Custom OpenSearch with TSDB patches
│   └── distribution/
│       ├── docker/                # Docker build outputs
│       └── archives/linux-tar/    # TAR distribution outputs
│
└── opensearch-tsdb-internal/     # This repository
    ├── build-custom.sh            # Automated build script
    ├── Dockerfile                 # For custom Docker image method
    ├── Dockerfile.custom          # For TAR method
    └── docker-compose.yml
```

## Recommendation
**Use TAR method (Option 1)** if:
- Docker build is not available in your OpenSearch version
- You want more control over the Dockerfile
- You need the TAR for non-Docker deployments

**Use Docker Image method (Option 2)** if:
- You have Docker build support in OpenSearch
- You want faster iteration during development
- You don't need portable TAR distributions
