#!/bin/bash
set -e

# Configuration
OPENSEARCH_HOME="${OPENSEARCH_HOME:-../OpenSearch}"
BUILD_METHOD="${BUILD_METHOD:-docker}"  # 'docker' or 'tar'
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TSDB_HOME="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=== Building Custom OpenSearch for TSDB ==="
echo "OpenSearch location: $OPENSEARCH_HOME"
echo "Build method: $BUILD_METHOD"
echo ""

# Verify OpenSearch directory exists
if [ ! -d "$OPENSEARCH_HOME" ]; then
    echo "❌ ERROR: OpenSearch directory not found at $OPENSEARCH_HOME"
    echo ""
    echo "Please either:"
    echo "  1. Clone OpenSearch: git clone https://github.com/opensearch-project/OpenSearch.git"
    echo "  2. Set OPENSEARCH_HOME: export OPENSEARCH_HOME=/path/to/OpenSearch"
    echo ""
    exit 1
fi

# Verify translog.read_forward setting exists
if ! grep -q "read_forward" "$OPENSEARCH_HOME/server/src/main/java/org/opensearch/index/IndexSettings.java" 2>/dev/null; then
    echo "❌ ERROR: Custom translog.read_forward setting not found in OpenSearch"
    echo ""
    echo "Your OpenSearch repository doesn't have the required TSDB patches."
    echo "Required setting: index.translog.read_forward"
    echo ""
    echo "Location checked: $OPENSEARCH_HOME/server/src/main/java/org/opensearch/index/IndexSettings.java"
    echo ""
    echo "You need an OpenSearch fork with TSDB support."
    exit 1
fi

echo "✓ OpenSearch directory found with TSDB patches"
echo ""

# Build TSDB plugin first (does clean, so must happen before copying artifacts)
cd "$TSDB_HOME"
echo "Building TSDB plugin..."
./gradlew clean assemble

if [ ! -f build/distributions/tsdb-*.zip ]; then
    echo "❌ ERROR: TSDB plugin build failed"
    exit 1
fi

PLUGIN_FILE=$(ls -1 build/distributions/tsdb-*.zip | head -1)
echo "✓ TSDB plugin built: $PLUGIN_FILE"
echo ""

# Build OpenSearch
# Copy the build artifacts after the TSDB plugin clean/build is done, or the file copied to build/ folder will get deleted again.
cd "$OPENSEARCH_HOME"

if [ "$BUILD_METHOD" == "docker" ]; then
    echo "Building OpenSearch Docker image..."
    echo "This will take 20-40 minutes on first build..."
    echo ""

    # Detect architecture
    ARCH=$(uname -m)
    if [ "$ARCH" == "arm64" ] || [ "$ARCH" == "aarch64" ]; then
        echo "Detected ARM64 architecture"
        DOCKER_TASK=":distribution:docker:docker-arm64-export:assemble"
    else
        echo "Detected x64 architecture"
        DOCKER_TASK=":distribution:docker:docker-export:assemble"
    fi

    ./gradlew $DOCKER_TASK

    # Find the built image
    IMAGE_NAME=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep "^opensearch:" | head -1)

    if [ -z "$IMAGE_NAME" ]; then
        echo "❌ ERROR: Docker image not found after build"
        exit 1
    fi

    echo ""
    echo "✓ OpenSearch Docker image built: $IMAGE_NAME"

    # Tag it as opensearch-custom:latest
    docker tag "$IMAGE_NAME" opensearch-custom:latest
    echo "✓ Tagged as: opensearch-custom:latest"

elif [ "$BUILD_METHOD" == "tar" ]; then
    echo "Building OpenSearch TAR distribution..."
    echo "This will take 15-30 minutes on first build..."
    echo ""

    # Detect architecture for TAR build
    ARCH=$(uname -m)
    if [ "$ARCH" == "arm64" ] || [ "$ARCH" == "aarch64" ]; then
        echo "Detected ARM64 architecture"
        TAR_TASK=":distribution:archives:linux-arm64-tar:assemble"
        TAR_DIR="distribution/archives/linux-arm64-tar/build/distributions"
    else
        echo "Detected x64 architecture"
        TAR_TASK=":distribution:archives:linux-tar:assemble"
        TAR_DIR="distribution/archives/linux-tar/build/distributions"
    fi

    ./gradlew $TAR_TASK

    # Find the TAR file
    TAR_FILE=$(ls -1 $TAR_DIR/opensearch-*.tar.gz 2>/dev/null | head -1)

    if [ -z "$TAR_FILE" ]; then
        echo "❌ ERROR: TAR distribution not found after build"
        exit 1
    fi

    echo ""
    echo "✓ OpenSearch TAR built: $TAR_FILE"

    # Copy to build/distributions directory for Docker build (same location as plugins)
    # This happens AFTER TSDB clean, so it won't get deleted
    cp "$TAR_FILE" "$TSDB_HOME/build/distributions/"

    echo "✓ TAR copied to: $TSDB_HOME/build/distributions/$(basename "$TAR_FILE")"

else
    echo "❌ ERROR: Invalid BUILD_METHOD: $BUILD_METHOD"
    echo "Valid options: 'docker' or 'tar'"
    exit 1
fi

# Build telemetry-otel plugin
echo ""
echo "Building telemetry-otel plugin..."
cd "$OPENSEARCH_HOME"
./gradlew :plugins:telemetry-otel:assemble

if [ ! -f plugins/telemetry-otel/build/distributions/telemetry-otel-*.zip ]; then
    echo "❌ ERROR: telemetry-otel plugin build failed"
    exit 1
fi

TELEMETRY_PLUGIN=$(ls -1 plugins/telemetry-otel/build/distributions/telemetry-otel-*.zip | head -1)
echo "✓ telemetry-otel plugin built: $TELEMETRY_PLUGIN"

# Copy telemetry-otel plugin to TSDB build directory for Docker build
echo ""
echo "Copying telemetry-otel plugin to TSDB build directory..."
mkdir -p "$TSDB_HOME/build/distributions"
cp "$OPENSEARCH_HOME/$TELEMETRY_PLUGIN" "$TSDB_HOME/build/distributions/"
echo "✓ Copied telemetry-otel plugin to: $TSDB_HOME/build/distributions/$(basename "$TELEMETRY_PLUGIN")"

echo ""
echo "=== Build Complete ✓ ==="
echo ""

if [ "$BUILD_METHOD" == "docker" ]; then
    echo "Next steps:"
    echo "  1. Update Dockerfile to use custom image:"
    echo "     FROM opensearch-custom:latest"
    echo ""
    echo "  2. Build Docker containers (from dev-docker/):"
    echo "     cd dev-docker"
    echo "     docker-compose build --no-cache"
    echo ""
    echo "  3. Start cluster:"
    echo "     docker-compose up -d"
    echo ""
    echo "  4. Verify TSDB plugin:"
    echo "     curl \"localhost:9200/_cat/plugins?v\" | grep tsdb"
else
    echo "Next steps:"
    echo "  IMPORTANT: Use the same docker-compose file for build AND run!"
    echo "  All commands should be run from the dev-docker/ directory."
    echo ""
    echo "  cd dev-docker"
    echo ""
    echo "  Option A - Single Node:"
    echo "    docker-compose -f docker-compose.single.yml build --no-cache"
    echo "    docker-compose -f docker-compose.single.yml up -d"
    echo ""
    echo "  Option B - 2-Node Cluster:"
    echo "    docker-compose build --no-cache"
    echo "    docker-compose up -d"
    echo ""
    echo "  Verify plugins:"
    echo "    curl \"localhost:9200/_cat/plugins?v\""
fi

echo ""
echo "See dev-docker/DOCKER.md for detailed documentation."
