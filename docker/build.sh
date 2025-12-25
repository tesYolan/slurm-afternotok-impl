#!/bin/bash
# Build script for Slurm Docker Cluster with Escalation Support
#
# This script:
# 1. Clones upstream giovtorres/slurm-docker-cluster if not present
# 2. Applies our custom overlays (docker-compose.yml, Dockerfile, slurm.conf, etc.)
# 3. Builds and starts the cluster
#
# Usage:
#   ./build.sh              # Build and start
#   ./build.sh --rebuild    # Force rebuild of images
#   ./build.sh --down       # Stop and remove containers

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
UPSTREAM_URL="https://github.com/giovtorres/slurm-docker-cluster.git"
CLUSTER_DIR="$SCRIPT_DIR/slurm-docker-cluster"
OVERLAYS_DIR="$SCRIPT_DIR/overlays"

# Parse arguments
REBUILD=false
DOWN_ONLY=false
for arg in "$@"; do
    case $arg in
        --rebuild)
            REBUILD=true
            ;;
        --down)
            DOWN_ONLY=true
            ;;
    esac
done

# Handle --down
if $DOWN_ONLY; then
    if [[ -d "$CLUSTER_DIR" ]]; then
        echo "Stopping cluster..."
        cd "$CLUSTER_DIR"
        docker compose down -v
        echo "Cluster stopped."
    else
        echo "No cluster directory found."
    fi
    exit 0
fi

# Clone upstream if not exists
if [[ ! -d "$CLUSTER_DIR" ]]; then
    echo "Cloning upstream slurm-docker-cluster..."
    git clone "$UPSTREAM_URL" "$CLUSTER_DIR"
else
    echo "Using existing slurm-docker-cluster directory"
fi

# Apply overlays (includes modified Dockerfile with python3-pyyaml and R-core)
echo "Applying customizations from overlays..."
cp -r "$OVERLAYS_DIR"/* "$CLUSTER_DIR/"
echo "Overlays applied (Dockerfile includes python3-pyyaml and R-core)"

# Build and start
cd "$CLUSTER_DIR"

if $REBUILD; then
    echo "Rebuilding images (--no-cache)..."
    docker compose build --no-cache
else
    echo "Building images..."
    docker compose build
fi

echo "Starting cluster..."
docker compose up -d

echo ""
echo "========================================"
echo "Cluster ready!"
echo "========================================"
echo ""
echo "Verify with:  docker exec slurmctld sinfo"
echo ""
echo "Run quick test:"
echo "  docker exec slurmctld bash -c 'cd /data && /data/jobs/mem-escalate.sh \\"
echo "    --config /data/jobs/tests/docker-escalation.yaml \\"
echo "    --array=0-9 /data/jobs/tests/docker-job.sh'"
echo ""
echo "Stop cluster: ./build.sh --down"
