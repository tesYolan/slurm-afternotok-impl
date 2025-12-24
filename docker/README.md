# Multi-Partition Slurm Docker Cluster

Docker-based Slurm cluster with 18 nodes across 4 partitions for testing memory/time escalation.

**Base Repository:** https://github.com/giovtorres/slurm-docker-cluster

These config files extend the base Slurm Docker cluster with a multi-partition setup for escalation testing.

## Cluster Layout

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Control Services                                   │
├──────────────┬──────────────┬──────────────┬──────────────────────────────────┤
│  slurmctld   │  slurmdbd    │    mysql     │  slurmrestd                      │
│  (control)   │  (acct db)   │  (mariadb)   │  (REST API)                      │
├──────────────┴──────────────┴──────────────┴──────────────────────────────────┤
│                                                                               │
│  DEVEL PARTITION (10 nodes @ 1GB each)                                        │
│  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬─────┐                         │
│  │ d1 │ d2 │ d3 │ d4 │ d5 │ d6 │ d7 │ d8 │ d9 │ d10 │                         │
│  └────┴────┴────┴────┴────┴────┴────┴────┴────┴─────┘                         │
│                                                                               │
│  DAY PARTITION (5 nodes @ 2GB each) - DEFAULT                                 │
│  ┌────┬────┬────┬────┬────┐                                                   │
│  │ n1 │ n2 │ n3 │ n4 │ n5 │                                                   │
│  └────┴────┴────┴────┴────┘                                                   │
│                                                                               │
│  WEEK PARTITION (2 nodes @ 8GB each)                                          │
│  ┌────┬────┐                                                                  │
│  │ w1 │ w2 │                                                                  │
│  └────┴────┘                                                                  │
│                                                                               │
│  PI_JETZ PARTITION (1 node @ 16GB)                                            │
│  ┌────┐                                                                       │
│  │ p1 │                                                                       │
│  └────┘                                                                       │
└───────────────────────────────────────────────────────────────────────────────┘
```

## Partition Configuration

| Partition | Nodes     | Memory/Node | Time Limit | Default |
|-----------|-----------|-------------|------------|---------|
| devel     | d[1-10]   | 1 GB        | 1 min      | No      |
| day       | n[1-5]    | 2 GB        | 1 min      | Yes     |
| week      | w[1-2]    | 8 GB        | 2 min      | No      |
| pi_jetz   | p1        | 16 GB       | 8 min      | No      |

## Escalation Flow

Based on `escalation-target.yaml`:

```
Level 1: -p devel,day --mem=1G --time=00:01:00
    ↓ (on failure)
Level 2: -p week --mem=4G --time=00:02:00
    ↓ (on failure)
Level 3: -p week --mem=8G --time=00:04:00
    ↓ (on failure)
Level 4: -p pi_jetz --mem=16G --time=00:08:00
```

## Quick Start

### 1. Clone Base Repository

```bash
git clone https://github.com/giovtorres/slurm-docker-cluster.git
cd slurm-docker-cluster
```

### 2. Copy Config Files

Copy these files to the appropriate locations in the base repo:

```bash
# From slurm-docker-cluster root directory
cp slurm-afternotok-impl/docker/docker-compose.yml .
cp slurm-afternotok-impl/docker/docker-entrypoint.sh .
cp slurm-afternotok-impl/docker/slurm.conf config/25.05/slurm.conf
```

### 3. Build and Start the Cluster

```bash
# Clean up any existing containers and volumes
docker compose down -v

# Start the cluster (18 compute nodes + 4 control services)
docker compose up -d

# Wait for all services to be healthy (~30-60 seconds)
docker compose ps
```

### 4. Verify the Cluster

```bash
# Check partition status
docker exec slurmctld sinfo

# Expected output:
# PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
# devel        up       1:00     10   idle d[1-10]
# day*         up       1:00      5   idle n[1-5]
# week         up       2:00      2   idle w[1-2]
# pi_jetz      up       8:00      1   idle p1

# Show detailed partition config
docker exec slurmctld scontrol show partition
```

### 5. Submit Test Jobs

#### Standard Slurm Jobs
```bash
# Submit to devel partition
docker exec slurmctld sbatch -p devel --mem=500M --wrap="hostname; sleep 5"

# Submit to multiple partitions (Slurm picks first available)
docker exec slurmctld sbatch -p devel,day --mem=1G --wrap="hostname"
```

#### Memory/Time Escalation Tests
These tests use the automatic escalation system to retry failed jobs with more resources.

```bash
# Run a specific scenario (e.g., 'levels' to test all 4 escalation steps)
docker exec slurmctld bash -c "cd /data && /data/jobs/tests/test-scenarios.sh levels"

# Run a large scale test (1000 tasks with mixed behavior)
docker exec slurmctld bash -c "cd /data && /data/jobs/tests/test-scenarios.sh mixed"
```

### 6. Monitoring and Watcher

The escalation system creates a **Chain ID** for each submission (e.g., `20251224-123456-abcd`). Use this ID to monitor progress.

```bash
# List all active and past escalation chains
docker exec slurmctld /data/jobs/mem-escalate.sh --list

# View status of a specific chain
docker exec slurmctld /data/jobs/mem-escalate.sh --status <chain_id>

# Follow progress live (Checkpoint Watcher)
docker exec -it slurmctld /data/jobs/mem-escalate.sh --status <chain_id> --watch
```

### 8. Cleanup

To clear all job history, checkpoints, and output files:

```bash
docker exec slurmctld /data/jobs/clean-slurm.sh
```

### 9. Monitor Jobs (General Slurm)

```bash
# Check job queue
docker exec slurmctld squeue

# Check job history
docker exec slurmctld sacct -X --format=JobID,Partition,State,MaxRSS,Elapsed

# Check specific node
docker exec slurmctld scontrol show node d1
```

## Container Management

```bash
# Check all containers
docker ps --filter "name=slurm" --filter "name=mysql"

# View logs for a specific service
docker logs slurmctld
docker logs d1

# Enter a compute node
docker exec -it d1 bash

# Restart the cluster
docker compose restart

# Stop the cluster (preserves volumes)
docker compose down

# Stop and remove all data
docker compose down -v
```

## Files in This Directory

| File | Description |
|------|-------------|
| `docker-compose.yml` | Docker Compose config with 18 compute nodes |
| `slurm.conf` | Slurm configuration with 4 partitions |
| `docker-entrypoint.sh` | Container entrypoint with node hostname detection |
| `escalation-target.yaml` | Escalation levels configuration |
| `README.md` | This file |

## Resource Requirements

Running all 18 nodes requires:
- **Memory**: ~30 GB total (10×1G + 5×2G + 2×8G + 1×16G + overhead)
- **CPU**: Minimal (containers are mostly idle)
- **Disk**: ~2 GB for images and volumes

To reduce resources, edit `docker-compose.yml` to remove nodes (e.g., keep only d1-d4, n1-n2, w1, p1).

## Troubleshooting

### Nodes show as DOWN

```bash
# Check node status
docker exec slurmctld sinfo -N -l

# Resume nodes
docker exec slurmctld scontrol update nodename=d[1-10] state=resume
```

### slurmctld fails to start

Check logs:
```bash
docker logs slurmctld
```

Common issues:
- Permission denied on `/var/spool/slurm` - ensure Dockerfile creates this directory
- Database connection failed - wait for mysql to be healthy

### Job stuck in PENDING

```bash
# Check why job is pending
docker exec slurmctld scontrol show job <job_id> | grep Reason

# Common reasons:
# - Resources: Not enough memory on nodes
# - PartitionNodeLimit: Requested more nodes than available
```
