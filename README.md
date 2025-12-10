# Slurm Memory & Time Escalation System

Automatically retry failed Slurm jobs with escalated resources. When jobs fail due to OOM (Out of Memory) or TIMEOUT, the system automatically resubmits them with more memory or time.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         USER SUBMITS JOB                            │
│                    ./mem-escalate.sh my-job.sh                      │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      mem-escalate.sh (Main CLI)                     │
│  • Parses arguments                                                 │
│  • Creates checkpoint file                                          │
│  • Submits main job to Slurm (sbatch)                              │
│  • Submits handler with --dependency=afternotok:<main_job>         │
│  • Submits success-handler with --dependency=afterok:<main_job>    │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                              ▼
         ┌──────────────────┐           ┌──────────────────┐
         │    MAIN JOB      │           │  HANDLER (waits) │
         │   (your-job.sh)  │           │   afternotok     │
         └──────────────────┘           └──────────────────┘
                    │                              │
         ┌──────────┴──────────┐                   │
         ▼                     ▼                   │
    ┌─────────┐          ┌─────────┐               │
    │ SUCCESS │          │  FAIL   │               │
    │ (exit)  │          │(OOM/TO) │───────────────┘
    └─────────┘          └─────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                 mem-escalate-handler.sh (Runs on failure)           │
│  • Queries sacct for failed task states (OOM, TIMEOUT)             │
│  • Determines which tasks failed and why                            │
│  • Escalates memory or time to next level                          │
│  • Resubmits ONLY failed tasks with new resources                  │
│  • Submits NEW handler for the retry job                           │
│  • Updates checkpoint file                                          │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
                         (Cycle repeats until
                          all tasks complete
                          or max level reached)
```

### Component Roles

| File | Role | When it runs |
|------|------|--------------|
| `mem-escalate.sh` | **Main CLI** - User interface, submits initial job | User runs it manually |
| `mem-escalate-handler.sh` | **Failure handler** - Detects failures, escalates, resubmits | Slurm runs it when job fails |
| `mem-escalate-lib.py` | **Shared library** - Checkpoint management, config parsing, status display | Called by both shell scripts |

### Slurm Dependencies

The system uses Slurm's job dependency feature:

```bash
# When mem-escalate.sh submits:
sbatch my-job.sh                           # Main job (ID: 100)
sbatch --dependency=afternotok:100 handler # Runs ONLY if job 100 fails
sbatch --dependency=afterok:100 success    # Runs ONLY if job 100 succeeds
```

- `afternotok` = Run after job fails (any task OOM/TIMEOUT/error)
- `afterok` = Run after job succeeds (all tasks complete)

### Data Flow

```
mem-escalate.sh ──creates──▶ /data/tracker/checkpoints/<chain_id>.checkpoint(DB also)
       │                              │
       │                              │ (shared state)
       ▼                              ▼
mem-escalate-handler.sh ──reads/updates──▶ checkpoint file
       │
       │ ──calls──▶ mem-escalate-lib.py (Python utilities)
```

The checkpoint file tracks the entire escalation chain - which jobs ran, what failed, current level, etc.

## Current Features

- Automatic OOM detection and memory escalation, automatic TIMEOUT detection and time limit escalation
- Stride compression for large array jobs (reduces 2000+ char specs to ~10 chars)(This isn't really necessary but i had long tests that has 18,19 28,29, etc that failed. )
- Checkpoint system for tracking job chains
- Watch mode for live status monitoring (`--status <chain> --watch`)

## Files

| File | Description |
|------|-------------|
| `mem-escalate.sh` | Main CLI - submit jobs with escalation |
| `mem-escalate-handler.sh` | Handler - runs on failure, escalates and resubmits |
| `mem-escalate-lib.py` | Python library for checkpoints and compression |
| `escalation-config.yaml` | Configuration for memory/time ladders |
| `variable-job.sh` | Test job with predictable failures |
| `test-scenarios.sh` | Pre-configured test scenarios |
| `clean-slurm.sh` | Cleanup utility for testing |

## Test Environment

### Docker Cluster Setup

This system is tested on a Docker-based Slurm cluster - https://github.com/giovtorres/slurm-docker-cluster

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Containers                         │
├─────────────┬─────────────┬─────────────┬─────────────┬─────┤
│  slurmctld  │  slurmdbd   │    mysql    │ slurmrestd  │     │
│  (control)  │  (acct db)  │   (mariadb) │   (API)     │     │
├─────────────┴─────────────┴─────────────┴─────────────┴─────┤
│                    Compute Nodes                             │
├─────────────┬─────────────┬─────────────┬─────────────┬─────┤
│     c1      │     c2      │     c3      │     c4      │     │
│  4 CPU/16GB │  4 CPU/16GB │  4 CPU/16GB │  4 CPU/16GB │     │
└─────────────┴─────────────┴─────────────┴─────────────┴─────┘
```

**Current Cluster Configuration:**
- **Cluster Name:** `oomtest`
- **Nodes:** 4 (c1-c4), each with 4 CPUs and 16GB RAM
- **MaxArraySize:** 10001 (supports up to 10,000 array tasks)
- **Partition:** `normal`

**Volume Mounts:**
- `/data/jobs` → `examples/jobs/` (scripts)
- `/data` → job output files and tracker data

### Escalation Ladders (from escalation-config.yaml)

| Level | Memory | Time |
|-------|--------|------|
| 0 | 1G | 1 min |
| 1 | 2G | 2 min |
| 2 | 4G | 4 min |
| 3 | 8G | 8 min |
| 4 | 16G | 15 min |

## Quick Start

### 1. Clean Previous State

```bash
docker exec slurmctld /data/jobs/clean-slurm.sh
```

### 2. Run a Test Scenario

```bash
# Small test (20 tasks) - good for quick validation
docker exec slurmctld bash -c "cd /data && /data/jobs/mem-escalate.sh --array=0-19 --throttle=10 /data/jobs/variable-job.sh"

# Fast scenario (500 tasks) - ~5 min with escalations
docker exec slurmctld bash -c "cd /data && /data/jobs/test-scenarios.sh fast"
```

**Available test scenarios:**

| Scenario | Tasks | Expected Behavior |
|----------|-------|-------------------|
| `quick` | 100 | All complete quickly, no escalation |
| `fast` | 500 | Mixed OOM/TIMEOUT, multiple escalations |
| `oom` | 100 | All OOM at 1G, escalate to 2G |
| `timeout` | 100 | All timeout at 1min, escalate to 2min |
| `mixed` | 1000 | Full mix of behaviors |
| `large` | 5000 | Stress test |

### 3. Test Job Behavior (variable-job.sh)

The test job `variable-job.sh` has predictable behavior based on task ID:

| Task ID mod 10 | Behavior | Level 0 | Level 1 | Level 2 | Level 3 |
|----------------|----------|---------|---------|---------|---------|
| 0-4 | Quick (5s) | ✓ Complete | - | - | - |
| 5-6 | Medium (75s) | TIMEOUT | ✓ Complete | - | - |
| 7 | Slow (150s) | TIMEOUT | TIMEOUT | ✓ Complete | - |
| 8 | Memory hog (1.5GB) | OOM | ✓ Complete | - | - |
| 9 | Very slow (300s) | TIMEOUT | TIMEOUT | TIMEOUT | ✓ Complete |

### 4. Monitor Progress

```bash
# Check job queue
docker exec slurmctld squeue -a

# Check job states
docker exec slurmctld sacct -j <job_id> -X --format=JobID,State,MaxRSS,Elapsed

# View chain status
docker exec slurmctld bash -c "cd /data && ./mem-escalate.sh --status <chain_id>"

# Watch mode - live updates (like nvidia-smi -l)
docker exec -it slurmctld bash -c "cd /data && ./mem-escalate.sh --status <chain_id> --watch"

# List all chains
docker exec slurmctld ls /data/tracker/checkpoints/
```

### 5. Web Visualizer (Optional)

Start the visualizer server (on the host machine):
```bash
python3 visualizer-server.py --data-dir /path/to/data
```

Open http://localhost:8080 in your browser.

## Configuration

Edit `escalation-config.yaml` to customize behavior:

```yaml
# Memory escalation ladder
memory:
  ladder:
    - 1G      # Level 0: Start here
    - 2G      # Level 1: After first OOM
    - 4G      # Level 2
    - 8G      # Level 3
    - 16G     # Level 4: Maximum
  max_level: 4

# Time escalation ladder
# NOTE: Slurm has 1-minute minimum resolution
time:
  ladder:
    - "00:01:00"   # Level 0: 1 minute
    - "00:02:00"   # Level 1: 2 minutes
    - "00:04:00"   # Level 2: 4 minutes
    - "00:08:00"   # Level 3: 8 minutes
    - "00:15:00"   # Level 4: 15 minutes
  max_level: 4

# Retry settings
retry:
  max_retries: 5      # Maximum escalation attempts
  retry_delay: 5      # Seconds between retries
```

## Command Reference

### mem-escalate.sh

```bash
./mem-escalate.sh [OPTIONS] <script> [script-args...]

Options:
  --config <file>      Use custom config file
  --array=<spec>       Submit as array job (e.g., 0-999)
  --throttle=<n>       Limit concurrent array tasks
  --start-mem <mem>    Override starting memory (e.g., 2G)
  --max-level <n>      Override max escalation level
  --status <chain_id>  Show status of existing chain
  --list               List all checkpoints
  --no-wait            Don't wait for job completion
```

### test-scenarios.sh

```bash
./test-scenarios.sh <scenario>

Scenarios:
  quick    100 tasks, all complete quickly
  fast     500 tasks, ~5 min with escalations
  mixed    1000 tasks, mixed behavior
  large    5000 tasks, stress test
  max      10000 tasks (requires MaxArraySize >= 10001)
```

### clean-slurm.sh

```bash
./clean-slurm.sh

# Cleans up:
# - Cancels all running jobs
# - Removes output files (/data/*.out)
# - Clears tracker directory
# - Purges job history from database
```

## How It Works

### Escalation Flow

1. **Initial Submit**: Job submitted at Level 0 (1G memory, 1 min time)
2. **Handler Attached**: A handler job waits with `--dependency=afternotok`
3. **Failure Detection**: If any task fails, handler runs
4. **Analysis**: Handler queries `sacct` to find OOM and TIMEOUT failures
5. **Escalation**: Failed tasks resubmitted at next level (2G, 2 min)
6. **Repeat**: New handler attached, process repeats until success or max level

### Stride Compression

For large array jobs, the system compresses task indices:

| Pattern | Before | After | Savings |
|---------|--------|-------|---------|
| OOM tasks (mod 8) | `8,18,28,...,4998` | `8-4998:10` | 99% |
| Interleaved | `5,6,15,16,25,26,...` | `5-4995:10,6-4996:10` | 99% |

This prevents "Pathname too long" errors when resubmitting large arrays.

### Checkpoints

Each job chain has a checkpoint file in `/data/tracker/checkpoints/`:

```yaml
chain_id: 20251210-082008-yf2o
state:
  current_level: 1
  current_memory: 2G
  completed_count: 25
  status: RUNNING
rounds:
  - job_id: 2518
    level: 0
    memory: 1G
    status: COMPLETED
  - job_id: 2637
    level: 1
    memory: 2G
    status: RUNNING
```

## Expected Behavior with variable-job.sh

The test job has predictable behavior based on task ID:

| Task ID mod 10 | Behavior | Level 0 | Level 1 | Level 2 | Level 3 |
|----------------|----------|---------|---------|---------|---------|
| 0-4 | Quick (5s) | COMPLETE | - | - | - |
| 5-6 | Medium (75s) | TIMEOUT | COMPLETE | - | - |
| 7 | Slow (150s) | TIMEOUT | TIMEOUT | COMPLETE | - |
| 8 | Memory hog | OOM | COMPLETE | - | - |
| 9 | Very slow (300s) | TIMEOUT | TIMEOUT | TIMEOUT | COMPLETE |
