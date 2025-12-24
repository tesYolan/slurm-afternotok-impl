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
│  • Submits handler with --dependency=afternotok:<main_job>         |
|  * In some cases long cases might need to submit afterany          │
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
- `afterany` = sometimes if the array size is too big; we can create multiple batch arrays and use afterany to check escalation logic. 

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

This system is tested on a Docker-based Slurm cluster. See `docker/README.md` for full setup instructions.

**Multi-Partition Cluster (18 nodes):**

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Control Services                                   │
│  slurmctld | slurmdbd | mysql | slurmrestd                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│  DEVEL (10 nodes @ 1GB)    d1  d2  d3  d4  d5  d6  d7  d8  d9  d10          │
│  DAY* (5 nodes @ 2GB)      n1  n2  n3  n4  n5                                │
│  WEEK (2 nodes @ 8GB)      w1  w2                                            │
│  PI_JETZ (1 node @ 16GB)   p1                                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

| Partition | Nodes     | Memory | Time Limit | Purpose |
|-----------|-----------|--------|------------|---------|
| devel     | d[1-10]   | 1 GB   | 1 min      | Quick tests |
| day*      | n[1-5]    | 2 GB   | 1 min      | Default partition |
| week      | w[1-2]    | 8 GB   | 2 min      | Medium escalation |
| pi_jetz   | p1        | 16 GB  | 8 min      | Final fallback |

**Volume Mounts:**
- `/data/jobs` → `slurm-afternotok-impl/` (scripts)
- `/data` → job output files and tracker data

### Escalation Levels (from escalation-target.yaml)

| Level | Partition   | Memory | Time |
|-------|-------------|--------|------|
| 1     | devel,day   | 1G     | 1 min |
| 2     | week        | 4G     | 2 min |
| 3     | week        | 8G     | 4 min |
| 4     | pi_jetz     | 16G    | 8 min |

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

# Big scenario (10500 tasks)
docker exec slurmctld bash -c "cd /data/jobs && ./mem-escalate.sh --config escalation-target.yaml --array=0-10499 tests/variable-job.sh"
```

Check indices: `tests/compress_indices_test.sh` exercises the array compression helper.
# TODO conditions need more tests. 

**Available test scenarios:**

| Scenario | Tasks | Expected Behavior |
|----------|-------|-------------------|
| `levels` | 20 | Tests all 4 escalation levels (recommended) |
| `quick` | 100 | All complete quickly, no escalation |
| `timeout` | 100 | All timeout at 1min, escalate to 2min |
| `oom` | 100 | All OOM at 1G, escalate to 2G |
| `mixed` | 1000 | Full mix of behaviors |
| `mixed-fail` | 30 | Mix of complete, escalate, and code errors |
| `large` | 5000 | Stress test |
| `max` | 10000 | Maximum array size test |

### 3. Test Job Behavior (variable-job.sh)

The test job `variable-job.sh` has predictable behavior based on task ID:

| Task ID mod 10 | Behavior | Level 0 | Level 1 | Level 2 | Level 3 |
|----------------|----------|---------|---------|---------|---------|
| 0-4 | Quick (5s) | ✓ Complete | - | - | - |
| 5-6 | Medium (75s) | TIMEOUT | ✓ Complete | - | - |
| 7 | Slow (150s) | TIMEOUT | TIMEOUT | ✓ Complete | - |
| 8 | Memory hog (1.5GB) | OOM | ✓ Complete | - | - |
| 9 | Very slow (300s) | TIMEOUT | TIMEOUT | TIMEOUT | ✓ Complete |

**Code Error Testing with FAIL_TASKS:**

You can make specific tasks fail with exit code 1 (not retried) using the `FAIL_TASKS` environment variable:

```bash
# Tasks 10, 11, 12 will exit with code 1 and NOT be retried
./mem-escalate.sh --array=0-29 --export="FAIL_TASKS=10:11:12" variable-job.sh
```

Note: Use colons (`:`) to separate task IDs since commas conflict with Slurm's `--export` syntax.

### 4. Monitor Progress

```bash
# Check job queue
docker exec slurmctld squeue -a

# Check job states
docker exec slurmctld sacct -j <job_id> -X --format=JobID,State,MaxRSS,Elapsed

# Find the chains running
docker exec slurmctld bash -c "cd /data && ./mem-escalate.sh --list"

# View chain status
docker exec slurmctld bash -c "cd /data && ./mem-escalate.sh --status <chain_id>"

# Watch mode - live updates (like nvidia-smi -l)
docker exec -it slurmctld bash -c "cd /data && ./mem-escalate.sh --status <chain_id> --watch"

# List all chains
docker exec slurmctld ls /data/tracker/checkpoints/
```

### 5. Generate Test Reports

Generate markdown reports from checkpoint files and the database:

```bash
# Basic report with job IDs
docker exec slurmctld python3 /data/jobs/mem-escalate-lib.py generate-report \
  /data/tracker/checkpoints/<chain_id>.checkpoint

# With database summary (shows task counts)
docker exec slurmctld python3 /data/jobs/mem-escalate-lib.py generate-report \
  /data/tracker/checkpoints/<chain_id>.checkpoint \
  --db /data/tracker/escalation.db

# Full detail (per-round breakdown with status distribution, runtime stats, node distribution)
docker exec slurmctld python3 /data/jobs/mem-escalate-lib.py generate-report \
  /data/tracker/checkpoints/<chain_id>.checkpoint \
  --db /data/tracker/escalation.db --detailed

# All chains report
docker exec slurmctld python3 /data/jobs/mem-escalate-lib.py generate-report \
  /data/tracker/checkpoints --all --db /data/tracker/escalation.db

# Save to file
docker exec slurmctld python3 /data/jobs/mem-escalate-lib.py generate-report \
  /data/tracker/checkpoints/<chain_id>.checkpoint \
  --db /data/tracker/escalation.db --detailed > test-report.md
```

**Example output (with `--detailed`):**

```
## Chain: 20251224-053904-t0l3

### Escalation Rounds

| Round | Job ID | Handler | Memory | Tasks | Done | OOM | Timeout | Failed | Status |
|-------|--------|---------|--------|-------|------|-----|---------|--------|--------|
| 1 | 50069 | 50070 | 1G | 30 | 12 | 3 | 9 | 6 | ESCALATING |
| 2 | 50101 | 50102 | 2G | 12 | 6 | 0 | 6 | 0 | ESCALATING |
| 3 | 50115 | 50116 | 4G | 6 | 6 | 0 | 0 | 0 | COMPLETED |

### Task Details (from database)

#### Round 1: 1G

**Status Distribution:**

| Status | Count |
|--------|-------|
| COMPLETED | 12 |
| TIMEOUT | 9 |
| FAILED | 6 |
| OUT_OF_MEMORY | 3 |

**Runtime:** min=00:00:01, max=00:01:26

### Failed Tasks (Not Retried)

**6** tasks failed with code errors and were not escalated.

**Failed task details (first 20):**

| Task ID | Status | Exit Code | Node | Elapsed |
|---------|--------|-----------|------|---------|
| 0 | FAILED | 1 | d1 | 00:00:03 |
| 5 | FAILED | 1 | d6 | 00:00:02 |
| 10 | FAILED | 1 | n1 | 00:00:03 |
| 15 | FAILED | 1 | n3 | 00:00:02 |
| 20 | FAILED | 1 | w1 | 00:00:01 |
| 25 | FAILED | 1 | w2 | 00:00:03 |

### Summary

**24** of 30 tasks completed. **6** failed (not retried).
```

### 6. Web Visualizer (Not really tested)

Start the visualizer server (on the host machine):
```bash
python3 visualizer-server.py --data-dir /path/to/data
```

Open http://localhost:8080 in your browser.

## Configuration

Edit `escalation-target.yaml` to customize behavior:

```yaml
levels:
  - partition: devel,day,week,pi_jetz
    mem: 1G
    time: "00:01:00"

  - partition: day,week,pi_jetz          # 2G: devel excluded (1G nodes)
    mem: 2G
    time: "00:02:00"

  - partition: week,pi_jetz              # 4G: only 8G+ nodes
    mem: 4G
    time: "00:04:00"

  - partition: pi_jetz                   # 8G: final fallback (1 node)
    mem: 8G
    time: "00:08:00"
```

## Command Reference

### mem-escalate.sh

```bash
./mem-escalate.sh [OPTIONS] <script> [script-args...]

Options:
  -a, --array <spec>     Submit as array job (e.g., 0-999) [REQUIRED]
  -t, --throttle <n>     Limit concurrent array tasks
  -c, --config <file>    Use custom config file
  -e, --export <vars>    Pass environment variables to job (colon-separated values)
  -l, --max-level <n>    Override max escalation level
  -s, --status <chain>   Show status of existing chain
  -w, --watch [sec]      Watch mode with refresh interval (default: 5s)
      --list             List all checkpoints
      --no-wait          Don't wait for job completion
```

**Example with --export:**
```bash
# Pass FAIL_TASKS to make specific tasks exit with code 1
./mem-escalate.sh --array=0-99 --export="FAIL_TASKS=10:11:12" myjob.sh

# Note: Use colons (:) not commas (,) to separate values in --export
# because commas are Slurm's variable separator otherwise it fails for partition and related things. 
```

Notes:
- `--start-mem` maps to the nearest ladder entry; if the value is not in the ladder, level 0 is used. `--max-level` caps escalation regardless of the ladder length.
- Handlers depend on Slurm `afternotok`/`afterok` chaining; cancelling a handler cancels further retries for that chain.
- Script arguments containing spaces are preserved end-to-end; keep them quoted on the CLI (they are encoded between handlers to avoid splitting).
- To log into SQLite, set `logging.enabled: true` and ensure `logging.db_path` is on a path visible to the Slurm nodes (e.g., a host-mounted volume).

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

### Failure States

| State | Slurm States | Action |
|-------|--------------|--------|
| **OOM** | `OUT_OF_MEMORY`, exit 137 | Retry with more memory |
| **TIMEOUT** | `TIMEOUT` | Retry with more time |
| **FAILED** | `FAILED`, `CANCELLED`, exit 1+ | NOT retried (reported as failed) |
| **NODE_FAIL** | `NODE_FAIL` | Treated as failed (not retried) |

Tasks that fail with exit code 1 (or other non-OOM/TIMEOUT states) are recorded but **not retried**. This allows us to distinguish between resource issues (retryable) and code errors (not retryable).

### Stride Compression

For large array jobs, the system compresses task indices:

| Pattern | Before | After | Savings |
|---------|--------|-------|---------|
| OOM tasks (mod 8) | `8,18,28,...,4998` | `8-4998:10` | 99% |
| Interleaved | `5,6,15,16,25,26,...` | `5-4995:10,6-4996:10` | 99% |

This prevents "Pathname too long" errors when resubmitting large arrays.

### Batch Submissions for Large Arrays

The following part is mainly for handling conditions which were made visible by FAIL_MOD in which we fail many thigns; and thus escalaiton script / array compression can't do it in array that fit in the max_length limit. The approach i tried was the afterany where we split it.  If the cases of failure aren't as numerous; the afternotok approach is more elegant and better way. 

When the compressed array spec still exceeds Slurm's ~10,000 character limit, the handler automatically splits the submission into multiple batch jobs:

```
┌─────────────────────────────────────────────────────────────────────┐
│  Handler needs to retry 4941 failed tasks                           │
│  Compressed spec: "8,18,28,38,48,58,68,78,88,98,108,..."           │
│  Length: 15,234 characters  → TOO LONG (max ~10,000)               │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Split into batches of 500 tasks each:                              │
│                                                                     │
│  Batch 1: sbatch --array=8,18,28,...,4998 job.sh    → Job 11308    │
│  Batch 2: sbatch --array=5008,5018,...,9998 job.sh  → Job 11309    │
│  ...                                                                │
│  Batch 10: sbatch --array=... job.sh                → Job 11317    │
│                                                                     │
│  Returns: job_ids="11308,11309,11310,...,11317"                    │
└─────────────────────────────────────────────────────────────────────┘
```

### Handler Dependencies for Batch Jobs

When multiple batch jobs are submitted, the handler builds dependencies on ALL of them:

```bash
# Handler builds dependency string for all batch jobs:
dep_str="afternotok:11308,afternotok:11309,afternotok:11310,...,afternotok:11317"

# Submit next handler with combined dependency:
sbatch --dependency="$dep_str" handler.sh
```


### Batch Job Dependency Handling (afterany vs afternotok)

**Problem with afternotok for batch jobs:**

When using `afternotok:A,afternotok:B,afternotok:C` for multiple batch jobs, if ANY batch completes 100% successfully (no failures), Slurm marks the entire dependency as `DependencyNeverSatisfied`:

```
# If batch A has 0 failures:
afternotok:A  →  can NEVER be satisfied
afternotok:A,afternotok:B,afternotok:C  →  entire chain stuck!
```

**Solution:**

For batch jobs (multiple job IDs), the handler uses `afterany` instead:

```bash
# Single job: use afternotok (efficient)
--dependency=afternotok:12345

# Multiple batches: use afterany (handler checks for failures inside)
--dependency=afterany:11308,afterany:11309,afterany:11310,...
```

With `afterany`:
- Handler runs after ALL batches complete (regardless of success/failure)
- Handler checks sacct for failures and decides what to do
- If no failures: exits early (success handler takes over)
- If failures: escalates as usual

This ensures the chain never gets stuck with `DependencyNeverSatisfied`.

## Checkpoints

Each job chain has a checkpoint file in `/data/tracker/checkpoints/`:

## Controlling Which Failures Get Escalated

Not all failures should be retried with more resources. Code errors (bugs) will fail no matter how much memory/time you give them. The escalation system distinguishes between:

- **Escalate**: Retry with more resources (OOM, TIMEOUT)
- **No Retry**: Report as failed, don't waste resources retrying (code bugs)

### Configuration

In your escalation config YAML (e.g., `escalation-target.yaml`):

```yaml
state_handling:
  # These get retried with more resources
  OUT_OF_MEMORY: escalate
  TIMEOUT: escalate
  DEADLINE: escalate
  PREEMPTED: escalate
  BOOT_FAIL: escalate
  NODE_FAIL: escalate

  # These are NOT retried (code errors)
  FAILED: no_retry
  CANCELLED: no_retry

  # Exit code overrides (for FAILED state)
  exit_codes:
    137: escalate    # OOM killer (128+9) - retry this
    1: no_retry      # Generic error - don't retry (default)
```

### How failed jobs are filtered from escalation

When the `afternotok` handler runs, it goes through this process to exclude non-retryable failures:

```
┌─────────────────────────────────────────────────────────────────────┐
│                    HANDLER TRIGGERED (afternotok)                    │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  1. Query sacct for all task states                                 │
│     sacct -j <job_id> --format=JobID,State,ExitCode -X -P           │
│                                                                     │
│     Example output:                                                 │
│       806_0|COMPLETED|0:0                                           │
│       806_1|OUT_OF_MEMORY|0:9                                       │
│       806_2|TIMEOUT|0:15                                            │
│       806_3|FAILED|1:0         ← code error, exit(1)                │
│       806_17|FAILED|1:0        ← FAIL_MOD=17 triggered              │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  2. Classify each task using state_handling config                  │
│                                                                     │
│     For each task:                                                  │
│       - Check if exit code has override (e.g., 137 → escalate)     │
│       - Otherwise use state's default action                        │
│                                                                     │
│     Task 806_0:  COMPLETED       → skip (not a failure)            │
│     Task 806_1:  OUT_OF_MEMORY   → escalate                        │
│     Task 806_2:  TIMEOUT         → escalate                        │
│     Task 806_3:  FAILED (exit 1) → no_retry                        │
│     Task 806_17: FAILED (exit 1) → no_retry                        │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  3. Build separate index lists                                      │
│                                                                     │
│     ESCALATE_INDICES = [1, 2]      ← will be resubmitted           │
│     NO_RETRY_INDICES = [3, 17]     ← logged but NOT resubmitted    │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  4. Submit retry job with ONLY escalate indices                     │
│                                                                     │
│     sbatch --array=1,2 --mem=2G --time=00:10:00 job.sh             │
│            ^^^^^^^^                                                 │
│            Only tasks 1 and 2 are retried                          │
│            Tasks 3 and 17 are EXCLUDED                             │
└─────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│  5. Handler logs both categories                                    │
│                                                                     │
│     "Escalating: 2 tasks (OOM: 1, TIMEOUT: 1)"                     │
│     "NOT RETRYING: 2 tasks (other failures)"                       │
└─────────────────────────────────────────────────────────────────────┘
```

### What happens to no_retry tasks

Tasks marked `no_retry` are:
1. **Logged** in the handler output and checkpoint
2. **Tracked** in the status display (`40 FAILED` count)
3. **Reported** in the final summary
4. **NOT resubmitted** - they're excluded from the retry array

### Testing with intentional failures

Use `FAIL_MOD` to create code errors that should NOT be retried:

```bash
# Tasks divisible by 17 will exit(1) and NOT be escalated
./mem-escalate.sh --config escalation-target.yaml --array 0-999 --export "FAIL_MOD=17" job.sh
```