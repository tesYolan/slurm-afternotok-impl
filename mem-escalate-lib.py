#!/usr/bin/env python3
"""
Resource Escalation Library - Python utilities for mem-escalate.sh

This module provides checkpoint management, config parsing, and status
display for the Slurm levels-based escalation system.

Usage from bash:
    python3 mem-escalate-lib.py <command> [args...]

Commands:
    load-config <config_file>
    create-checkpoint <checkpoint_dir> <chain_id> <partition> <script> <max_level> <initial_mem> <initial_time> [args...]
    create-array-checkpoint <checkpoint_dir> <chain_id> <partition> <array_spec> <total_tasks> <script> <max_level> <initial_mem> <initial_time> [args...]
    load-checkpoint <checkpoint_file>
    show-status <checkpoint_file>
    analyze-job <job_id>
    compress-indices <indices>
    mark-completed <file> <job_id> <completed_count>
    mark-failed <file> <failed_indices> [LEVEL]
    update-escalation <file> <next_level> <next_mem> <failed_indices> <retry_job> <handler_id> <completed_count> <failed_count>
    log-action <db_path> <chain_id> <action_type> [--job-id] [--memory-level] [--indices] [--details]
    db-* commands for database operations
"""
from __future__ import annotations

import argparse
import sys
import json
import sqlite3
import yaml
import shlex
from datetime import datetime
from pathlib import Path
import subprocess
from contextlib import contextmanager

@contextmanager
def modify_checkpoint(path: str):
    """Read checkpoint, yield for modification, write back with updated timestamp."""
    try:
        with open(path) as f:
            data = yaml.safe_load(f)
        yield data
        data['updated'] = datetime.now().isoformat()
        with open(path, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    except Exception as e:
        print(f"Warning: Could not update checkpoint: {e}", file=sys.stderr)


def _output_global_config(config: dict) -> None:
    """Output global configuration settings."""
    # Timeout settings
    if 'timeout' in config:
        timeout = config['timeout']
        if 'sacct_delay' in timeout:
            print(f"SACCT_DELAY={timeout['sacct_delay']}")

    # Tracker paths
    if 'tracker' in config:
        tracker = config['tracker']
        if 'base_dir' in tracker:
            print(f"TRACKER_DIR={shlex.quote(str(tracker['base_dir']))}")
        if 'history_log' in tracker:
            print(f"HISTORY_LOG={shlex.quote(str(tracker['history_log']))}")
        if 'checkpoint_dir' in tracker:
            print(f"CHECKPOINT_DIR={shlex.quote(str(tracker['checkpoint_dir']))}")
        if 'output_dir' in tracker:
            print(f"OUTPUT_DIR={shlex.quote(str(tracker['output_dir']))}")

    # Logging settings
    if 'logging' in config:
        logging_cfg = config['logging']
        if 'enabled' in logging_cfg:
            print(f"LOGGING_ENABLED={'true' if logging_cfg['enabled'] else 'false'}")
        if 'db_path' in logging_cfg:
            print(f"LOGGING_DB_PATH={shlex.quote(str(logging_cfg['db_path']))}")
            print(f"DB_PATH={shlex.quote(str(logging_cfg['db_path']))}")

    # Array spec settings
    if 'max_array_spec_len' in config:
        print(f"MAX_ARRAY_SPEC_LEN={config['max_array_spec_len']}")

    # Cluster settings
    if 'cluster' in config:
        cluster = config['cluster']
        if 'name' in cluster:
            print(f"CLUSTER_NAME={shlex.quote(str(cluster['name']))}")
        if 'partition' in cluster:
            print(f"CLUSTER_PARTITION={shlex.quote(str(cluster['partition']))}")
        if 'nodes' in cluster:
            print(f"CLUSTER_NODES={shlex.quote(str(cluster['nodes']))}")


def load_config(config_path: str) -> None:
    """Parse YAML config and output shell variable assignments.

    Only supports levels-based config format (escalation-target.yaml).
    """
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"echo 'ERROR: Failed to parse config: {e}' >&2", file=sys.stdout)
        sys.exit(1)

    if 'levels' not in config:
        print("echo 'ERROR: Config must have levels section' >&2", file=sys.stdout)
        sys.exit(1)

    levels = config['levels']

    # Get partition from first level
    first_partition = levels[0].get('partition', 'devel')
    print(f"PARTITION={shlex.quote(first_partition)}")

    # Output max level
    print(f"MAX_LEVEL={len(levels) - 1}")
    print(f"LEVELS_CONFIG=true")

    # Output each level's config
    for i, lvl in enumerate(levels):
        print(f"LEVEL_{i}_PARTITION={shlex.quote(lvl.get('partition', 'devel'))}")
        print(f"LEVEL_{i}_MEM={shlex.quote(lvl['mem'])}")
        print(f"LEVEL_{i}_TIME={shlex.quote(lvl['time'])}")

    _output_global_config(config)


def create_checkpoint(checkpoint_dir: str, chain_id: str, partition: str, script: str,
                      max_level: int, initial_mem: str, initial_time: str,
                      script_args: list) -> None:
    """Create a new checkpoint file for a job chain (levels-based)."""
    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
    checkpoint_file = Path(checkpoint_dir) / f"{chain_id}.checkpoint"

    checkpoint = {
        'chain_id': chain_id,
        'partition': partition,
        'original_script': script,
        'script_args': script_args,
        'max_level': max_level,
        'created': datetime.now().isoformat(),
        'updated': datetime.now().isoformat(),
        'state': {
            'current_level': 0,
            'current_memory': initial_mem,
            'current_time': initial_time,
            'attempts': 0,
            'status': 'STARTING',
            'last_escalation_reason': None
        },
        'jobs': []
    }

    with open(checkpoint_file, 'w') as f:
        yaml.dump(checkpoint, f, default_flow_style=False, sort_keys=False)

    print(str(checkpoint_file))


def create_array_checkpoint(checkpoint_dir: str, chain_id: str, partition: str, array_spec: str,
                            total_tasks: int, script: str, max_level: int,
                            initial_mem: str, initial_time: str, script_args: list) -> None:
    """Create a checkpoint file for an array job chain (levels-based)."""
    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
    checkpoint_file = Path(checkpoint_dir) / f"{chain_id}.checkpoint"

    checkpoint = {
        'chain_id': chain_id,
        'mode': 'handler_chain',
        'partition': partition,
        'original_script': script,
        'script_args': script_args,
        'original_array_spec': array_spec,
        'total_tasks': total_tasks,
        'max_level': max_level,
        'created': datetime.now().isoformat(),
        'updated': datetime.now().isoformat(),
        'state': {
            'current_level': 0,
            'current_memory': initial_mem,
            'current_time': initial_time,
            'pending_indices': array_spec,
            'completed_count': 0,
            'status': 'STARTING',
            'last_escalation_reason': None
        },
        'rounds': []
    }

    with open(checkpoint_file, 'w') as f:
        yaml.dump(checkpoint, f, default_flow_style=False, sort_keys=False)

    print(str(checkpoint_file))


def load_checkpoint(checkpoint_file: str) -> None:
    """Load checkpoint and output shell variable assignments."""
    try:
        with open(checkpoint_file) as f:
            checkpoint = yaml.safe_load(f)
    except Exception as e:
        print(f"echo 'ERROR: Failed to parse checkpoint: {e}' >&2")
        sys.exit(1)

    # Output shell variables
    print(f"CHECKPOINT_FILE={shlex.quote(str(checkpoint_file))}")
    print(f"SCRIPT={shlex.quote(str(checkpoint['original_script']))}")
    if 'partition' in checkpoint:
        print(f"PARTITION={shlex.quote(str(checkpoint['partition']))}")

    # Script args
    args = checkpoint.get('script_args', [])
    if args:
        args_str = ' '.join(shlex.quote(str(a)) for a in args)
        print(f"SCRIPT_ARGS=({args_str})")
    else:
        print("SCRIPT_ARGS=()")

    # State
    state = checkpoint.get('state', {})
    print(f"RESUME_LEVEL={state.get('current_level', 0)}")
    print(f"RESUME_MEMORY={shlex.quote(str(state.get('current_memory', '1G')))}")
    print(f"RESUME_TIME={shlex.quote(str(state.get('current_time', '00:05:00')))}")
    print(f"RESUME_STATUS={shlex.quote(str(state.get('status', 'UNKNOWN')))}")
    print(f"MAX_LEVEL={checkpoint.get('max_level', 3)}")

    # Build job chain from history
    jobs = checkpoint.get('jobs', [])
    job_ids = ','.join(str(j['job_id']) for j in jobs)
    print(f"RESUME_CHAIN={shlex.quote(job_ids)}")


def list_checkpoints(checkpoint_dir: str) -> None:
    """List all checkpoints in the directory."""
    print("=" * 40)
    print("Available Checkpoints")
    print("=" * 40)

    checkpoint_path = Path(checkpoint_dir)
    if not checkpoint_path.exists():
        print(f"No checkpoint directory found: {checkpoint_dir}")
        return

    count = 0
    for checkpoint_file in sorted(checkpoint_path.glob("*.checkpoint")):
        count += 1
        try:
            with open(checkpoint_file) as f:
                cp = yaml.safe_load(f)

            chain_id = cp.get('chain_id', 'unknown')
            script = cp.get('original_script', 'unknown')
            state = cp.get('state', {})
            status = state.get('status', 'UNKNOWN')
            level = state.get('current_level', 0)
            memory = state.get('current_memory', '?')
            attempts = state.get('attempts', 0)
            updated = cp.get('updated', 'unknown')

            print(f"  {chain_id}")
            print(f"    Script:   {script}")
            print(f"    Status:   {status}")
            print(f"    Level:    {level} ({memory})")
            print(f"    Attempts: {attempts}")
            print(f"    Updated:  {updated}")
            print()
        except Exception as e:
            print(f"  Error reading {checkpoint_file}: {e}")

    if count == 0:
        print("No checkpoints found.")
    else:
        print(f"Total: {count} checkpoint(s)")

    print()
    print("To resume: ./mem-escalate.sh --resume <chain_id>")


def show_status(checkpoint_file: str) -> None:
    """Show detailed status of an escalation chain."""
    try:
        with open(checkpoint_file, 'r') as f:
            cp = yaml.safe_load(f)
    except Exception as e:
        print(f"ERROR: Could not read checkpoint: {e}", file=sys.stderr)
        sys.exit(1)

    print("=" * 50)
    print(f"Chain Status: {cp['chain_id']}")
    print("=" * 50)

    # Basic info
    print(f"Mode:     {cp.get('mode', 'single')}")
    print(f"Script:   {cp.get('original_script', 'unknown')}")
    args = cp.get('script_args', [])
    if args:
        print(f"Args:     {' '.join(args)}")

    if cp.get('original_array_spec'):
        print(f"Array:    {cp['original_array_spec']} ({cp.get('total_tasks', '?')} tasks)")

    print(f"Created:  {cp.get('created', 'unknown')}")
    print(f"Updated:  {cp.get('updated', 'unknown')}")
    print()

    # Current state
    state = cp.get('state', {})
    max_level = cp.get('max_level', '?')
    print(f"Status:   {state.get('status', 'UNKNOWN')}")
    print(f"Level:    {state.get('current_level', 0)} / {max_level}")
    print(f"Memory:   {state.get('current_memory', '?')}")
    print(f"Time:     {state.get('current_time', '?')}")

    # Show failure summary from last round
    rounds = cp.get('rounds', [])
    if rounds and state.get('status') == 'ESCALATING':
        last_round = rounds[-1]
        oom_count = last_round.get('oom_count', 0) or 0
        timeout_count = last_round.get('timeout_count', 0) or 0
        failed_count = last_round.get('failed_count', 0) or state.get('failed_count', 0) or 0
        escalate_total = oom_count + timeout_count
        print()
        print("Failures from last round:")
        if escalate_total > 0:
            print(f"  Escalating:  {escalate_total} tasks", end="")
            if oom_count or timeout_count:
                print(f" (OOM: {oom_count}, TIMEOUT: {timeout_count})", end="")
            print()
        if failed_count > 0:
            print(f"  Not Retried: {failed_count} tasks (code errors)")
        # Show indices folder location
        tracker_base = cp.get('config', {}).get('tracker', {}).get('base_dir', '/data/tracker')
        chain_id = cp.get('chain_id', 'unknown')
        print(f"  Indices:     {tracker_base}/indices/{chain_id}/")

    if state.get('pending_indices'):
        pending = state['pending_indices']
        if len(pending) > 50:
            pending = pending[:50] + "..."
        print(f"Pending:  {pending}")
    print()

    # Rounds history
    rounds = cp.get('rounds', [])
    if rounds:
        print("Rounds:")
        print("-" * 50)
        for i, r in enumerate(rounds, 1):
            job_id = r.get('job_id', '?')
            status = r.get('status', 'UNKNOWN')
            array_spec = r.get('array_spec', '')

            # Get memory level info (if present)
            mem_level = r.get('level')
            mem = r.get('memory')
            # Get time level info (if present)
            time_level = r.get('time_level')
            time_val = r.get('time')

            # Build level/resource string
            level_parts = []
            if mem_level is not None and mem:
                level_parts.append(f"Mem L{mem_level}: {mem}")
            if time_level is not None and time_val:
                level_parts.append(f"Time L{time_level}: {time_val}")
            level_str = ", ".join(level_parts) if level_parts else "?"

            # Count tasks in array spec
            task_count = 0
            if array_spec:
                for part in array_spec.split(','):
                    if '-' in part:
                        parts = part.split('-')[:2]
                        if len(parts) == 2:
                            try:
                                task_count += int(parts[1]) - int(parts[0]) + 1
                            except ValueError:
                                task_count += 1
                    else:
                        task_count += 1

            # Show all job IDs if batched
            all_job_ids = r.get('job_ids', [job_id])
            if isinstance(all_job_ids, list) and len(all_job_ids) > 1:
                job_display = f"Jobs {all_job_ids[0]}..{all_job_ids[-1]} ({len(all_job_ids)} batches)"
            else:
                job_display = f"Job {job_id}"
            print(f"  Round {i}: {job_display} ({level_str})")
            if task_count > 0:
                print(f"           Tasks: {task_count}")
            print(f"           Status: {status}")

            # Show handler status
            handler_id = r.get('handler_id')
            success_handler_id = r.get('success_handler_id') or (handler_id + 1 if handler_id else None)

            # Determine dependency type: afterany for batch jobs, afternotok/afterok for single
            is_batch = isinstance(all_job_ids, list) and len(all_job_ids) > 1
            fail_dep_type = "afterany" if is_batch else "afternotok"
            ok_dep_type = "afterany" if is_batch else "afterok"

            if handler_id:
                try:
                    # Get failure handler job state from squeue
                    result = subprocess.run(
                        ['squeue', '-j', str(handler_id), '-h', '-o', '%T %r'],
                        capture_output=True, text=True, timeout=5
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        handler_state = result.stdout.strip().split()[0]
                        if handler_state == 'PENDING':
                            print(f"           Failure Handler: Job {handler_id} - WAITING ({fail_dep_type})")
                        elif handler_state == 'RUNNING':
                            print(f"           Failure Handler: Job {handler_id} - RUNNING (escalating...)")
                    else:
                        # Handler not in queue - check if it ran
                        result = subprocess.run(
                            ['sacct', '-nX', '-j', str(handler_id), '-o', 'State%20'],
                            capture_output=True, text=True, timeout=5
                        )
                        if result.returncode == 0 and result.stdout.strip():
                            hstate = result.stdout.strip().split()[0]
                            if 'COMPLETED' in hstate:
                                print(f"           Failure Handler: Job {handler_id} - COMPLETED (escalated)")
                            elif 'CANCELLED' in hstate:
                                print(f"           Failure Handler: Job {handler_id} - CANCELLED (all succeeded)")
                except Exception:
                    pass

            if success_handler_id:
                try:
                    # Get success handler job state from squeue
                    result = subprocess.run(
                        ['squeue', '-j', str(success_handler_id), '-h', '-o', '%T %r'],
                        capture_output=True, text=True, timeout=5
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        handler_state = result.stdout.strip().split()[0]
                        if handler_state == 'PENDING':
                            print(f"           Success Handler: Job {success_handler_id} - WAITING ({ok_dep_type})")
                        elif handler_state == 'RUNNING':
                            print(f"           Success Handler: Job {success_handler_id} - RUNNING (completing...)")
                    else:
                        # Handler not in queue - check if it ran
                        result = subprocess.run(
                            ['sacct', '-nX', '-j', str(success_handler_id), '-o', 'State%20'],
                            capture_output=True, text=True, timeout=5
                        )
                        if result.returncode == 0 and result.stdout.strip():
                            hstate = result.stdout.strip().split()[0]
                            if 'COMPLETED' in hstate:
                                print(f"           Success Handler: Job {success_handler_id} - COMPLETED (all done!)")
                            elif 'CANCELLED' in hstate:
                                print(f"           Success Handler: Job {success_handler_id} - CANCELLED (had failures)")
                except Exception:
                    pass

            # Show array indices info for batch jobs
            if is_batch:
                chain_id = cp.get('chain_id', 'unknown')
                array_spec = r.get('array_spec', '')
                indices_hint = array_spec[:60]
                if len(array_spec) > 60:
                    indices_hint += f'... ({len(array_spec)} chars)'
                print(f"           Array Indices: {indices_hint}")
                # Point to indices folder for detailed failure logs
                tracker_base = cp.get('config', {}).get('tracker', {}).get('base_dir', '/data/tracker')
                print(f"           Indices folder: {tracker_base}/indices/{chain_id}/")

            # Try to get live status from sacct (query ALL job IDs if batched)
            job_ids = r.get('job_ids', [job_id])  # Use job_ids list if present
            if not isinstance(job_ids, list):
                job_ids = [job_ids]
            try:
                all_states = []
                for jid in job_ids:
                    result = subprocess.run(
                        ['sacct', '-nX', '-j', str(jid), '-o', 'State%20'],
                        capture_output=True, text=True, timeout=5
                    )
                    if result.returncode == 0 and result.stdout.strip():
                        all_states.extend(result.stdout.strip().split('\n'))
                if all_states:
                    completed = sum(1 for s in all_states if 'COMPLETED' in s)
                    oom = sum(1 for s in all_states if 'OUT_OF_MEMORY' in s)
                    timeout = sum(1 for s in all_states if 'TIMEOUT' in s)
                    failed = sum(1 for s in all_states if 'FAILED' in s and 'NODE_FAIL' not in s)
                    running = sum(1 for s in all_states if 'RUNNING' in s or 'PENDING' in s)
                    processed = completed + oom + timeout + failed + running
                    remaining = task_count - processed if task_count > 0 else 0
                    jobs_note = f" (from {len(job_ids)} batches)" if len(job_ids) > 1 else ""
                    remaining_note = f", {remaining} remaining" if remaining > 0 else ""
                    print(f"           Current: {completed} done, {oom} OOM, {timeout} TIMEOUT, {failed} FAILED, {running} active{remaining_note}{jobs_note}")
            except Exception:
                pass

            # Show result breakdown if available
            completed_count = r.get('completed_count', 0) or 0
            oom_count = r.get('oom_count', 0) or 0
            timeout_count = r.get('timeout_count', 0) or 0
            failed_count = r.get('failed_count', 0) or 0
            escalate_total = oom_count + timeout_count

            if completed_count or escalate_total or failed_count:
                parts = []
                if completed_count:
                    parts.append(f"{completed_count} done")
                if escalate_total:
                    esc_detail = []
                    if oom_count:
                        esc_detail.append(f"{oom_count} OOM")
                    if timeout_count:
                        esc_detail.append(f"{timeout_count} TIMEOUT")
                    esc_str = f"{escalate_total} escalating"
                    if esc_detail:
                        esc_str += f" ({', '.join(esc_detail)})"
                    parts.append(esc_str)
                if failed_count:
                    parts.append(f"{failed_count} failed (not retried)")
                print(f"           Results: {' | '.join(parts)}")

            print()
    else:
        print("No rounds recorded yet.")

    print("=" * 50)


def generate_report(checkpoint_path: str, all_checkpoints: bool = False,
                    db_path: str = None, detailed: bool = False) -> None:
    """Generate a markdown test report from checkpoint files and database.

    Args:
        checkpoint_path: Path to checkpoint file or directory (if --all)
        all_checkpoints: If True, generate report for all checkpoints in directory
        db_path: Path to SQLite database for task-level details
        detailed: If True, include per-task breakdown
    """
    from pathlib import Path

    # Collect checkpoint files
    files = []
    if all_checkpoints:
        checkpoint_dir = Path(checkpoint_path)
        if checkpoint_dir.is_dir():
            files = sorted(checkpoint_dir.glob('*.checkpoint'),
                          key=lambda f: f.stat().st_mtime, reverse=True)
        else:
            print(f"ERROR: {checkpoint_path} is not a directory", file=sys.stderr)
            sys.exit(1)
    else:
        files = [Path(checkpoint_path)]

    if not files:
        print("No checkpoint files found.", file=sys.stderr)
        sys.exit(1)

    # Connect to database if provided
    db_conn = None
    if db_path and Path(db_path).exists():
        db_conn = sqlite3.connect(db_path)
        db_conn.row_factory = sqlite3.Row

    # Generate report
    print("# Escalation Test Report")
    print()
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    for cp_file in files:
        try:
            with open(cp_file, 'r') as f:
                cp = yaml.safe_load(f)
        except Exception as e:
            print(f"## Error reading {cp_file.name}")
            print(f"Could not read: {e}")
            print()
            continue

        chain_id = cp.get('chain_id', 'unknown')
        state = cp.get('state', {})
        status = state.get('status', 'UNKNOWN')

        print(f"## Chain: {chain_id}")
        print()

        # Configuration table
        print("### Configuration")
        print()
        print("| Setting | Value |")
        print("|---------|-------|")
        print(f"| Script | `{cp.get('original_script', 'unknown')}` |")
        args = cp.get('script_args', [])
        if args:
            print(f"| Arguments | `{' '.join(args)}` |")
        if cp.get('original_array_spec'):
            print(f"| Array | `{cp['original_array_spec']}` ({cp.get('total_tasks', '?')} tasks) |")
        print(f"| Partition | {cp.get('partition', 'unknown')} |")
        print(f"| Max Level | {cp.get('max_level', '?')} |")
        print(f"| Status | **{status}** |")
        print(f"| Created | {cp.get('created', 'unknown')} |")
        print(f"| Updated | {cp.get('updated', 'unknown')} |")
        print()

        # Rounds table with job IDs
        rounds = cp.get('rounds', [])
        if rounds:
            print("### Escalation Rounds")
            print()
            print("| Round | Job ID | Handler | Memory | Tasks | Done | OOM | Timeout | Failed | Status |")
            print("|-------|--------|---------|--------|-------|------|-----|---------|--------|--------|")

            for i, r in enumerate(rounds, 1):
                job_id = r.get('job_id', '?')
                job_ids = r.get('job_ids', [job_id])
                handler_id = r.get('handler_id', '?')
                mem = r.get('memory', '?')

                # Format job ID display
                if isinstance(job_ids, list) and len(job_ids) > 1:
                    job_display = f"{job_ids[0]}..{job_ids[-1]}"
                else:
                    job_display = str(job_id)

                # Count tasks in array spec
                array_spec = r.get('array_spec', '')
                task_count = 0
                if array_spec:
                    for part in array_spec.split(','):
                        if '-' in part:
                            parts = part.split('-')[:2]
                            if len(parts) == 2:
                                try:
                                    task_count += int(parts[1]) - int(parts[0]) + 1
                                except ValueError:
                                    task_count += 1
                        else:
                            task_count += 1

                completed = r.get('completed_count', 0) or 0
                oom = r.get('oom_count', 0) or 0
                timeout = r.get('timeout_count', 0) or 0
                failed = r.get('failed_count', 0) or 0
                rstatus = r.get('status', 'UNKNOWN')

                print(f"| {i} | {job_display} | {handler_id} | {mem} | {task_count} | {completed} | {oom} | {timeout} | {failed} | {rstatus} |")

            print()

            # Task-level details from database
            if db_conn and detailed:
                cursor = db_conn.cursor()

                # Get task statistics per round
                print("### Task Details (from database)")
                print()

                for i, r in enumerate(rounds, 1):
                    job_id = r.get('job_id', 0)
                    job_ids = r.get('job_ids', [job_id])
                    mem = r.get('memory', '?')

                    print(f"#### Round {i}: {mem}")
                    print()

                    # Query task stats for all job IDs in this round
                    job_id_list = job_ids if isinstance(job_ids, list) else [job_id]
                    placeholders = ','.join('?' * len(job_id_list))

                    # Status distribution
                    cursor.execute(f'''
                        SELECT status, COUNT(*) as cnt
                        FROM tasks
                        WHERE chain_id = ? AND job_id IN ({placeholders})
                        GROUP BY status
                        ORDER BY cnt DESC
                    ''', (chain_id, *job_id_list))
                    status_rows = cursor.fetchall()

                    if status_rows:
                        print("**Status Distribution:**")
                        print()
                        print("| Status | Count |")
                        print("|--------|-------|")
                        for sr in status_rows:
                            print(f"| {sr['status']} | {sr['cnt']} |")
                        print()

                    # Runtime stats
                    cursor.execute(f'''
                        SELECT
                            COUNT(*) as total,
                            MIN(elapsed) as min_time,
                            MAX(elapsed) as max_time
                        FROM tasks
                        WHERE chain_id = ? AND job_id IN ({placeholders})
                    ''', (chain_id, *job_id_list))
                    time_row = cursor.fetchone()

                    if time_row and time_row['total'] > 0:
                        print(f"**Runtime:** min={time_row['min_time'] or 'N/A'}, max={time_row['max_time'] or 'N/A'}")
                        print()

                    # Node distribution
                    cursor.execute(f'''
                        SELECT node, COUNT(*) as cnt
                        FROM tasks
                        WHERE chain_id = ? AND job_id IN ({placeholders})
                        GROUP BY node
                        ORDER BY cnt DESC
                        LIMIT 10
                    ''', (chain_id, *job_id_list))
                    node_rows = cursor.fetchall()

                    if node_rows:
                        print("**Node Distribution (top 10):**")
                        print()
                        print("| Node | Tasks |")
                        print("|------|-------|")
                        for nr in node_rows:
                            print(f"| {nr['node']} | {nr['cnt']} |")
                        print()

            # Database summary stats (even without --detailed)
            elif db_conn:
                cursor = db_conn.cursor()
                cursor.execute('''
                    SELECT COUNT(*) as total,
                           SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as completed,
                           SUM(CASE WHEN status = 'OUT_OF_MEMORY' THEN 1 ELSE 0 END) as oom,
                           SUM(CASE WHEN status = 'TIMEOUT' THEN 1 ELSE 0 END) as timeout,
                           SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed
                    FROM tasks
                    WHERE chain_id = ?
                ''', (chain_id,))
                stats = cursor.fetchone()

                if stats and stats['total'] > 0:
                    print("### Database Task Summary")
                    print()
                    print(f"Total task records: {stats['total']}")
                    print(f"- Completed: {stats['completed']}")
                    print(f"- OOM: {stats['oom']}")
                    print(f"- Timeout: {stats['timeout']}")
                    print(f"- Failed: {stats['failed']}")
                    print()
                    print("*Use `--detailed` for per-round breakdown*")
                    print()

            # Failed tasks section (tasks that weren't retried due to code errors)
            # Sum failed_count from all rounds (state.failed_count may not be aggregated)
            final_failed = state.get('failed_count', 0) or 0
            if final_failed == 0:
                for r in rounds:
                    final_failed += r.get('failed_count', 0) or 0
            failed_indices = state.get('failed_indices', '')

            if final_failed > 0 or failed_indices:
                print("### Failed Tasks (Not Retried)")
                print()
                print(f"**{final_failed}** tasks failed with code errors and were not escalated.")
                print()
                if failed_indices:
                    # Truncate if too long
                    if len(failed_indices) > 200:
                        display_indices = failed_indices[:200] + f"... ({len(failed_indices)} chars total)"
                    else:
                        display_indices = failed_indices
                    print(f"Failed task indices: `{display_indices}`")
                    print()

                # Query database for failed task details if available
                if db_conn:
                    cursor = db_conn.cursor()
                    cursor.execute('''
                        SELECT task_id, status, exit_code, node, elapsed
                        FROM tasks
                        WHERE chain_id = ? AND (status = 'FAILED' OR status LIKE '%CANCEL%')
                        ORDER BY task_id
                        LIMIT 20
                    ''', (chain_id,))
                    failed_rows = cursor.fetchall()

                    if failed_rows:
                        print("**Failed task details (first 20):**")
                        print()
                        print("| Task ID | Status | Exit Code | Node | Elapsed |")
                        print("|---------|--------|-----------|------|---------|")
                        for fr in failed_rows:
                            print(f"| {fr['task_id']} | {fr['status']} | {fr['exit_code']} | {fr['node'] or 'N/A'} | {fr['elapsed'] or 'N/A'} |")
                        print()

            # Summary
            total_tasks = cp.get('total_tasks', 0) or 0

            print("### Summary")
            print()
            if status == 'COMPLETED':
                if final_failed > 0:
                    completed = total_tasks - final_failed
                    print(f"**{completed}** of {total_tasks} tasks completed. **{final_failed}** failed (not retried).")
                else:
                    print(f"All **{total_tasks}** tasks completed successfully.")
            elif status == 'FAILED':
                print(f"Chain failed with {final_failed} unrecoverable tasks.")
            else:
                print(f"Chain status: {status}")
            print()
        else:
            print("*No rounds recorded yet.*")
            print()

        print("---")
        print()

    if db_conn:
        db_conn.close()


def get_chain_state(checkpoint_file: str) -> None:
    """Get simple chain state for scripting (prints: RUNNING/COMPLETED/FAILED/ESCALATING)."""
    try:
        with open(checkpoint_file, 'r') as f:
            cp = yaml.safe_load(f)
    except Exception:
        print("UNKNOWN")
        return

    state = cp.get('state', {})
    status = state.get('status', 'UNKNOWN')
    print(status)


def update_array_round(checkpoint_file: str, job_ids_str: str, handler_id: int,
                       array_spec: str, level: int, memory: str) -> None:
    """Update checkpoint with a new array round.

    Args:
        job_ids_str: Comma-separated job IDs (when batched) or single job ID
    """
    # Parse job IDs (may be comma-separated if batched)
    job_ids = [int(jid.strip()) for jid in job_ids_str.split(',') if jid.strip()]
    first_job = job_ids[0] if job_ids else 0

    with modify_checkpoint(checkpoint_file) as cp:
        cp['state']['status'] = 'RUNNING'
        if 'rounds' not in cp:
            cp['rounds'] = []
        cp['rounds'].append({
            'job_id': first_job,  # Backward compat
            'job_ids': job_ids,   # All batch job IDs
            'handler_id': handler_id, 'array_spec': array_spec,
            'level': level, 'memory': memory, 'status': 'RUNNING',
            'submitted': datetime.now().isoformat()
        })


def mark_completed(checkpoint_file: str, job_id: int, completed_count: int) -> None:
    """Mark the chain as completed."""
    with modify_checkpoint(checkpoint_file) as cp:
        cp['state']['status'] = 'COMPLETED'
        cp['state']['completed_count'] = completed_count
        cp['state']['pending_indices'] = ''
        # Update the specific round with this job_id, or last round if not found
        round_updated = False
        for round_data in cp.get('rounds', []):
            if round_data.get('job_id') == job_id:
                round_data['status'] = 'COMPLETED'
                round_data['completed_count'] = completed_count
                round_updated = True
                break
        if not round_updated and cp.get('rounds'):
            cp['rounds'][-1]['status'] = 'COMPLETED'
            cp['rounds'][-1]['completed_count'] = completed_count
    print(f"Checkpoint updated: {checkpoint_file}")


def mark_failed(checkpoint_file: str, failed_indices: str, reason: str = "MEMORY") -> None:
    """Mark the chain as failed at max level. reason='MEMORY' or 'TIME'"""
    with modify_checkpoint(checkpoint_file) as cp:
        cp['state']['status'] = f'FAILED_MAX_{reason}'
        cp['state']['failed_indices'] = failed_indices


def update_escalation(checkpoint_file: str, next_level: int, next_mem: str,
                      escalate_indices: str, retry_job_str: str, handler_id: int,
                      completed_count: int, escalate_count: int,
                      oom_count: int = 0, timeout_count: int = 0,
                      failed_count: int = 0) -> None:
    """Update checkpoint during escalation to next level.

    Args:
        escalate_indices: Combined indices that will be retried (OOM + TIMEOUT)
        retry_job_str: Comma-separated job IDs (when batched) or single job ID
        escalate_count: Total count being escalated
        oom_count: Number of OOM failures
        timeout_count: Number of TIMEOUT failures
        failed_count: Number of failures NOT being retried (code errors)
    """
    # Parse job IDs (may be comma-separated if batched)
    job_ids = [int(jid.strip()) for jid in str(retry_job_str).split(',') if jid.strip()]
    first_job = job_ids[0] if job_ids else 0

    with modify_checkpoint(checkpoint_file) as cp:
        cp['state']['current_level'] = next_level
        cp['state']['current_memory'] = next_mem
        cp['state']['pending_indices'] = escalate_indices
        cp['state']['status'] = 'ESCALATING'
        cp['state']['escalate_count'] = escalate_count  # Total being retried
        cp['state']['failed_count'] = failed_count  # Track no_retry failures
        if cp.get('rounds'):
            cp['rounds'][-1]['status'] = 'ESCALATING'
            cp['rounds'][-1]['completed_count'] = completed_count
            cp['rounds'][-1]['oom_count'] = oom_count
            cp['rounds'][-1]['timeout_count'] = timeout_count
            cp['rounds'][-1]['failed_count'] = failed_count
            cp['rounds'][-1]['escalate_indices'] = escalate_indices
        if 'rounds' not in cp:
            cp['rounds'] = []
        cp['rounds'].append({
            'job_id': first_job,  # Backward compat
            'job_ids': job_ids,   # All batch job IDs
            'handler_id': handler_id, 'array_spec': escalate_indices,
            'level': next_level, 'memory': next_mem, 'status': 'PENDING',
            'submitted': datetime.now().isoformat()
        })
    print(f"Checkpoint updated: {checkpoint_file}")

# ============================================================
# DATABASE STORAGE (SQLite)
# ============================================================

def init_db(db_path: str) -> None:
    """Initialize the database with all tables."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Chain metadata
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS chains (
            chain_id TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            partition TEXT,
            original_script TEXT NOT NULL,
            script_args TEXT,
            original_array_spec TEXT,
            total_tasks INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            status TEXT NOT NULL,
            current_level INTEGER DEFAULT 0,
            current_memory TEXT,
            current_time_level INTEGER DEFAULT 0,
            current_time TEXT,
            last_escalation_reason TEXT,
            pending_indices TEXT,
            completed_count INTEGER DEFAULT 0
        )
    ''')

    # Escalation rounds
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rounds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain_id TEXT NOT NULL,
            round_num INTEGER NOT NULL,
            job_id INTEGER NOT NULL,
            handler_id INTEGER,
            array_spec TEXT,
            level INTEGER NOT NULL,
            memory TEXT NOT NULL,
            time_level INTEGER,
            time TEXT,
            partition TEXT,
            status TEXT NOT NULL,
            submitted_at TEXT NOT NULL,
            completed_at TEXT,
            oom_count INTEGER DEFAULT 0,
            timeout_count INTEGER DEFAULT 0,
            oom_indices TEXT,
            timeout_indices TEXT,
            output_pattern TEXT,
            error_pattern TEXT,
            FOREIGN KEY (chain_id) REFERENCES chains(chain_id)
        )
    ''')

    # Per-task tracking
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chain_id TEXT NOT NULL,
            round_id INTEGER NOT NULL,
            job_id INTEGER NOT NULL,
            task_id INTEGER NOT NULL,
            status TEXT,
            exit_code INTEGER,
            signal INTEGER,
            max_rss TEXT,
            elapsed TEXT,
            timelimit TEXT,
            node TEXT,
            submit_time TEXT,
            start_time TEXT,
            end_time TEXT,
            output_path TEXT,
            error_path TEXT,
            FOREIGN KEY (chain_id) REFERENCES chains(chain_id),
            FOREIGN KEY (round_id) REFERENCES rounds(id)
        )
    ''')

    # Actions log
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            chain_id TEXT NOT NULL,
            action_type TEXT NOT NULL,
            job_id TEXT,
            memory_level INTEGER,
            time_level INTEGER,
            indices TEXT,
            details TEXT
        )
    ''')

    # Config snapshots
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS configs (
            chain_id TEXT PRIMARY KEY,
            config_yaml TEXT NOT NULL,
            levels_json TEXT,
            FOREIGN KEY (chain_id) REFERENCES chains(chain_id)
        )
    ''')

    # Create indexes
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rounds_chain ON rounds(chain_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_rounds_job ON rounds(job_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_chain ON tasks(chain_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_job ON tasks(job_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_actions_chain ON actions(chain_id)')

    conn.commit()
    conn.close()


def db_create_chain(db_path: str, chain_id: str, mode: str, partition: str,
                    script: str, script_args: list, array_spec: str | None = None,
                    total_tasks: int | None = None, memory: str | None = None, time: str | None = None) -> None:
    """Create a new chain record in the database."""
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    now = datetime.now().isoformat()
    cursor.execute('''
        INSERT OR REPLACE INTO chains
        (chain_id, mode, partition, original_script, script_args, original_array_spec,
         total_tasks, created_at, updated_at, status, current_level, current_memory,
         current_time_level, current_time, completed_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        chain_id, mode, partition, script, json.dumps(script_args),
        array_spec, total_tasks, now, now, 'RUNNING', 0, memory, 0, time, 0
    ))

    conn.commit()
    conn.close()


def db_add_round(db_path: str, chain_id: str, job_id: int, handler_id: int,
                 array_spec: str, level: int, memory: str, time_level: int = 0,
                 time: str | None = None, partition: str | None = None,
                 output_pattern: str | None = None, error_pattern: str | None = None) -> int:
    """Add a new round to the database. Returns round_id."""
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get next round number
    cursor.execute('SELECT COALESCE(MAX(round_num), 0) + 1 FROM rounds WHERE chain_id = ?',
                   (chain_id,))
    round_num = cursor.fetchone()[0]

    now = datetime.now().isoformat()
    cursor.execute('''
        INSERT INTO rounds
        (chain_id, round_num, job_id, handler_id, array_spec, level, memory,
         time_level, time, partition, status, submitted_at, output_pattern, error_pattern)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        chain_id, round_num, job_id, handler_id, array_spec, level, memory,
        time_level, time, partition, 'RUNNING', now, output_pattern, error_pattern
    ))

    round_id = cursor.lastrowid
    assert round_id is not None  # Just inserted a row

    # Update chain
    cursor.execute('''
        UPDATE chains SET updated_at = ?, current_level = ?, current_memory = ?,
                          current_time_level = ?, current_time = ?, status = ?
        WHERE chain_id = ?
    ''', (now, level, memory, time_level, time, 'RUNNING', chain_id))

    conn.commit()
    conn.close()
    return round_id


def db_update_round_status(db_path: str, chain_id: str, job_id: int, status: str,
                           oom_count: int = 0, timeout_count: int = 0,
                           oom_indices: str | None = None, timeout_indices: str | None = None) -> None:
    """Update round status after job completion."""
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    now = datetime.now().isoformat()
    cursor.execute('''
        UPDATE rounds SET status = ?, completed_at = ?, oom_count = ?, timeout_count = ?,
                          oom_indices = ?, timeout_indices = ?
        WHERE chain_id = ? AND job_id = ?
    ''', (status, now, oom_count, timeout_count, oom_indices, timeout_indices,
          chain_id, job_id))

    # Update chain status
    chain_status = 'ESCALATING' if status in ('OOM', 'TIMEOUT') else status
    cursor.execute('''
        UPDATE chains SET updated_at = ?, status = ?,
                          last_escalation_reason = CASE WHEN ? IN ('OOM', 'TIMEOUT') THEN ? ELSE last_escalation_reason END,
                          pending_indices = ?
        WHERE chain_id = ?
    ''', (now, chain_status, status, status, oom_indices or timeout_indices, chain_id))

    conn.commit()
    conn.close()


def db_update_chain_completed(db_path: str, chain_id: str, completed_count: int) -> None:
    """Mark chain as completed."""
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    now = datetime.now().isoformat()
    cursor.execute('''
        UPDATE chains SET updated_at = ?, status = ?, completed_count = ?, pending_indices = NULL
        WHERE chain_id = ?
    ''', (now, 'COMPLETED', completed_count, chain_id))

    conn.commit()
    conn.close()


def db_save_task_metrics(db_path: str, chain_id: str, round_id: int, job_id: int,
                         tasks_data: list) -> None:
    """Save per-task metrics from sacct.

    tasks_data is a list of dicts with keys:
        task_id, status, exit_code, signal, max_rss, elapsed, timelimit,
        node, submit_time, start_time, end_time, output_path, error_path
    """
    if not tasks_data:
        return

    init_db(db_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for task in tasks_data:
        cursor.execute('''
            INSERT OR REPLACE INTO tasks
            (chain_id, round_id, job_id, task_id, status, exit_code, signal,
             max_rss, elapsed, timelimit, node, submit_time, start_time, end_time,
             output_path, error_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            chain_id, round_id, job_id,
            task.get('task_id'),
            task.get('status'),
            task.get('exit_code'),
            task.get('signal'),
            task.get('max_rss'),
            task.get('elapsed'),
            task.get('timelimit'),
            task.get('node'),
            task.get('submit_time'),
            task.get('start_time'),
            task.get('end_time'),
            task.get('output_path'),
            task.get('error_path')
        ))

    conn.commit()
    conn.close()


def db_save_tasks(db_path: str, chain_id: str, job_id: int) -> None:
    """Query sacct and save task metrics with output paths to database.

    Gets detailed task info from sacct and the output pattern from the rounds table,
    then constructs actual file paths and saves everything to the tasks table.
    """
    init_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Get round info for this job
    cursor.execute('''
        SELECT id, output_pattern, error_pattern
        FROM rounds
        WHERE chain_id = ? AND job_id = ?
    ''', (chain_id, job_id))
    round_row = cursor.fetchone()
    if not round_row:
        conn.close()
        return

    round_id = round_row['id']
    output_pattern = round_row['output_pattern'] or ''
    error_pattern = round_row['error_pattern'] or ''

    # Query sacct for detailed task info
    # Fields: JobID, State, ExitCode, MaxRSS, Elapsed, Timelimit, NodeList, Submit, Start, End
    try:
        cmd = [
            'sacct', '-n', '-X', '-j', str(job_id),
            '-o', 'JobID,State,ExitCode,MaxRSS,Elapsed,Timelimit,NodeList,Submit,Start,End',
            '--parsable2'
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError:
        conn.close()
        return

    for line in result.stdout.strip().split('\n'):
        if not line:
            continue

        parts = line.split('|')
        if len(parts) < 10:
            continue

        job_spec = parts[0]
        state = parts[1].split()[0] if parts[1] else ''
        exit_code_str = parts[2]
        max_rss = parts[3]
        elapsed = parts[4]
        timelimit = parts[5]
        node = parts[6]
        submit_time = parts[7]
        start_time = parts[8]
        end_time = parts[9]

        # Extract task ID (array: 100_1 -> 1, single: 100 -> 0)
        task_id = 0
        if '_' in job_spec:
            try:
                task_id = int(job_spec.split('_')[1])
            except ValueError:
                pass

        # Parse exit code (format: ReturnCode:Signal)
        exit_code = 0
        signal = 0
        if ':' in exit_code_str:
            try:
                parts_ec = exit_code_str.split(':')
                exit_code = int(parts_ec[0])
                signal = int(parts_ec[1])
            except ValueError:
                pass

        # Construct actual output paths by replacing %A (job_id) and %a (task_id)
        output_path = output_pattern.replace('%A', str(job_id)).replace('%a', str(task_id))
        error_path = error_pattern.replace('%A', str(job_id)).replace('%a', str(task_id))

        # Insert or replace task record
        cursor.execute('''
            INSERT OR REPLACE INTO tasks
            (chain_id, round_id, job_id, task_id, status, exit_code, signal,
             max_rss, elapsed, timelimit, node, submit_time, start_time, end_time,
             output_path, error_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            chain_id, round_id, job_id, task_id, state, exit_code, signal,
            max_rss, elapsed, timelimit, node, submit_time, start_time, end_time,
            output_path, error_path
        ))

    conn.commit()
    conn.close()

def log_to_db(db_path: str, chain_id: str, action_type: str, job_id: str | None = None,
              memory_level: int | None = None, time_level: int | None = None,
              indices: str | None = None, details: str | None = None) -> None:
    """Log an action to the SQLite database."""
    try:
        # Create parent directories if needed
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS actions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                chain_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                job_id TEXT,
                memory_level INTEGER,
                time_level INTEGER,
                indices TEXT,
                details TEXT
            )
        ''')

        # Insert action
        cursor.execute('''
            INSERT INTO actions (timestamp, chain_id, action_type, job_id,
                                 memory_level, time_level, indices, details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            chain_id,
            action_type,
            job_id,
            memory_level,
            time_level,
            indices,
            details
        ))

        conn.commit()
        conn.close()
        print(f"Logged: {action_type} for chain {chain_id}")
    except Exception as e:
        print(f"Warning: Could not log to database: {e}")

def compress_indices(indices_str: str) -> None:
    """Compress comma-separated indices into Slurm array ranges (e.g., 0-10:2).

    Handles:
    - Consecutive ranges: 0,1,2,3,4 -> 0-4
    - Strided ranges: 8,18,28,38 -> 8-38:10
    - Interleaved patterns: 5,6,15,16,25,26 -> 5-25:10,6-26:10
    """
    if not indices_str:
        print("")
        return

    try:
        # Parse indices
        indices = sorted(set(int(x) for x in indices_str.split(',') if x.strip()))
        if not indices:
            print("")
            return
    except ValueError:
        print(indices_str)  # Fallback
        return

    count = len(indices)

    # Handle trivial cases
    if count == 1:
        print(str(indices[0]))
        return
    elif count == 2:
        if indices[1] == indices[0] + 1:
            print(f"{indices[0]}-{indices[1]}")
        else:
            print(f"{indices[0]},{indices[1]}")
        return

    # Compute gaps between consecutive elements
    gaps = [indices[i] - indices[i-1] for i in range(1, count)]

    # Check if all gaps are the same (simple stride case)
    all_same = all(g == gaps[0] for g in gaps)

    # Try to detect interleaved stride patterns (only if gaps vary)
    detected_period = 0
    if not all_same:
        # Look for repeating gap patterns with period 2, 3, 4, or 5
        # Need at least 3 occurrences of the first gap (to form a valid range)
        # For period P, we need gaps at indices 0, P, 2P - so len(gaps) >= 2*P + 1
        for period in range(2, 6):
            if len(gaps) >= period * 2 + 1:
                is_periodic = True
                for i in range(period, len(gaps)):
                    if gaps[i] != gaps[i % period]:
                        is_periodic = False
                        break
                if is_periodic:
                    detected_period = period
                    break

    if detected_period > 1:
        # Interleaved pattern detected - extract each stride separately
        # Calculate total stride (sum of one period of gaps)
        total_stride = sum(gaps[:detected_period])

        # Extract each interleaved sequence
        result_parts = []
        for offset in range(detected_period):
            # Collect elements at this offset
            seq_indices = [indices[j] for j in range(offset, count, detected_period)]
            seq_start = seq_indices[0]
            seq_end = seq_indices[-1]
            seq_count = len(seq_indices)

            if seq_count >= 3:
                if total_stride == 1:
                    result_parts.append(f"{seq_start}-{seq_end}")
                else:
                    result_parts.append(f"{seq_start}-{seq_end}:{total_stride}")
            elif seq_count == 2:
                if seq_end == seq_start + 1:
                    result_parts.append(f"{seq_start}-{seq_end}")
                else:
                    result_parts.append(f"{seq_start},{seq_end}")
            else:
                result_parts.append(str(seq_start))

        print(','.join(result_parts))
        return

    # Try stride-grouped compression for interleaved patterns with gaps
    # e.g., 5,6,7,8,9,15,16,18,19,25,... -> group by mod-10, compress each group
    def compress_group_greedy(group: list[int], stride: int) -> list[str]:
        """Compress a single stride group using greedy strided ranges."""
        result = []
        gi = 0
        while gi < len(group):
            g_start = group[gi]
            g_end = g_start
            g_count = 1
            gj = gi + 1
            while gj < len(group):
                if group[gj] == g_end + stride:
                    g_end = group[gj]
                    g_count += 1
                    gj += 1
                else:
                    break
            if g_count >= 2:
                result.append(f"{g_start}-{g_end}:{stride}")
                gi = gj
            else:
                result.append(str(g_start))
                gi += 1
        return result

    for stride in [10, 5, 2]:
        if count < stride * 3:
            continue

        # Group by mod value
        groups: dict[int, list[int]] = {}
        for idx in indices:
            mod = idx % stride
            if mod not in groups:
                groups[mod] = []
            groups[mod].append(idx)

        # Need multiple groups with enough indices to benefit
        useful_groups = sum(1 for g in groups.values() if len(g) >= 3)
        if useful_groups < 2:
            continue

        # Compress each group
        all_parts = []
        for mod in sorted(groups.keys()):
            group = groups[mod]
            if len(group) == 1:
                all_parts.append(str(group[0]))
            elif len(group) == 2:
                if group[1] - group[0] == stride:
                    all_parts.append(f"{group[0]}-{group[1]}:{stride}")
                else:
                    all_parts.append(f"{group[0]},{group[1]}")
            else:
                all_parts.extend(compress_group_greedy(group, stride))

        grouped_result = ','.join(all_parts)

        # Compare with what greedy would produce (estimate)
        # If grouped is significantly shorter, use it
        if len(grouped_result) < count * 2:  # Less than 2 chars per index on average
            print(grouped_result)
            return

    # Fall back to simple stride detection (greedy algorithm)
    ranges = []
    i = 0

    while i < count:
        start = indices[i]

        if i + 1 < count:
            stride = indices[i + 1] - indices[i]
            range_end = start
            range_count = 1

            # Count how many elements follow this stride
            j = i + 1
            while j < count:
                expected = range_end + stride
                if indices[j] == expected:
                    range_end = indices[j]
                    range_count += 1
                    j += 1
                else:
                    break

            # Decide how to output this range
            if range_count >= 3:
                if stride == 1:
                    ranges.append(f"{start}-{range_end}")
                else:
                    ranges.append(f"{start}-{range_end}:{stride}")
                i = j
            elif range_count == 2 and stride == 1:
                ranges.append(f"{start}-{range_end}")
                i = j
            else:
                ranges.append(str(start))
                i += 1
        else:
            ranges.append(str(start))
            i += 1

    print(','.join(ranges))

def analyze_job(job_id: str, config_file: str | None = None) -> None:
    """Analyze a job's status via sacct and output failure indices.

    Uses state_handling config to determine which states should escalate vs no_retry.
    """
    # Load state handling config if provided
    state_handling: dict = {}
    exit_code_handling: dict = {}
    if config_file and Path(config_file).exists():
        try:
            with open(config_file) as f:
                config = yaml.safe_load(f)
                state_handling = config.get('state_handling', {})
                exit_code_handling = state_handling.pop('exit_codes', {})
        except Exception:
            pass  # Use defaults if config fails

    # Default state handling if not configured
    if not state_handling:
        state_handling = {
            'OUT_OF_MEMORY': 'escalate',
            'TIMEOUT': 'escalate',
            'DEADLINE': 'escalate',
            'PREEMPTED': 'escalate',
            'BOOT_FAIL': 'escalate',
            'NODE_FAIL': 'escalate',
            'FAILED': 'no_retry',
            'CANCELLED': 'no_retry',
        }
        exit_code_handling = {137: 'escalate'}

    try:
        # Query sacct
        cmd = ['sacct', '-n', '-X', '-j', job_id, '-o', 'JobID,State,ExitCode', '--parsable2']
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"echo 'ERROR: sacct failed: {e}' >&2")
        return

    # Track by original state for breakdown display
    oom_indices = []
    timeout_indices = []
    # Track by action (escalate vs no_retry)
    escalate_indices = []
    no_retry_indices = []
    completed_count = 0
    total_count = 0

    for line in result.stdout.strip().split('\n'):
        if not line:
            continue

        parts = line.split('|')
        if len(parts) < 3:
            continue

        job_spec = parts[0]
        state = parts[1].split()[0]  # Take first word (e.g. CANCELLED by...)
        exit_code = parts[2]

        total_count += 1

        # Extract task ID
        task_id = 0
        if '_' in job_spec:
            try:
                task_id = int(job_spec.split('_')[1])
            except ValueError:
                pass

        if 'COMPLETED' in state:
            completed_count += 1
            continue

        # Parse exit code
        main_exit = 0
        if ':' in exit_code:
            try:
                main_exit = int(exit_code.split(':')[0])
            except ValueError:
                pass

        # Track by original state for breakdown
        if 'OUT_OF_MEMORY' in state:
            oom_indices.append(task_id)
        elif 'TIMEOUT' in state:
            timeout_indices.append(task_id)

        # Determine action based on config
        action = 'no_retry'  # default

        # Check exit code overrides first (for FAILED state)
        if main_exit in exit_code_handling:
            action = exit_code_handling[main_exit]
        else:
            # Check state handling
            for cfg_state, cfg_action in state_handling.items():
                if cfg_state in state:
                    action = cfg_action
                    break

        if action == 'escalate':
            escalate_indices.append(task_id)
        else:
            no_retry_indices.append(task_id)

    # Sort indices
    oom_indices.sort()
    timeout_indices.sort()
    escalate_indices.sort()
    no_retry_indices.sort()

    # Output variables (keeping backwards compatibility + new fields)
    print(f"TOTAL_COUNT={total_count}")
    print(f"COMPLETED_COUNT={completed_count}")
    print(f"OOM_COUNT={len(oom_indices)}")
    print(f"TIMEOUT_COUNT={len(timeout_indices)}")
    print(f"OTHER_FAILED_COUNT={len(no_retry_indices)}")
    print(f"ESCALATE_COUNT={len(escalate_indices)}")
    print(f"NO_RETRY_COUNT={len(no_retry_indices)}")

    # Join with commas
    print(f"OOM_INDICES={','.join(map(str, oom_indices))}")
    print(f"TIMEOUT_INDICES={','.join(map(str, timeout_indices))}")
    print(f"OTHER_FAILED_INDICES={','.join(map(str, no_retry_indices))}")
    print(f"ESCALATE_INDICES={','.join(map(str, escalate_indices))}")
    print(f"NO_RETRY_INDICES={','.join(map(str, no_retry_indices))}")

def main():
    parser = argparse.ArgumentParser(
        description='Resource Escalation Library - Python utilities for mem-escalate.sh'
    )
    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # compress-indices
    p = subparsers.add_parser('compress-indices', help='Compress indices to ranges')
    p.add_argument('indices', help='Comma-separated indices')

    # analyze-job
    p = subparsers.add_parser('analyze-job', help='Analyze job failure status')
    p.add_argument('job_id', help='Job ID to analyze')
    p.add_argument('--config', '-c', help='Config file with state_handling settings')

    # load-config
    p = subparsers.add_parser('load-config', help='Parse YAML config file')
    p.add_argument('config_file', help='Path to config file')

    # create-checkpoint
    p = subparsers.add_parser('create-checkpoint', help='Create a new checkpoint (levels-based)')
    p.add_argument('checkpoint_dir', help='Checkpoint directory')
    p.add_argument('chain_id', help='Chain ID')
    p.add_argument('partition', help='Partition name')
    p.add_argument('script', help='Script path')
    p.add_argument('max_level', type=int, help='Maximum escalation level')
    p.add_argument('initial_mem', help='Initial memory allocation')
    p.add_argument('initial_time', help='Initial time allocation')
    p.add_argument('script_args', nargs='*', help='Script arguments')

    # create-array-checkpoint
    p = subparsers.add_parser('create-array-checkpoint', help='Create array job checkpoint (levels-based)')
    p.add_argument('checkpoint_dir', help='Checkpoint directory')
    p.add_argument('chain_id', help='Chain ID')
    p.add_argument('partition', help='Partition name')
    p.add_argument('array_spec', help='Array specification')
    p.add_argument('total_tasks', type=int, help='Total number of tasks')
    p.add_argument('script', help='Script path')
    p.add_argument('max_level', type=int, help='Maximum escalation level')
    p.add_argument('initial_mem', help='Initial memory allocation')
    p.add_argument('initial_time', help='Initial time allocation')
    p.add_argument('script_args', nargs='*', help='Script arguments')

    # load-checkpoint
    p = subparsers.add_parser('load-checkpoint', help='Load checkpoint and output shell vars')
    p.add_argument('checkpoint_file', help='Checkpoint file path')

    # list-checkpoints
    p = subparsers.add_parser('list-checkpoints', help='List all checkpoints')
    p.add_argument('checkpoint_dir', help='Checkpoint directory')

    # show-status
    p = subparsers.add_parser('show-status', help='Show chain status')
    p.add_argument('checkpoint_file', help='Checkpoint file path')

    # generate-report
    p = subparsers.add_parser('generate-report', help='Generate markdown test report')
    p.add_argument('checkpoint_path', help='Checkpoint file or directory (with --all)')
    p.add_argument('--all', action='store_true', dest='all_checkpoints',
                   help='Generate report for all checkpoints in directory')
    p.add_argument('--db', dest='db_path', help='Database path for task-level details')
    p.add_argument('--detailed', action='store_true',
                   help='Include per-round task breakdown from database')

    # get-chain-state
    p = subparsers.add_parser('get-chain-state', help='Get chain state (RUNNING/COMPLETED/FAILED)')
    p.add_argument('checkpoint_file', help='Checkpoint file path')

    # update-array-round
    p = subparsers.add_parser('update-array-round', help='Update checkpoint with array round')
    p.add_argument('checkpoint_file', help='Checkpoint file path')
    p.add_argument('job_ids', help='Job ID(s) - comma-separated if batched')
    p.add_argument('handler_id', type=int, help='Handler job ID')
    p.add_argument('array_spec', help='Array specification')
    p.add_argument('level', type=int, help='Escalation level')
    p.add_argument('memory', help='Memory allocation')

    # mark-completed
    p = subparsers.add_parser('mark-completed', help='Mark chain as completed')
    p.add_argument('checkpoint_file', help='Checkpoint file path')
    p.add_argument('job_id', type=int, help='Job ID that completed')
    p.add_argument('completed_count', type=int, help='Number of completed tasks')

    # mark-failed
    p = subparsers.add_parser('mark-failed', help='Mark chain as failed at max level')
    p.add_argument('checkpoint_file', help='Checkpoint file path')
    p.add_argument('failed_indices', help='Comma-separated failed indices')
    p.add_argument('reason', nargs='?', default='LEVEL', help='Failure reason (default: LEVEL)')

    # update-escalation
    p = subparsers.add_parser('update-escalation', help='Update checkpoint during escalation')
    p.add_argument('checkpoint_file', help='Checkpoint file path')
    p.add_argument('next_level', type=int, help='Next escalation level')
    p.add_argument('next_mem', help='Next memory allocation')
    p.add_argument('escalate_indices', help='Comma-separated indices to escalate')
    p.add_argument('retry_job', help='Retry job ID(s) - comma-separated if batched')
    p.add_argument('handler_id', type=int, help='Handler job ID')
    p.add_argument('completed_count', type=int, help='Number of completed tasks')
    p.add_argument('escalate_count', type=int, help='Number of tasks being escalated')
    p.add_argument('--oom-count', type=int, default=0, help='Number of OOM tasks')
    p.add_argument('--timeout-count', type=int, default=0, help='Number of TIMEOUT tasks')
    p.add_argument('--failed-count', type=int, default=0, help='Number of failed tasks (not retried)')

    # log-action
    p = subparsers.add_parser('log-action', help='Log an action to the SQLite database')
    p.add_argument('db_path', help='Path to SQLite database')
    p.add_argument('chain_id', help='Chain ID')
    p.add_argument('action_type', help='Action type (SUBMIT, ESCALATE_MEM, ESCALATE_TIME, COMPLETE, FAIL)')
    p.add_argument('--job-id', help='Job ID')
    p.add_argument('--memory-level', type=int, help='Memory level')
    p.add_argument('--time-level', type=int, help='Time level')
    p.add_argument('--indices', help='Affected indices')
    p.add_argument('--details', help='Additional details (JSON)')

    # Database commands
    p = subparsers.add_parser('db-create-chain', help='Create chain in database')
    p.add_argument('db_path', help='Path to SQLite database')
    p.add_argument('chain_id', help='Chain ID')
    p.add_argument('mode', help='Mode (single or handler_chain)')
    p.add_argument('partition', help='Partition name')
    p.add_argument('script', help='Script path')
    p.add_argument('--script-args', nargs='*', default=[], help='Script arguments')
    p.add_argument('--array-spec', help='Array specification')
    p.add_argument('--total-tasks', type=int, help='Total number of tasks')
    p.add_argument('--memory', help='Initial memory')
    p.add_argument('--time', help='Initial time')

    p = subparsers.add_parser('db-add-round', help='Add round to database')
    p.add_argument('db_path', help='Path to SQLite database')
    p.add_argument('chain_id', help='Chain ID')
    p.add_argument('job_id', type=int, help='Job ID')
    p.add_argument('handler_id', type=int, help='Handler job ID')
    p.add_argument('array_spec', help='Array specification')
    p.add_argument('level', type=int, help='Escalation level')
    p.add_argument('memory', help='Memory allocation')
    p.add_argument('--time-level', type=int, default=0, help='Time level')
    p.add_argument('--time', help='Time allocation')
    p.add_argument('--partition', help='Partition name')
    p.add_argument('--output-pattern', help='Output file pattern')
    p.add_argument('--error-pattern', help='Error file pattern')

    p = subparsers.add_parser('db-update-round', help='Update round status in database')
    p.add_argument('db_path', help='Path to SQLite database')
    p.add_argument('chain_id', help='Chain ID')
    p.add_argument('job_id', type=int, help='Job ID')
    p.add_argument('status', help='Status')
    p.add_argument('--oom-count', type=int, default=0, help='OOM count')
    p.add_argument('--timeout-count', type=int, default=0, help='TIMEOUT count')
    p.add_argument('--oom-indices', help='OOM task indices')
    p.add_argument('--timeout-indices', help='TIMEOUT task indices')

    p = subparsers.add_parser('db-complete-chain', help='Mark chain as completed')
    p.add_argument('db_path', help='Path to SQLite database')
    p.add_argument('chain_id', help='Chain ID')
    p.add_argument('completed_count', type=int, help='Number of completed tasks')

    p = subparsers.add_parser('db-save-tasks', help='Save task metrics from sacct to database')
    p.add_argument('db_path', help='Path to SQLite database')
    p.add_argument('chain_id', help='Chain ID')
    p.add_argument('job_id', type=int, help='Job ID to save tasks for')

    args = parser.parse_args()

    if args.command == 'compress-indices':
        compress_indices(args.indices)
    elif args.command == 'analyze-job':
        analyze_job(args.job_id, getattr(args, 'config', None))
    elif args.command == 'load-config':
        load_config(args.config_file)
    elif args.command == 'create-checkpoint':
        create_checkpoint(args.checkpoint_dir, args.chain_id, args.partition, args.script,
                          args.max_level, args.initial_mem, args.initial_time, args.script_args)
    elif args.command == 'create-array-checkpoint':
        create_array_checkpoint(args.checkpoint_dir, args.chain_id, args.partition, args.array_spec,
                                args.total_tasks, args.script, args.max_level,
                                args.initial_mem, args.initial_time, args.script_args)
    elif args.command == 'load-checkpoint':
        load_checkpoint(args.checkpoint_file)
    elif args.command == 'list-checkpoints':
        list_checkpoints(args.checkpoint_dir)
    elif args.command == 'show-status':
        show_status(args.checkpoint_file)
    elif args.command == 'generate-report':
        generate_report(args.checkpoint_path, args.all_checkpoints,
                        args.db_path, args.detailed)
    elif args.command == 'get-chain-state':
        get_chain_state(args.checkpoint_file)
    elif args.command == 'update-array-round':
        update_array_round(args.checkpoint_file, args.job_ids, args.handler_id,
                           args.array_spec, args.level, args.memory)
    elif args.command == 'mark-completed':
        mark_completed(args.checkpoint_file, args.job_id, args.completed_count)
    elif args.command == 'mark-failed':
        mark_failed(args.checkpoint_file, args.failed_indices, args.reason)
    elif args.command == 'update-escalation':
        update_escalation(args.checkpoint_file, args.next_level, args.next_mem,
                          args.escalate_indices, args.retry_job, args.handler_id,
                          args.completed_count, args.escalate_count,
                          oom_count=args.oom_count, timeout_count=args.timeout_count,
                          failed_count=args.failed_count)
    elif args.command == 'log-action':
        log_to_db(args.db_path, args.chain_id, args.action_type,
                  job_id=args.job_id, memory_level=args.memory_level,
                  time_level=args.time_level, indices=args.indices,
                  details=args.details)
    elif args.command == 'db-create-chain':
        db_create_chain(args.db_path, args.chain_id, args.mode, args.partition,
                        args.script, args.script_args, args.array_spec,
                        args.total_tasks, args.memory, args.time)
    elif args.command == 'db-add-round':
        round_id = db_add_round(args.db_path, args.chain_id, args.job_id, args.handler_id,
                                args.array_spec, args.level, args.memory, args.time_level,
                                args.time, args.partition, args.output_pattern, args.error_pattern)
        print(f"ROUND_ID={round_id}")
    elif args.command == 'db-update-round':
        db_update_round_status(args.db_path, args.chain_id, args.job_id, args.status,
                               args.oom_count, args.timeout_count,
                               args.oom_indices, args.timeout_indices)
    elif args.command == 'db-complete-chain':
        db_update_chain_completed(args.db_path, args.chain_id, args.completed_count)
    elif args.command == 'db-save-tasks':
        db_save_tasks(args.db_path, args.chain_id, args.job_id)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == '__main__':
    main()
