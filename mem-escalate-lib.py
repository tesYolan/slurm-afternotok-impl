#!/usr/bin/env python3
"""
Resource Escalation Library - Python utilities for mem-escalate.sh

This module provides checkpoint management, config parsing, and status
display for the Slurm memory and time escalation system.

Usage from bash:
    python3 mem-escalate-lib.py <command> [args...]

Commands:
    load-config <config_file>
    create-checkpoint <checkpoint_dir> <chain_id> <script> <memory_ladder> <time_ladder> [args...]
    create-array-checkpoint <checkpoint_dir> <chain_id> <array_spec> <total_tasks> <script> <memory_ladder> <time_ladder> [args...]
    update-checkpoint <file> <job_id> <level> <memory> <status>
    update-job-status <file> <job_id> <status>
    load-checkpoint <checkpoint_file>
    list-checkpoints <checkpoint_dir>
    show-status <checkpoint_file>
    update-array-round <file> <job_id> <handler_id> <array_spec> <level> <memory>
    mark-completed <file> <job_id> <completed_count>
    mark-failed <file> <failed_indices> [MEMORY|TIME]
    update-escalation <file> <next_level> <next_mem> <oom_indices> <retry_job> <handler_id> <completed_count> <oom_count>
    update-time-escalation <file> <next_time_level> <next_time> <timeout_indices> <retry_job> <handler_id> <completed_count> <timeout_count>
    log-action <db_path> <chain_id> <action_type> [job_id] [memory_level] [time_level] [indices] [details]
"""

import argparse
import sys
import json
import sqlite3
import yaml
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


def load_config(config_path: str) -> None:
    """Parse YAML config and output shell variable assignments."""
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"echo 'ERROR: Failed to parse config: {e}' >&2", file=sys.stdout)
        sys.exit(1)

    # Memory settings
    if 'memory' in config:
        mem = config['memory']
        if 'ladder' in mem:
            ladder = ' '.join(mem['ladder'])
            print(f"MEMORY_LADDER=({ladder})")
        if 'max_level' in mem:
            print(f"MAX_LEVEL={mem['max_level']}")

    # Time ladder settings
    if 'time' in config:
        time_cfg = config['time']
        if 'ladder' in time_cfg:
            ladder = ' '.join(time_cfg['ladder'])
            print(f"TIME_LADDER=({ladder})")
        if 'max_level' in time_cfg:
            print(f"TIME_MAX_LEVEL={time_cfg['max_level']}")

    # Timeout settings (for polling)
    if 'timeout' in config:
        timeout = config['timeout']
        if 'job_timeout' in timeout:
            print(f"JOB_TIMEOUT='{timeout['job_timeout']}'")
        if 'poll_interval' in timeout:
            print(f"POLL_INTERVAL={timeout['poll_interval']}")
        if 'sacct_delay' in timeout:
            print(f"SACCT_DELAY={timeout['sacct_delay']}")

    # Retry settings
    if 'retry' in config:
        retry = config['retry']
        if 'max_retries' in retry:
            print(f"MAX_RETRIES={retry['max_retries']}")
        if 'retry_delay' in retry:
            print(f"RETRY_DELAY={retry['retry_delay']}")

    # Tracker paths
    if 'tracker' in config:
        tracker = config['tracker']
        if 'base_dir' in tracker:
            print(f"TRACKER_DIR='{tracker['base_dir']}'")
        if 'jobs_dir' in tracker:
            print(f"JOBS_DIR='{tracker['jobs_dir']}'")
        if 'history_log' in tracker:
            print(f"HISTORY_LOG='{tracker['history_log']}'")
        if 'checkpoint_dir' in tracker:
            print(f"CHECKPOINT_DIR='{tracker['checkpoint_dir']}'")

    # Job defaults
    if 'defaults' in config:
        defaults = config['defaults']
        if 'partition' in defaults:
            print(f"DEFAULT_PARTITION='{defaults['partition']}'")
        if 'output_pattern' in defaults:
            print(f"OUTPUT_PATTERN='{defaults['output_pattern']}'")

    # Logging settings
    if 'logging' in config:
        logging_cfg = config['logging']
        if 'enabled' in logging_cfg:
            print(f"LOGGING_ENABLED={'true' if logging_cfg['enabled'] else 'false'}")
        if 'db_path' in logging_cfg:
            print(f"LOGGING_DB_PATH='{logging_cfg['db_path']}'")

    # Cluster settings
    if 'cluster' in config:
        cluster = config['cluster']
        if 'name' in cluster:
            print(f"CLUSTER_NAME='{cluster['name']}'")
        if 'partition' in cluster:
            print(f"CLUSTER_PARTITION='{cluster['partition']}'")
        if 'nodes' in cluster:
            print(f"CLUSTER_NODES='{cluster['nodes']}'")


def create_checkpoint(checkpoint_dir: str, chain_id: str, script: str,
                      memory_ladder: str, time_ladder: str, script_args: list) -> None:
    """Create a new checkpoint file for a job chain."""
    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
    checkpoint_file = Path(checkpoint_dir) / f"{chain_id}.checkpoint"

    # Parse memory ladder
    mem_ladder = memory_ladder.split()
    first_mem = mem_ladder[0] if mem_ladder else "1G"

    # Parse time ladder
    t_ladder = time_ladder.split()
    first_time = t_ladder[0] if t_ladder else "00:05:00"

    checkpoint = {
        'chain_id': chain_id,
        'original_script': script,
        'script_args': script_args,
        'created': datetime.now().isoformat(),
        'updated': datetime.now().isoformat(),
        'state': {
            'current_level': 0,
            'current_memory': first_mem,
            'current_time_level': 0,
            'current_time': first_time,
            'attempts': 0,
            'status': 'STARTING',
            'last_escalation_reason': None
        },
        'jobs': []
    }

    with open(checkpoint_file, 'w') as f:
        yaml.dump(checkpoint, f, default_flow_style=False, sort_keys=False)

    print(str(checkpoint_file))


def create_array_checkpoint(checkpoint_dir: str, chain_id: str, array_spec: str,
                            total_tasks: int, script: str, memory_ladder: str,
                            time_ladder: str, script_args: list) -> None:
    """Create a checkpoint file for an array job chain."""
    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
    checkpoint_file = Path(checkpoint_dir) / f"{chain_id}.checkpoint"

    # Parse memory ladder
    mem_ladder = memory_ladder.split()
    first_mem = mem_ladder[0] if mem_ladder else "1G"

    # Parse time ladder
    t_ladder = time_ladder.split()
    first_time = t_ladder[0] if t_ladder else "00:05:00"

    checkpoint = {
        'chain_id': chain_id,
        'mode': 'handler_chain',
        'original_script': script,
        'script_args': script_args,
        'original_array_spec': array_spec,
        'total_tasks': total_tasks,
        'created': datetime.now().isoformat(),
        'updated': datetime.now().isoformat(),
        'state': {
            'current_level': 0,
            'current_memory': first_mem,
            'current_time_level': 0,
            'current_time': first_time,
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


def update_checkpoint(checkpoint_file: str, job_id: int, level: int,
                      memory: str, status: str) -> None:
    """Update checkpoint with a new job submission."""
    with modify_checkpoint(checkpoint_file) as cp:
        cp['state']['current_level'] = level
        cp['state']['current_memory'] = memory
        cp['state']['attempts'] = len(cp.get('jobs', [])) + 1
        cp['state']['status'] = status
        if 'jobs' not in cp:
            cp['jobs'] = []
        cp['jobs'].append({
            'job_id': job_id, 'level': level, 'memory': memory,
            'status': status, 'submitted': datetime.now().isoformat()
        })


def update_job_status(checkpoint_file: str, job_id: int, status: str) -> None:
    """Update the status of a job in the checkpoint."""
    with modify_checkpoint(checkpoint_file) as cp:
        for job in cp.get('jobs', []):
            if job['job_id'] == job_id:
                job['status'] = status
                job['completed'] = datetime.now().isoformat()
                break
        cp['state']['status'] = status


def load_checkpoint(checkpoint_file: str) -> None:
    """Load checkpoint and output shell variable assignments."""
    try:
        with open(checkpoint_file) as f:
            checkpoint = yaml.safe_load(f)
    except Exception as e:
        print(f"echo 'ERROR: Failed to parse checkpoint: {e}' >&2")
        sys.exit(1)

    # Output shell variables
    print(f"CHECKPOINT_FILE='{checkpoint_file}'")
    print(f"SCRIPT='{checkpoint['original_script']}'")

    # Script args
    args = checkpoint.get('script_args', [])
    if args:
        args_str = ' '.join(f'"{a}"' for a in args)
        print(f"SCRIPT_ARGS=({args_str})")
    else:
        print("SCRIPT_ARGS=()")

    # State
    state = checkpoint.get('state', {})
    print(f"RESUME_LEVEL={state.get('current_level', 0)}")
    print(f"RESUME_MEMORY='{state.get('current_memory', '1G')}'")
    print(f"RESUME_TIME_LEVEL={state.get('current_time_level', 0)}")
    print(f"RESUME_TIME='{state.get('current_time', '00:05:00')}'")
    print(f"RESUME_STATUS='{state.get('status', 'UNKNOWN')}'")

    # Build job chain from history
    jobs = checkpoint.get('jobs', [])
    job_ids = ','.join(str(j['job_id']) for j in jobs)
    print(f"RESUME_CHAIN='{job_ids}'")


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
    print(f"Status:   {state.get('status', 'UNKNOWN')}")
    print(f"Memory:   Level {state.get('current_level', 0)} ({state.get('current_memory', '?')})")
    print(f"Time:     Level {state.get('current_time_level', 0)} ({state.get('current_time', '?')})")
    if state.get('last_escalation_reason'):
        print(f"Last:     {state['last_escalation_reason']}")
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

            print(f"  Round {i}: Job {job_id} ({level_str})")
            if task_count > 0:
                print(f"           Tasks: {task_count}")
            print(f"           Status: {status}")

            # Try to get live status from sacct
            try:
                result = subprocess.run(
                    ['sacct', '-nX', '-j', str(job_id), '-o', 'State%20'],
                    capture_output=True, text=True, timeout=5
                )
                if result.returncode == 0 and result.stdout.strip():
                    states = result.stdout.strip().split('\n')
                    completed = sum(1 for s in states if 'COMPLETED' in s)
                    oom = sum(1 for s in states if 'OUT_OF_MEMORY' in s)
                    timeout = sum(1 for s in states if 'TIMEOUT' in s)
                    running = sum(1 for s in states if 'RUNNING' in s or 'PENDING' in s)
                    print(f"           Live: {completed} completed, {oom} OOM, {timeout} TIMEOUT, {running} running/pending")
            except Exception:
                pass

            if r.get('oom_count'):
                print(f"           OOM count: {r['oom_count']}")
            if r.get('timeout_count'):
                print(f"           TIMEOUT count: {r['timeout_count']}")

            print()
    else:
        print("No rounds recorded yet.")

    print("=" * 50)


def update_array_round(checkpoint_file: str, job_id: int, handler_id: int,
                       array_spec: str, level: int, memory: str) -> None:
    """Update checkpoint with a new array round."""
    with modify_checkpoint(checkpoint_file) as cp:
        cp['state']['status'] = 'RUNNING'
        if 'rounds' not in cp:
            cp['rounds'] = []
        cp['rounds'].append({
            'job_id': job_id, 'handler_id': handler_id, 'array_spec': array_spec,
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
                      oom_indices: str, retry_job: int, handler_id: int,
                      completed_count: int, oom_count: int) -> None:
    """Update checkpoint during memory escalation to next level."""
    with modify_checkpoint(checkpoint_file) as cp:
        cp['state']['current_level'] = next_level
        cp['state']['current_memory'] = next_mem
        cp['state']['pending_indices'] = oom_indices
        cp['state']['status'] = 'ESCALATING'
        cp['state']['last_escalation_reason'] = 'OOM'
        if cp.get('rounds'):
            cp['rounds'][-1]['status'] = 'OOM'
            cp['rounds'][-1]['completed_count'] = completed_count
            cp['rounds'][-1]['oom_count'] = oom_count
            cp['rounds'][-1]['oom_indices'] = oom_indices
        if 'rounds' not in cp:
            cp['rounds'] = []
        cp['rounds'].append({
            'job_id': retry_job, 'handler_id': handler_id, 'array_spec': oom_indices,
            'level': next_level, 'memory': next_mem, 'status': 'PENDING',
            'submitted': datetime.now().isoformat()
        })
    print(f"Checkpoint updated: {checkpoint_file}")


def update_time_escalation(checkpoint_file: str, next_time_level: int, next_time: str,
                           timeout_indices: str, retry_job: int, handler_id: int,
                           completed_count: int, timeout_count: int) -> None:
    """Update checkpoint during time escalation to next level."""
    with modify_checkpoint(checkpoint_file) as cp:
        cp['state']['current_time_level'] = next_time_level
        cp['state']['current_time'] = next_time
        cp['state']['pending_indices'] = timeout_indices
        cp['state']['status'] = 'ESCALATING'
        cp['state']['last_escalation_reason'] = 'TIMEOUT'
        if cp.get('rounds'):
            cp['rounds'][-1]['status'] = 'TIMEOUT'
            cp['rounds'][-1]['completed_count'] = completed_count
            cp['rounds'][-1]['timeout_count'] = timeout_count
            cp['rounds'][-1]['timeout_indices'] = timeout_indices
        if 'rounds' not in cp:
            cp['rounds'] = []
        cp['rounds'].append({
            'job_id': retry_job, 'handler_id': handler_id, 'array_spec': timeout_indices,
            'time_level': next_time_level, 'time': next_time, 'status': 'PENDING',
            'submitted': datetime.now().isoformat()
        })
    print(f"Checkpoint updated: {checkpoint_file}")




def log_to_db(db_path: str, chain_id: str, action_type: str, job_id: str = None,
              memory_level: int = None, time_level: int = None,
              indices: str = None, details: str = None) -> None:
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


def main():
    parser = argparse.ArgumentParser(
        description='Resource Escalation Library - Python utilities for mem-escalate.sh'
    )
    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # load-config
    p = subparsers.add_parser('load-config', help='Parse YAML config file')
    p.add_argument('config_file', help='Path to config file')

    # create-checkpoint
    p = subparsers.add_parser('create-checkpoint', help='Create a new checkpoint')
    p.add_argument('checkpoint_dir', help='Checkpoint directory')
    p.add_argument('chain_id', help='Chain ID')
    p.add_argument('script', help='Script path')
    p.add_argument('memory_ladder', help='Space-separated memory levels')
    p.add_argument('time_ladder', help='Space-separated time levels')
    p.add_argument('script_args', nargs='*', help='Script arguments')

    # create-array-checkpoint
    p = subparsers.add_parser('create-array-checkpoint', help='Create array job checkpoint')
    p.add_argument('checkpoint_dir', help='Checkpoint directory')
    p.add_argument('chain_id', help='Chain ID')
    p.add_argument('array_spec', help='Array specification')
    p.add_argument('total_tasks', type=int, help='Total number of tasks')
    p.add_argument('script', help='Script path')
    p.add_argument('memory_ladder', help='Space-separated memory levels')
    p.add_argument('time_ladder', help='Space-separated time levels')
    p.add_argument('script_args', nargs='*', help='Script arguments')

    # update-checkpoint
    p = subparsers.add_parser('update-checkpoint', help='Update checkpoint with job info')
    p.add_argument('checkpoint_file', help='Checkpoint file path')
    p.add_argument('job_id', type=int, help='Job ID')
    p.add_argument('level', type=int, help='Escalation level')
    p.add_argument('memory', help='Memory allocation')
    p.add_argument('status', help='Job status')

    # update-job-status
    p = subparsers.add_parser('update-job-status', help='Update job status in checkpoint')
    p.add_argument('checkpoint_file', help='Checkpoint file path')
    p.add_argument('job_id', type=int, help='Job ID')
    p.add_argument('status', help='New status')

    # load-checkpoint
    p = subparsers.add_parser('load-checkpoint', help='Load checkpoint and output shell vars')
    p.add_argument('checkpoint_file', help='Checkpoint file path')

    # list-checkpoints
    p = subparsers.add_parser('list-checkpoints', help='List all checkpoints')
    p.add_argument('checkpoint_dir', help='Checkpoint directory')

    # show-status
    p = subparsers.add_parser('show-status', help='Show chain status')
    p.add_argument('checkpoint_file', help='Checkpoint file path')

    # update-array-round
    p = subparsers.add_parser('update-array-round', help='Update checkpoint with array round')
    p.add_argument('checkpoint_file', help='Checkpoint file path')
    p.add_argument('job_id', type=int, help='Job ID')
    p.add_argument('handler_id', type=int, help='Handler job ID')
    p.add_argument('array_spec', help='Array specification')
    p.add_argument('level', type=int, help='Escalation level')
    p.add_argument('memory', help='Memory allocation')

    # mark-completed
    p = subparsers.add_parser('mark-completed', help='Mark chain as completed')
    p.add_argument('checkpoint_file', help='Checkpoint file path')
    p.add_argument('job_id', type=int, help='Job ID that completed')
    p.add_argument('completed_count', type=int, help='Number of completed tasks')

    # mark-failed (new unified command)
    p = subparsers.add_parser('mark-failed', help='Mark chain as failed at max level')
    p.add_argument('checkpoint_file', help='Checkpoint file path')
    p.add_argument('failed_indices', help='Comma-separated failed indices')
    p.add_argument('reason', nargs='?', default='MEMORY', choices=['MEMORY', 'TIME'],
                   help='Failure reason (default: MEMORY)')

    # update-escalation
    p = subparsers.add_parser('update-escalation', help='Update checkpoint during memory escalation')
    p.add_argument('checkpoint_file', help='Checkpoint file path')
    p.add_argument('next_level', type=int, help='Next escalation level')
    p.add_argument('next_mem', help='Next memory allocation')
    p.add_argument('oom_indices', help='Comma-separated OOM indices')
    p.add_argument('retry_job', type=int, help='Retry job ID')
    p.add_argument('handler_id', type=int, help='Handler job ID')
    p.add_argument('completed_count', type=int, help='Number of completed tasks')
    p.add_argument('oom_count', type=int, help='Number of OOM tasks')

    # update-time-escalation
    p = subparsers.add_parser('update-time-escalation', help='Update checkpoint during time escalation')
    p.add_argument('checkpoint_file', help='Checkpoint file path')
    p.add_argument('next_time_level', type=int, help='Next time escalation level')
    p.add_argument('next_time', help='Next time allocation')
    p.add_argument('timeout_indices', help='Comma-separated TIMEOUT indices')
    p.add_argument('retry_job', type=int, help='Retry job ID')
    p.add_argument('handler_id', type=int, help='Handler job ID')
    p.add_argument('completed_count', type=int, help='Number of completed tasks')
    p.add_argument('timeout_count', type=int, help='Number of TIMEOUT tasks')

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

    args = parser.parse_args()

    if args.command == 'load-config':
        load_config(args.config_file)
    elif args.command == 'create-checkpoint':
        create_checkpoint(args.checkpoint_dir, args.chain_id, args.script,
                          args.memory_ladder, args.time_ladder, args.script_args)
    elif args.command == 'create-array-checkpoint':
        create_array_checkpoint(args.checkpoint_dir, args.chain_id, args.array_spec,
                                args.total_tasks, args.script, args.memory_ladder,
                                args.time_ladder, args.script_args)
    elif args.command == 'update-checkpoint':
        update_checkpoint(args.checkpoint_file, args.job_id, args.level,
                          args.memory, args.status)
    elif args.command == 'update-job-status':
        update_job_status(args.checkpoint_file, args.job_id, args.status)
    elif args.command == 'load-checkpoint':
        load_checkpoint(args.checkpoint_file)
    elif args.command == 'list-checkpoints':
        list_checkpoints(args.checkpoint_dir)
    elif args.command == 'show-status':
        show_status(args.checkpoint_file)
    elif args.command == 'update-array-round':
        update_array_round(args.checkpoint_file, args.job_id, args.handler_id,
                           args.array_spec, args.level, args.memory)
    elif args.command == 'mark-completed':
        mark_completed(args.checkpoint_file, args.job_id, args.completed_count)
    elif args.command == 'mark-failed':
        mark_failed(args.checkpoint_file, args.failed_indices, args.reason)
    elif args.command == 'update-escalation':
        update_escalation(args.checkpoint_file, args.next_level, args.next_mem,
                          args.oom_indices, args.retry_job, args.handler_id,
                          args.completed_count, args.oom_count)
    elif args.command == 'update-time-escalation':
        update_time_escalation(args.checkpoint_file, args.next_time_level, args.next_time,
                               args.timeout_indices, args.retry_job, args.handler_id,
                               args.completed_count, args.timeout_count)
    elif args.command == 'log-action':
        log_to_db(args.db_path, args.chain_id, args.action_type,
                  job_id=args.job_id, memory_level=args.memory_level,
                  time_level=args.time_level, indices=args.indices,
                  details=args.details)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
