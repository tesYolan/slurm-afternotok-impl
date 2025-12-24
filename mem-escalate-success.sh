#!/bin/bash
# =============================================================================
# Memory Escalation Success Handler
# =============================================================================
# Runs when all tasks complete successfully (afterok dependency).
# Updates checkpoint to mark the chain as COMPLETED.
#
# Environment variables (set by parent):
#   PARENT_JOB      - The job ID that succeeded
#   CHAIN_ID        - The escalation chain ID
#   CHECKPOINT_DIR  - Directory for checkpoint files
#   PYLIB           - Path to Python helper library
#   CURRENT_LEVEL   - Memory escalation level
#   ARRAY_SPEC      - Array indices that were run
#   LOGGING_ENABLED - Whether to log to database
#   LOGGING_DB_PATH - Path to SQLite database (actions log)
#   DB_PATH         - Path to SQLite database (structured storage)
# =============================================================================

set -e

LOG_FILE="${CHECKPOINT_DIR}/../success-handler-${SLURM_JOB_ID}.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log "========================================"
log "Success Handler Started"
log "========================================"
log "Handler Job ID:  $SLURM_JOB_ID"
log "Parent Job:      $PARENT_JOB"
log "Chain ID:        $CHAIN_ID"
log "Memory Level:    $CURRENT_LEVEL"

CHECKPOINT_FILE="${CHECKPOINT_DIR}/${CHAIN_ID}.checkpoint"

if [[ ! -f "$CHECKPOINT_FILE" ]]; then
    log "ERROR: Checkpoint not found: $CHECKPOINT_FILE"
    exit 1
fi

if [[ ! -f "$PYLIB" ]]; then
    log "ERROR: Python library not found: $PYLIB"
    exit 1
fi

# Count completed and failed tasks from sacct (handle multiple job IDs for batches)
completed_count=0
failed_count=0
total_tasks=0

for jid in ${PARENT_JOB//,/ }; do
    job_completed=$(sacct -nX -j "$jid" -o State --parsable2 | grep -c "COMPLETED" || echo "0")
    job_failed=$(sacct -nX -j "$jid" -o State --parsable2 | grep -cE "FAILED|TIMEOUT|OUT_OF_MEMORY" || echo "0")
    job_total=$(sacct -nX -j "$jid" -o State --parsable2 | wc -l)
    completed_count=$((completed_count + job_completed))
    failed_count=$((failed_count + job_failed))
    total_tasks=$((total_tasks + job_total))
done

log "Jobs $PARENT_JOB: $completed_count / $total_tasks tasks completed, $failed_count failed"

# If there are failures, the failure handler will handle it - exit early
if (( failed_count > 0 )); then
    log "Found $failed_count failed tasks - failure handler will process them"
    log "Success handler exiting (not marking as complete)"
    exit 0
fi

# Update checkpoint using library
log "Marking chain as COMPLETED..."
python3 "$PYLIB" mark-completed "$CHECKPOINT_FILE" "$PARENT_JOB" "$completed_count"

# Log completion event to database (if enabled)
if [[ "$LOGGING_ENABLED" == "true" ]] && [[ -n "$LOGGING_DB_PATH" ]]; then
    log "Logging completion to database..."
    python3 "$PYLIB" log-action "$LOGGING_DB_PATH" "$CHAIN_ID" "COMPLETED" \
        --job-id "$PARENT_JOB" \
        --memory-level "$CURRENT_LEVEL" \
        --details "All $completed_count tasks completed successfully"
fi

# Update structured database (dual-write)
if [[ "$LOGGING_ENABLED" == "true" ]] && [[ -n "$DB_PATH" ]]; then
    log "Updating structured database..."

    # Mark the final round as completed
    python3 "$PYLIB" db-update-round "$DB_PATH" "$CHAIN_ID" "$PARENT_JOB" "COMPLETED" 2>/dev/null || true

    # Save task metrics with output paths
    python3 "$PYLIB" db-save-tasks "$DB_PATH" "$CHAIN_ID" "$PARENT_JOB" 2>/dev/null || true

    # Mark the entire chain as completed
    python3 "$PYLIB" db-complete-chain "$DB_PATH" "$CHAIN_ID" "$completed_count" 2>/dev/null || true
fi

# Cancel the corresponding failure handler (it will never run now)
if [[ -n "$HANDLER_JOB_ID" ]] && [[ "$HANDLER_JOB_ID" != "0" ]]; then
    log "Cancelling failure handler job $HANDLER_JOB_ID (no longer needed)..."
    scancel "$HANDLER_JOB_ID" 2>/dev/null || true
fi

# Cancel all stale escalate handlers from earlier rounds (stuck with DependencyNeverSatisfied)
stale_jobs=$(squeue -u "$USER" -h -o "%i %j %T" 2>/dev/null | grep -E "escalate.*(PD|PENDING)" | awk '{print $1}' | tr '\n' ' ')
if [[ -n "$stale_jobs" ]]; then
    log "Cleaning up stale handler jobs: $stale_jobs"
    scancel $stale_jobs 2>/dev/null || true
fi

# Print final status report
log ""
log "========================================"
log "CHAIN COMPLETED SUCCESSFULLY"
log "========================================"
log ""
log "Chain:     $CHAIN_ID"
log "Tasks:     $completed_count completed"
log "Level:     $CURRENT_LEVEL"
log ""
python3 "$PYLIB" show-status "$CHECKPOINT_FILE" 2>&1 | tee -a "$LOG_FILE"
log "========================================"
