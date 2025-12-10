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
#   LOGGING_DB_PATH - Path to SQLite database
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

# Count completed tasks from sacct
completed_count=$(sacct -nX -j "$PARENT_JOB" -o State --parsable2 | grep -c "COMPLETED" || echo "0")
total_tasks=$(sacct -nX -j "$PARENT_JOB" -o State --parsable2 | wc -l)

log "Job $PARENT_JOB: $completed_count / $total_tasks tasks completed"

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

# Print final status report
log ""
log "================ FINAL STATUS ================"
python3 "$PYLIB" show-status "$CHECKPOINT_FILE" 2>&1 | tee -a "$LOG_FILE"
log "=============================================="

# TODO: Future enhancements:
#   - Send notification ?
#   - Clean up old output files
#   - Archive checkpoint to external storage
