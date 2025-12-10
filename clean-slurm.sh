#!/bin/bash
# Clean all Slurm test artifacts
# Usage: ./clean-slurm.sh
#
# Run from inside the slurmctld container:
#   /data/jobs/clean-slurm.sh

set -e

echo "=== Slurm Test Cleanup ==="
echo ""

# 1. Cancel all running jobs
echo "[1/4] Cancelling all jobs..."
scancel -u root 2>/dev/null || true
sleep 1

# 2. Clear job output files in /data
echo "[2/4] Removing job output files..."
rm -f /data/*.out /data/*.log 2>/dev/null || true
rm -f /data/handler-*.log /data/success-handler-*.out 2>/dev/null || true

# 3. Clear tracker directory (checkpoints and logs)
echo "[3/4] Clearing tracker directory..."
rm -rf /data/tracker/* 2>/dev/null || true

# 4. Purge Slurm accounting database
echo "[4/4] Purging Slurm accounting history..."
# Detect cluster name and purge job history
CLUSTER_NAME=$(mariadb -h mysql -u slurm -ppassword -N -e "
USE slurm_acct_db;
SELECT name FROM cluster_table LIMIT 1;
" 2>/dev/null || true)

if [[ -n "$CLUSTER_NAME" ]]; then
    mariadb -h mysql -u slurm -ppassword -e "
    USE slurm_acct_db;
    DELETE FROM ${CLUSTER_NAME}_job_table;
    DELETE FROM ${CLUSTER_NAME}_step_table;
    DELETE FROM ${CLUSTER_NAME}_job_env_table;
    DELETE FROM ${CLUSTER_NAME}_job_script_table;
    " 2>/dev/null && echo "  Purged job history for cluster: $CLUSTER_NAME"
else
    echo "  (Could not detect cluster name - DB not purged)"
fi

echo ""
echo "=== Cleanup Complete ==="
echo ""

# Verify
echo "Verification:"
echo "  Jobs in queue: $(squeue -h 2>/dev/null | wc -l)"
echo "  Output files:  $(ls /data/*.out 2>/dev/null | wc -l || echo 0)"
echo "  Tracker files: $(ls /data/tracker/* 2>/dev/null | wc -l || echo 0)"
echo ""
echo "Ready for fresh testing!"
