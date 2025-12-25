#!/bin/bash
#SBATCH --job-name=species-py
#SBATCH --output=/data/tracker/outputs/%x-%A_%a.out
#SBATCH --error=/data/tracker/outputs/%x-%A_%a.err

# Species Demographics - Python Wrapper
# Calls the Python demographic model script

# Use the tests directory where scripts are located
SCRIPT_DIR="/data/jobs/tests"

# Run the Python demographic model
python3 "$SCRIPT_DIR/species-demo.py"
