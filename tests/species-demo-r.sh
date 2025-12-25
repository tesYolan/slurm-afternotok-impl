#!/bin/bash
#SBATCH --job-name=species-r
#SBATCH --output=/data/tracker/outputs/%x-%A_%a.out
#SBATCH --error=/data/tracker/outputs/%x-%A_%a.err

# Species Demographics - R Wrapper
# Calls the R demographic model script

# Use the tests directory where scripts are located
SCRIPT_DIR="/data/jobs/tests"

# Run the R demographic model
Rscript "$SCRIPT_DIR/species-demo.R"
