#!/usr/bin/env bash
#SBATCH --job-name=spire-fetch
#SBATCH --output=/tmp/slurm-spire-jobs/slurm-%j.out
#SBATCH --error=/tmp/slurm-spire-jobs/slurm-%j.out
#SBATCH --time=00:10:00
#
# Launches one SVID-fetching task per allocated node.
#
# The srun is essential, not decoration. sbatch runs this script exactly once, on
# the first node of the allocation -- a batch step is not executed on every node.
# A job spanning two nodes would otherwise only ever fetch an SVID on one of
# them, and the other node's entry would go unverified.
#
# srun blocks until its tasks finish, and each task blocks until cancelled, so
# the job stays running and its entries stay live for the test to assert on.

set -uo pipefail

TASK="${FETCH_TASK_SCRIPT:?FETCH_TASK_SCRIPT must be exported by the submitter}"

echo "job ${SLURM_JOB_ID}: launching one task per node across ${SLURM_JOB_NODELIST:-unknown}"

exec srun --ntasks-per-node=1 --export=ALL "${TASK}"
