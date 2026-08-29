#!/usr/bin/env bash
#SBATCH --job-name=spire-fetch
#SBATCH --output=/tmp/slurm-spire-jobs/slurm-%j.out
#SBATCH --error=/tmp/slurm-spire-jobs/slurm-%j.out
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=1
#
# Fetches this job's X509-SVID from the SPIRE agent, records the SPIFFE ID it was
# issued, then blocks until cancelled.
#
# The retry loop is what makes the ordering work: the job starts before its
# registration entry exists, because the syncer cannot see the job until squeue
# reports it running. So the job keeps asking until the entry shows up.
#
# Blocking afterwards is deliberate. If the job exited here the syncer would
# correctly delete its entry, and the lifecycle assertion could no longer tell a
# correct deletion from a premature one.

set -uo pipefail

SOCK="${SPIRE_AGENT_SOCKET:-/run/spire/agent/sockets/main/public/api.sock}"
BIN="${SPIRE_AGENT_BIN:-spire-agent}"
WORKDIR="/tmp/slurm-spire-jobs/${SLURM_JOB_ID}"
MARKER="${WORKDIR}/RESULT"

mkdir -p "${WORKDIR}"

echo "job ${SLURM_JOB_ID} account=${SLURM_JOB_ACCOUNT:-unknown} node=${SLURMD_NODENAME:-unknown}"
# The cgroup path is what the slurm workload attestor matches on; printing it
# makes an attestation failure diagnosable from the job log alone.
echo "cgroup: $(cat /proc/self/cgroup 2>/dev/null || echo unavailable)"

DEADLINE=$(( $(date +%s) + 180 ))
attempt=0
while [ "$(date +%s)" -lt "${DEADLINE}" ]; do
    attempt=$((attempt + 1))
    if OUT=$("${BIN}" api fetch x509 -socketPath "${SOCK}" -write "${WORKDIR}" 2>&1); then
        if [ -f "${WORKDIR}/svid.0.pem" ]; then
            SPIFFE_ID=$(printf '%s\n' "${OUT}" | grep -m1 'SPIFFE ID' | sed 's/.*SPIFFE ID:[[:space:]]*//')
            {
                echo "FETCH_SUCCESS job=${SLURM_JOB_ID}"
                echo "SPIFFE_ID=${SPIFFE_ID}"
                echo "ACCOUNT=${SLURM_JOB_ACCOUNT:-unknown}"
            } > "${MARKER}"
            echo "job ${SLURM_JOB_ID} was issued ${SPIFFE_ID} after ${attempt} attempt(s)"

            # Hold the allocation open so the entry stays live for the test's
            # assertions. scancel ends the job.
            while true; do sleep 5; done
        fi
    fi
    echo "job ${SLURM_JOB_ID}: attempt ${attempt}: not issued yet: ${OUT}"
    sleep 3
done

echo "FETCH_TIMEOUT job=${SLURM_JOB_ID}" > "${MARKER}"
echo "job ${SLURM_JOB_ID}: gave up after ${attempt} attempts"
exit 1
