#!/usr/bin/env bash
# The per-node half of the SVID fetch, launched once per allocated node by srun
# from fetch-svid.sh.
#
# Run as a job step rather than the batch step, so its cgroup is
# .../job_<id>/step_<n>/ instead of step_batch. The attestor therefore reports
# slurm:step:<n>, and the entry still matches because it carries only the job
# selector -- which is the design: one job-scoped entry covers every step.
#
# The retry loop is what makes the ordering work: the job starts before its
# registration entry exists, because the syncer cannot see the job until squeue
# reports it running. So it keeps asking until the entry shows up.
#
# Blocking afterwards is deliberate. If the task exited here the syncer would
# correctly delete the entry, and the lifecycle assertion could no longer tell a
# correct deletion from a premature one.

set -uo pipefail

# Resolved here rather than passed in, because a job can span nodes served by
# different agents: every task has to find the agent on the node it landed on.
# The rule matches spire_agent_instance in scripts/lib.sh -- the first node uses
# the instance spire-dev-action deployed, every other is named after its node.
if [ -n "${SPIRE_AGENT_SOCKET:-}" ]; then
    SOCK="${SPIRE_AGENT_SOCKET}"
else
    SOCK_DIR="${SPIRE_AGENT_SOCKET_DIR:-/run/spire/agent/sockets}"
    INSTANCE="${SLURMD_NODENAME:-}"
    if [ -z "${INSTANCE}" ] || [ "${INSTANCE}" = "${SPIRE_PRIMARY_NODE:-node1}" ]; then
        INSTANCE="${SPIRE_PRIMARY_AGENT_INSTANCE:-main}"
    fi
    SOCK="${SOCK_DIR}/${INSTANCE}/public/api.sock"
fi
BIN="${SPIRE_AGENT_BIN:-spire-agent}"
# Keyed by node as well as job: a job spanning nodes runs this once per node,
# and each has its own identity to record.
WORKDIR="/tmp/slurm-spire-jobs/${SLURM_JOB_ID}/${SLURMD_NODENAME:-unknown}"
MARKER="${WORKDIR}/RESULT"

mkdir -p "${WORKDIR}"

echo "job ${SLURM_JOB_ID} account=${SLURM_JOB_ACCOUNT:-unknown} node=${SLURMD_NODENAME:-unknown} socket=${SOCK}"
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
                echo "NODE=${SLURMD_NODENAME:-unknown}"
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
