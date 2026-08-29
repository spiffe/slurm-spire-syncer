#!/usr/bin/env bash
# Installs a single-node Slurm cluster on the runner.
#
# The configuration mirrors spire/test/integration/suites/slurm-x509, which is
# the only setup known to produce a working slurmstepd cgroup hierarchy on a
# GitHub Actions runner -- that hierarchy is what the SPIRE slurm workload
# attestor reads, so getting it wrong means no workload is ever attested.
#
# The node name is pinned rather than taken from $(hostname -s): the syncer
# templates each entry's parent ID from the node name, and the SPIRE agent's
# node-id has to match it. NodeHostname carries the runner's real hostname so
# slurmd can still resolve which node it is.

. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

log "installing slurm and munge"
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
  slurm-wlm munge

log "slurm version: $(sinfo --version 2>/dev/null || echo unknown)"

if [ ! -f /etc/munge/munge.key ]; then
  log "creating a munge key"
  sudo /usr/sbin/mungekey --create --keyfile /etc/munge/munge.key 2>/dev/null ||
    sudo dd if=/dev/urandom of=/etc/munge/munge.key bs=1024 count=1 status=none
fi
sudo chown munge:munge /etc/munge/munge.key
sudo chmod 0400 /etc/munge/munge.key

sudo mkdir -p /var/spool/slurmctld /var/spool/slurmd /var/log/slurm
sudo chown slurm:slurm /var/spool/slurmctld /var/log/slurm

# node1 has to resolve for slurmctld to reach slurmd.
grep -q " ${SLURM_NODE_NAME}\$" /etc/hosts ||
  echo "127.0.0.1 ${SLURM_NODE_NAME}" | sudo tee -a /etc/hosts >/dev/null

log "writing /etc/slurm/slurm.conf for node ${SLURM_NODE_NAME}"
sudo mkdir -p /etc/slurm
sudo tee /etc/slurm/slurm.conf >/dev/null <<CONF
ClusterName=cluster
SlurmctldHost=$(hostname -s)

SlurmUser=slurm
AuthType=auth/munge

SlurmctldPort=6817
SlurmdPort=6818

StateSaveLocation=/var/spool/slurmctld
SlurmdSpoolDir=/var/spool/slurmd
SlurmctldPidFile=/run/slurmctld.pid
SlurmdPidFile=/run/slurmd.pid
SlurmctldLogFile=/var/log/slurm/slurmctld.log
SlurmdLogFile=/var/log/slurm/slurmd.log

# proctrack/cgroup is what creates the slurmstepd scope the SPIRE slurm
# attestor reads. Without it no job is attestable and the whole test is moot.
ProctrackType=proctrack/cgroup
TaskPlugin=task/cgroup

SchedulerType=sched/backfill
SelectType=select/cons_tres
SelectTypeParameters=CR_Core

MpiDefault=none
ReturnToService=2

# Two CPUs so the two jobs the test submits run concurrently rather than
# queueing, which is what lets it prove they get distinct identities at once.
NodeName=${SLURM_NODE_NAME} NodeHostname=$(hostname -s) NodeAddr=127.0.0.1 CPUs=2 RealMemory=100 State=UNKNOWN
PartitionName=debug Nodes=ALL Default=YES MaxTime=INFINITE State=UP
CONF

log "writing /etc/slurm/cgroup.conf"
# Constraints are off deliberately: only the cgroup hierarchy matters here, and
# RealMemory=100 would otherwise get the jobs OOM-killed.
sudo tee /etc/slurm/cgroup.conf >/dev/null <<'CONF'
CgroupPlugin=cgroup/v2
ConstrainCores=no
ConstrainRAMSpace=no
ConstrainDevices=no
CONF

log "starting munge and slurm"
sudo systemctl restart munge
poll 30 "munge to answer" sh -c 'munge -n | unmunge' || fail "munge is not working"

sudo systemctl restart slurmctld
sudo systemctl restart slurmd

poll 60 "node ${SLURM_NODE_NAME} to become idle" \
  sh -c "sinfo -h -N -n '${SLURM_NODE_NAME}' -o '%T' | grep -qx idle" || {
  sudo scontrol show node "${SLURM_NODE_NAME}" >&2 || true
  sudo journalctl -u slurmctld -u slurmd --no-pager -n 100 >&2 || true
  fail "node ${SLURM_NODE_NAME} never became idle"
}

# The syncer reads squeue --json. If this build lacks the JSON serializer the
# syncer just reports zero jobs, which reads like a syncer bug rather than a
# missing plugin -- so check it here where the cause is unambiguous.
log "checking squeue --json is supported"
if ! squeue --json >/dev/null 2>&1; then
  squeue --json >&2 2>&1 || true
  fail "squeue --json is not supported by this slurm build"
fi

log "slurm is up; squeue --json works"
