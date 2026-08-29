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

if [ ! -f /etc/munge/munge.key ]; then
  log "creating a munge key"
  sudo /usr/sbin/mungekey --create --keyfile /etc/munge/munge.key 2>/dev/null ||
    sudo dd if=/dev/urandom of=/etc/munge/munge.key bs=1024 count=1 status=none
fi
sudo chown munge:munge /etc/munge/munge.key
sudo chmod 0400 /etc/munge/munge.key

sudo mkdir -p /var/spool/slurmctld /var/spool/slurmd /var/log/slurm
sudo chown slurm:slurm /var/spool/slurmctld /var/log/slurm

# Every node has to resolve for slurmctld to reach its slurmd.
while IFS= read -r node; do
  grep -q " ${node}\$" /etc/hosts ||
    echo "127.0.0.1 ${node}" | sudo tee -a /etc/hosts >/dev/null
done < <(slurm_nodes)

# node_lines — one NodeName line per node.
#
# NodeHostname is the runner's real hostname for every node, which a stock build
# rejects as a duplicate; the multiple-slurmd build this relies on drops that
# check. NodeAddr plus a distinct Port is how slurmctld tells them apart.
node_lines() {
  local node port=""
  while IFS= read -r node; do
    # A single node keeps the default SlurmdPort, which is the configuration
    # that is known to work; only co-located daemons need distinct ports.
    if [ "${SLURM_NODE_COUNT}" -gt 1 ]; then
      port=" Port=$(slurmd_port "${node}")"
    fi
    echo "NodeName=${node} NodeHostname=$(hostname -s) NodeAddr=127.0.0.1${port}" \
      "CPUs=2 RealMemory=100 State=UNKNOWN"
  done < <(slurm_nodes)
}

log "writing /etc/slurm/slurm.conf for $(slurm_nodes | tr '\n' ' ')"
sudo mkdir -p /etc/slurm
sudo tee /etc/slurm/slurm.conf >/dev/null <<CONF
ClusterName=cluster
SlurmctldHost=$(hostname -s)

SlurmUser=slurm
AuthType=auth/munge

SlurmctldPort=6817
SlurmdPort=6818

StateSaveLocation=/var/spool/slurmctld
# %n expands to the node name, so co-located slurmd instances do not collide on
# a spool directory, a pidfile or a log. slurmd expands these itself, with no
# need for the multiple-slurmd build.
SlurmdSpoolDir=/var/spool/slurmd.%n
SlurmctldPidFile=/run/slurmctld.pid
SlurmdPidFile=/run/slurmd.%n.pid
SlurmctldLogFile=/var/log/slurm/slurmctld.log
SlurmdLogFile=/var/log/slurm/slurmd.%n.log

# proctrack/cgroup is what creates the slurmstepd scope the SPIRE slurm
# attestor reads. Without it no job is attestable and the whole test is moot.
ProctrackType=proctrack/cgroup
TaskPlugin=task/cgroup

SchedulerType=sched/backfill
SelectType=select/cons_tres
SelectTypeParameters=CR_Core

MpiDefault=none
ReturnToService=2

# Two CPUs per node so the jobs the test submits run concurrently rather than
# queueing, which is what lets it prove they get distinct identities at once.
# One NodeName line per node, each with its own port: they share a host.
$(node_lines)
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

if [ "${SLURM_NODE_COUNT}" -le 1 ]; then
  sudo systemctl restart slurmd
else
  # Several slurmd on one host need the --enable-multiple-slurmd build. Without
  # it every daemon writes the same /system.slice/slurmstepd.scope, so the
  # attestor cannot tell the nodes apart and the daemons fight over the scope.
  # Ubuntu's slurm-wlm does enable it; check rather than assume, because the
  # failure is otherwise a confusing scope error much later.
  log "checking this build supports multiple slurmd"
  if ! scontrol show config 2>/dev/null | grep -qi 'MULTIPLE_SLURMD *= *Yes'; then
    fail "this slurm build lacks --enable-multiple-slurmd, so ${SLURM_NODE_COUNT} nodes cannot share a host"
  fi

  # The packaged slurmd unit runs a single unnamed daemon, which would claim the
  # first node. Replaced with a template so each node gets its own.
  log "installing a templated slurmd unit"
  sudo systemctl stop slurmd 2>/dev/null || true
  sudo systemctl disable slurmd 2>/dev/null || true
  sudo tee /etc/systemd/system/slurmd@.service >/dev/null <<'UNIT'
[Unit]
Description=Slurm node daemon (%i)
After=munge.service network-online.target
Wants=munge.service network-online.target

[Service]
Type=simple
# -D keeps it in the foreground so systemd tracks the process directly, and -N
# selects which configured node this daemon is.
ExecStart=/usr/sbin/slurmd -D -N %i
Restart=on-failure
RestartSec=5s
# slurmstepd is moved into its own cgroup scope, which needs the daemon to be
# able to write outside its own unit's subtree.
Delegate=yes
KillMode=process
LimitNOFILE=131072
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
UNIT
  sudo systemctl daemon-reload

  while IFS= read -r node; do
    log "starting slurmd for ${node}"
    sudo systemctl restart "slurmd@${node}"
  done < <(slurm_nodes)
fi

while IFS= read -r node; do
  poll 60 "node ${node} to become idle" \
    sh -c "sinfo -h -N -n '${node}' -o '%T' | grep -qx idle" || {
    sudo scontrol show node "${node}" >&2 || true
    sudo journalctl -u slurmctld -u slurmd -u "slurmd@${node}" --no-pager -n 100 >&2 || true
    fail "node ${node} never became idle"
  }
  log "  ${node} is idle"
done < <(slurm_nodes)

# The syncer reads squeue --json. If this build lacks the JSON serializer the
# syncer just reports zero jobs, which reads like a syncer bug rather than a
# missing plugin -- so check it here where the cause is unambiguous.
# Logged here rather than right after install: the client tools need
# /etc/slurm/slurm.conf to exist before they will answer at all, which is why
# this first reported "unknown".
log "slurm version: $(sinfo --version 2>/dev/null || echo unknown)"
log "$(scontrol show config 2>/dev/null | grep -i MULTIPLE_SLURMD || echo 'MULTIPLE_SLURMD = unknown')"

log "checking squeue --json is supported"
if ! squeue --json >/dev/null 2>&1; then
  squeue --json >&2 2>&1 || true
  fail "squeue --json is not supported by this slurm build"
fi

log "slurm is up; squeue --json works"
