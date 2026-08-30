#!/usr/bin/env bash
# Shared helpers for the integration setup scripts.
#
# These scripts modify the machine they run on -- installing packages, writing
# under /etc, starting services -- so they are meant only for a disposable CI
# runner.

set -euo pipefail

# Everything is keyed on this one name so the Slurm node, the SPIRE agent's
# node-id and the syncer's parentIDTemplate cannot drift apart. The whole test
# turns on them matching: the entry's parent ID has to be the agent that serves
# the workload, or no SVID is ever issued.
: "${SLURM_NODE_COUNT:=1}"
: "${SLURM_NODE_NAME:=node1}"

# slurm_nodes — the node names in this cluster, one per line.
#
# Every node beyond the first exists only because Ubuntu's slurm-wlm is built
# with --enable-multiple-slurmd, which gives each slurmd its own
# /system.slice/<nodename>_slurmstepd.scope cgroup tree and its own munge replay
# window. Without that the daemons collide on one shared scope.
slurm_nodes() {
  local i
  for i in $(seq 1 "${SLURM_NODE_COUNT}"); do
    echo "node${i}"
  done
}

# slurmd_port <node> — the port that node's slurmd listens on.
#
# Co-located slurmd instances cannot share the default 6818, and the obvious
# 6818+N walks straight into 6819, which is slurmdbd's DbdPort: slurmd claimed it
# first and slurmdbd died with "Address already in use", while slurmd logged
# "protocol_version not supported" for every accounting message it was handed by
# mistake. 17000 is well clear of everything Slurm reserves.
slurmd_port() {
  echo $((17000 + ${1#node}))
}

# spire_agent_instance <node> — the SPIRE agent instance serving a node.
#
# The first node is served by the agent spire-dev-action deployed, whose instance
# name is its own concern and not the node name. Every additional node gets an
# agent instance named after it. One rule, written down once, because the test
# and the setup scripts both need it and disagreeing would be invisible.
spire_agent_instance() {
  if [ "$1" = "${SLURM_NODE_NAME}" ]; then
    echo "${SPIRE_AGENT_INSTANCE:-main}"
  else
    echo "$1"
  fi
}

# spire_agent_socket <node> — the workload API socket a job on that node uses.
spire_agent_socket() {
  echo "/run/spire/agent/sockets/$(spire_agent_instance "$1")/public/api.sock"
}
: "${TRUST_DOMAIN:=example.org}"
: "${SLURM_ACCOUNTS:=physics,chemistry}"

# The syncer unit is templated, and the instance name selects which SPIRE
# server's socket it talks to. main matches spire-dev-action's default instance,
# so /run/spire/server/sockets/main/private/api.sock falls out of %i with nothing
# else to configure.
: "${SYNCER_INSTANCE:=main}"
: "${SYNCER_CONFIG_DIR:=/etc/spire/slurm-syncer}"
: "${SYNCER_BIN:=/usr/bin/slurm-spire-syncer}"
: "${SYNCER_METRICS_ADDR:=127.0.0.1:9091}"

: "${JOB_DIR:=/tmp/slurm-spire-jobs}"

export SLURM_NODE_COUNT SLURM_NODE_NAME TRUST_DOMAIN SLURM_ACCOUNTS SYNCER_INSTANCE \
  SYNCER_CONFIG_DIR SYNCER_BIN SYNCER_METRICS_ADDR JOB_DIR

log() { echo "[$(basename "${0}")] $*"; }

fail() {
  echo "::error::$*" >&2
  exit 1
}

# poll <timeout_seconds> <description> <command...>
#
# Retries until the command succeeds. On timeout it runs the command once more
# with output shown, so the failure message says what was actually wrong rather
# than only that a deadline passed.
poll() {
  local timeout="$1" description="$2"
  shift 2
  local deadline=$((SECONDS + timeout))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "::error::timed out after ${timeout}s waiting for ${description}" >&2
  echo "--- final attempt ---" >&2
  "$@" >&2 2>&1 || true
  return 1
}
