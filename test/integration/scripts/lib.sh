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
: "${SLURM_NODE_NAME:=node1}"
: "${TRUST_DOMAIN:=example.org}"
: "${SLURM_ACCOUNTS:=physics,chemistry}"

# The syncer unit is templated, and the instance name selects which SPIRE
# server's socket it talks to. main matches spire-dev-action's default instance,
# so /run/spire/server/sockets/main/private/api.sock falls out of %i with nothing
# else to configure.
: "${SYNCER_INSTANCE:=main}"
: "${SYNCER_CONFIG_DIR:=/etc/spire/slurm-syncer}"
: "${SYNCER_BIN:=/usr/local/bin/slurm-spire-syncer}"
: "${SYNCER_METRICS_ADDR:=127.0.0.1:9091}"

: "${JOB_DIR:=/tmp/slurm-spire-jobs}"

export SLURM_NODE_NAME TRUST_DOMAIN SLURM_ACCOUNTS SYNCER_INSTANCE SYNCER_CONFIG_DIR \
  SYNCER_BIN SYNCER_METRICS_ADDR JOB_DIR

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
