#!/usr/bin/env bash
# Installs and starts the syncer as a systemd unit.
#
# Running it as a unit under root is what keeps the SPIRE server's socket
# permissions untouched: the server creates its private API socket directory
# root-owned and mode 0750, and the deployment is left exactly as the action
# shipped it rather than being loosened to suit the test.
#
# Takes the path to the built binary as its only argument.

. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

BINARY="${1:?usage: install-syncer.sh <path-to-built-binary>}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

log "installing ${BINARY} to ${SYNCER_BIN}"
sudo install -m 0755 "${BINARY}" "${SYNCER_BIN}"

# The shipped default.conf is installed unchanged and no per-instance config is
# written, so this exercises the path a fresh deployment actually takes: the
# packaged default, templated, with everything that differs supplied through the
# instance's environment file.
log "installing the packaged defaults"
sudo mkdir -p "${SYNCER_CONFIG_DIR}"
sudo install -m 0644 "${REPO_ROOT}/config/slurm-syncer/default.conf" \
  "${SYNCER_CONFIG_DIR}/default.conf"
sudo install -m 0644 "${REPO_ROOT}/config/slurm-syncer/default.env" \
  "${SYNCER_CONFIG_DIR}/default.env"

# Written by the SPIRE packages in a real deployment; supplied here only if the
# SPIRE deployment did not leave one.
if [ ! -f /etc/spiffe/default-trust-domain.env ]; then
  log "writing /etc/spiffe/default-trust-domain.env"
  sudo mkdir -p /etc/spiffe
  echo "SPIFFE_TRUST_DOMAIN=${TRUST_DOMAIN}" |
    sudo tee /etc/spiffe/default-trust-domain.env >/dev/null
fi

# Everything this instance needs that differs from the shipped default. A short
# interval so the test does not spend most of its time waiting.
log "writing ${SYNCER_CONFIG_DIR}/${SYNCER_INSTANCE}.env"
# The parent ID template has to name something the serving agent actually holds.
# The packaged default.env expects a node alias, spiffe://<td>/node/<node>, which
# an operator creates once per node; this deployment has no aliases, and
# spire-dev-action attests with join_token, so the agent holds
# spiffe://<td>/agent/<node-id> instead.
#
# Overriding it here rather than creating aliases keeps the test pointed at what
# the deployment really provides, and exercises the layering: this file is read
# after default.env, so it wins.
sudo tee "${SYNCER_CONFIG_DIR}/${SYNCER_INSTANCE}.env" >/dev/null <<CONF
SLURM_SYNCER_INTERVAL=2s
SLURM_SYNCER_METRICS_ADDR=${SYNCER_METRICS_ADDR}
SLURM_SYNCER_PARENT_ID_TEMPLATE=spiffe://{{.TrustDomain}}/agent/{{.Node}}
CONF

log "validating the configuration"
# Sourced from the same files the unit reads, in the same order, rather than
# hand-listed. Hand-listing had already drifted: it omitted the SPIFFE ID
# template, so -validate reported a configuration the service would never run.
#
# No sudo: -validate only reads the configuration and renders the templates, and
# never opens the SPIRE socket. Running unprivileged also proves the files are
# readable by a non-root user.
(
  set -a
  # Set before the files, matching the unit, where Environment= precedes every
  # EnvironmentFile and a file may override it.
  SPIRE_SERVER_SOCKET="unix:///run/spire/server/sockets/${SYNCER_INSTANCE}/private/api.sock"
  for env_file in /etc/spiffe/default-trust-domain.env \
    "${SYNCER_CONFIG_DIR}/default.env" \
    "${SYNCER_CONFIG_DIR}/${SYNCER_INSTANCE}.env"; do
    if [ -r "${env_file}" ]; then
      # shellcheck disable=SC1090
      . "${env_file}"
    fi
  done
  set +a

  "${SYNCER_BIN}" -config "${SYNCER_CONFIG_DIR}" -instance "${SYNCER_INSTANCE}" \
    -expand-env -validate
) || fail "the syncer rejected its own configuration"

log "installing the systemd unit"
sudo install -m 0644 "${REPO_ROOT}/systemd/slurm-spire-syncer@.service" \
  /etc/systemd/system/"slurm-spire-syncer@.service"
sudo systemctl daemon-reload
sudo systemctl restart "slurm-spire-syncer@${SYNCER_INSTANCE}"

# Wait on the metrics rather than sleeping: a successful squeue loop and a
# successful spire_list loop together mean both halves of the syncer are
# genuinely working, which no sleep can establish.
metrics_url="http://${SYNCER_METRICS_ADDR}/metrics"
loop_succeeded() {
  curl -sf "${metrics_url}" |
    grep -qE "^slurm_spire_syncer_success_total\{loop=\"$1\"\} [1-9]"
}

for loop in squeue spire_list; do
  poll 60 "the ${loop} loop to report a success" loop_succeeded "${loop}" || {
    sudo journalctl -u "slurm-spire-syncer@${SYNCER_INSTANCE}" --no-pager -n 100 >&2 || true
    fail "the syncer's ${loop} loop never succeeded"
  }
  log "  ${loop} loop is healthy"
done

mkdir -p "${JOB_DIR}"
chmod 0777 "${JOB_DIR}"

log "syncer is running as slurm-spire-syncer@${SYNCER_INSTANCE}"
