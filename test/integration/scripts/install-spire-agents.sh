#!/usr/bin/env bash
# Starts one extra SPIRE agent per node beyond the first.
#
# spire-dev-action deploys a single agent, which serves the first node. Every
# other node needs its own, because SPIRE hands an entry only to the agent named
# as that entry's parent: without a second agent, a second node's entries would
# have to name the first node's agent, and the per-node parent ID mapping the
# syncer exists to produce would be untestable.
#
# The extra agents reuse the deb's spire-agent@.service template and the config
# the action already generated, so this depends on the packaging contract rather
# than on the action's internals. Only the join token differs, which is what
# gives each agent its own SPIFFE ID.

. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

if [ "${SLURM_NODE_COUNT}" -le 1 ]; then
  log "single node: the agent spire-dev-action deployed is the only one needed"
  exit 0
fi

PRIMARY_INSTANCE="$(spire_agent_instance "${SLURM_NODE_NAME}")"
SERVER_SOCKET="/run/spire/server/sockets/${SYNCER_INSTANCE}/private/api.sock"

# Copied rather than written from scratch: whatever attestor and socket settings
# the action chose for its own agent are the ones that work on this host.
PRIMARY_CONF="/etc/spire/agent/${PRIMARY_INSTANCE}.conf"
PRIMARY_ENV="/etc/spire/agent/${PRIMARY_INSTANCE}.env"
sudo test -f "${PRIMARY_CONF}" ||
  fail "expected the deployed agent config at ${PRIMARY_CONF}"

while IFS= read -r node; do
  [ "${node}" = "${SLURM_NODE_NAME}" ] && continue

  instance="$(spire_agent_instance "${node}")"
  agent_id="spiffe://${TRUST_DOMAIN}/agent/${node}"

  log "adding a SPIRE agent for ${node} as instance ${instance} (${agent_id})"

  sudo cp "${PRIMARY_CONF}" "/etc/spire/agent/${instance}.conf"

  # A join token binds this agent to the SPIFFE ID given here, which is what the
  # syncer's parentIDTemplate has to render to for that node.
  token="$(sudo spire-server token generate \
    -spiffeID "${agent_id}" \
    -socketPath "${SERVER_SOCKET}" |
    sed -n 's/^Token: *//p')"
  [ -n "${token}" ] || fail "could not generate a join token for ${node}"
  echo "::add-mask::${token}"

  # The action's env file carries the server address and log level; only the
  # token is per-agent.
  if sudo test -f "${PRIMARY_ENV}"; then
    sudo grep -v '^JOIN_TOKEN=' "${PRIMARY_ENV}" |
      sudo tee "/etc/spire/agent/${instance}.env" >/dev/null
  else
    sudo tee "/etc/spire/agent/${instance}.env" >/dev/null </dev/null
  fi
  echo "JOIN_TOKEN=\"${token}\"" |
    sudo tee -a "/etc/spire/agent/${instance}.env" >/dev/null

  sudo systemctl restart "spire-agent@${instance}"

  socket="$(spire_agent_socket "${node}")"
  poll 60 "the agent for ${node} to become healthy" \
    sudo spire-agent healthcheck -socketPath "${socket}" || {
    sudo journalctl -u "spire-agent@${instance}" --no-pager -n 100 >&2 || true
    fail "the agent for ${node} never became healthy"
  }

  # Prove it attested as the intended identity rather than merely coming up: a
  # wrong SPIFFE ID here would surface much later as entries that are never
  # delivered.
  listed="$(sudo spire-server agent list -socketPath "${SERVER_SOCKET}" 2>/dev/null || true)"
  printf '%s' "${listed}" | grep -qF "${agent_id}" ||
    fail "agent for ${node} is not registered as ${agent_id}; server reports: ${listed}"

  log "  ${node} is served by ${agent_id} on ${socket}"
done < <(slurm_nodes)

log "all node agents are up"
