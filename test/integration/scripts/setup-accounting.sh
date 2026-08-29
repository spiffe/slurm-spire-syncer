#!/usr/bin/env bash
# Adds slurmdbd accounting and creates the accounts the test submits under.
#
# The account is the point of the whole exercise: the syncer templates it into
# each SPIFFE ID, so the test proves a job submitted under one account cannot end
# up holding the other's identity. Without slurmdbd, Slurm accepts any --account
# string it is given, and the test would prove nothing about associations.
#
# AccountingStorageEnforce=associations is what makes the accounts real: without
# it an unknown account is silently accepted.

. "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

DB_NAME="slurm_acct_db"
DB_USER="slurm"
DB_PASS="slurmdbpass"
RUNNER_USER="$(id -un)"

log "installing slurmdbd and mariadb"
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq --no-install-recommends \
  slurmdbd mariadb-server

sudo systemctl start mariadb
poll 60 "mariadb to accept connections" sudo mysqladmin ping || fail "mariadb did not start"

log "creating the accounting database"
sudo mysql <<SQL
CREATE DATABASE IF NOT EXISTS ${DB_NAME};
CREATE USER IF NOT EXISTS '${DB_USER}'@'localhost' IDENTIFIED BY '${DB_PASS}';
GRANT ALL ON ${DB_NAME}.* TO '${DB_USER}'@'localhost';
FLUSH PRIVILEGES;
SQL

log "writing /etc/slurm/slurmdbd.conf"
sudo tee /etc/slurm/slurmdbd.conf >/dev/null <<CONF
AuthType=auth/munge
DbdHost=localhost
DbdPort=6819
SlurmUser=slurm
StorageType=accounting_storage/mysql
StorageHost=localhost
StorageUser=${DB_USER}
StoragePass=${DB_PASS}
StorageLoc=${DB_NAME}
LogFile=/var/log/slurm/slurmdbd.log
PidFile=/run/slurmdbd.pid
CONF
# slurmdbd refuses to start if this is group- or world-readable, because it
# holds the database password.
sudo chown slurm:slurm /etc/slurm/slurmdbd.conf
sudo chmod 0600 /etc/slurm/slurmdbd.conf

log "pointing slurm.conf at slurmdbd"
sudo tee -a /etc/slurm/slurm.conf >/dev/null <<CONF

AccountingStorageType=accounting_storage/slurmdbd
AccountingStorageHost=localhost
AccountingStoragePort=6819
# Without this an unknown --account is accepted silently, which would make the
# account half of this test vacuous.
AccountingStorageEnforce=associations
JobAcctGatherType=jobacct_gather/cgroup
CONF

# slurmdbd has to be up before slurmctld, and its first start creates the schema,
# which is slow enough that slurmctld will otherwise come up unable to reach it.
log "starting slurmdbd"
sudo systemctl restart slurmdbd
poll 90 "slurmdbd to finish creating its schema" sudo sacctmgr -i show cluster || {
  sudo journalctl -u slurmdbd --no-pager -n 100 >&2 || true
  fail "slurmdbd never became usable"
}

log "registering the cluster and accounts"
# The cluster name must match ClusterName in slurm.conf.
sudo sacctmgr -i add cluster cluster 2>/dev/null || true
sudo sacctmgr -i add account "${SLURM_ACCOUNTS}" 2>/dev/null || true

default_account="${SLURM_ACCOUNTS%%,*}"
sudo sacctmgr -i add user "${RUNNER_USER}" \
  "account=${SLURM_ACCOUNTS}" "defaultaccount=${default_account}" 2>/dev/null || true

log "restarting slurm to pick up the accounting configuration"
sudo systemctl restart slurmctld
if [ "${SLURM_NODE_COUNT}" -le 1 ]; then
  sudo systemctl restart slurmd
else
  while IFS= read -r node; do
    sudo systemctl restart "slurmd@${node}"
  done < <(slurm_nodes)
fi

while IFS= read -r node; do
  poll 60 "node ${node} to become idle again" \
    sh -c "sinfo -h -N -n '${node}' -o '%T' | grep -qx idle" ||
    fail "node ${node} did not return to idle after enabling accounting"
done < <(slurm_nodes)

# Prove the associations actually exist, rather than trusting that sacctmgr's
# output was suppressed for the right reason. Every `add` above is tolerant of
# "already exists", so a genuine failure would otherwise pass unnoticed.
log "verifying the associations"
assoc="$(sudo sacctmgr -n -P show assoc user="${RUNNER_USER}" format=account)"
for account in ${SLURM_ACCOUNTS//,/ }; do
  printf '%s\n' "${assoc}" | grep -qx "${account}" ||
    fail "user ${RUNNER_USER} is not associated with account ${account}; got: ${assoc}"
  log "  ${RUNNER_USER} is associated with ${account}"
done

log "accounting is configured"
