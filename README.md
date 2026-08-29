# slurm-spire-syncer

[![Apache 2.0 License](https://img.shields.io/github/license/spiffe/helm-charts)](https://opensource.org/licenses/Apache-2.0)
[![Development Phase](https://github.com/spiffe/spiffe/blob/main/.img/maturity/dev.svg)](https://github.com/spiffe/spiffe/blob/main/MATURITY.md#development)

A tool to sync SLURM job data into SPIRE registration entries. Used along with the SPIRE SLURM Workkload Attestor

The SPIRE agent's [`slurm` workload attestor](https://github.com/spiffe/spire) derives
`slurm:job_id:<id>` (or `slurm:sluid:<sluid>`) and `slurm:step:<step>` selectors from the
cgroup hierarchy `slurmstepd` creates for each job step. Nothing, however, creates the
registration entries those selectors are supposed to match. This daemon does: it polls
`squeue --json`, renders a parent ID and a SPIFFE ID per running job/node pair, and
reconciles the SPIRE server's entry list to match.

## How it works

Three independent loops, each on its own interval:

| Loop | Work |
| --- | --- |
| `squeue` | Runs `squeue --json`, keeps running CORE-allocated jobs, flattens to one record per (job, node) |
| `spire_list` | Lists the registration entries this syncer owns |
| `reconcile` | Diffs the two snapshots and creates, updates or deletes entries |

Each loop does its work first and waits afterwards, so there is no cold start and only one
invocation of a loop is ever in flight. Snapshots are published through `atomic.Pointer`,
so the reconciler reads a consistent view without locking and never blocks a gatherer.

The reconciler will not act until **both** snapshots exist. Reconciling against a
never-populated job list would read as "nothing is running" and delete every managed
entry. For the same reason, a failing `squeue` leaves the last good snapshot in place
rather than clearing it.

## Entries this syncer creates

For a job on a node it produces one entry:

| Field | Value |
| --- | --- |
| Entry ID | `<className>.<uuid>` — a v4 UUID, generated once when the entry is created |
| Parent ID | `parentIDTemplate` rendered against the job/node |
| SPIFFE ID | `spiffeIDTemplate` rendered against the job/node |
| Selectors | exactly one: `slurm:job_id:<id>` or `slurm:sluid:<sluid>` |
| Hint | `hint` (default `slurm`) |

**Only the job identifier is used as a selector, never the step.** SPIRE matches an entry
when its selectors are a *subset* of the workload's, so one job-scoped entry covers every
step of that job — `batch`, `extern` and every numbered step — with a single registration.

Template fields: `.TrustDomain`, `.ClassName`, `.JobID`, `.SLUID`, `.JobKey`, `.Account`,
`.Node`. `.JobKey` is the identifier actually in use (the SLUID when there is one).
Templates are compiled with `missingkey=error`, so a reference to a field that does not
exist fails loudly instead of rendering `<no value>` into a SPIFFE ID.

## Ownership, and why deletion is safe

An entry belongs to this syncer only if **both** markers hold:

1. its `Hint` equals the configured `hint`, and
2. its entry ID starts with `<className>.`

The hint is pushed down to the SPIRE server as an indexed exact-match `ByHint` list
filter, so the syncer never has to page the whole entry table. It is not sufficient on its
own, though — nothing stops another tool from stamping the same hint — so the ID prefix is
checked client-side over whatever comes back. An entry matching only one marker is never
updated and never deleted.

Setting `hint: ""` disables both hint stamping and the server-side filter; ownership then
rests on the ID prefix alone and the full entry list is paged. That is the degraded path,
not the recommended one.

The hint is also useful in its own right: a workload holding SVIDs from several sources
can pick out the Slurm-issued one by name.

> **Changing `hint` on a running deployment orphans existing entries.** The list filter
> will not return entries stamped with the old value, so the syncer can neither see nor
> clean them up, and it will create a fresh set alongside. Clean up first — run once with
> the old hint and no running jobs, or delete them with `spire-server entry delete`.

## Matching, and why entries are updated rather than replaced

Entry IDs are random and cannot be recomputed from Slurm data, so an existing entry is
matched to a job on **(parent ID, selector set)** — which is precisely the identity of a
job on a node, since the parent ID derives from the node and the selectors from the job.

Anything else that drifts — a changed account, a retuned TTL, an edited `spiffeIDTemplate`
— is therefore an update in place that preserves the entry ID, rather than a delete and
recreate that would churn the workload's SVID.

## Configuration

A YAML file passed with `-config`. Every setting also reads from an environment variable
named `SLURM_SPIRE_SYNCER_<SETTING>`; precedence is **file > environment > default**.

```yaml
# Base interval for all three loops.
interval: 10s
# Optional per-loop overrides; each defaults to `interval`.
squeueInterval: 10s
spireInterval: 10s
reconcileInterval: 10s

# Entry IDs are "<className>.<uuid>". SPIRE restricts entry ID characters to
# [-._0-9A-Za-z], and className is validated against that set.
className: slurm
# Stamped on every managed entry and used as the server-side list filter.
hint: slurm

# Required. No default.
trustDomain: example.org

squeueCommand: ["squeue", "--json"]
squeueTimeout: 30s

spireServerSocket: unix:///tmp/spire-server/private/api.sock
jobIdentifier: auto        # auto | job_id | sluid — see below

parentIDTemplate: "spiffe://{{.TrustDomain}}/spire/agent/x509pop/{{.Node}}"
spiffeIDTemplate: "spiffe://{{.TrustDomain}}/slurm/{{.Account}}/{{.JobKey}}"

x509SVIDTTL: 1h            # 0 uses the server default
jwtSVIDTTL: 5m             # 0 uses the server default

metricsAddr: ":9091"       # empty disables the endpoint
dryRun: false              # log the intended changes, mutate nothing
```

Unknown keys are rejected, so a typo fails at startup instead of silently leaving a
default in place.

### Templating the configuration file

With `-expand-env`, `${VAR}` and `$VAR` references in the configuration file are
replaced from the environment before the YAML is parsed:

```yaml
trustDomain: ${SPIFFE_TRUST_DOMAIN}
spireServerSocket: ${SPIRE_SERVER_SOCKET}
```

`SPIFFE_TRUST_DOMAIN` is the variable the SPIRE packages already write to
`/etc/spiffe/default-trust-domain.env` and every SPIFFE unit on the host reads, so a
templated `trustDomain` needs nothing else configured.

The real payoff is running more than one instance: one configuration file serves them
all, and each takes its own values from its own `EnvironmentFile`. See
[running several instances](#running-several-instances).

Two things worth knowing:

- **An undefined variable expands to the empty string**, not an error — the same as
  `os.ExpandEnv` and SPIRE's own `-expandEnv`. A required setting left empty is caught
  by validation, which names the field.
- **Expansion happens before parsing**, so what it produces is indistinguishable from
  text written in the file. It therefore beats the `SLURM_SPIRE_SYNCER_*` environment
  overrides, which apply only to keys the file omits.

Leave it off if a Go template in your configuration uses `$` variables
(`{{$n := .Node}}`), since expansion would eat them. It is off by default.

### Job identifier autodetection

Slurm ≥ 26.05 reports a `sluid` per job. The syncer autodetects per job: it uses the
`sluid` when the JSON carries one and falls back to `job_id` otherwise. No configuration
is needed in the normal case.

`jobIdentifier` exists for one situation that genuinely cannot be inferred. The attestor's
choice depends on `CgroupJobIdPaths` in `cgroup.conf`, not only on the Slurm version: with
`CgroupJobIdPaths=yes` on Slurm ≥ 26.05 the attestor emits `slurm:job_id` even though
`squeue --json` still reports a `sluid`. Autodetection would pick the SLUID and the entries
would never match any workload. If that describes your cluster, set `jobIdentifier: job_id`.

**Symptom of getting this wrong:** entries are created and `managed_entries` looks healthy,
but no workload ever receives an SVID.

## Slurm version support

`squeue --json` has changed shape twice, so the syncer detects which it is looking at
rather than being told.

| | Slurm 23.11 | 24.05 – 25.11 | 26.x |
| --- | --- | --- | --- |
| core allocation | no `select_type`; inferred from `allocated_cores > 0` | `select_type: ["CORE"]` | `select_type: ["CR_CORE", …]` |
| node hostlist | `job_resources.nodes` (a string) | `job_resources.nodes.list` | `job_resources.nodes.list` |
| expanded nodes | `allocated_nodes[].nodename` | `nodes.allocation[].name` | `nodes.allocation[].name` |

`job_id`, `job_state` and `account` are unchanged across all three.

Expanded node lists are preferred wherever Slurm provides one; the hostlist string is
only parsed as a fallback.

**One caveat on 23.11.** With no `select_type` to read, core allocation is inferred from
`allocated_cores`, which Slurm emits as a real count only for core- *or socket*-based
selection and as zero otherwise. A socket-allocated job is therefore accepted on 23.11
where a newer Slurm would exclude it. The two are indistinguishable in that version's
output; if the distinction matters to you, run a newer Slurm.

## Permissions

The SPIRE server's private API socket is plaintext and unauthenticated at the transport
layer — the server tags every unix-domain peer as a local caller, and its default
authorization policy grants local callers full access to the Entry API. Access is gated by
filesystem permissions on the socket (mode `0770`), so **this process must run as the SPIRE
server's user or share its group**.

## Metrics

Prometheus metrics on `metricsAddr` at `/metrics`, served from a private registry. Every
series is pre-initialized to zero at startup so that "no data" and "healthy, zero events"
stay distinguishable.

Per loop, labelled `loop="squeue"`, `loop="spire_list"` or `loop="reconcile"`:

```
slurm_spire_syncer_failing                          # 1 while the last run failed
slurm_spire_syncer_success_total
slurm_spire_syncer_failure_total
slurm_spire_syncer_overrun_total                    # runs that outlasted their interval
slurm_spire_syncer_duration_seconds                 # histogram
slurm_spire_syncer_last_success_timestamp_seconds
```

Plus:

```
slurm_spire_syncer_slurm_job_hosts       # job/node pairs in the latest snapshot
slurm_spire_syncer_managed_entries       # owned SPIRE entries in the latest snapshot
slurm_spire_syncer_entries_created_total
slurm_spire_syncer_entries_updated_total
slurm_spire_syncer_entries_deleted_total
```

Errors caused by shutdown are not counted as failures and do not raise `failing`.

## Usage

```bash
slurm-spire-syncer -config /etc/spire/slurm-syncer -instance main
```

Check a configuration without starting the daemon — this renders both templates against a
sample job and prints the entry that would result:

```bash
slurm-spire-syncer -config /etc/spire/slurm-syncer -instance main -validate
```

Log level is `-log-level debug|info|warn|error`. The daemon shuts down cleanly on SIGINT
and SIGTERM.

### Running under systemd

`systemd/slurm-spire-syncer@.service` is a templated unit, so the instance name selects
which SPIRE server to sync:

```bash
sudo systemctl start slurm-spire-syncer@main
```

It runs as root, because the SPIRE server creates its private API socket directory
root-owned and mode `0750`. To avoid that, run as a user in the server's group and
adjust `ReadWritePaths`.

The unit derives the server socket from the instance name
(`/run/spire/server/sockets/%i/private/api.sock`), which is the layout the SPIRE
packages use — so naming the instance after the SPIRE server instance is all the
wiring needed.

Configuration lives under `/etc/spire/slurm-syncer`, laid out the way the packages lay
out `spire-server`. The first file that exists is loaded:

| File | For |
| --- | --- |
| `/etc/spire/slurm-syncer/<instance>/config` | one instance, replacing the default outright |
| `/etc/spire/slurm-syncer/<instance>.conf` | one instance, flat form |
| `/etc/spire/slurm-syncer/default.conf` | shipped defaults, shared by every instance |

`default.conf` ships templated and the unit loads it with `-expand-env`, so most
deployments never write a per-instance config at all — they set the variables it
references in an environment file instead. Those are read in order, last one winning,
and all are optional:

| File | For |
| --- | --- |
| `/etc/spiffe/default-trust-domain.env` | `SPIFFE_TRUST_DOMAIN`, shared with every SPIFFE unit on the host |
| `/etc/spire/slurm-syncer/default.env` | variables shared by every instance |
| `/etc/spire/slurm-syncer/<instance>.env` | variables for one instance |

### Running several instances

A host running more than one `spire-server` — an HA setup, for instance — needs each
one's Slurm jobs synced separately. Start one syncer per server:

```bash
sudo systemctl start slurm-spire-syncer@main slurm-spire-syncer@backup
```

Both read the shipped `/etc/spire/slurm-syncer/default.conf`. Anything that differs
between them comes from their own environment file — the server socket already does,
derived from the instance name by the unit:

```ini
# /etc/spire/slurm-syncer/backup.env
SLURM_SYNCER_METRICS_ADDR=127.0.0.1:9092
SLURM_SYNCER_CLASS_NAME=slurm-backup
```

Give each instance its own metrics port, or only the first to start gets one.

An instance that needs a genuinely different configuration, rather than different
values, gets its own file at `/etc/spire/slurm-syncer/<instance>/config`, which replaces
the default outright.

One caution: two syncers sharing a `className` and `hint` while pointed at the *same*
SPIRE server would each treat the other's entries as strays and delete them. Point each
at its own server, or give each its own `className`.

## Development

```bash
go vet ./... && go test -race ./... && go build ./...
```

Neither Slurm nor SPIRE is needed to run the tests. The Slurm side is driven by JSON
fixtures in `internal/slurm/testdata` and by pointing `squeueCommand` at a shell command;
the SPIRE side runs against `internal/spiretest`, an in-memory Entry API served over a real
unix socket, so the tests exercise the generated gRPC client, the wire encoding and real
status codes.
