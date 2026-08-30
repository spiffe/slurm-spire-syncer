// Package metrics holds the Prometheus instrumentation and the shared loop
// runner that drives every periodic task in the syncer.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Namespace prefixes every metric this program exports.
const Namespace = "slurm_spire_syncer"

// Loop names.
const (
	LoopSqueue    = "squeue"
	LoopSpireList = "spire_list"
	LoopReconcile = "reconcile"
)

// Metrics owns a private registry rather than the global default one, so that
// tests can collect from a known-empty registry and assert exact values.
type Metrics struct {
	Registry *prometheus.Registry

	loops map[string]*LoopMetrics

	// SlurmJobHosts and ManagedEntries track the size of the two in-memory
	// snapshots the reconciler works from.
	SlurmJobHosts  prometheus.Gauge
	ManagedEntries prometheus.Gauge

	EntriesCreated prometheus.Counter
	EntriesUpdated prometheus.Counter
	EntriesDeleted prometheus.Counter
}

// LoopMetrics is the per-loop metric set. Every loop reports the same shape so
// that a single set of alerting rules covers all of them.
type LoopMetrics struct {
	// Failing is 1 while the most recent run failed, 0 once one succeeds.
	Failing prometheus.Gauge
	Success prometheus.Counter
	Failure prometheus.Counter
	// Overrun counts runs that took longer than the interval, i.e. that were
	// still working when the next run was due.
	Overrun     prometheus.Counter
	Duration    prometheus.Histogram
	LastSuccess prometheus.Gauge
}

// New builds the metric set with every series pre-initialized.
//
// Pre-initializing matters: without it, "no data" and "healthy, zero events" are
// indistinguishable on a dashboard, and a failing-but-never-yet-succeeded loop
// exports nothing at all.
func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		Registry: reg,
		loops:    make(map[string]*LoopMetrics),

		SlurmJobHosts: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "slurm_job_hosts",
			Help:      "Number of running Slurm job/node pairs in the most recent snapshot.",
		}),
		ManagedEntries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "managed_entries",
			Help:      "Number of SPIRE registration entries owned by this syncer in the most recent snapshot.",
		}),
		EntriesCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "entries_created_total",
			Help:      "Total registration entries created.",
		}),
		EntriesUpdated: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "entries_updated_total",
			Help:      "Total registration entries updated.",
		}),
		EntriesDeleted: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "entries_deleted_total",
			Help:      "Total registration entries deleted.",
		}),
	}
	reg.MustRegister(m.SlurmJobHosts, m.ManagedEntries,
		m.EntriesCreated, m.EntriesUpdated, m.EntriesDeleted)

	for _, loop := range []string{LoopSqueue, LoopSpireList, LoopReconcile} {
		m.loops[loop] = newLoopMetrics(reg, loop)
	}
	return m
}

// Loop returns the metric set for a named loop.
func (m *Metrics) Loop(name string) *LoopMetrics { return m.loops[name] }

func newLoopMetrics(reg prometheus.Registerer, loop string) *LoopMetrics {
	labels := prometheus.Labels{"loop": loop}
	lm := &LoopMetrics{
		Failing: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "failing", ConstLabels: labels,
			Help: "1 while the most recent run of this loop failed, 0 otherwise.",
		}),
		Success: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "success_total", ConstLabels: labels,
			Help: "Total successful runs of this loop.",
		}),
		Failure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "failure_total", ConstLabels: labels,
			Help: "Total failed runs of this loop.",
		}),
		Overrun: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace, Name: "overrun_total", ConstLabels: labels,
			Help: "Total runs of this loop that took longer than the configured interval.",
		}),
		Duration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace, Name: "duration_seconds", ConstLabels: labels,
			Help:    "Duration of each run of this loop.",
			Buckets: prometheus.DefBuckets,
		}),
		LastSuccess: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace, Name: "last_success_timestamp_seconds", ConstLabels: labels,
			Help: "Unix timestamp of the most recent successful run of this loop.",
		}),
	}
	reg.MustRegister(lm.Failing, lm.Success, lm.Failure, lm.Overrun, lm.Duration, lm.LastSuccess)

	// Touch each series so it is exported from startup rather than from first use.
	lm.Failing.Set(0)
	lm.Success.Add(0)
	lm.Failure.Add(0)
	lm.Overrun.Add(0)
	lm.LastSuccess.Set(0)
	return lm
}
