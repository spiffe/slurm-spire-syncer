package syncer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/spiffe/slurm-spire-syncer/internal/config"
	"github.com/spiffe/slurm-spire-syncer/internal/metrics"
	"github.com/spiffe/slurm-spire-syncer/internal/slurm"
	"github.com/spiffe/slurm-spire-syncer/internal/spireentry"
	"github.com/spiffe/slurm-spire-syncer/internal/spiretest"
	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
)

// harness wires a real Syncer to a fake squeue and a fake SPIRE Entry API. The
// squeue fixture is a file the test can rewrite between cycles to simulate jobs
// starting and finishing.
type harness struct {
	t       *testing.T
	syncer  *Syncer
	server  *spiretest.FakeEntryServer
	fixture string
}

func newHarness(t *testing.T, extraConfig string) *harness {
	t.Helper()

	dir := t.TempDir()
	fixture := filepath.Join(dir, "squeue.json")
	writeFixture(t, fixture, `{"jobs":[]}`)

	cfgPath := filepath.Join(dir, "config.yaml")
	cfg := fmt.Sprintf(`
trustDomain: example.org
squeueCommand: ["sh", "-c", "cat %s"]
%s
`, fixture, extraConfig)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("writing config: %v", err)
	}

	loaded, err := config.Load(cfgPath, false)
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	server := &spiretest.FakeEntryServer{}
	client := spireentry.New(spiretest.Start(t, server), loaded, log)

	s := New(loaded, slurm.NewGatherer(loaded, log), client, metrics.New(), log)
	// Deterministic IDs so tests can assert on exact entry IDs.
	n := 0
	s.newID = func() string {
		n++
		return fmt.Sprintf("uuid-%d", n)
	}

	return &harness{t: t, syncer: s, server: server, fixture: fixture}
}

func writeFixture(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing squeue fixture: %v", err)
	}
}

// setJobs rewrites the squeue fixture with one running, CORE-allocated job per
// entry given.
func (h *harness) setJobs(jobs ...string) {
	h.t.Helper()
	out := `{"jobs":[` + joinStrings(jobs, ",") + `]}`
	writeFixture(h.t, h.fixture, out)
}

func job(id, account, nodeList string) string {
	return fmt.Sprintf(`{"job_id":%s,"account":%q,"job_state":["RUNNING"],`+
		`"job_resources":{"select_type":["CORE"],"nodes":{"list":%q}}}`, id, account, nodeList)
}

// cycle runs one full pass of all three loops in the order the daemon would.
func (h *harness) cycle() {
	h.t.Helper()
	ctx := context.Background()
	if err := h.syncer.gatherJobs(ctx); err != nil {
		h.t.Fatalf("gatherJobs: %v", err)
	}
	if err := h.syncer.gatherEntries(ctx); err != nil {
		h.t.Fatalf("gatherEntries: %v", err)
	}
	if err := h.syncer.reconcile(ctx); err != nil {
		h.t.Fatalf("reconcile: %v", err)
	}
}

func (h *harness) entryIDs() []string {
	ids := h.server.IDs()
	sort.Strings(ids)
	return ids
}

func (h *harness) entryByID(id string) *types.Entry {
	h.t.Helper()
	for _, e := range h.server.Snapshot() {
		if e.Id == id {
			return e
		}
	}
	h.t.Fatalf("no entry with ID %q; have %v", id, h.entryIDs())
	return nil
}

func joinStrings(parts []string, sep string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += sep
		}
		out += p
	}
	return out
}

// The full path: a running job becomes a registration entry with the templated
// parent and SPIFFE IDs, the attestor's selector, and the ownership hint.
func TestSyncerCreatesEntriesForRunningJobs(t *testing.T) {
	h := newHarness(t, "")
	h.setJobs(job("1001", "physics", "node[01-02]"))
	h.cycle()

	if got, want := h.entryIDs(), []string{"slurm.uuid-1", "slurm.uuid-2"}; !equalStrings(got, want) {
		t.Fatalf("entry IDs = %v, want %v", got, want)
	}

	e := h.entryByID("slurm.uuid-1")
	if e.ParentId.Path != "/spire/agent/x509pop/node01" {
		t.Errorf("parent ID path = %q, want the node's agent path", e.ParentId.Path)
	}
	if e.SpiffeId.Path != "/slurm/physics/1001" {
		t.Errorf("SPIFFE ID path = %q, want /slurm/physics/1001", e.SpiffeId.Path)
	}
	if e.SpiffeId.TrustDomain != "example.org" {
		t.Errorf("trust domain = %q, want example.org", e.SpiffeId.TrustDomain)
	}
	if e.Hint != "slurm" {
		t.Errorf("hint = %q, want slurm", e.Hint)
	}
	// One selector only: the job identifier. The attestor also emits
	// slurm:step, and entry selectors must be a subset of the workload's, so a
	// job-scoped entry covers every step.
	if len(e.Selectors) != 1 {
		t.Fatalf("selectors = %+v, want exactly one", e.Selectors)
	}
	if e.Selectors[0].Type != "slurm" || e.Selectors[0].Value != "job_id:1001" {
		t.Errorf("selector = %s:%s, want slurm:job_id:1001",
			e.Selectors[0].Type, e.Selectors[0].Value)
	}
}

// Running again with unchanged input must issue no writes at all — otherwise the
// syncer would churn SVIDs every interval.
func TestSyncerIsIdempotent(t *testing.T) {
	h := newHarness(t, "")
	h.setJobs(job("1001", "physics", "node01"))

	h.cycle()
	afterFirst := h.entryIDs()

	h.cycle()
	h.cycle()

	if got := h.entryIDs(); !equalStrings(got, afterFirst) {
		t.Fatalf("entry IDs = %v, want them unchanged at %v", got, afterFirst)
	}
	if n := len(h.server.CreateRequests()); n != 1 {
		t.Errorf("BatchCreateEntry called %d times, want 1", n)
	}
	if n := len(h.server.UpdateRequests()); n != 0 {
		t.Errorf("BatchUpdateEntry called %d times, want 0", n)
	}
	if n := len(h.server.DeleteRequests()); n != 0 {
		t.Errorf("BatchDeleteEntry called %d times, want 0", n)
	}
}

func TestSyncerDeletesEntriesForFinishedJobs(t *testing.T) {
	h := newHarness(t, "")
	h.setJobs(
		job("1001", "physics", "node01"),
		job("1002", "chemistry", "node02"),
	)
	h.cycle()

	if got := h.entryIDs(); len(got) != 2 {
		t.Fatalf("entry IDs = %v, want two entries after the first cycle", got)
	}

	// Job 1002 finishes.
	h.setJobs(job("1001", "physics", "node01"))
	h.cycle()

	got := h.entryIDs()
	if len(got) != 1 {
		t.Fatalf("entry IDs = %v, want one entry after job 1002 finished", got)
	}
	if h.entryByID(got[0]).SpiffeId.Path != "/slurm/physics/1001" {
		t.Errorf("surviving entry = %q, want the one for job 1001", got[0])
	}
}

// A job whose account changes keeps its identity — same job, same node — so the
// entry is updated in place rather than recreated, preserving the entry ID.
func TestSyncerUpdatesInPlaceOnAccountChange(t *testing.T) {
	h := newHarness(t, "")
	h.setJobs(job("1001", "physics", "node01"))
	h.cycle()

	before := h.entryIDs()
	if len(before) != 1 {
		t.Fatalf("entry IDs = %v, want one", before)
	}

	h.setJobs(job("1001", "chemistry", "node01"))
	h.cycle()

	after := h.entryIDs()
	if !equalStrings(before, after) {
		t.Fatalf("entry IDs = %v, want them preserved at %v across an update", after, before)
	}
	if got := h.entryByID(after[0]).SpiffeId.Path; got != "/slurm/chemistry/1001" {
		t.Errorf("SPIFFE ID path = %q, want the new account", got)
	}
	if n := len(h.server.UpdateRequests()); n != 1 {
		t.Errorf("BatchUpdateEntry called %d times, want 1", n)
	}
	if n := len(h.server.DeleteRequests()); n != 0 {
		t.Errorf("BatchDeleteEntry called %d times, want the entry updated rather than replaced", n)
	}
}

// Entries belonging to other tools must survive a reconcile even when no Slurm
// job corresponds to them.
func TestSyncerLeavesUnownedEntriesAlone(t *testing.T) {
	h := newHarness(t, "")
	h.server.Seed(
		&types.Entry{
			Id:        "otherapp.abc",
			Hint:      "slurm", // same hint, different owner
			SpiffeId:  &types.SPIFFEID{TrustDomain: "example.org", Path: "/other/workload"},
			ParentId:  &types.SPIFFEID{TrustDomain: "example.org", Path: "/spire/agent/x509pop/node01"},
			Selectors: []*types.Selector{{Type: "unix", Value: "uid:1000"}},
		},
		&types.Entry{
			Id:        "slurm.manual",
			Hint:      "hand-made", // our prefix, different hint
			SpiffeId:  &types.SPIFFEID{TrustDomain: "example.org", Path: "/manual/workload"},
			ParentId:  &types.SPIFFEID{TrustDomain: "example.org", Path: "/spire/agent/x509pop/node02"},
			Selectors: []*types.Selector{{Type: "unix", Value: "uid:1001"}},
		},
	)

	h.setJobs() // no jobs at all: everything owned would be deleted
	h.cycle()

	got := h.entryIDs()
	want := []string{"otherapp.abc", "slurm.manual"}
	if !equalStrings(got, want) {
		t.Fatalf("entry IDs = %v, want the unowned entries untouched at %v", got, want)
	}
	if n := len(h.server.DeleteRequests()); n != 0 {
		t.Errorf("BatchDeleteEntry called %d times, want 0", n)
	}
}

// Reconciling before Slurm has reported once would read an empty job list as
// "nothing is running" and delete every managed entry.
func TestSyncerWaitsForBothSnapshots(t *testing.T) {
	h := newHarness(t, "")
	h.server.Seed(&types.Entry{
		Id:        "slurm.existing",
		Hint:      "slurm",
		SpiffeId:  &types.SPIFFEID{TrustDomain: "example.org", Path: "/slurm/physics/1"},
		ParentId:  &types.SPIFFEID{TrustDomain: "example.org", Path: "/spire/agent/x509pop/node01"},
		Selectors: []*types.Selector{{Type: "slurm", Value: "job_id:1"}},
	})

	ctx := context.Background()

	// Entries listed, but squeue has never run.
	if err := h.syncer.gatherEntries(ctx); err != nil {
		t.Fatalf("gatherEntries: %v", err)
	}
	if err := h.syncer.reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if got := h.entryIDs(); len(got) != 1 {
		t.Fatalf("entry IDs = %v, want the existing entry left alone", got)
	}
	if n := len(h.server.DeleteRequests()); n != 0 {
		t.Fatalf("BatchDeleteEntry called %d times before squeue ever succeeded, want 0", n)
	}
}

// A failing squeue must not look like an empty cluster. The last good snapshot
// stays in place, so the entries survive.
func TestSyncerKeepsLastGoodSnapshotWhenSqueueFails(t *testing.T) {
	h := newHarness(t, "")
	h.setJobs(job("1001", "physics", "node01"))
	h.cycle()

	if got := h.entryIDs(); len(got) != 1 {
		t.Fatalf("entry IDs = %v, want one entry", got)
	}

	// squeue starts failing.
	writeFixture(t, h.fixture, "this is not json")
	ctx := context.Background()
	if err := h.syncer.gatherJobs(ctx); err == nil {
		t.Fatal("gatherJobs succeeded on malformed output, want an error")
	}
	if err := h.syncer.gatherEntries(ctx); err != nil {
		t.Fatalf("gatherEntries: %v", err)
	}
	if err := h.syncer.reconcile(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if got := h.entryIDs(); len(got) != 1 {
		t.Fatalf("entry IDs = %v, want the entry preserved while squeue is failing", got)
	}
}

func TestSyncerDryRunMakesNoChanges(t *testing.T) {
	h := newHarness(t, "dryRun: true\n")
	h.setJobs(job("1001", "physics", "node01"))
	h.cycle()

	if got := h.entryIDs(); len(got) != 0 {
		t.Fatalf("entry IDs = %v, want no entries created in dry run", got)
	}
	if n := len(h.server.CreateRequests()); n != 0 {
		t.Errorf("BatchCreateEntry called %d times in dry run, want 0", n)
	}
}

func TestSyncerHonoursCustomTemplatesAndClassName(t *testing.T) {
	h := newHarness(t, `
className: hpc
hint: hpc-jobs
parentIDTemplate: "spiffe://{{.TrustDomain}}/agents/{{.Node}}"
spiffeIDTemplate: "spiffe://{{.TrustDomain}}/{{.ClassName}}/{{.Account}}/job/{{.JobKey}}"
`)
	h.setJobs(job("77", "astro", "n1"))
	h.cycle()

	ids := h.entryIDs()
	if len(ids) != 1 {
		t.Fatalf("entry IDs = %v, want one", ids)
	}
	if ids[0] != "hpc.uuid-1" {
		t.Errorf("entry ID = %q, want the configured class name prefix", ids[0])
	}

	e := h.entryByID(ids[0])
	if e.ParentId.Path != "/agents/n1" {
		t.Errorf("parent ID path = %q, want /agents/n1", e.ParentId.Path)
	}
	if e.SpiffeId.Path != "/hpc/astro/job/77" {
		t.Errorf("SPIFFE ID path = %q, want /hpc/astro/job/77", e.SpiffeId.Path)
	}
	if e.Hint != "hpc-jobs" {
		t.Errorf("hint = %q, want hpc-jobs", e.Hint)
	}
}

func TestSyncerUsesSLUIDSelectorWhenPresent(t *testing.T) {
	h := newHarness(t, "")
	writeFixture(t, h.fixture, `{"jobs":[{"job_id":5,"sluid":"s5K1KKYAYG5D00","account":"modern",`+
		`"job_state":["RUNNING"],"job_resources":{"select_type":["CORE"],"nodes":{"list":"n1"}}}]}`)
	h.cycle()

	ids := h.entryIDs()
	if len(ids) != 1 {
		t.Fatalf("entry IDs = %v, want one", ids)
	}
	e := h.entryByID(ids[0])
	if e.Selectors[0].Value != "sluid:s5K1KKYAYG5D00" {
		t.Errorf("selector value = %q, want the SLUID form", e.Selectors[0].Value)
	}
	// The SPIFFE ID keys off the same identifier the selector uses.
	if e.SpiffeId.Path != "/slurm/modern/s5K1KKYAYG5D00" {
		t.Errorf("SPIFFE ID path = %q, want it keyed on the SLUID", e.SpiffeId.Path)
	}
}

// A template that cannot render for one job must not take down the whole
// reconcile.
func TestSyncerSkipsHostsWithUnrenderableTemplates(t *testing.T) {
	h := newHarness(t, "spiffeIDTemplate: \"spiffe://{{.TrustDomain}}/slurm/{{.Account}}\"\n")

	// An empty account renders to "spiffe://example.org/slurm/", which is a
	// valid SPIFFE ID, so use a job whose account contains a character that is
	// not allowed in a path instead.
	h.setJobs(
		`{"job_id":1,"account":"bad account","job_state":["RUNNING"],`+
			`"job_resources":{"select_type":["CORE"],"nodes":{"list":"n1"}}}`,
		job("2", "good", "n2"),
	)
	h.cycle()

	ids := h.entryIDs()
	if len(ids) != 1 {
		t.Fatalf("entry IDs = %v, want only the renderable job to produce an entry", ids)
	}
	if got := h.entryByID(ids[0]).SpiffeId.Path; got != "/slurm/good" {
		t.Errorf("SPIFFE ID path = %q, want /slurm/good", got)
	}
}

func TestSyncerMetricsTrackSnapshotSizes(t *testing.T) {
	h := newHarness(t, "")
	h.setJobs(job("1001", "physics", "node[01-03]"))
	h.cycle()
	// A second cycle so the entry list reflects what the first one created.
	h.cycle()

	if got := readGauge(t, h.syncer.metrics.SlurmJobHosts); got != 3 {
		t.Errorf("slurm_job_hosts = %v, want 3", got)
	}
	if got := readGauge(t, h.syncer.metrics.ManagedEntries); got != 3 {
		t.Errorf("managed_entries = %v, want 3", got)
	}
}

func readGauge(t *testing.T, g prometheus.Gauge) float64 {
	t.Helper()
	return testutil.ToFloat64(g)
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
