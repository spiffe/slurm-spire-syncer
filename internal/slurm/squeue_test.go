package slurm

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spiffe/slurm-spire-syncer/internal/config"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testGatherer(t *testing.T, jobIdentifier string, command ...string) *Gatherer {
	t.Helper()
	if len(command) == 0 {
		command = []string{"true"}
	}
	return &Gatherer{
		command:       command,
		timeout:       10 * time.Second,
		jobIdentifier: jobIdentifier,
		log:           discardLogger(),
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return b
}

func TestParseFiltersAndFlattens(t *testing.T) {
	g := testGatherer(t, config.JobIdentifierAuto)
	got, err := g.parse(readFixture(t, "mixed.json"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// 1001 RUNNING+CORE over two nodes, 1004 RUNNING+CORE over three allocated
	// nodes. 1002 is PENDING, 1003 is not CORE, 1005 has no job_resources and
	// 1006 is COMPLETED.
	want := []JobHost{
		{JobID: "1001", Account: "physics", Node: "node01"},
		{JobID: "1001", Account: "physics", Node: "node02"},
		{JobID: "1004", Account: "math", Node: "gpu1"},
		{JobID: "1004", Account: "math", Node: "gpu2"},
		{JobID: "1004", Account: "math", Node: "gpu3"},
	}
	assertJobHosts(t, got, want)
}

// The allocation array is already expanded, so it is preferred over re-parsing
// the hostlist string. Job 1004's fixture carries both.
func TestParsePrefersAllocationOverHostlist(t *testing.T) {
	raw := []byte(`{"jobs":[{
		"job_id": 1, "account": "a", "job_state": ["RUNNING"],
		"job_resources": {
			"select_type": ["CORE"],
			"nodes": {"list": "wrong[1-9]", "allocation": [{"name":"right1"},{"name":"right2"}]}
		}
	}]}`)

	g := testGatherer(t, config.JobIdentifierAuto)
	got, err := g.parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	assertJobHosts(t, got, []JobHost{
		{JobID: "1", Account: "a", Node: "right1"},
		{JobID: "1", Account: "a", Node: "right2"},
	})
}

// Older Slurm reports job_resources.nodes as a bare hostlist string rather than
// an object.
func TestParseNodesAsString(t *testing.T) {
	g := testGatherer(t, config.JobIdentifierAuto)
	got, err := g.parse(readFixture(t, "legacy_nodes_string.json"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	assertJobHosts(t, got, []JobHost{
		{JobID: "2001", Account: "legacy", Node: "old1"},
		{JobID: "2001", Account: "legacy", Node: "old2"},
	})
}

// Some Slurm versions report select_type on the nested nodes object instead of
// on job_resources; the fallback must find it there without admitting jobs whose
// nested select_type is something else.
func TestParseSelectTypeOnNodes(t *testing.T) {
	g := testGatherer(t, config.JobIdentifierAuto)
	got, err := g.parse(readFixture(t, "select_type_on_nodes.json"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	assertJobHosts(t, got, []JobHost{
		{JobID: "4001", Account: "nested", Node: "n1"},
		{JobID: "4001", Account: "nested", Node: "n2"},
	})
}

// Slurm 23.11, which Ubuntu 24.04 ships, has no select_type at all: nodes is a
// bare hostlist string and the already-expanded node list is allocated_nodes,
// keyed nodename, as a sibling of nodes rather than nested inside it.
func TestParseSlurm2311(t *testing.T) {
	g := testGatherer(t, config.JobIdentifierAuto)
	got, err := g.parse(readFixture(t, "slurm_23_11.json"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// 1001 is core-allocated over two nodes. 1002 is CPU-allocated
	// (allocated_cores 0) and must be dropped. 1003 is core-allocated but has an
	// empty allocated_nodes, so it falls back to expanding the hostlist. 1004 is
	// PENDING with the empty job_resources object 23.11 emits in place of null.
	assertJobHosts(t, got, []JobHost{
		{JobID: "1001", Account: "physics", Node: "node01"},
		{JobID: "1001", Account: "physics", Node: "node02"},
		{JobID: "1003", Account: "biology", Node: "node04"},
		{JobID: "1003", Account: "biology", Node: "node05"},
	})
}

// The 23.11 equivalent of TestParsePrefersAllocationOverHostlist. The hostlist
// and allocated_nodes deliberately disagree, so this fails if nodename is not
// read: agreeing values would fall through to hostlist expansion and produce the
// right answer for the wrong reason.
func TestParseSlurm2311PrefersAllocatedNodes(t *testing.T) {
	raw := []byte(`{"jobs":[{
		"job_id": 1, "account": "a", "job_state": ["RUNNING"],
		"job_resources": {
			"nodes": "wrong[1-9]",
			"allocated_cores": 2,
			"allocated_hosts": 2,
			"allocated_nodes": [{"nodename":"right1"},{"nodename":"right2"}]
		}
	}]}`)

	g := testGatherer(t, config.JobIdentifierAuto)
	got, err := g.parse(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	assertJobHosts(t, got, []JobHost{
		{JobID: "1", Account: "a", Node: "right1"},
		{JobID: "1", Account: "a", Node: "right2"},
	})
}

// Slurm 26.x renamed the select_type values to CR_CORE, CR_CORE_MEMORY and so
// on, and marked the bare "CORE" spelling hidden so it is skipped on dump.
// Matching only "CORE" dropped every job on that version.
func TestParseSlurm26CRCore(t *testing.T) {
	g := testGatherer(t, config.JobIdentifierAuto)
	got, err := g.parse(readFixture(t, "slurm_26_cr_core.json"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	// 2001 is CR_CORE and kept; 2002 is CR_CPU and dropped.
	assertJobHosts(t, got, []JobHost{
		{JobID: "2001", Account: "physics", Node: "gpu1"},
		{JobID: "2001", Account: "physics", Node: "gpu2"},
	})
}

func TestCoreAllocated(t *testing.T) {
	cases := []struct {
		name string
		res  resources
		want bool
	}{
		// 24.05 through 25.05, the spelling Ubuntu 26.04's Slurm 25.11 emits.
		{"bare CORE", resources{SelectType: []string{"CORE"}}, true},
		{"bare CORE among others", resources{SelectType: []string{"CORE", "MEMORY"}}, true},
		{"bare CPU", resources{SelectType: []string{"CPU"}}, false},
		{"bare SOCKET", resources{SelectType: []string{"SOCKET"}}, false},
		{"LINEAR", resources{SelectType: []string{"LINEAR"}}, false},

		// 26.x.
		{"CR_CORE", resources{SelectType: []string{"CR_CORE"}}, true},
		{"CR_CORE_MEMORY", resources{SelectType: []string{"CR_CORE_MEMORY"}}, true},
		{"CR_CPU", resources{SelectType: []string{"CR_CPU"}}, false},
		{"CR_SOCKET", resources{SelectType: []string{"CR_SOCKET"}}, false},

		// select_type reported on the nested nodes object instead.
		{"nested CORE", resources{Nodes: nodes{SelectType: []string{"CORE"}}}, true},
		{"nested CPU", resources{Nodes: nodes{SelectType: []string{"CPU"}}}, false},

		// 23.11: no select_type, so allocated_cores is the proxy.
		{"23.11 cores allocated", resources{AllocatedCores: 4}, true},
		{"23.11 no cores allocated", resources{AllocatedCores: 0}, false},

		// select_type present but unrecognised must not silently fall through to
		// the 23.11 proxy, or a CPU-allocated job on a new Slurm would be kept.
		{"unknown select type wins over the proxy",
			resources{SelectType: []string{"SOMETHING_NEW"}, AllocatedCores: 4}, false},

		{"nothing at all", resources{}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := coreAllocated(&tc.res); got != tc.want {
				t.Fatalf("coreAllocated() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseJobIdentifier(t *testing.T) {
	cases := []struct {
		name string
		mode string
		want []JobHost
	}{
		{
			// auto prefers the SLUID when present and falls back per job.
			name: "auto",
			mode: config.JobIdentifierAuto,
			want: []JobHost{
				{SLUID: "s5K1KKYAYG5D00", Account: "modern", Node: "node1"},
				{SLUID: "s5K1KKYAYG5D00", Account: "modern", Node: "node2"},
				{JobID: "3002", Account: "no-sluid", Node: "node3"},
			},
		},
		{
			// Forcing job_id ignores the SLUID even where one exists.
			name: "forced job_id",
			mode: config.JobIdentifierJobID,
			want: []JobHost{
				{JobID: "3001", Account: "modern", Node: "node1"},
				{JobID: "3001", Account: "modern", Node: "node2"},
				{JobID: "3002", Account: "no-sluid", Node: "node3"},
			},
		},
		{
			// Forcing sluid drops jobs that do not have one.
			name: "forced sluid",
			mode: config.JobIdentifierSLUID,
			want: []JobHost{
				{SLUID: "s5K1KKYAYG5D00", Account: "modern", Node: "node1"},
				{SLUID: "s5K1KKYAYG5D00", Account: "modern", Node: "node2"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := testGatherer(t, tc.mode)
			got, err := g.parse(readFixture(t, "sluid.json"))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			assertJobHosts(t, got, tc.want)
		})
	}
}

// A single job with an unparseable hostlist must not discard the rest of the
// gather: returning an error here would look like a total squeue failure and let
// the reconciler delete every other job's entries.
func TestParseSkipsUnparseableHostlist(t *testing.T) {
	raw := []byte(`{"jobs":[
		{"job_id": 1, "account": "bad", "job_state": ["RUNNING"],
		 "job_resources": {"select_type": ["CORE"], "nodes": {"list": "node[1-2"}}},
		{"job_id": 2, "account": "good", "job_state": ["RUNNING"],
		 "job_resources": {"select_type": ["CORE"], "nodes": {"list": "node9"}}}
	]}`)

	g := testGatherer(t, config.JobIdentifierAuto)
	got, err := g.parse(raw)
	if err != nil {
		t.Fatalf("parse returned an error, want the bad job skipped instead: %v", err)
	}
	assertJobHosts(t, got, []JobHost{{JobID: "2", Account: "good", Node: "node9"}})
}

func TestParseEmptyAndMalformed(t *testing.T) {
	g := testGatherer(t, config.JobIdentifierAuto)

	got, err := g.parse([]byte(`{"jobs":[]}`))
	if err != nil {
		t.Fatalf("parse of empty job list: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %v, want no job hosts", got)
	}

	if _, err := g.parse([]byte(`not json`)); err == nil {
		t.Fatal("expected an error for unparseable JSON")
	}
}

func TestSelectorValueAndKey(t *testing.T) {
	withSLUID := JobHost{JobID: "1", SLUID: "sABC", Node: "n1"}
	if got, want := withSLUID.Key(), "sABC"; got != want {
		t.Errorf("Key() = %q, want %q", got, want)
	}
	if got, want := withSLUID.SelectorValue(), "sluid:sABC"; got != want {
		t.Errorf("SelectorValue() = %q, want %q", got, want)
	}

	withJobID := JobHost{JobID: "1", Node: "n1"}
	if got, want := withJobID.Key(), "1"; got != want {
		t.Errorf("Key() = %q, want %q", got, want)
	}
	if got, want := withJobID.SelectorValue(), "job_id:1"; got != want {
		t.Errorf("SelectorValue() = %q, want %q", got, want)
	}
}

func TestGatherRunsCommand(t *testing.T) {
	g := testGatherer(t, config.JobIdentifierAuto, "sh", "-c",
		`printf '%s' '{"jobs":[{"job_id":7,"account":"acct","job_state":["RUNNING"],`+
			`"job_resources":{"select_type":["CORE"],"nodes":{"list":"n1"}}}]}'`)

	got, err := g.Gather(context.Background())
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	assertJobHosts(t, got, []JobHost{{JobID: "7", Account: "acct", Node: "n1"}})
}

func TestGatherReportsStderrOnFailure(t *testing.T) {
	g := testGatherer(t, config.JobIdentifierAuto, "sh", "-c", "echo 'slurm_load_jobs error' >&2; exit 1")

	_, err := g.Gather(context.Background())
	if err == nil {
		t.Fatal("expected an error from a failing command")
	}
	if !strings.Contains(err.Error(), "slurm_load_jobs error") {
		t.Fatalf("error = %q, want it to include the command's stderr", err)
	}
}

// An exec timeout surfaces as "signal: killed", which is indistinguishable from
// an external kill unless the deadline is named explicitly.
func TestGatherTimeoutIsReported(t *testing.T) {
	g := testGatherer(t, config.JobIdentifierAuto, "sleep", "30")
	g.timeout = 50 * time.Millisecond

	start := time.Now()
	_, err := g.Gather(context.Background())
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error = %q, want it to mention the timeout", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("Gather took %s, want it to stop at the timeout", elapsed)
	}
}

func TestGatherCancellation(t *testing.T) {
	g := testGatherer(t, config.JobIdentifierAuto, "sleep", "30")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	if _, err := g.Gather(ctx); err == nil {
		t.Fatal("expected an error when the context is cancelled")
	}
}

func TestGatherRejectsUnparseableOutput(t *testing.T) {
	g := testGatherer(t, config.JobIdentifierAuto, "sh", "-c", "printf 'not json'")

	if _, err := g.Gather(context.Background()); err == nil {
		t.Fatal("expected an error for unparseable output")
	}
}

func assertJobHosts(t *testing.T, got, want []JobHost) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d job hosts %+v, want %d %+v", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("job host %d = %+v, want %+v (full result %+v)", i, got[i], want[i], got)
		}
	}
}
