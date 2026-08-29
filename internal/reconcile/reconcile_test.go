package reconcile

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
)

const (
	testClass = "slurm"
	testHint  = "slurm"
	testTD    = "example.org"
)

// counterIDs hands out predictable UUID substitutes so tests can assert on exact
// entry IDs.
func counterIDs() func() string {
	n := 0
	return func() string {
		n++
		return fmt.Sprintf("uuid-%d", n)
	}
}

// testOptions builds Options with the standard ownership rule: an entry is ours
// only if it carries both the hint and the "<className>." ID prefix.
func testOptions() Options {
	return Options{
		ClassName: testClass,
		Hint:      testHint,
		NewID:     counterIDs(),
		Owns: func(e *types.Entry) bool {
			return e != nil && strings.HasPrefix(e.Id, testClass+".") && e.Hint == testHint
		},
	}
}

func id(path string) *types.SPIFFEID {
	return &types.SPIFFEID{TrustDomain: testTD, Path: path}
}

func jobSelectors(job string) []*types.Selector {
	return []*types.Selector{{Type: "slurm", Value: "job_id:" + job}}
}

// desiredFor builds the Desired record for a job on a node using the default
// template shapes.
func desiredFor(job, node, account string) Desired {
	return Desired{
		ParentID:  id("/node/" + node),
		SpiffeID:  id("/slurm/" + account + "/" + job),
		Selectors: jobSelectors(job),
		Node:      node,
		JobKey:    job,
	}
}

// entryFor builds the entry SPIRE would already be holding for a job on a node.
func entryFor(entryID, job, node, account string) *types.Entry {
	return &types.Entry{
		Id:        entryID,
		ParentId:  id("/node/" + node),
		SpiffeId:  id("/slurm/" + account + "/" + job),
		Selectors: jobSelectors(job),
		Hint:      testHint,
	}
}

func TestDiffNoChanges(t *testing.T) {
	desired := []Desired{desiredFor("1", "node1", "physics")}
	existing := []*types.Entry{entryFor("slurm.a", "1", "node1", "physics")}

	plan := Diff(desired, existing, testOptions())
	if !plan.Empty() {
		t.Fatalf("plan = %+v, want no changes", plan)
	}
}

func TestDiffCreates(t *testing.T) {
	desired := []Desired{
		desiredFor("1", "node1", "physics"),
		desiredFor("1", "node2", "physics"),
	}

	plan := Diff(desired, nil, testOptions())

	if len(plan.Create) != 2 || len(plan.Update) != 0 || len(plan.Delete) != 0 {
		t.Fatalf("plan = %+v, want two creates only", plan)
	}
	// IDs are "<className>.<uuid>" and every entry is stamped with the hint.
	for i, e := range plan.Create {
		wantID := fmt.Sprintf("%s.uuid-%d", testClass, i+1)
		if e.Id != wantID {
			t.Errorf("create[%d].Id = %q, want %q", i, e.Id, wantID)
		}
		if e.Hint != testHint {
			t.Errorf("create[%d].Hint = %q, want %q", i, e.Hint, testHint)
		}
		if len(e.Selectors) != 1 || e.Selectors[0].Type != "slurm" {
			t.Errorf("create[%d].Selectors = %+v, want a single slurm selector", i, e.Selectors)
		}
	}
}

func TestDiffDeletes(t *testing.T) {
	existing := []*types.Entry{
		entryFor("slurm.b", "2", "node2", "physics"),
		entryFor("slurm.a", "1", "node1", "physics"),
	}

	plan := Diff(nil, existing, testOptions())

	if len(plan.Create) != 0 || len(plan.Update) != 0 {
		t.Fatalf("plan = %+v, want deletes only", plan)
	}
	// Deletes come from a map, so they are sorted to keep plans deterministic.
	want := []string{"slurm.a", "slurm.b"}
	if len(plan.Delete) != len(want) {
		t.Fatalf("Delete = %v, want %v", plan.Delete, want)
	}
	for i := range want {
		if plan.Delete[i] != want[i] {
			t.Fatalf("Delete = %v, want %v (sorted)", plan.Delete, want)
		}
	}
}

// A job that moves accounts keeps its identity — same node, same job id — so the
// SPIFFE ID change must be an update in place rather than a delete and recreate,
// which would churn the workload's SVID.
func TestDiffUpdatesOnSpiffeIDDrift(t *testing.T) {
	desired := []Desired{desiredFor("1", "node1", "chemistry")}
	existing := []*types.Entry{entryFor("slurm.a", "1", "node1", "physics")}

	plan := Diff(desired, existing, testOptions())

	if len(plan.Create) != 0 || len(plan.Delete) != 0 || len(plan.Update) != 1 {
		t.Fatalf("plan = %+v, want a single update", plan)
	}
	got := plan.Update[0]
	if got.Id != "slurm.a" {
		t.Errorf("update reused ID %q, want the existing entry's ID slurm.a", got.Id)
	}
	if got.SpiffeId.Path != "/slurm/chemistry/1" {
		t.Errorf("update SPIFFE ID path = %q, want the new account", got.SpiffeId.Path)
	}
	if got.Hint != testHint {
		t.Errorf("update Hint = %q, want %q", got.Hint, testHint)
	}
}

func TestDiffUpdatesOnTTLDrift(t *testing.T) {
	d := desiredFor("1", "node1", "physics")
	d.X509SVIDTTL = 3600
	d.JWTSVIDTTL = 300

	existing := []*types.Entry{entryFor("slurm.a", "1", "node1", "physics")}

	plan := Diff([]Desired{d}, existing, testOptions())

	if len(plan.Update) != 1 {
		t.Fatalf("plan = %+v, want a single update", plan)
	}
	if plan.Update[0].X509SvidTtl != 3600 || plan.Update[0].JwtSvidTtl != 300 {
		t.Errorf("update TTLs = %d/%d, want 3600/300",
			plan.Update[0].X509SvidTtl, plan.Update[0].JwtSvidTtl)
	}
}

// Matching is on (parent ID, selector set); selector ordering is not part of the
// identity, so a reordered set must not read as a change.
func TestDiffSelectorOrderDoesNotMatter(t *testing.T) {
	d := desiredFor("1", "node1", "physics")
	d.Selectors = []*types.Selector{
		{Type: "slurm", Value: "job_id:1"},
		{Type: "slurm", Value: "extra:x"},
	}

	e := entryFor("slurm.a", "1", "node1", "physics")
	e.Selectors = []*types.Selector{
		{Type: "slurm", Value: "extra:x"},
		{Type: "slurm", Value: "job_id:1"},
	}

	plan := Diff([]Desired{d}, []*types.Entry{e}, testOptions())
	if !plan.Empty() {
		t.Fatalf("plan = %+v, want no changes for reordered selectors", plan)
	}
}

func TestDiffDuplicateDesiredKeysWarnAndCollapse(t *testing.T) {
	// The same job on the same node twice, which would otherwise produce two
	// entries competing for one identity.
	desired := []Desired{
		desiredFor("1", "node1", "physics"),
		desiredFor("1", "node1", "physics"),
	}

	plan := Diff(desired, nil, testOptions())

	if len(plan.Create) != 1 {
		t.Fatalf("Create = %+v, want the duplicate collapsed to one", plan.Create)
	}
	if len(plan.Warnings) != 1 {
		t.Fatalf("Warnings = %v, want one warning about the duplicate", plan.Warnings)
	}
	if !strings.Contains(plan.Warnings[0], "duplicate") {
		t.Errorf("warning = %q, want it to describe the duplicate", plan.Warnings[0])
	}
}

// Ownership is the conjunction of the hint and the ID prefix. An entry missing
// either marker belongs to someone else and must never be updated or deleted —
// this is what makes the delete pass safe.
func TestDiffLeavesUnownedEntriesAlone(t *testing.T) {
	cases := []struct {
		name  string
		entry *types.Entry
	}{
		{
			name: "right hint, foreign ID prefix",
			entry: &types.Entry{
				Id:        "otherapp.xyz",
				ParentId:  id("/node/node9"),
				SpiffeId:  id("/other/thing"),
				Selectors: jobSelectors("999"),
				Hint:      testHint,
			},
		},
		{
			name: "right ID prefix, foreign hint",
			entry: &types.Entry{
				Id:        "slurm.xyz",
				ParentId:  id("/node/node9"),
				SpiffeId:  id("/other/thing"),
				Selectors: jobSelectors("999"),
				Hint:      "something-else",
			},
		},
		{
			name: "neither marker",
			entry: &types.Entry{
				Id:        "otherapp.xyz",
				ParentId:  id("/node/node9"),
				SpiffeId:  id("/other/thing"),
				Selectors: jobSelectors("999"),
				Hint:      "",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// No desired entries at all: everything owned would be deleted.
			plan := Diff(nil, []*types.Entry{tc.entry}, testOptions())
			if len(plan.Delete) != 0 {
				t.Fatalf("Delete = %v, want the unowned entry left alone", plan.Delete)
			}
			if len(plan.Update) != 0 {
				t.Fatalf("Update = %+v, want the unowned entry left alone", plan.Update)
			}
		})
	}
}

// An unowned entry occupying the same identity must not shadow the desired one:
// the syncer still asks to create its own.
func TestDiffCreatesEvenWhenAnUnownedEntryShadowsTheIdentity(t *testing.T) {
	shadow := entryFor("otherapp.xyz", "1", "node1", "physics")

	plan := Diff([]Desired{desiredFor("1", "node1", "physics")}, []*types.Entry{shadow}, testOptions())

	if len(plan.Create) != 1 {
		t.Fatalf("Create = %+v, want one create despite the shadowing entry", plan.Create)
	}
	if len(plan.Delete) != 0 {
		t.Fatalf("Delete = %v, want the shadowing entry left alone", plan.Delete)
	}
}

func TestDiffMixedPlan(t *testing.T) {
	desired := []Desired{
		desiredFor("1", "node1", "physics"),   // unchanged
		desiredFor("2", "node2", "chemistry"), // account drift -> update
		desiredFor("3", "node3", "biology"),   // new -> create
	}
	existing := []*types.Entry{
		entryFor("slurm.one", "1", "node1", "physics"),
		entryFor("slurm.two", "2", "node2", "physics"),
		entryFor("slurm.gone", "9", "node9", "math"), // no longer running -> delete
	}

	plan := Diff(desired, existing, testOptions())

	if len(plan.Create) != 1 || plan.Create[0].SpiffeId.Path != "/slurm/biology/3" {
		t.Errorf("Create = %+v, want one create for job 3", plan.Create)
	}
	if len(plan.Update) != 1 || plan.Update[0].Id != "slurm.two" {
		t.Errorf("Update = %+v, want one update of slurm.two", plan.Update)
	}
	if len(plan.Delete) != 1 || plan.Delete[0] != "slurm.gone" {
		t.Errorf("Delete = %v, want [slurm.gone]", plan.Delete)
	}
}

// With no Owns predicate every entry is fair game; this documents that Diff
// itself does not assume the caller pre-filtered.
func TestDiffWithoutOwnsPredicate(t *testing.T) {
	opts := testOptions()
	opts.Owns = nil

	plan := Diff(nil, []*types.Entry{{Id: "anything", Hint: "x"}}, opts)
	if len(plan.Delete) != 1 {
		t.Fatalf("Delete = %v, want the entry considered in scope when Owns is nil", plan.Delete)
	}
}

func TestDiffScalesToManyHosts(t *testing.T) {
	const n = 500
	var desired []Desired
	var existing []*types.Entry
	for i := range n {
		job := fmt.Sprint(i)
		node := fmt.Sprintf("node%03d", i)
		desired = append(desired, desiredFor(job, node, "acct"))
		if i%2 == 0 {
			existing = append(existing, entryFor("slurm."+job, job, node, "acct"))
		}
	}

	plan := Diff(desired, existing, testOptions())

	if len(plan.Create) != n/2 {
		t.Errorf("Create = %d entries, want %d", len(plan.Create), n/2)
	}
	if len(plan.Update) != 0 || len(plan.Delete) != 0 {
		t.Errorf("Update/Delete = %d/%d, want none", len(plan.Update), len(plan.Delete))
	}
	if !sort.StringsAreSorted(plan.Delete) {
		t.Error("Delete is not sorted")
	}
}
