package integration

import (
	"sort"
	"testing"

	"github.com/spiffe/slurm-spire-syncer/internal/slurm"
	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
)

// TestJobsGetAccountScopedSVIDs is the whole point of this suite.
//
// Two jobs run concurrently on one node under different Slurm accounts. Each
// must end up holding an SVID whose SPIFFE ID names its own account and its own
// job — proving the chain from squeue, through the syncer, through the SPIRE
// slurm workload attestor, to an issued SVID. Then one is cancelled, to prove
// the syncer withdraws exactly the identity that stopped running.
//
// Deliberately one test rather than several: the two jobs have to be in flight
// at the same time for the cross-account assertion to mean anything, and Go
// gives no ordering guarantee between top-level tests.
func TestJobsGetAccountScopedSVIDs(t *testing.T) {
	requireIntegration(t)

	cfg := loadConfig(t)
	client := entryClient(t, cfg)

	const (
		accountA = "physics"
		accountB = "chemistry"
	)

	// On a multi-node leg the two jobs are pinned to different nodes, so the
	// account assertions below also cover two different agents attesting them.
	all := nodes(t)
	specA := jobSpec{account: accountA}
	specB := jobSpec{account: accountB}
	if len(all) > 1 {
		specA.nodeList = []string{all[0]}
		specB.nodeList = []string{all[1]}
	}

	jobA := submitJob(t, specA)
	jobB := submitJob(t, specB)
	t.Cleanup(func() {
		// Leaving jobs running would hold the node's CPUs and leave entries
		// behind for anything that runs after this.
		for _, id := range []string{jobA, jobB} {
			_ = scancel(id)
		}
	})

	// Checked before anything else, because getting it wrong is invisible until
	// the SVID fetch times out 90 seconds later with no indication why. The
	// entry would be created and accepted; it just would not reach any workload.
	assertParentMatchesAgent(t, cfg)

	waitForJobRunning(t, jobA)
	waitForJobRunning(t, jobB)

	t.Run("the syncer creates one entry per running job", func(t *testing.T) {
		for _, tc := range []struct{ job, account, node string }{
			{jobA, accountA, nodeOf(t, specA, all)},
			{jobB, accountB, nodeOf(t, specB, all)},
		} {
			wantParent := expectedParentID(t, cfg, tc.node)
			entry := waitForEntry(t, client, tc.job)

			if got, want := idString(entry.SpiffeId), expectedSPIFFEID(t, cfg, tc.account, tc.job); got != want {
				t.Errorf("job %s: SPIFFE ID = %q, want %q", tc.job, got, want)
			}
			// The parent has to be the agent that actually serves this node, or
			// the entry is never delivered to any workload.
			if got := idString(entry.ParentId); got != wantParent {
				t.Errorf("job %s: parent ID = %q, want %q", tc.job, got, wantParent)
			}
			if entry.Hint != cfg.Hint {
				t.Errorf("job %s: hint = %q, want %q", tc.job, entry.Hint, cfg.Hint)
			}
			if !hasPrefix(entry.Id, cfg.EntryIDPrefix()) {
				t.Errorf("job %s: entry ID = %q, want the %q prefix", tc.job, entry.Id, cfg.EntryIDPrefix())
			}
			// Exactly one selector, the job identifier. The attestor also emits
			// slurm:step, and adding it here would scope the entry to a single
			// step rather than the whole job.
			if len(entry.Selectors) != 1 {
				t.Fatalf("job %s: selectors = %+v, want exactly one", tc.job, entry.Selectors)
			}
			if got, want := entry.Selectors[0].Type, slurm.SelectorType; got != want {
				t.Errorf("job %s: selector type = %q, want %q", tc.job, got, want)
			}
			if got, want := entry.Selectors[0].Value, "job_id:"+tc.job; got != want {
				t.Errorf("job %s: selector value = %q, want %q", tc.job, got, want)
			}
		}
	})

	// The assertion that matters: each job holds the identity for its own
	// account, issued by a real agent after real attestation.
	t.Run("each job is issued the SVID for its own account", func(t *testing.T) {
		resultA := jobResult(t, jobA, nodeOf(t, specA, all))
		resultB := jobResult(t, jobB, nodeOf(t, specB, all))

		wantA := expectedSPIFFEID(t, cfg, accountA, jobA)
		wantB := expectedSPIFFEID(t, cfg, accountB, jobB)

		if resultA["SPIFFE_ID"] != wantA {
			t.Errorf("job %s (%s) fetched %q, want %q",
				jobA, accountA, resultA["SPIFFE_ID"], wantA)
		}
		if resultB["SPIFFE_ID"] != wantB {
			t.Errorf("job %s (%s) fetched %q, want %q",
				jobB, accountB, resultB["SPIFFE_ID"], wantB)
		}

		// Stated separately from the equality checks above: crossed identities
		// are the specific failure this suite exists to rule out, and a bug that
		// gave both jobs the same SVID should say so in those words.
		if resultA["SPIFFE_ID"] == resultB["SPIFFE_ID"] {
			t.Errorf("both jobs were issued the same SPIFFE ID %q; accounts are not being distinguished",
				resultA["SPIFFE_ID"])
		}

		// Slurm's own view of the account has to agree, or the test could pass
		// with the syncer reading an account the scheduler never assigned.
		if got := resultA["ACCOUNT"]; got != accountA {
			t.Errorf("job %s ran under account %q, want %q", jobA, got, accountA)
		}
		if got := resultB["ACCOUNT"]; got != accountB {
			t.Errorf("job %s ran under account %q, want %q", jobB, got, accountB)
		}
	})

	// The assertion a two-node cluster cannot make: with three nodes and a job
	// allocated two of them, the third must have no entry for that job. An
	// implementation that fanned every job out across the whole cluster would
	// pass on two nodes and fail here.
	t.Run("a job spanning some nodes gets an entry on exactly those nodes", func(t *testing.T) {
		if len(all) < 3 {
			t.Skipf("needs at least 3 nodes to be meaningful, have %d", len(all))
		}

		spanned := all[:2]
		unused := all[2]

		jobC := submitJob(t, jobSpec{account: accountA, nodeList: spanned})
		t.Cleanup(func() { _ = scancel(jobC) })
		waitForJobRunning(t, jobC)

		byParent := waitForEntryCount(t, client, jobC, len(spanned))

		for _, node := range spanned {
			want := expectedParentID(t, cfg, node)
			entry, ok := byParent[want]
			if !ok {
				t.Errorf("job %s has no entry parented to %s (%s); got %v",
					jobC, node, want, parentIDsOf(byParent))
				continue
			}
			// Same job, so the same SPIFFE ID on every node it landed on. The
			// entries differ only in which agent delivers them.
			if got, expect := idString(entry.SpiffeId), expectedSPIFFEID(t, cfg, accountA, jobC); got != expect {
				t.Errorf("job %s on %s: SPIFFE ID = %q, want %q", jobC, node, got, expect)
			}
		}

		if unwanted := expectedParentID(t, cfg, unused); byParent[unwanted] != nil {
			t.Errorf("job %s was allocated %v but also got an entry parented to unused node %s (%s)",
				jobC, spanned, unused, unwanted)
		}

		// Each task fetched its own SVID, through the agent on its own node.
		for _, node := range spanned {
			result := jobResult(t, jobC, node)
			if got, want := result["SPIFFE_ID"], expectedSPIFFEID(t, cfg, accountA, jobC); got != want {
				t.Errorf("job %s on %s fetched %q, want %q", jobC, node, got, want)
			}
			if got := result["NODE"]; got != node {
				t.Errorf("the result recorded for %s says it ran on %q", node, got)
			}
		}
	})

	t.Run("cancelling a job withdraws only its entry", func(t *testing.T) {
		if err := scancel(jobA); err != nil {
			t.Fatalf("scancel %s: %v", jobA, err)
		}
		t.Logf("cancelled job %s", jobA)

		waitForEntryGone(t, client, jobA)

		// The surviving job's entry must be untouched. A reconciler that deleted
		// too much would still pass the assertion above.
		if entryForJob(managedEntries(t, client), jobB) == nil {
			t.Fatalf("the entry for job %s disappeared too; deletion is not correctly scoped", jobB)
		}
	})
}

// nodeOf reports which node a job spec landed on, for asserting its parent ID.
func nodeOf(t *testing.T, spec jobSpec, all []string) string {
	t.Helper()
	if len(spec.nodeList) > 0 {
		return spec.nodeList[0]
	}
	return all[0]
}

func parentIDsOf(byParent map[string]*types.Entry) []string {
	out := make([]string, 0, len(byParent))
	for parent := range byParent {
		out = append(out, parent)
	}
	sort.Strings(out)
	return out
}
