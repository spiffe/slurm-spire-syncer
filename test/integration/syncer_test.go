package integration

import (
	"testing"

	"github.com/spiffe/slurm-spire-syncer/internal/slurm"
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
	cfg := loadConfig(t)
	client := entryClient(t, cfg)

	const (
		accountA = "physics"
		accountB = "chemistry"
	)

	jobA := submitJob(t, accountA)
	jobB := submitJob(t, accountB)
	t.Cleanup(func() {
		// Leaving jobs running would hold the node's CPUs and leave entries
		// behind for anything that runs after this.
		for _, id := range []string{jobA, jobB} {
			_ = scancel(id)
		}
	})

	waitForJobRunning(t, jobA)
	waitForJobRunning(t, jobB)

	t.Run("the syncer creates one entry per running job", func(t *testing.T) {
		wantParent := expectedParentID(t, cfg)

		for _, tc := range []struct{ job, account string }{
			{jobA, accountA},
			{jobB, accountB},
		} {
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
		resultA := jobResult(t, jobA)
		resultB := jobResult(t, jobB)

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
