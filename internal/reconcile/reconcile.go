// Package reconcile computes the difference between the registration entries
// Slurm implies and the ones SPIRE currently holds.
package reconcile

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
)

// Desired is one registration entry implied by a running job on a node.
type Desired struct {
	ParentID    *types.SPIFFEID
	SpiffeID    *types.SPIFFEID
	Selectors   []*types.Selector
	X509SVIDTTL int32
	JWTSVIDTTL  int32

	// Node and JobKey are carried for diagnostics only; they take no part in
	// matching.
	Node   string
	JobKey string
}

// Plan is the set of changes needed to bring SPIRE in line with Slurm.
type Plan struct {
	Create   []*types.Entry
	Update   []*types.Entry
	Delete   []string
	Warnings []string
}

// Empty reports whether the plan would change nothing.
func (p Plan) Empty() bool {
	return len(p.Create) == 0 && len(p.Update) == 0 && len(p.Delete) == 0
}

// Options controls entry construction and ownership.
type Options struct {
	// ClassName prefixes every generated entry ID.
	ClassName string
	// Hint is stamped on every managed entry.
	Hint string
	// NewID generates the UUID half of a new entry ID. Injectable so tests can
	// assert on exact IDs.
	NewID func() string
	// Owns reports whether an existing entry belongs to this syncer. Entries it
	// rejects are never updated and never deleted.
	Owns func(*types.Entry) bool
}

// Diff computes the plan. It is pure: no clients, no context, no clock.
//
// Entries are matched to jobs on (parent ID, selector set) rather than on entry
// ID, because entry IDs are randomly generated and cannot be recomputed from
// Slurm data. That pairing is exactly the identity of a job on a node — the
// parent ID derives from the node and the selectors from the job — so anything
// else that drifts (a changed account, a retuned TTL, an edited spiffeIDTemplate)
// becomes an update in place rather than a delete and recreate that would churn
// the workload's SVID.
func Diff(desired []Desired, existing []*types.Entry, opts Options) Plan {
	var plan Plan

	byKey := make(map[string]*types.Entry, len(existing))
	for _, e := range existing {
		if opts.Owns != nil && !opts.Owns(e) {
			continue
		}
		byKey[entryKey(e)] = e
	}

	matched := make(map[string]bool, len(desired))
	seen := make(map[string]bool, len(desired))

	for _, d := range desired {
		key := desiredKey(d)
		if seen[key] {
			plan.Warnings = append(plan.Warnings, fmt.Sprintf(
				"duplicate job/node identity (parent %s, selectors %s) from job %s on %s; keeping the first",
				spiffeIDString(d.ParentID), selectorsString(d.Selectors), d.JobKey, d.Node))
			continue
		}
		seen[key] = true

		current, ok := byKey[key]
		if !ok {
			plan.Create = append(plan.Create, newEntry(d, opts))
			continue
		}
		matched[key] = true
		if drifted(d, current) {
			plan.Update = append(plan.Update, updatedEntry(current.Id, d, opts))
		}
	}

	for key, e := range byKey {
		if !matched[key] {
			plan.Delete = append(plan.Delete, e.Id)
		}
	}
	// Map iteration order is random; sorting keeps plans deterministic, which
	// matters for both logs and tests.
	sort.Strings(plan.Delete)

	return plan
}

// drifted reports whether the managed fields that are not part of the match key
// differ between what Slurm implies and what SPIRE holds.
//
// Hint is not compared: ownership already requires it to match.
func drifted(d Desired, current *types.Entry) bool {
	return !spiffeIDEqual(d.SpiffeID, current.SpiffeId) ||
		d.X509SVIDTTL != current.X509SvidTtl ||
		d.JWTSVIDTTL != current.JwtSvidTtl
}

func newEntry(d Desired, opts Options) *types.Entry {
	e := updatedEntry("", d, opts)
	e.Id = opts.ClassName + "." + opts.NewID()
	return e
}

func updatedEntry(id string, d Desired, opts Options) *types.Entry {
	return &types.Entry{
		Id:          id,
		SpiffeId:    d.SpiffeID,
		ParentId:    d.ParentID,
		Selectors:   d.Selectors,
		X509SvidTtl: d.X509SVIDTTL,
		JwtSvidTtl:  d.JWTSVIDTTL,
		Hint:        opts.Hint,
	}
}

func desiredKey(d Desired) string {
	return spiffeIDString(d.ParentID) + "\x00" + selectorsKey(d.Selectors)
}

func entryKey(e *types.Entry) string {
	return spiffeIDString(e.ParentId) + "\x00" + selectorsKey(e.Selectors)
}

func selectorsKey(selectors []*types.Selector) string {
	parts := make([]string, 0, len(selectors))
	for _, s := range selectors {
		parts = append(parts, s.GetType()+":"+s.GetValue())
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x00")
}

func selectorsString(selectors []*types.Selector) string {
	parts := make([]string, 0, len(selectors))
	for _, s := range selectors {
		parts = append(parts, s.GetType()+":"+s.GetValue())
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func spiffeIDString(id *types.SPIFFEID) string {
	if id == nil {
		return ""
	}
	return "spiffe://" + id.TrustDomain + id.Path
}

func spiffeIDEqual(a, b *types.SPIFFEID) bool {
	return spiffeIDString(a) == spiffeIDString(b)
}
