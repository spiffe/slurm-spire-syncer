// Package syncer wires the Slurm gatherer, the SPIRE entry client and the
// reconciler together into three independent periodic loops.
package syncer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/google/uuid"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/slurm-spire-syncer/internal/config"
	"github.com/spiffe/slurm-spire-syncer/internal/metrics"
	"github.com/spiffe/slurm-spire-syncer/internal/reconcile"
	"github.com/spiffe/slurm-spire-syncer/internal/slurm"
	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
)

// Gatherer is the Slurm side of the syncer.
type Gatherer interface {
	Gather(ctx context.Context) ([]slurm.JobHost, error)
}

// EntryClient is the SPIRE side of the syncer.
type EntryClient interface {
	ListManaged(ctx context.Context) ([]*types.Entry, error)
	Create(ctx context.Context, entries []*types.Entry) (int, error)
	Update(ctx context.Context, entries []*types.Entry) (int, error)
	Delete(ctx context.Context, ids []string) (int, error)
	Owns(e *types.Entry) bool
}

// jobSnapshot and entrySnapshot are immutable. The loops publish new ones by
// swapping an atomic pointer, so the reconciler reads a consistent view without
// any locking and never blocks a gatherer.
type jobSnapshot struct {
	hosts      []slurm.JobHost
	gatheredAt time.Time
}

type entrySnapshot struct {
	entries    []*types.Entry
	gatheredAt time.Time
}

// Syncer holds the running state of the daemon.
type Syncer struct {
	cfg     *config.Config
	slurm   Gatherer
	spire   EntryClient
	metrics *metrics.Metrics
	log     *slog.Logger

	jobs    atomic.Pointer[jobSnapshot]
	entries atomic.Pointer[entrySnapshot]

	// newID generates the UUID half of an entry ID; overridable in tests.
	newID func() string
}

// New builds a Syncer.
func New(cfg *config.Config, gatherer Gatherer, spire EntryClient, m *metrics.Metrics, log *slog.Logger) *Syncer {
	return &Syncer{
		cfg:     cfg,
		slurm:   gatherer,
		spire:   spire,
		metrics: m,
		log:     log,
		newID:   uuid.NewString,
	}
}

// Run starts the three loops and blocks until ctx is cancelled.
func (s *Syncer) Run(ctx context.Context) {
	loops := []struct {
		name     string
		interval time.Duration
		fn       func(context.Context) error
	}{
		{metrics.LoopSqueue, s.cfg.SqueueInterval, s.gatherJobs},
		{metrics.LoopSpireList, s.cfg.SpireInterval, s.gatherEntries},
		{metrics.LoopReconcile, s.cfg.ReconcileInterval, s.reconcile},
	}

	var wg sync.WaitGroup
	for _, l := range loops {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runner := &metrics.Runner{
				Name:     l.name,
				Interval: l.interval,
				Metrics:  s.metrics.Loop(l.name),
				Log:      s.log,
			}
			runner.Run(ctx, l.fn)
		}()
	}
	wg.Wait()
}

func (s *Syncer) gatherJobs(ctx context.Context) error {
	hosts, err := s.slurm.Gather(ctx)
	if err != nil {
		return err
	}
	s.jobs.Store(&jobSnapshot{hosts: hosts, gatheredAt: time.Now()})
	s.metrics.SlurmJobHosts.Set(float64(len(hosts)))
	s.log.Debug("gathered slurm jobs", "hosts", len(hosts))
	return nil
}

func (s *Syncer) gatherEntries(ctx context.Context) error {
	return s.refreshEntries(ctx)
}

// refreshEntries re-lists the managed entries and publishes a new snapshot.
//
// Separate from the loop so the reconciler can call it after writing. The three
// loops run independently, so a reconcile that creates entries leaves its own
// view of them stale: the next reconcile, arriving before the list loop has run
// again, would see none of what it just created and try to create it all over
// again. SPIRE rejects the duplicates, so nothing is corrupted, but the syncer
// spends every cycle in between re-sending creates it knows will fail.
func (s *Syncer) refreshEntries(ctx context.Context) error {
	entries, err := s.spire.ListManaged(ctx)
	if err != nil {
		return err
	}
	s.entries.Store(&entrySnapshot{entries: entries, gatheredAt: time.Now()})
	s.metrics.ManagedEntries.Set(float64(len(entries)))
	s.log.Debug("listed managed spire entries", "entries", len(entries))
	return nil
}

func (s *Syncer) reconcile(ctx context.Context) error {
	jobs := s.jobs.Load()
	entries := s.entries.Load()
	// Reconciling before both sides have reported once would read an empty Slurm
	// list as "no jobs are running" and delete every managed entry.
	if jobs == nil || entries == nil {
		s.log.Debug("waiting for both snapshots before reconciling",
			"haveJobs", jobs != nil, "haveEntries", entries != nil)
		return nil
	}

	desired, warnings := s.buildDesired(jobs.hosts)
	for _, w := range warnings {
		s.log.Warn(w)
	}

	plan := reconcile.Diff(desired, entries.entries, reconcile.Options{
		ClassName: s.cfg.ClassName,
		Hint:      s.cfg.Hint,
		NewID:     s.newID,
		Owns:      s.spire.Owns,
	})
	for _, w := range plan.Warnings {
		s.log.Warn(w)
	}

	if plan.Empty() {
		s.log.Debug("nothing to reconcile", "desired", len(desired), "existing", len(entries.entries))
		return nil
	}

	if s.cfg.DryRun {
		s.log.Info("dry run: skipping all changes",
			"create", len(plan.Create), "update", len(plan.Update), "delete", len(plan.Delete))
		for _, e := range plan.Create {
			s.log.Info("dry run: would create", "entryID", e.Id, "spiffeID", idString(e.SpiffeId))
		}
		for _, e := range plan.Update {
			s.log.Info("dry run: would update", "entryID", e.Id, "spiffeID", idString(e.SpiffeId))
		}
		for _, id := range plan.Delete {
			s.log.Info("dry run: would delete", "entryID", id)
		}
		return nil
	}

	return s.apply(ctx, plan)
}

// apply runs all three operations even if an earlier one fails, so that a
// persistent failure in one (say, a create that keeps colliding) does not stall
// the others indefinitely.
func (s *Syncer) apply(ctx context.Context, plan reconcile.Plan) error {
	var errs []string

	if len(plan.Create) > 0 {
		n, err := s.spire.Create(ctx, plan.Create)
		s.metrics.EntriesCreated.Add(float64(n))
		if n > 0 {
			s.log.Info("created registration entries", "count", n)
		}
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(plan.Update) > 0 {
		n, err := s.spire.Update(ctx, plan.Update)
		s.metrics.EntriesUpdated.Add(float64(n))
		if n > 0 {
			s.log.Info("updated registration entries", "count", n)
		}
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(plan.Delete) > 0 {
		n, err := s.spire.Delete(ctx, plan.Delete)
		s.metrics.EntriesDeleted.Add(float64(n))
		if n > 0 {
			s.log.Info("deleted registration entries", "count", n)
		}
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	// The snapshot this reconcile worked from no longer describes the server,
	// because this function just changed it. Refreshing here rather than waiting
	// for the list loop stops the next reconcile from redoing the same work
	// against a stale view.
	if err := s.refreshEntries(ctx); err != nil {
		s.log.Debug("could not refresh the entry list after applying changes; "+
			"the list loop will catch up", "error", err)
	}

	if len(errs) > 0 {
		return fmt.Errorf("reconcile: %s", strings.Join(errs, "; "))
	}
	return nil
}

// buildDesired renders the templates for each job host. A host whose templates
// fail to render is skipped with a warning rather than failing the whole
// reconcile, which would leave every other job's entries unmanaged.
func (s *Syncer) buildDesired(hosts []slurm.JobHost) ([]reconcile.Desired, []string) {
	var (
		desired  []reconcile.Desired
		warnings []string
	)
	for _, h := range hosts {
		data := h.TemplateData(s.cfg.TrustDomain, s.cfg.ClassName)

		parentID, err := renderID(s.cfg.ParentIDTemplate, data)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf(
				"skipping job %s on %s: parentIDTemplate: %v", h.Key(), h.Node, err))
			continue
		}
		spiffeID, err := renderID(s.cfg.SpiffeIDTemplate, data)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf(
				"skipping job %s on %s: spiffeIDTemplate: %v", h.Key(), h.Node, err))
			continue
		}

		desired = append(desired, reconcile.Desired{
			ParentID: parentID,
			SpiffeID: spiffeID,
			Selectors: []*types.Selector{
				{Type: slurm.SelectorType, Value: h.SelectorValue()},
			},
			X509SVIDTTL: int32(s.cfg.X509SVIDTTL.Seconds()),
			JWTSVIDTTL:  int32(s.cfg.JWTSVIDTTL.Seconds()),
			Node:        h.Node,
			JobKey:      h.Key(),
		})
	}
	return desired, warnings
}

// RenderID renders a template and parses the result as a SPIFFE ID.
func RenderID(t *template.Template, data slurm.TemplateData) (*types.SPIFFEID, error) {
	return renderID(t, data)
}

func renderID(t *template.Template, data slurm.TemplateData) (*types.SPIFFEID, error) {
	var buf strings.Builder
	if err := t.Execute(&buf, data); err != nil {
		return nil, err
	}
	rendered := strings.TrimSpace(buf.String())
	id, err := spiffeid.FromString(rendered)
	if err != nil {
		return nil, fmt.Errorf("%q is not a valid SPIFFE ID: %w", rendered, err)
	}
	return &types.SPIFFEID{
		TrustDomain: id.TrustDomain().Name(),
		Path:        id.Path(),
	}, nil
}

func idString(id *types.SPIFFEID) string {
	if id == nil {
		return ""
	}
	return "spiffe://" + id.TrustDomain + id.Path
}
