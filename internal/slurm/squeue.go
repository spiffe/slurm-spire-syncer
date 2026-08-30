package slurm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"time"

	"github.com/spiffe/slurm-spire-syncer/internal/config"
)

// The two filters applied to squeue output, mirroring the reference jq:
//
//	.jobs[] | select(IN(.job_state[]; "RUNNING"))
//	        | .job_resources | select(IN(.select_type[]; "CORE"))
//
// The select_type half needs more care than the jq suggests, because Slurm has
// spelled it three different ways. See coreAllocated.
const (
	stateRunning = "RUNNING"

	// selectTypeCore is the value Slurm 24.05 through 25.05 emit for a
	// core-based allocation.
	selectTypeCore = "CORE"

	// selectTypeCorePrefix covers Slurm 26.x, which renamed the values to
	// CR_CORE, CR_CORE_MEMORY and so on, and hid the bare spellings from the
	// dump. Matching only "CORE" there drops every job.
	selectTypeCorePrefix = "CR_CORE"
)

// document is the outer squeue --json shape. Only the fields the syncer needs
// are decoded; Slurm adds and reshapes fields between releases, so decoding
// leniently keeps the syncer working across versions.
type document struct {
	Jobs []job `json:"jobs"`
}

type job struct {
	JobID     json.Number `json:"job_id"`
	SLUID     string      `json:"sluid"`
	Account   string      `json:"account"`
	JobState  []string    `json:"job_state"`
	Resources *resources  `json:"job_resources"`
}

// resources spans every shape job_resources has taken. Slurm 23.11 and 24.05+
// disagree about almost all of it, so both sets of fields are decoded and the
// readers below pick whichever the running version actually populated.
type resources struct {
	// SelectType is present from 24.05 onwards; 23.11 has no equivalent.
	SelectType []string `json:"select_type"`

	// AllocatedCores is 23.11 only. Slurm emits the count only when the
	// selection type is core- or socket-based and zero otherwise, which makes
	// it the one usable proxy for select_type on that version.
	AllocatedCores int `json:"allocated_cores"`

	// AllocatedNodes is 23.11 only, and is a sibling of Nodes rather than
	// nested inside it. 24.05+ moved the equivalent to nodes.allocation.
	AllocatedNodes []allocation `json:"allocated_nodes"`

	Nodes nodes `json:"nodes"`
}

// nodes accommodates both shapes Slurm has used for job_resources.nodes: a bare
// hostlist string in older releases, and an object carrying the hostlist plus an
// already-expanded per-node allocation in newer ones.
type nodes struct {
	List       string       `json:"list"`
	SelectType []string     `json:"select_type"`
	Allocation []allocation `json:"allocation"`
}

// allocation is one already-expanded node in an allocation. The key holding the
// node name was renamed between 23.11 and 24.05, so both are decoded.
type allocation struct {
	Name     string `json:"name"`     // 24.05+
	NodeName string `json:"nodename"` // 23.11
}

// name returns whichever spelling this Slurm version used.
func (a allocation) name() string {
	if a.Name != "" {
		return a.Name
	}
	return a.NodeName
}

func (n *nodes) UnmarshalJSON(b []byte) error {
	b = bytes.TrimSpace(b)
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		return nil
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return err
		}
		n.List = s
		return nil
	}
	// Alias to avoid recursing back into this method.
	type plain nodes
	var p plain
	if err := json.Unmarshal(b, &p); err != nil {
		return err
	}
	*n = nodes(p)
	return nil
}

// Gatherer runs squeue and turns its output into JobHost records.
type Gatherer struct {
	command       []string
	timeout       time.Duration
	jobIdentifier string
	log           *slog.Logger
}

// NewGatherer builds a Gatherer from the resolved configuration.
func NewGatherer(cfg *config.Config, log *slog.Logger) *Gatherer {
	return &Gatherer{
		command:       cfg.SqueueCommand,
		timeout:       cfg.SqueueTimeout,
		jobIdentifier: cfg.JobIdentifier,
		log:           log,
	}
}

// Gather executes squeue and returns one JobHost per (running job, node).
func (g *Gatherer) Gather(ctx context.Context) ([]JobHost, error) {
	out, err := g.run(ctx)
	if err != nil {
		return nil, err
	}
	return g.parse(out)
}

func (g *Gatherer) run(ctx context.Context) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, g.command[0], g.command[1:]...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		// Report the deadline explicitly: a plain "signal: killed" from an
		// exec timeout is otherwise indistinguishable from an external kill.
		if ctx.Err() != nil {
			return nil, fmt.Errorf("squeue: %s timed out after %s: %w",
				strings.Join(g.command, " "), g.timeout, ctx.Err())
		}
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, fmt.Errorf("squeue: %s failed: %w: %s", strings.Join(g.command, " "), err, msg)
		}
		return nil, fmt.Errorf("squeue: %s failed: %w", strings.Join(g.command, " "), err)
	}
	return stdout.Bytes(), nil
}

func (g *Gatherer) parse(raw []byte) ([]JobHost, error) {
	var doc document
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("squeue: parsing output: %w", err)
	}

	var hosts []JobHost
	for _, j := range doc.Jobs {
		if !contains(j.JobState, stateRunning) {
			continue
		}
		if j.Resources == nil {
			continue
		}
		if !coreAllocated(j.Resources) {
			continue
		}

		jobID, sluid, ok := g.identify(j)
		if !ok {
			continue
		}

		nodeNames, err := g.nodeNames(j.Resources)
		if err != nil {
			// One malformed hostlist should not discard the whole gather, which
			// would look like a total squeue failure and trigger deletion of
			// every other job's entries on the next reconcile.
			g.log.Warn("skipping job with unparseable node list",
				"job", jobID+sluid, "error", err)
			continue
		}

		for _, node := range nodeNames {
			hosts = append(hosts, JobHost{
				JobID:   jobID,
				SLUID:   sluid,
				Account: j.Account,
				Node:    node,
			})
		}
	}
	return hosts, nil
}

// identify picks the job identifier to key this job by, returning ok=false when
// the configured identifier is unavailable for this job.
func (g *Gatherer) identify(j job) (jobID, sluid string, ok bool) {
	id := strings.TrimSpace(j.JobID.String())
	s := strings.TrimSpace(j.SLUID)

	switch g.jobIdentifier {
	case config.JobIdentifierJobID:
		if id == "" {
			g.log.Warn("skipping job with no job_id", "jobIdentifier", g.jobIdentifier)
			return "", "", false
		}
		return id, "", true
	case config.JobIdentifierSLUID:
		if s == "" {
			g.log.Warn("skipping job with no sluid", "job_id", id, "jobIdentifier", g.jobIdentifier)
			return "", "", false
		}
		return "", s, true
	default: // auto
		if s != "" {
			return "", s, true
		}
		if id == "" {
			g.log.Warn("skipping job with neither job_id nor sluid")
			return "", "", false
		}
		return id, "", true
	}
}

// coreAllocated reports whether a job holds a core-based allocation.
//
// The value to test moved twice, so all three spellings are handled:
//
//   - 24.05 to 25.05 emit select_type containing the bare "CORE".
//   - 26.x renamed the values to CR_CORE, CR_CORE_MEMORY and so on, and marked
//     the bare spellings hidden so they are skipped on dump. Matching only
//     "CORE" there silently drops every job.
//   - 23.11 has no select_type at all. Its allocated_cores is emitted as the
//     real count only when the selection type is core- or socket-based, and as
//     zero otherwise, which makes a non-zero value the one available proxy.
//
// The 23.11 proxy is deliberately wider than the others: it cannot separate
// CR_Core from CR_Socket, so a socket-allocated job is accepted there where a
// newer Slurm would exclude it. The two are indistinguishable in that version's
// output.
func coreAllocated(r *resources) bool {
	// Some versions report select_type on the nested nodes object rather than on
	// job_resources. Reading both can only admit a job that would otherwise be
	// dropped silently; it never admits one with the wrong selection type.
	selectTypes := r.SelectType
	if len(selectTypes) == 0 {
		selectTypes = r.Nodes.SelectType
	}
	if len(selectTypes) > 0 {
		for _, t := range selectTypes {
			if t == selectTypeCore || strings.HasPrefix(t, selectTypeCorePrefix) {
				return true
			}
		}
		return false
	}

	return r.AllocatedCores > 0
}

// nodeNames prefers a list Slurm has already expanded, and falls back to parsing
// the hostlist string only when the running version does not provide one.
//
// The expanded list lives in a different place per version: nodes.allocation
// from 24.05, allocated_nodes on 23.11.
func (g *Gatherer) nodeNames(r *resources) ([]string, error) {
	for _, alloc := range [][]allocation{r.Nodes.Allocation, r.AllocatedNodes} {
		names := make([]string, 0, len(alloc))
		for _, a := range alloc {
			if name := strings.TrimSpace(a.name()); name != "" {
				names = append(names, name)
			}
		}
		if len(names) > 0 {
			return names, nil
		}
	}
	return ExpandHostList(r.Nodes.List)
}

func contains(haystack []string, needle string) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}
