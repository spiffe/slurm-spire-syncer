// Package slurm gathers the set of running Slurm jobs and flattens them into one
// record per (job, node) pair.
package slurm

// SelectorType is the selector type emitted by the SPIRE "slurm" workload
// attestor. Registration entries must use the same type for their selectors to
// match an attested workload.
const SelectorType = "slurm"

// JobHost is one running job on one node. Exactly one of JobID and SLUID is set;
// which one depends on the Slurm version and configuration, and determines which
// selector the SPIRE slurm attestor will present for the workload.
type JobHost struct {
	JobID   string
	SLUID   string
	Account string
	Node    string
}

// Key returns the job identifier in use, preferring the SLUID.
func (j JobHost) Key() string {
	if j.SLUID != "" {
		return j.SLUID
	}
	return j.JobID
}

// SelectorValue returns the value half of the single selector placed on this
// job's registration entry, e.g. "sluid:s5K1KKYAYG5D00" or "job_id:12345".
//
// The attestor also emits a slurm:step selector, but entries deliberately carry
// only the job identifier: SPIRE requires an entry's selectors to be a subset of
// the workload's, so one job-scoped entry matches every step of that job.
func (j JobHost) SelectorValue() string {
	if j.SLUID != "" {
		return "sluid:" + j.SLUID
	}
	return "job_id:" + j.JobID
}

// TemplateData is the context passed to the parentIDTemplate and
// spiffeIDTemplate. Field names are part of the configuration surface.
type TemplateData struct {
	TrustDomain string
	ClassName   string
	JobID       string
	SLUID       string
	JobKey      string
	Account     string
	Node        string
}

// TemplateData builds the rendering context for this job host.
func (j JobHost) TemplateData(trustDomain, className string) TemplateData {
	return TemplateData{
		TrustDomain: trustDomain,
		ClassName:   className,
		JobID:       j.JobID,
		SLUID:       j.SLUID,
		JobKey:      j.Key(),
		Account:     j.Account,
		Node:        j.Node,
	}
}
