// Package integration exercises the syncer against a real Slurm cluster and a
// real SPIRE server.
//
// Everything here needs a machine that has been set up by
// test/integration/scripts, so the whole package is skipped unless
// SLURM_SPIRE_SYNCER_INTEGRATION is set. See .github/workflows/integration.yaml.
package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/spiffe/slurm-spire-syncer/internal/config"
	"github.com/spiffe/slurm-spire-syncer/internal/slurm"
	"github.com/spiffe/slurm-spire-syncer/internal/spireentry"
	"github.com/spiffe/slurm-spire-syncer/internal/syncer"
	entryv1 "github.com/spiffe/spire-api-sdk/proto/spire/api/server/entry/v1"
	"github.com/spiffe/spire-api-sdk/proto/spire/api/types"
)

const (
	enableEnv = "SLURM_SPIRE_SYNCER_INTEGRATION"

	// jobDir must match the --output path in testdata/fetch-svid.sh.
	jobDir = "/tmp/slurm-spire-jobs"

	// How long to wait for the syncer to converge. Generous relative to its 2s
	// interval, because the job also has to be scheduled and start running.
	convergeTimeout = 90 * time.Second
	pollInterval    = 2 * time.Second
)

func TestMain(m *testing.M) {
	if os.Getenv(enableEnv) == "" {
		fmt.Fprintf(os.Stderr,
			"skipping integration tests: set %s=1 to run them, "+
				"but only on a machine prepared by test/integration/scripts\n", enableEnv)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// env reads a setting with a default, keeping the test and the setup scripts
// agreeable about paths without duplicating literals in two languages.
func env(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

// loadConfig reads the same configuration file the running syncer was given.
//
// Deriving expectations from it rather than hardcoding them means the test
// cannot quietly diverge from the deployment: if the templates change, the
// expected SPIFFE IDs change with them.
func loadConfig(t *testing.T) *config.Config {
	t.Helper()
	dir := env("SYNCER_CONFIG_DIR", "/etc/spire/slurm-syncer")
	instance := env("SYNCER_INSTANCE", "main")

	// Resolved exactly the way the unit resolves it, so the test reads whichever
	// file the running syncer actually loaded rather than assuming one.
	path, err := config.ResolvePath(dir, instance)
	if err != nil {
		t.Fatalf("resolving the config under %s for instance %s: %v", dir, instance, err)
	}

	// The unit starts the syncer with -expand-env, so the same expansion has to
	// happen here or a templated trustDomain would not resolve and every
	// expected SPIFFE ID would be wrong.
	cfg, err := config.Load(path, true)
	if err != nil {
		t.Fatalf("loading the syncer config from %s: %v", path, err)
	}
	return cfg
}

// entryClient dials the SPIRE server's private API.
//
// That socket is root-owned mode 0750, so this test binary is built with
// `go test -c` and run under sudo rather than the permissions being loosened.
func entryClient(t *testing.T, cfg *config.Config) *spireentry.Client {
	t.Helper()
	conn, err := spireentry.Dial(cfg.SpireServerSocket)
	if err != nil {
		t.Fatalf("connecting to the SPIRE server at %s: %v", cfg.SpireServerSocket, err)
	}
	t.Cleanup(func() { conn.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return spireentry.New(entryv1.NewEntryClient(conn), cfg, log)
}

// slurmCmd builds a Slurm client command that runs as the unprivileged user who
// invoked the test.
//
// This binary runs under sudo, because reading the SPIRE server's private API
// socket needs root. Submitting as root would be wrong: the accounts are
// associated with the invoking user, and with
// AccountingStorageEnforce=associations Slurm rejects a submission from a user
// with no association. Dropping back to that user also matches how jobs are
// really submitted.
func slurmCmd(name string, args ...string) *exec.Cmd {
	if os.Geteuid() == 0 {
		if user := os.Getenv("SUDO_USER"); user != "" && user != "root" {
			return exec.Command("sudo", append([]string{"-n", "-u", user, name}, args...)...)
		}
	}
	return exec.Command(name, args...)
}

// run executes a Slurm command and returns its trimmed stdout.
func run(t *testing.T, name string, args ...string) string {
	t.Helper()
	cmd := slurmCmd(name, args...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s %s: %v: %s", name, strings.Join(args, " "), err, stderr.String())
	}
	return strings.TrimSpace(string(out))
}

// submitJob submits the SVID-fetching payload under an account and returns its
// job ID.
func submitJob(t *testing.T, account string) string {
	t.Helper()
	script := env("FETCH_SVID_SCRIPT", "test/integration/testdata/fetch-svid.sh")

	args := []string{"--parsable", "--account=" + account, "--nodes=1"}
	// Forwarded on the command line rather than inherited: submission drops to
	// the unprivileged user through sudo, which strips the environment, so an
	// exported variable would never reach the job.
	if sock := os.Getenv("SPIRE_AGENT_SOCKET"); sock != "" {
		args = append(args, "--export=ALL,SPIRE_AGENT_SOCKET="+sock)
	}
	args = append(args, script)

	id := run(t, "sbatch", args...)
	if id == "" {
		t.Fatalf("sbatch --account=%s returned no job id", account)
	}
	t.Logf("submitted job %s under account %s", id, account)
	return id
}

// waitForJobRunning blocks until squeue reports the job RUNNING. Until then the
// syncer cannot see it, so asserting earlier would only be racing.
func waitForJobRunning(t *testing.T, jobID string) {
	t.Helper()
	deadline := time.Now().Add(convergeTimeout)
	for time.Now().Before(deadline) {
		out, err := slurmCmd("squeue", "-h", "-j", jobID, "-o", "%T").Output()
		if err == nil && strings.TrimSpace(string(out)) == "RUNNING" {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("job %s never reached RUNNING within %s", jobID, convergeTimeout)
}

// managedEntries lists the entries the syncer owns.
func managedEntries(t *testing.T, c *spireentry.Client) []*types.Entry {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	entries, err := c.ListManaged(ctx)
	if err != nil {
		t.Fatalf("listing managed entries: %v", err)
	}
	return entries
}

// entryForJob returns the managed entry carrying this job's selector, or nil.
func entryForJob(entries []*types.Entry, jobID string) *types.Entry {
	want := "job_id:" + jobID
	for _, e := range entries {
		for _, s := range e.Selectors {
			if s.Type == slurm.SelectorType && s.Value == want {
				return e
			}
		}
	}
	return nil
}

// waitForEntry polls until the syncer has created this job's entry.
func waitForEntry(t *testing.T, c *spireentry.Client, jobID string) *types.Entry {
	t.Helper()
	deadline := time.Now().Add(convergeTimeout)
	for time.Now().Before(deadline) {
		if e := entryForJob(managedEntries(t, c), jobID); e != nil {
			return e
		}
		time.Sleep(pollInterval)
	}
	dumpDiagnostics(t)
	t.Fatalf("no registration entry appeared for job %s within %s", jobID, convergeTimeout)
	return nil
}

// waitForEntryGone polls until the syncer has deleted this job's entry.
func waitForEntryGone(t *testing.T, c *spireentry.Client, jobID string) {
	t.Helper()
	deadline := time.Now().Add(convergeTimeout)
	for time.Now().Before(deadline) {
		if entryForJob(managedEntries(t, c), jobID) == nil {
			return
		}
		time.Sleep(pollInterval)
	}
	dumpDiagnostics(t)
	t.Fatalf("the entry for job %s was still present %s after the job was cancelled",
		jobID, convergeTimeout)
}

// jobResult reads what the job recorded about the SVID it was issued.
func jobResult(t *testing.T, jobID string) map[string]string {
	t.Helper()
	path := fmt.Sprintf("%s/%s/RESULT", jobDir, jobID)

	deadline := time.Now().Add(convergeTimeout)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil {
			result := map[string]string{}
			for _, line := range strings.Split(string(b), "\n") {
				if k, v, ok := strings.Cut(strings.TrimSpace(line), "="); ok {
					result[k] = v
				}
			}
			if _, ok := result["SPIFFE_ID"]; ok {
				return result
			}
		}
		time.Sleep(pollInterval)
	}

	dumpJobLog(t, jobID)
	t.Fatalf("job %s never recorded a fetched SVID at %s", jobID, path)
	return nil
}

// expectedSPIFFEID renders what the syncer's configured template should produce
// for a job, so the assertion tracks the deployed configuration.
func expectedSPIFFEID(t *testing.T, cfg *config.Config, account, jobID string) string {
	t.Helper()
	host := slurm.JobHost{JobID: jobID, Account: account, Node: env("SLURM_NODE_NAME", "node1")}
	id, err := syncer.RenderID(cfg.SpiffeIDTemplate, host.TemplateData(cfg.TrustDomain, cfg.ClassName))
	if err != nil {
		t.Fatalf("rendering the expected SPIFFE ID: %v", err)
	}
	return "spiffe://" + id.TrustDomain + id.Path
}

func expectedParentID(t *testing.T, cfg *config.Config) string {
	t.Helper()
	host := slurm.JobHost{Node: env("SLURM_NODE_NAME", "node1")}
	id, err := syncer.RenderID(cfg.ParentIDTemplate, host.TemplateData(cfg.TrustDomain, cfg.ClassName))
	if err != nil {
		t.Fatalf("rendering the expected parent ID: %v", err)
	}
	return "spiffe://" + id.TrustDomain + id.Path
}

func idString(id *types.SPIFFEID) string {
	if id == nil {
		return ""
	}
	return "spiffe://" + id.TrustDomain + id.Path
}

func dumpJobLog(t *testing.T, jobID string) {
	t.Helper()
	if b, err := os.ReadFile(fmt.Sprintf("%s/slurm-%s.out", jobDir, jobID)); err == nil {
		t.Logf("--- job %s output ---\n%s", jobID, b)
	}
}

// dumpDiagnostics prints what squeue and the syncer think is going on. When an
// entry fails to appear, the cause is almost always visible in one of the two.
func dumpDiagnostics(t *testing.T) {
	t.Helper()
	if out, err := slurmCmd("squeue", "--json").Output(); err == nil {
		t.Logf("--- squeue --json ---\n%s", out)
	}
	unit := "slurm-spire-syncer@" + env("SYNCER_INSTANCE", "main")
	if out, err := exec.Command("sudo", "journalctl", "-u", unit,
		"--no-pager", "-n", "80").Output(); err == nil {
		t.Logf("--- syncer log ---\n%s", out)
	}
}

// scancel cancels a job, tolerating one that has already finished.
func scancel(jobID string) error {
	return slurmCmd("scancel", jobID).Run()
}

func hasPrefix(s, prefix string) bool { return strings.HasPrefix(s, prefix) }
