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
	"path/filepath"
	"sort"
	"strconv"
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

// requireIntegration skips a test that needs a machine prepared by
// test/integration/scripts -- a real Slurm cluster and a real SPIRE deployment.
//
// Gating per test rather than in TestMain so that the parts which are pure logic,
// like the environment-file layering below, still run on a developer's machine.
// Those are exactly the parts worth catching before a twenty-minute CI leg: the
// layering bug this guards against was invisible locally when the whole package
// skipped.
//
// The unit CI job runs `go test ./...` and legitimately skips the heavy tests.
// Making sure the integration workflow does not skip them is that workflow's job;
// it asserts the suite reported a PASS.
func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv(enableEnv) == "" {
		t.Skipf("set %s=1 to run this, but only on a machine prepared by "+
			"test/integration/scripts", enableEnv)
	}
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

	// The config is templated and the unit expands it against its own
	// environment, which this process does not inherit: sudo -E carries the
	// workflow's variables, not the unit's EnvironmentFile stack. Without this
	// ${SPIFFE_TRUST_DOMAIN} expands to empty and loading fails with
	// "trustDomain is required".
	loadUnitEnvironment(t, dir, instance)

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

// loadUnitEnvironment reproduces the environment systemd builds for the syncer,
// so the test expands the configuration file exactly as the running syncer did.
//
// The order mirrors systemd/slurm-spire-syncer@.service: the Environment=
// directive first, then each EnvironmentFile in turn, with **later files
// overriding earlier ones**. That last part is the whole point of the layering --
// the packaged default.env supplies the defaults and <instance>.env overrides
// them -- and getting it backwards means the test expects a different
// configuration from the one the syncer is running under.
func loadUnitEnvironment(t *testing.T, dir, instance string) {
	t.Helper()

	// The unit derives this from the instance name; there is no file to read it
	// from, so it is derived the same way here. Set before the files, because
	// the unit's Environment= line comes before its EnvironmentFile lines and a
	// file is allowed to override it.
	t.Setenv("SPIRE_SERVER_SOCKET",
		fmt.Sprintf("unix:///run/spire/server/sockets/%s/private/api.sock", instance))

	// All optional, matching the unit's leading "-". The trust domain file is
	// written by the SPIRE packages and is where SPIFFE_TRUST_DOMAIN comes from.
	// Applied in order, last value winning, exactly as systemd does.
	for _, path := range []string{
		"/etc/spiffe/default-trust-domain.env",
		filepath.Join(dir, "default.env"),
		filepath.Join(dir, instance+".env"),
	} {
		for key, value := range readEnvFile(t, path) {
			t.Setenv(key, value)
		}
	}
}

// readEnvFile parses a systemd EnvironmentFile: KEY=value per line, blank lines
// and # comments ignored, and surrounding quotes stripped. A missing file is not
// an error, because every one of them is declared optional in the unit.
func readEnvFile(t *testing.T, path string) map[string]string {
	t.Helper()

	out := map[string]string{}
	raw, err := os.ReadFile(path)
	if err != nil {
		return out
	}

	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0] {
			value = value[1 : len(value)-1]
		}
		out[key] = value
	}
	return out
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

// nodeCount is how many Slurm nodes this leg of the matrix runs.
func nodeCount(t *testing.T) int {
	t.Helper()
	n, err := strconv.Atoi(env("SLURM_NODE_COUNT", "1"))
	if err != nil || n < 1 {
		t.Fatalf("SLURM_NODE_COUNT = %q, want a positive integer", env("SLURM_NODE_COUNT", "1"))
	}
	return n
}

// nodes returns the cluster's node names, matching scripts/lib.sh.
func nodes(t *testing.T) []string {
	t.Helper()
	out := make([]string, 0, nodeCount(t))
	for i := 1; i <= nodeCount(t); i++ {
		out = append(out, fmt.Sprintf("node%d", i))
	}
	return out
}

// agentSocket returns the workload API socket a job on a node talks to.
//
// The rule matches spire_agent_instance in scripts/lib.sh: the first node is
// served by the agent spire-dev-action deployed, whose instance name is its own
// concern; every other node has an agent instance named after it.
func agentSocket(t *testing.T, node string) string {
	t.Helper()
	instance := node
	if node == env("SLURM_NODE_NAME", "node1") {
		instance = env("SPIRE_AGENT_INSTANCE", "main")
	}
	return fmt.Sprintf("/run/spire/agent/sockets/%s/public/api.sock", instance)
}

// jobSpec describes a job to submit.
type jobSpec struct {
	account string
	// nodeList pins the job to specific nodes. Empty lets Slurm choose.
	nodeList []string
}

// submitJob submits the SVID-fetching payload and returns its job ID.
func submitJob(t *testing.T, spec jobSpec) string {
	t.Helper()
	script := env("FETCH_SVID_SCRIPT", "test/integration/testdata/fetch-svid.sh")

	count := len(spec.nodeList)
	if count == 0 {
		count = 1
	}
	args := []string{
		"--parsable",
		"--account=" + spec.account,
		fmt.Sprintf("--nodes=%d", count),
	}
	if len(spec.nodeList) > 0 {
		args = append(args, "--nodelist="+strings.Join(spec.nodeList, ","))
		// The socket differs per node, so the job has to resolve it at run time
		// from SLURMD_NODENAME rather than being handed one here.
		args = append(args, "--ntasks-per-node=1")
	}
	// Forwarded on the command line rather than inherited: submission drops to
	// the unprivileged user through sudo, which strips the environment, so an
	// exported variable would never reach the job. The directory is passed
	// rather than a socket path because a job may span nodes with different
	// agents; the job script picks its own from SLURMD_NODENAME.
	// The batch script runs on one node only, so it sruns this per-node task
	// across the allocation. sbatch copies the batch script into its spool
	// directory, so the task cannot be found relative to it and its absolute
	// path has to be handed over.
	task, err := filepath.Abs(env("FETCH_SVID_TASK_SCRIPT",
		"test/integration/testdata/fetch-svid-task.sh"))
	if err != nil {
		t.Fatalf("resolving the task script path: %v", err)
	}

	exports := []string{"ALL",
		"FETCH_TASK_SCRIPT=" + task,
		"SPIRE_AGENT_SOCKET_DIR=/run/spire/agent/sockets",
		"SPIRE_PRIMARY_NODE=" + env("SLURM_NODE_NAME", "node1"),
		"SPIRE_PRIMARY_AGENT_INSTANCE=" + env("SPIRE_AGENT_INSTANCE", "main"),
	}
	args = append(args, "--export="+strings.Join(exports, ","))
	args = append(args, script)

	id := run(t, "sbatch", args...)
	if id == "" {
		t.Fatalf("sbatch --account=%s returned no job id", spec.account)
	}
	t.Logf("submitted job %s under account %s on %v", id, spec.account, spec.nodeList)
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

// entriesForJob returns every managed entry carrying this job's selector.
//
// A job allocated several nodes produces one entry per node, all sharing the job
// selector and differing in parent ID.
func entriesForJob(entries []*types.Entry, jobID string) []*types.Entry {
	want := "job_id:" + jobID
	var out []*types.Entry
	for _, e := range entries {
		for _, s := range e.Selectors {
			if s.Type == slurm.SelectorType && s.Value == want {
				out = append(out, e)
				break
			}
		}
	}
	return out
}

// entryForJob returns the single managed entry for a job, or nil.
func entryForJob(entries []*types.Entry, jobID string) *types.Entry {
	if found := entriesForJob(entries, jobID); len(found) > 0 {
		return found[0]
	}
	return nil
}

// waitForEntryCount polls until a job has exactly the expected number of
// entries, then returns them keyed by the parent ID that owns each.
func waitForEntryCount(t *testing.T, c *spireentry.Client, jobID string, want int) map[string]*types.Entry {
	t.Helper()
	deadline := time.Now().Add(convergeTimeout)
	var found []*types.Entry
	for time.Now().Before(deadline) {
		found = entriesForJob(managedEntries(t, c), jobID)
		if len(found) == want {
			byParent := make(map[string]*types.Entry, len(found))
			for _, e := range found {
				byParent[idString(e.ParentId)] = e
			}
			// A duplicate parent would silently collapse the map and hide a real
			// bug, so the count has to survive the keying too.
			if len(byParent) != want {
				t.Fatalf("job %s produced %d entries but only %d distinct parents: %v",
					jobID, len(found), len(byParent), parentIDs(found))
			}
			return byParent
		}
		time.Sleep(pollInterval)
	}
	dumpDiagnostics(t)
	t.Fatalf("job %s has %d entries after %s, want %d (parents: %v)",
		jobID, len(found), convergeTimeout, want, parentIDs(found))
	return nil
}

func parentIDs(entries []*types.Entry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, idString(e.ParentId))
	}
	sort.Strings(out)
	return out
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

// jobResult reads what a job recorded about the SVID it was issued on one node.
//
// Keyed by node because a job spanning nodes fetches once per node, and each
// task's identity has to be checked separately.
func jobResult(t *testing.T, jobID, node string) map[string]string {
	t.Helper()
	path := fmt.Sprintf("%s/%s/%s/RESULT", jobDir, jobID, node)

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
	t.Fatalf("job %s on node %s never recorded a fetched SVID at %s", jobID, node, path)
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

func expectedParentID(t *testing.T, cfg *config.Config, node string) string {
	t.Helper()
	host := slurm.JobHost{Node: node}
	id, err := syncer.RenderID(cfg.ParentIDTemplate, host.TemplateData(cfg.TrustDomain, cfg.ClassName))
	if err != nil {
		t.Fatalf("rendering the expected parent ID: %v", err)
	}
	return "spiffe://" + id.TrustDomain + id.Path
}

// assertParentMatchesAgent checks that the configured parentIDTemplate renders to
// the SPIFFE ID of the agent actually serving this node.
//
// SPIRE delivers an entry only to the agent named as its parent, so a template
// that renders to anything else produces entries that exist, look correct, and
// are never handed to a workload. The agent's ID depends on how it attested --
// join_token gives /agent/<node-id>, x509pop gives /spire/agent/x509pop/<...> --
// so this cannot be assumed from the trust domain alone.
func assertParentMatchesAgent(t *testing.T, cfg *config.Config) {
	t.Helper()

	want := os.Getenv("SPIRE_AGENT_ID")
	if want == "" {
		t.Skip("SPIRE_AGENT_ID is not set; cannot check the parent ID against the real agent")
	}

	got := expectedParentID(t, cfg, env("SLURM_NODE_NAME", "node1"))
	if got != want {
		t.Fatalf("parentIDTemplate renders to %q for node %q, but the agent serving it is %q.\n"+
			"Entries would be created and never delivered. Set "+
			"SLURM_SYNCER_PARENT_ID_TEMPLATE to match how the agent attested.",
			got, env("SLURM_NODE_NAME", "node1"), want)
	}
	t.Logf("parent ID template resolves to the serving agent: %s", got)
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

// The layering is the whole point of shipping a default.env: it supplies the
// defaults and <instance>.env overrides them. systemd applies EnvironmentFiles in
// order with the last value winning, and reproducing that backwards makes the
// test expect a different configuration from the one the syncer is running under
// -- which is what happened the first time a default.env existed.
//
// Runs without a prepared machine, so it fails on a laptop rather than twenty
// minutes into CI.
func TestLoadUnitEnvironmentPrefersLaterFiles(t *testing.T) {
	dir := t.TempDir()

	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	write("default.env", strings.Join([]string{
		"# the packaged default",
		"SLURM_SYNCER_PARENT_ID_TEMPLATE=spiffe://{{.TrustDomain}}/node/{{.Node}}",
		"SLURM_SYNCER_CLASS_NAME=slurm",
		"",
	}, "\n"))
	write("main.env", strings.Join([]string{
		"SLURM_SYNCER_PARENT_ID_TEMPLATE=spiffe://{{.TrustDomain}}/agent/{{.Node}}",
		"",
	}, "\n"))

	loadUnitEnvironment(t, dir, "main")

	if got, want := os.Getenv("SLURM_SYNCER_PARENT_ID_TEMPLATE"),
		"spiffe://{{.TrustDomain}}/agent/{{.Node}}"; got != want {
		t.Errorf("parent template = %q, want %q from the per-instance file, which is read last",
			got, want)
	}
	// Anything the instance file does not mention keeps the packaged default.
	if got, want := os.Getenv("SLURM_SYNCER_CLASS_NAME"), "slurm"; got != want {
		t.Errorf("class name = %q, want the packaged default %q", got, want)
	}
	// Derived from the instance name by the unit's Environment= line.
	if got := os.Getenv("SPIRE_SERVER_SOCKET"); !strings.Contains(got, "/sockets/main/") {
		t.Errorf("server socket = %q, want it derived from the instance name", got)
	}
}

// A systemd EnvironmentFile is not shell: comments and blank lines are ignored,
// and surrounding quotes are stripped from the value.
func TestReadEnvFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.env")
	content := strings.Join([]string{
		"# a comment",
		"",
		"PLAIN=value",
		`QUOTED="has spaces"`,
		"SPACED  =  trimmed  ",
		"WITH_EQUALS=a=b",
		"EMPTY=",
		"not-a-pair",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	got := readEnvFile(t, path)
	for key, want := range map[string]string{
		"PLAIN":       "value",
		"QUOTED":      "has spaces",
		"SPACED":      "trimmed",
		"WITH_EQUALS": "a=b",
		"EMPTY":       "",
	} {
		if got[key] != want {
			t.Errorf("%s = %q, want %q", key, got[key], want)
		}
	}
	if _, ok := got["not-a-pair"]; ok {
		t.Error("a line with no = was parsed as a variable")
	}
	if len(got) != 5 {
		t.Errorf("parsed %d variables, want 5: %v", len(got), got)
	}

	// Every file in the stack is optional in the unit, so a missing one is not
	// an error.
	if n := len(readEnvFile(t, filepath.Join(t.TempDir(), "absent.env"))); n != 0 {
		t.Errorf("a missing file yielded %d variables, want 0", n)
	}
}
