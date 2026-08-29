package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeConfig writes a config file into a temp dir and returns its path.
func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("writing config: %v", err)
	}
	return path
}

func loadOK(t *testing.T, path string) *Config {
	t.Helper()
	cfg, err := Load(path, false)
	if err != nil {
		t.Fatalf("Load(%q): %v", path, err)
	}
	return cfg
}

func TestDefaults(t *testing.T) {
	// trustDomain is the one setting with no default, so the minimum viable
	// config is just that.
	cfg := loadOK(t, writeConfig(t, "trustDomain: example.org\n"))

	if cfg.ClassName != DefaultClassName {
		t.Errorf("ClassName = %q, want %q", cfg.ClassName, DefaultClassName)
	}
	if cfg.Hint != DefaultHint {
		t.Errorf("Hint = %q, want %q", cfg.Hint, DefaultHint)
	}
	if cfg.Interval != DefaultInterval {
		t.Errorf("Interval = %s, want %s", cfg.Interval, DefaultInterval)
	}
	if cfg.SpireServerSocket != DefaultSpireServerSocket {
		t.Errorf("SpireServerSocket = %q, want %q", cfg.SpireServerSocket, DefaultSpireServerSocket)
	}
	if cfg.JobIdentifier != JobIdentifierAuto {
		t.Errorf("JobIdentifier = %q, want %q", cfg.JobIdentifier, JobIdentifierAuto)
	}
	if got, want := strings.Join(cfg.SqueueCommand, " "), "squeue --json"; got != want {
		t.Errorf("SqueueCommand = %q, want %q", got, want)
	}
	if cfg.SqueueTimeout != DefaultSqueueTimeout {
		t.Errorf("SqueueTimeout = %s, want %s", cfg.SqueueTimeout, DefaultSqueueTimeout)
	}
	if cfg.DryRun {
		t.Error("DryRun = true, want false")
	}
	if cfg.MetricsAddr != "" {
		t.Errorf("MetricsAddr = %q, want it disabled by default", cfg.MetricsAddr)
	}
	if cfg.EntryIDPrefix() != "slurm." {
		t.Errorf("EntryIDPrefix() = %q, want %q", cfg.EntryIDPrefix(), "slurm.")
	}
}

// The three per-loop intervals default to the base interval rather than to a
// constant, so raising `interval` alone must move all of them.
func TestPerLoopIntervalsDefaultToBaseInterval(t *testing.T) {
	cfg := loadOK(t, writeConfig(t, "trustDomain: example.org\ninterval: 45s\n"))

	for name, got := range map[string]time.Duration{
		"squeueInterval":    cfg.SqueueInterval,
		"spireInterval":     cfg.SpireInterval,
		"reconcileInterval": cfg.ReconcileInterval,
	} {
		if got != 45*time.Second {
			t.Errorf("%s = %s, want 45s", name, got)
		}
	}
}

func TestPerLoopIntervalOverrides(t *testing.T) {
	cfg := loadOK(t, writeConfig(t, `
trustDomain: example.org
interval: 10s
squeueInterval: 1m
reconcileInterval: 5s
`))

	if cfg.SqueueInterval != time.Minute {
		t.Errorf("SqueueInterval = %s, want 1m", cfg.SqueueInterval)
	}
	if cfg.ReconcileInterval != 5*time.Second {
		t.Errorf("ReconcileInterval = %s, want 5s", cfg.ReconcileInterval)
	}
	if cfg.SpireInterval != 10*time.Second {
		t.Errorf("SpireInterval = %s, want the base interval 10s", cfg.SpireInterval)
	}
}

func TestEnvBeatsDefault(t *testing.T) {
	t.Setenv("SLURM_SPIRE_SYNCER_TRUST_DOMAIN", "env.example")
	t.Setenv("SLURM_SPIRE_SYNCER_CLASS_NAME", "envclass")
	t.Setenv("SLURM_SPIRE_SYNCER_HINT", "envhint")
	t.Setenv("SLURM_SPIRE_SYNCER_INTERVAL", "30s")
	t.Setenv("SLURM_SPIRE_SYNCER_DRY_RUN", "true")
	t.Setenv("SLURM_SPIRE_SYNCER_SQUEUE_COMMAND", "squeue --json --all")

	cfg := loadOK(t, "")

	if cfg.TrustDomain != "env.example" {
		t.Errorf("TrustDomain = %q, want %q", cfg.TrustDomain, "env.example")
	}
	if cfg.ClassName != "envclass" {
		t.Errorf("ClassName = %q, want %q", cfg.ClassName, "envclass")
	}
	if cfg.Hint != "envhint" {
		t.Errorf("Hint = %q, want %q", cfg.Hint, "envhint")
	}
	if cfg.Interval != 30*time.Second {
		t.Errorf("Interval = %s, want 30s", cfg.Interval)
	}
	if !cfg.DryRun {
		t.Error("DryRun = false, want true")
	}
	if got, want := strings.Join(cfg.SqueueCommand, " "), "squeue --json --all"; got != want {
		t.Errorf("SqueueCommand = %q, want %q", got, want)
	}
}

func TestFileBeatsEnv(t *testing.T) {
	t.Setenv("SLURM_SPIRE_SYNCER_TRUST_DOMAIN", "env.example")
	t.Setenv("SLURM_SPIRE_SYNCER_CLASS_NAME", "envclass")
	t.Setenv("SLURM_SPIRE_SYNCER_INTERVAL", "30s")

	cfg := loadOK(t, writeConfig(t, `
trustDomain: file.example
className: fileclass
interval: 90s
`))

	if cfg.TrustDomain != "file.example" {
		t.Errorf("TrustDomain = %q, want the file value", cfg.TrustDomain)
	}
	if cfg.ClassName != "fileclass" {
		t.Errorf("ClassName = %q, want the file value", cfg.ClassName)
	}
	if cfg.Interval != 90*time.Second {
		t.Errorf("Interval = %s, want 90s", cfg.Interval)
	}
}

// An explicitly empty hint disables hint stamping and the server-side list
// filter. That is a real setting, so it must be distinguishable from "absent",
// which is why the wire struct uses pointers and lookups use LookupEnv.
func TestEmptyHintIsDistinctFromAbsent(t *testing.T) {
	absent := loadOK(t, writeConfig(t, "trustDomain: example.org\n"))
	if absent.Hint != "slurm" {
		t.Errorf("Hint with no key = %q, want the default %q", absent.Hint, "slurm")
	}

	explicit := loadOK(t, writeConfig(t, "trustDomain: example.org\nhint: \"\"\n"))
	if explicit.Hint != "" {
		t.Errorf("Hint set to \"\" = %q, want it to stay empty", explicit.Hint)
	}

	t.Setenv("SLURM_SPIRE_SYNCER_HINT", "")
	fromEnv := loadOK(t, writeConfig(t, "trustDomain: example.org\n"))
	if fromEnv.Hint != "" {
		t.Errorf("Hint from an empty env var = %q, want it to stay empty", fromEnv.Hint)
	}
}

// A typo'd key must fail loudly rather than silently leaving the default in
// place.
func TestUnknownFieldRejected(t *testing.T) {
	_, err := Load(writeConfig(t, "trustDomain: example.org\nclassname: oops\n"), false)
	if err == nil {
		t.Fatal("expected an error for an unknown field")
	}
	if !strings.Contains(err.Error(), "classname") {
		t.Fatalf("error = %q, want it to name the offending field", err)
	}
}

func TestMissingFileIsAnError(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "absent.yaml"), false); err == nil {
		t.Fatal("expected an error for a missing config file")
	}
}

func TestTemplatesAreParsed(t *testing.T) {
	cfg := loadOK(t, writeConfig(t, `
trustDomain: example.org
parentIDTemplate: "spiffe://{{.TrustDomain}}/agent/{{.Node}}"
spiffeIDTemplate: "spiffe://{{.TrustDomain}}/job/{{.JobKey}}"
`))

	if cfg.ParentIDTemplate == nil || cfg.SpiffeIDTemplate == nil {
		t.Fatal("templates were not parsed")
	}
	if !strings.Contains(cfg.ParentIDTemplateText, "{{.Node}}") {
		t.Errorf("ParentIDTemplateText = %q, want the raw text retained", cfg.ParentIDTemplateText)
	}
}

// Expansion is what lets one configuration file serve several systemd
// instances, each supplying its own values through an EnvironmentFile, and what
// lets trustDomain come from the SPIFFE_TRUST_DOMAIN variable the SPIRE packages
// already set.
func TestExpandEnv(t *testing.T) {
	t.Setenv("SPIFFE_TRUST_DOMAIN", "expanded.example")
	t.Setenv("SPIRE_SERVER_SOCKET", "unix:///run/spire/server/sockets/two/private/api.sock")

	content := "trustDomain: ${SPIFFE_TRUST_DOMAIN}\nspireServerSocket: ${SPIRE_SERVER_SOCKET}\n"

	// Off by default: the references are taken literally, and an invalid trust
	// domain is exactly what that produces.
	if _, err := Load(writeConfig(t, content), false); err == nil {
		t.Fatal("expected an error without expansion, since ${...} is not a valid trust domain")
	}

	cfg, err := Load(writeConfig(t, content), true)
	if err != nil {
		t.Fatalf("Load with expansion: %v", err)
	}
	if cfg.TrustDomain != "expanded.example" {
		t.Errorf("TrustDomain = %q, want the expanded value", cfg.TrustDomain)
	}
	if !strings.Contains(cfg.SpireServerSocket, "/sockets/two/") {
		t.Errorf("SpireServerSocket = %q, want the expanded value", cfg.SpireServerSocket)
	}
}

// $VAR is expanded as well as ${VAR}, matching os.ExpandEnv.
func TestExpandEnvBareForm(t *testing.T) {
	t.Setenv("SPIFFE_TRUST_DOMAIN", "bare.example")

	cfg, err := Load(writeConfig(t, "trustDomain: $SPIFFE_TRUST_DOMAIN\n"), true)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.TrustDomain != "bare.example" {
		t.Errorf("TrustDomain = %q, want %q", cfg.TrustDomain, "bare.example")
	}
}

// An undefined variable becomes the empty string rather than an error, matching
// os.ExpandEnv and SPIRE's own -expandEnv. Validation is what catches it, and it
// has to name the field so the cause is findable.
func TestExpandEnvUndefinedVariable(t *testing.T) {
	_, err := Load(writeConfig(t, "trustDomain: ${DEFINITELY_NOT_SET_ANYWHERE}\n"), true)
	if err == nil {
		t.Fatal("expected an error when a required value expands to empty")
	}
	if !strings.Contains(err.Error(), "trustDomain") {
		t.Fatalf("error = %q, want it to name the field left empty", err)
	}
}

// Expansion runs before parsing, so what it produces is indistinguishable from
// text written in the file. It therefore beats the environment overrides, which
// only apply to keys the file omits.
func TestExpandEnvBeatsEnvironmentOverride(t *testing.T) {
	t.Setenv("SPIFFE_TRUST_DOMAIN", "fromexpansion.example")
	t.Setenv("SLURM_SPIRE_SYNCER_TRUST_DOMAIN", "fromoverride.example")

	cfg, err := Load(writeConfig(t, "trustDomain: ${SPIFFE_TRUST_DOMAIN}\n"), true)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.TrustDomain != "fromexpansion.example" {
		t.Errorf("TrustDomain = %q, want the expanded file value to win", cfg.TrustDomain)
	}
}

// Go templates are the one place a $ can legitimately appear in this config, and
// expansion would eat it. Left un-expanded when the flag is off, which is the
// default and the documented advice for templates using $ variables.
func TestExpandEnvLeavesTemplatesAloneWhenOff(t *testing.T) {
	content := "trustDomain: example.org\n" +
		`spiffeIDTemplate: '{{$n := .Node}}spiffe://{{.TrustDomain}}/slurm/{{$n}}'` + "\n"

	cfg, err := Load(writeConfig(t, content), false)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !strings.Contains(cfg.SpiffeIDTemplateText, "$n") {
		t.Errorf("SpiffeIDTemplateText = %q, want the template variable preserved",
			cfg.SpiffeIDTemplateText)
	}
}

// For settings where the empty string is not a legal value, an empty entry means
// "use the default" rather than "set to empty". Without that, a templated
// configuration whose ${VAR} is unset fails validation instead of falling back,
// which is the whole point of shipping a templated default.conf.
func TestEmptyMeansDefaultWhereEmptyIsIllegal(t *testing.T) {
	cfg := loadOK(t, writeConfig(t, `
trustDomain: example.org
className: ""
spireServerSocket: ""
jobIdentifier: ""
parentIDTemplate: ""
spiffeIDTemplate: ""
`))

	if cfg.ClassName != DefaultClassName {
		t.Errorf("ClassName = %q, want the default %q", cfg.ClassName, DefaultClassName)
	}
	if cfg.SpireServerSocket != DefaultSpireServerSocket {
		t.Errorf("SpireServerSocket = %q, want the default", cfg.SpireServerSocket)
	}
	if cfg.JobIdentifier != JobIdentifierAuto {
		t.Errorf("JobIdentifier = %q, want the default", cfg.JobIdentifier)
	}
	if cfg.ParentIDTemplateText != DefaultParentIDTemplate {
		t.Errorf("ParentIDTemplateText = %q, want the default", cfg.ParentIDTemplateText)
	}
	if cfg.SpiffeIDTemplateText != DefaultSpiffeIDTemplate {
		t.Errorf("SpiffeIDTemplateText = %q, want the default", cfg.SpiffeIDTemplateText)
	}
	// Both must still have compiled, or rendering would nil-panic later.
	if cfg.ParentIDTemplate == nil || cfg.SpiffeIDTemplate == nil {
		t.Error("a template fell back to the default but was not parsed")
	}
}

// The two settings where empty IS a real value must not be swept up by the rule
// above: both use it to mean "disabled".
func TestEmptyStaysMeaningfulForHintAndMetrics(t *testing.T) {
	cfg := loadOK(t, writeConfig(t, "trustDomain: example.org\nhint: \"\"\nmetricsAddr: \"\"\n"))

	if cfg.Hint != "" {
		t.Errorf("Hint = %q, want it to stay empty", cfg.Hint)
	}
	if cfg.MetricsAddr != "" {
		t.Errorf("MetricsAddr = %q, want it to stay empty", cfg.MetricsAddr)
	}
}

// The layout the SPIRE packages use: a shipped default.conf underneath, with
// per-instance files taking precedence over it.
func TestResolvePath(t *testing.T) {
	dir := t.TempDir()
	write := func(rel string) string {
		path := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("trustDomain: example.org\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}

	// Only the shipped default exists: every instance uses it.
	wantDefault := write("default.conf")
	for _, instance := range []string{"main", "backup", ""} {
		got, err := ResolvePath(dir, instance)
		if err != nil {
			t.Fatalf("ResolvePath(%q): %v", instance, err)
		}
		if got != wantDefault {
			t.Errorf("instance %q resolved to %q, want the shipped default %q", instance, got, wantDefault)
		}
	}

	// The flat per-instance form beats the default, for that instance only.
	wantFlat := write("main.conf")
	if got, _ := ResolvePath(dir, "main"); got != wantFlat {
		t.Errorf("instance main resolved to %q, want %q", got, wantFlat)
	}
	if got, _ := ResolvePath(dir, "backup"); got != wantDefault {
		t.Errorf("instance backup resolved to %q, want the default %q", got, wantDefault)
	}

	// The directory form beats both.
	wantDir := write("main/config")
	if got, _ := ResolvePath(dir, "main"); got != wantDir {
		t.Errorf("instance main resolved to %q, want the directory form %q", got, wantDir)
	}
}

// A path naming a file is used as given, so a caller that knows which file it
// wants is unaffected by the directory layout.
func TestResolvePathPassesFilesThrough(t *testing.T) {
	file := writeConfig(t, "trustDomain: example.org\n")
	if got, err := ResolvePath(file, "main"); err != nil || got != file {
		t.Fatalf("ResolvePath(%q) = %q, %v; want it passed through", file, got, err)
	}

	// An empty path means defaults only.
	if got, err := ResolvePath("", "main"); err != nil || got != "" {
		t.Fatalf(`ResolvePath("") = %q, %v; want "", nil`, got, err)
	}

	// A missing path is left for Load to report, so the message is the same
	// however the file was named.
	missing := filepath.Join(t.TempDir(), "nope.yaml")
	if got, err := ResolvePath(missing, "main"); err != nil || got != missing {
		t.Fatalf("ResolvePath(%q) = %q, %v; want it passed through to Load", missing, got, err)
	}
}

// An empty directory has nothing to load, and the error has to say what was
// looked for or the layout is guesswork.
func TestResolvePathEmptyDirectory(t *testing.T) {
	_, err := ResolvePath(t.TempDir(), "main")
	if err == nil {
		t.Fatal("expected an error for a directory with no configuration in it")
	}
	for _, want := range []string{"main/config", "main.conf", "default.conf"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}
}

func TestValidationErrors(t *testing.T) {
	cases := []struct {
		name    string
		content string
		wantSub string
	}{
		{
			name:    "missing trust domain",
			content: "className: slurm\n",
			wantSub: "trustDomain is required",
		},
		{
			name:    "invalid trust domain",
			content: "trustDomain: \"not a trust domain\"\n",
			wantSub: "invalid trustDomain",
		},
		{
			// Entry IDs are "<className>.<uuid>" and SPIRE restricts the
			// characters allowed in an entry ID.
			name:    "class name with a slash",
			content: "trustDomain: example.org\nclassName: \"slurm/prod\"\n",
			wantSub: "className",
		},
		{
			name:    "unknown job identifier",
			content: "trustDomain: example.org\njobIdentifier: uuid\n",
			wantSub: "jobIdentifier",
		},
		{
			name:    "zero interval",
			content: "trustDomain: example.org\ninterval: 0s\n",
			wantSub: "interval must be positive",
		},
		{
			name:    "negative reconcile interval",
			content: "trustDomain: example.org\nreconcileInterval: -5s\n",
			wantSub: "reconcileInterval must be positive",
		},
		{
			name:    "unparseable duration",
			content: "trustDomain: example.org\ninterval: soon\n",
			wantSub: "is not a duration",
		},
		{
			name:    "empty squeue command",
			content: "trustDomain: example.org\nsqueueCommand: []\n",
			wantSub: "squeueCommand must not be empty",
		},
		{
			name:    "zero squeue timeout",
			content: "trustDomain: example.org\nsqueueTimeout: 0s\n",
			wantSub: "squeueTimeout must be positive",
		},
		{
			name:    "broken parent template",
			content: "trustDomain: example.org\nparentIDTemplate: \"{{.Node\"\n",
			wantSub: "parentIDTemplate",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, tc.content), false)
			if err == nil {
				t.Fatalf("Load succeeded with %+v, want an error", cfg)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.wantSub)
			}
		})
	}
}

func TestHintLengthLimit(t *testing.T) {
	long := strings.Repeat("x", hintMaximumLength+1)
	_, err := Load(writeConfig(t, "trustDomain: example.org\nhint: "+long+"\n"), false)
	if err == nil {
		t.Fatal("expected an error for a hint over the SPIRE maximum")
	}
	if !strings.Contains(err.Error(), "hint") {
		t.Fatalf("error = %q, want it to mention the hint", err)
	}

	// Exactly at the limit is accepted.
	atLimit := strings.Repeat("x", hintMaximumLength)
	if _, err := Load(writeConfig(t, "trustDomain: example.org\nhint: "+atLimit+"\n"), false); err != nil {
		t.Fatalf("a hint of exactly %d characters was rejected: %v", hintMaximumLength, err)
	}
}

func TestBadBoolEnv(t *testing.T) {
	t.Setenv("SLURM_SPIRE_SYNCER_TRUST_DOMAIN", "example.org")
	t.Setenv("SLURM_SPIRE_SYNCER_DRY_RUN", "yes-please")

	if _, err := Load("", false); err == nil {
		t.Fatal("expected an error for an unparseable boolean env var")
	}
}

func TestZeroTTLsAreAllowed(t *testing.T) {
	// A zero TTL means "use the server default", so it must not be rejected the
	// way a zero interval is.
	cfg := loadOK(t, writeConfig(t, "trustDomain: example.org\n"))
	if cfg.X509SVIDTTL != 0 || cfg.JWTSVIDTTL != 0 {
		t.Fatalf("TTLs = %s/%s, want both zero by default", cfg.X509SVIDTTL, cfg.JWTSVIDTTL)
	}

	cfg = loadOK(t, writeConfig(t, "trustDomain: example.org\nx509SVIDTTL: 1h\njwtSVIDTTL: 5m\n"))
	if cfg.X509SVIDTTL != time.Hour {
		t.Errorf("X509SVIDTTL = %s, want 1h", cfg.X509SVIDTTL)
	}
	if cfg.JWTSVIDTTL != 5*time.Minute {
		t.Errorf("JWTSVIDTTL = %s, want 5m", cfg.JWTSVIDTTL)
	}
}
