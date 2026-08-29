// Package config loads and validates the syncer configuration.
//
// Precedence is file > environment > default. That ordering is encoded by the
// pointer fields on fileConfig: a nil pointer means "absent from the file", which
// is distinct from a field explicitly set to an empty or zero value. Several
// settings (notably hint and metricsAddr) treat the empty string as a meaningful
// value, so the distinction matters.
package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"gopkg.in/yaml.v3"
)

// Job identifier selection modes.
const (
	JobIdentifierAuto  = "auto"
	JobIdentifierJobID = "job_id"
	JobIdentifierSLUID = "sluid"
)

// Defaults applied when a setting appears in neither the file nor the environment.
const (
	DefaultInterval          = 10 * time.Second
	DefaultClassName         = "slurm"
	DefaultHint              = "slurm"
	DefaultSqueueTimeout     = 30 * time.Second
	DefaultSpireServerSocket = "unix:///tmp/spire-server/private/api.sock"
	// A node alias rather than an agent's own SPIFFE ID. An agent's ID depends on
	// how it attested -- x509pop, join_token and the rest all produce different
	// shapes -- so anything derived from it ties this default to one attestor.
	// A node alias is an entry the operator creates per node, parented to the
	// server and selected on that node's attestation selectors, which makes the
	// mapping from Slurm node to SPIRE identity explicit and attestor-agnostic.
	DefaultParentIDTemplate = "spiffe://{{.TrustDomain}}/node/{{.Node}}"
	// Scoped to the account rather than the job, deliberately. A job identifier
	// lasts only as long as its job, so anything authorising on it must be
	// reissued per job -- which in practice means wildcarding the job away. An
	// account outlives any job, so a policy written against it keeps working.
	// Append /{{.JobKey}} where a relying party has to tell concurrent jobs in
	// one account apart.
	DefaultSpiffeIDTemplate = "spiffe://{{.TrustDomain}}/slurm/{{.Account}}"

	// hintMaximumLength mirrors the SPIRE server's own limit
	// (spire/pkg/server/api/entry.go). Exceeding it is rejected server-side, so
	// catching it at config load turns a per-entry runtime failure into a
	// startup error.
	hintMaximumLength = 1024
)

// envPrefix is prepended to every environment variable name.
const envPrefix = "SLURM_SPIRE_SYNCER_"

// classNamePattern is the subset of characters SPIRE accepts in an entry ID
// (spire/pkg/server/datastore/sqlstore/sqlstore.go validEntryIDChars). Entry IDs
// are built as "<className>.<uuid>", so an out-of-range className would produce
// entries the server rejects.
var classNamePattern = regexp.MustCompile(`^[-._0-9A-Za-z]+$`)

var defaultSqueueCommand = []string{"squeue", "--json"}

// Config is the resolved configuration. Templates are parsed once here so that
// rendering per job host cannot fail on syntax.
type Config struct {
	Interval          time.Duration
	SqueueInterval    time.Duration
	SpireInterval     time.Duration
	ReconcileInterval time.Duration

	ClassName   string
	Hint        string
	TrustDomain string

	SqueueCommand []string
	SqueueTimeout time.Duration

	SpireServerSocket string
	JobIdentifier     string

	ParentIDTemplate *template.Template
	SpiffeIDTemplate *template.Template

	X509SVIDTTL time.Duration
	JWTSVIDTTL  time.Duration

	MetricsAddr string
	DryRun      bool

	// Raw template text, retained for error messages and -validate output.
	ParentIDTemplateText string
	SpiffeIDTemplateText string
}

// EntryIDPrefix is the string every entry ID this syncer owns begins with.
// Ownership checks use this alongside the hint; see Config.Owns.
func (c *Config) EntryIDPrefix() string {
	return c.ClassName + "."
}

// fileConfig is the on-disk shape. Every field is a pointer so that presence can
// be distinguished from an empty value.
type fileConfig struct {
	Interval          *string `yaml:"interval"`
	SqueueInterval    *string `yaml:"squeueInterval"`
	SpireInterval     *string `yaml:"spireInterval"`
	ReconcileInterval *string `yaml:"reconcileInterval"`

	ClassName   *string `yaml:"className"`
	Hint        *string `yaml:"hint"`
	TrustDomain *string `yaml:"trustDomain"`

	SqueueCommand *[]string `yaml:"squeueCommand"`
	SqueueTimeout *string   `yaml:"squeueTimeout"`

	SpireServerSocket *string `yaml:"spireServerSocket"`
	JobIdentifier     *string `yaml:"jobIdentifier"`

	ParentIDTemplate *string `yaml:"parentIDTemplate"`
	SpiffeIDTemplate *string `yaml:"spiffeIDTemplate"`

	X509SVIDTTL *string `yaml:"x509SVIDTTL"`
	JWTSVIDTTL  *string `yaml:"jwtSVIDTTL"`

	MetricsAddr *string `yaml:"metricsAddr"`
	DryRun      *bool   `yaml:"dryRun"`
}

// ResolvePath picks the configuration file to load for an instance, following
// the layout the SPIRE packages already use for spire-server.
//
// When path names a directory, the first of these that exists wins:
//
//	<path>/<instance>/config    per-instance, the directory form
//	<path>/<instance>.conf      per-instance, the flat form
//	<path>/default.conf         shipped defaults, shared by every instance
//
// That ordering is what lets a package ship a working default.conf and an
// operator override one instance without touching it. When path names a file it
// is used as given, so a caller that knows exactly which file it wants is
// unaffected.
//
// An empty path resolves to an empty path: defaults only, no file.
func ResolvePath(path, instance string) (string, error) {
	if path == "" {
		return "", nil
	}

	info, err := os.Stat(path)
	if err != nil {
		// Left to Load to report, so a missing file is described the same way
		// however it was named.
		return path, nil
	}
	if !info.IsDir() {
		return path, nil
	}

	candidates := []string{}
	if instance != "" {
		candidates = append(candidates,
			filepath.Join(path, instance, "config"),
			filepath.Join(path, instance+".conf"),
		)
	}
	candidates = append(candidates, filepath.Join(path, "default.conf"))

	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("config: no configuration file for instance %q under %s; tried %s",
		instance, path, strings.Join(candidates, ", "))
}

// Load reads the YAML file at path and resolves it against the environment and
// the built-in defaults. An empty path yields defaults only.
//
// When expandEnv is set, ${VAR} and $VAR references in the file are replaced
// with their values from the environment before the YAML is parsed. That is what
// lets one configuration file serve several systemd instances, each supplying its
// own values through an EnvironmentFile — see systemd/slurm-spire-syncer@.service
// — and what lets trustDomain be taken from the SPIFFE_TRUST_DOMAIN variable the
// SPIRE packages already set.
//
// Expansion happens before parsing, so a value it produces takes effect exactly
// as if it had been written in the file. It therefore beats the environment
// overrides below, which apply only to keys the file leaves out.
func Load(path string, expandEnv bool) (*Config, error) {
	var fc fileConfig
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("config: opening %s: %w", path, err)
		}

		if expandEnv {
			// An undefined variable expands to the empty string rather than
			// failing. That is os.ExpandEnv's behaviour and the same as SPIRE's
			// own -expandEnv; a required setting left empty is then caught by
			// validation, which names the field.
			raw = []byte(os.ExpandEnv(string(raw)))
		}

		dec := yaml.NewDecoder(bytes.NewReader(raw))
		// Reject unknown fields so a typo'd key fails loudly instead of being
		// silently ignored and leaving the default in place.
		dec.KnownFields(true)
		if err := dec.Decode(&fc); err != nil {
			return nil, fmt.Errorf("config: parsing %s: %w", path, err)
		}
	}
	return resolve(&fc)
}

func resolve(fc *fileConfig) (*Config, error) {
	c := &Config{
		ClassName:         resolveNonEmptyString(fc.ClassName, "CLASS_NAME", DefaultClassName),
		Hint:              resolveString(fc.Hint, "HINT", DefaultHint),
		TrustDomain:       resolveString(fc.TrustDomain, "TRUST_DOMAIN", ""),
		SpireServerSocket: resolveNonEmptyString(fc.SpireServerSocket, "SPIRE_SERVER_SOCKET", DefaultSpireServerSocket),
		JobIdentifier:     resolveNonEmptyString(fc.JobIdentifier, "JOB_IDENTIFIER", JobIdentifierAuto),
		MetricsAddr:       resolveString(fc.MetricsAddr, "METRICS_ADDR", ""),
		SqueueCommand:     resolveStringSlice(fc.SqueueCommand, "SQUEUE_COMMAND", defaultSqueueCommand),

		ParentIDTemplateText: resolveNonEmptyString(fc.ParentIDTemplate, "PARENT_ID_TEMPLATE", DefaultParentIDTemplate),
		SpiffeIDTemplateText: resolveNonEmptyString(fc.SpiffeIDTemplate, "SPIFFE_ID_TEMPLATE", DefaultSpiffeIDTemplate),
	}

	var err error
	if c.DryRun, err = resolveBool(fc.DryRun, "DRY_RUN"); err != nil {
		return nil, err
	}

	// The base interval resolves first because the three per-loop intervals
	// default to it rather than to a constant.
	if c.Interval, err = resolveDuration(fc.Interval, "INTERVAL", DefaultInterval); err != nil {
		return nil, err
	}
	for _, d := range []struct {
		dst  *time.Duration
		ptr  *string
		env  string
		name string
	}{
		{&c.SqueueInterval, fc.SqueueInterval, "SQUEUE_INTERVAL", "squeueInterval"},
		{&c.SpireInterval, fc.SpireInterval, "SPIRE_INTERVAL", "spireInterval"},
		{&c.ReconcileInterval, fc.ReconcileInterval, "RECONCILE_INTERVAL", "reconcileInterval"},
	} {
		if *d.dst, err = resolveDuration(d.ptr, d.env, c.Interval); err != nil {
			return nil, err
		}
	}

	if c.SqueueTimeout, err = resolveDuration(fc.SqueueTimeout, "SQUEUE_TIMEOUT", DefaultSqueueTimeout); err != nil {
		return nil, err
	}
	// A zero TTL means "let the server decide", so these are allowed to be 0.
	if c.X509SVIDTTL, err = resolveDuration(fc.X509SVIDTTL, "X509_SVID_TTL", 0); err != nil {
		return nil, err
	}
	if c.JWTSVIDTTL, err = resolveDuration(fc.JWTSVIDTTL, "JWT_SVID_TTL", 0); err != nil {
		return nil, err
	}

	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) validate() error {
	if c.TrustDomain == "" {
		return fmt.Errorf("config: trustDomain is required")
	}
	if _, err := spiffeid.TrustDomainFromString(c.TrustDomain); err != nil {
		return fmt.Errorf("config: invalid trustDomain %q: %w", c.TrustDomain, err)
	}

	if !classNamePattern.MatchString(c.ClassName) {
		return fmt.Errorf("config: className %q must match %s (SPIRE restricts entry ID characters)",
			c.ClassName, classNamePattern)
	}
	if len(c.Hint) > hintMaximumLength {
		return fmt.Errorf("config: hint is %d characters, the SPIRE maximum is %d", len(c.Hint), hintMaximumLength)
	}

	switch c.JobIdentifier {
	case JobIdentifierAuto, JobIdentifierJobID, JobIdentifierSLUID:
	default:
		return fmt.Errorf("config: jobIdentifier %q must be one of %q, %q, %q",
			c.JobIdentifier, JobIdentifierAuto, JobIdentifierJobID, JobIdentifierSLUID)
	}

	if c.SpireServerSocket == "" {
		return fmt.Errorf("config: spireServerSocket must not be empty " +
			"(a ${...} reference that did not expand is the usual cause)")
	}
	if len(c.SqueueCommand) == 0 {
		return fmt.Errorf("config: squeueCommand must not be empty")
	}
	if c.SqueueTimeout <= 0 {
		return fmt.Errorf("config: squeueTimeout must be positive, got %s", c.SqueueTimeout)
	}
	for _, iv := range []struct {
		name string
		d    time.Duration
	}{
		{"interval", c.Interval},
		{"squeueInterval", c.SqueueInterval},
		{"spireInterval", c.SpireInterval},
		{"reconcileInterval", c.ReconcileInterval},
	} {
		if iv.d <= 0 {
			return fmt.Errorf("config: %s must be positive, got %s", iv.name, iv.d)
		}
	}
	if c.X509SVIDTTL < 0 {
		return fmt.Errorf("config: x509SVIDTTL must not be negative")
	}
	if c.JWTSVIDTTL < 0 {
		return fmt.Errorf("config: jwtSVIDTTL must not be negative")
	}

	var err error
	if c.ParentIDTemplate, err = parseTemplate("parentIDTemplate", c.ParentIDTemplateText); err != nil {
		return err
	}
	if c.SpiffeIDTemplate, err = parseTemplate("spiffeIDTemplate", c.SpiffeIDTemplateText); err != nil {
		return err
	}
	return nil
}

// parseTemplate compiles a template with missingkey=error so that a reference to
// a field that does not exist fails at render time rather than silently
// producing "<no value>" inside a SPIFFE ID.
func parseTemplate(name, text string) (*template.Template, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("config: %s must not be empty", name)
	}
	t, err := template.New(name).Option("missingkey=error").Parse(text)
	if err != nil {
		return nil, fmt.Errorf("config: parsing %s: %w", name, err)
	}
	return t, nil
}

func envName(suffix string) string { return envPrefix + suffix }

// resolveString applies file > env > default. LookupEnv rather than Getenv is
// deliberate: an explicitly empty environment value is a real setting for hint
// and metricsAddr, both of which use "" to mean "disabled".
func resolveString(filePtr *string, env, def string) string {
	if filePtr != nil {
		return *filePtr
	}
	if v, ok := os.LookupEnv(envName(env)); ok {
		return v
	}
	return def
}

// resolveNonEmptyString is resolveString for settings where the empty string is
// not a legal value, so an empty entry at any level means "fall back" rather
// than "set to empty".
//
// That is what makes a templated configuration usable: with -expand-env an unset
// ${VAR} expands to the empty string, and for these settings that has to mean
// the built-in default rather than a validation error. The durations already
// behave this way. The two settings where empty IS meaningful — hint and
// metricsAddr, both of which use it to mean "disabled" — keep resolveString.
func resolveNonEmptyString(filePtr *string, env, def string) string {
	if filePtr != nil && *filePtr != "" {
		return *filePtr
	}
	if v := os.Getenv(envName(env)); v != "" {
		return v
	}
	return def
}

// resolveStringSlice reads a command line from the environment as
// whitespace-separated fields. There is no quoting support; anything needing it
// belongs in the config file, where YAML gives a proper list.
func resolveStringSlice(filePtr *[]string, env string, def []string) []string {
	if filePtr != nil {
		return *filePtr
	}
	if v, ok := os.LookupEnv(envName(env)); ok {
		if fields := strings.Fields(v); len(fields) > 0 {
			return fields
		}
		return nil
	}
	return def
}

func resolveBool(filePtr *bool, env string) (bool, error) {
	if filePtr != nil {
		return *filePtr, nil
	}
	v, ok := os.LookupEnv(envName(env))
	if !ok || v == "" {
		return false, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("config: %s: %q is not a boolean", envName(env), v)
	}
	return b, nil
}

func resolveDuration(filePtr *string, env string, def time.Duration) (time.Duration, error) {
	raw := ""
	switch {
	case filePtr != nil:
		raw = *filePtr
	default:
		v, ok := os.LookupEnv(envName(env))
		if !ok {
			return def, nil
		}
		raw = v
	}
	if strings.TrimSpace(raw) == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s: %q is not a duration: %w", env, raw, err)
	}
	return d, nil
}
