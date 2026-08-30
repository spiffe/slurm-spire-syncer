// Command slurm-spire-syncer keeps SPIRE registration entries in step with the
// running jobs reported by Slurm.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spiffe/slurm-spire-syncer/internal/config"
	"github.com/spiffe/slurm-spire-syncer/internal/metrics"
	"github.com/spiffe/slurm-spire-syncer/internal/slurm"
	"github.com/spiffe/slurm-spire-syncer/internal/spireentry"
	"github.com/spiffe/slurm-spire-syncer/internal/syncer"
	entryv1 "github.com/spiffe/spire-api-sdk/proto/spire/api/server/entry/v1"
)

// Build metadata, set at link time by goreleaser. The defaults are what a plain
// `go build` produces, so a locally built binary says so rather than claiming to
// be a release.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	var (
		configPath  = flag.String("config", "", "path to the YAML configuration file, or to a directory laid out as <dir>/<instance>/config, <dir>/<instance>.conf, <dir>/default.conf")
		showVersion = flag.Bool("version", false, "print the version and exit")
		instance    = flag.String("instance", "", "instance name, used to pick a configuration file when -config names a directory")
		validate    = flag.Bool("validate", false, "load and validate the configuration, render the templates against a sample job, then exit")
		expandEnv   = flag.Bool("expand-env", false, "expand ${VAR} references in the configuration file from the environment")
		logLevel    = flag.String("log-level", "info", "log level: debug, info, warn or error")
	)
	flag.Parse()

	if *showVersion {
		fmt.Printf("slurm-spire-syncer %s (commit %s, built %s)\n", version, commit, date)
		return
	}

	log, err := newLogger(*logLevel)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if err := run(*configPath, *instance, *validate, *expandEnv, log); err != nil {
		log.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(configPath, instance string, validate, expandEnv bool, log *slog.Logger) error {
	resolved, err := config.ResolvePath(configPath, instance)
	if err != nil {
		return err
	}
	if resolved != configPath {
		log.Debug("resolved configuration file", "config", resolved, "instance", instance)
	}

	cfg, err := config.Load(resolved, expandEnv)
	if err != nil {
		return err
	}
	if validate {
		return printValidation(cfg, os.Stdout)
	}

	// Signal handling lets systemd stop the daemon cleanly; without it the
	// in-flight reconcile would be killed mid-batch.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	m := metrics.New()

	conn, err := spireentry.Dial(cfg.SpireServerSocket)
	if err != nil {
		return err
	}
	defer conn.Close()

	spire := spireentry.New(entryv1.NewEntryClient(conn), cfg, log)
	gatherer := slurm.NewGatherer(cfg, log)
	s := syncer.New(cfg, gatherer, spire, m, log)

	shutdownMetrics := serveMetrics(cfg.MetricsAddr, m, log)
	defer shutdownMetrics()

	log.Info("starting",
		"className", cfg.ClassName,
		"hint", cfg.Hint,
		"trustDomain", cfg.TrustDomain,
		"spireServerSocket", cfg.SpireServerSocket,
		"interval", cfg.Interval,
		"dryRun", cfg.DryRun)

	s.Run(ctx)
	log.Info("shutting down")
	return nil
}

// serveMetrics starts the /metrics endpoint when an address is configured and
// returns a function that shuts it down.
func serveMetrics(addr string, m *metrics.Metrics, log *slog.Logger) func() {
	if addr == "" {
		return func() {}
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry}))
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("metrics endpoint failed", "addr", addr, "error", err)
		}
	}()
	log.Info("serving metrics", "addr", addr)

	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}
}

// printValidation renders both templates against a synthetic job so that a
// template mistake shows up before the daemon is deployed. Unit tests cannot
// vouch for a site's own templates; this can.
func printValidation(cfg *config.Config, out *os.File) error {
	sample := slurm.JobHost{
		JobID:   "12345",
		Account: "example-account",
		Node:    "node01",
	}
	data := sample.TemplateData(cfg.TrustDomain, cfg.ClassName)

	parentID, err := syncer.RenderID(cfg.ParentIDTemplate, data)
	if err != nil {
		return fmt.Errorf("parentIDTemplate: %w", err)
	}
	spiffeID, err := syncer.RenderID(cfg.SpiffeIDTemplate, data)
	if err != nil {
		return fmt.Errorf("spiffeIDTemplate: %w", err)
	}

	fmt.Fprintf(out, "configuration is valid\n\n")
	fmt.Fprintf(out, "  className          %s\n", cfg.ClassName)
	fmt.Fprintf(out, "  hint               %q\n", cfg.Hint)
	fmt.Fprintf(out, "  trustDomain        %s\n", cfg.TrustDomain)
	fmt.Fprintf(out, "  spireServerSocket  %s\n", cfg.SpireServerSocket)
	fmt.Fprintf(out, "  squeueCommand      %v\n", cfg.SqueueCommand)
	fmt.Fprintf(out, "  jobIdentifier      %s\n", cfg.JobIdentifier)
	fmt.Fprintf(out, "  intervals          squeue=%s spire=%s reconcile=%s\n",
		cfg.SqueueInterval, cfg.SpireInterval, cfg.ReconcileInterval)
	fmt.Fprintf(out, "\nsample entry for job %s (%s) on %s:\n", sample.JobID, sample.Account, sample.Node)
	fmt.Fprintf(out, "  entry ID   %s.<uuid>\n", cfg.ClassName)
	fmt.Fprintf(out, "  parent ID  spiffe://%s%s\n", parentID.TrustDomain, parentID.Path)
	fmt.Fprintf(out, "  SPIFFE ID  spiffe://%s%s\n", spiffeID.TrustDomain, spiffeID.Path)
	fmt.Fprintf(out, "  selector   %s:%s\n", slurm.SelectorType, sample.SelectorValue())
	fmt.Fprintf(out, "  hint       %q\n", cfg.Hint)
	return nil
}

func newLogger(level string) (*slog.Logger, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid -log-level %q", level)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})), nil
}
