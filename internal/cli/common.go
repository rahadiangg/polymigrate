package cli

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/spf13/cobra"

	"github.com/weefer/mongomigrate/internal/config"
	"github.com/weefer/mongomigrate/internal/logx"
	"github.com/weefer/mongomigrate/internal/mongox"
	"github.com/weefer/mongomigrate/internal/namespace"
	"github.com/weefer/mongomigrate/internal/progress"
)

// commonFlags are shared by migrate, verify, and tail: how to connect and which
// namespaces to act on.
type commonFlags struct {
	srcURI, dstURI       string
	srcFlavor, dstFlavor string
	include, exclude     []string
	includeSystem        bool
	workers              int
	logLevel             string
	logJSON              bool
	timeout              time.Duration
}

func (f *commonFlags) register(cmd *cobra.Command) {
	fl := cmd.Flags()
	fl.StringVar(&f.srcURI, "src", "", "source MongoDB URI (required)")
	fl.StringVar(&f.dstURI, "dst", "", "destination MongoDB URI (required)")
	fl.StringVar(&f.srcFlavor, "src-flavor", "", "override detected source flavor (e.g. huawei-dds)")
	fl.StringVar(&f.dstFlavor, "dst-flavor", "", "override detected destination flavor")
	fl.StringArrayVar(&f.include, "include", nil, "include namespace glob (repeatable), e.g. shop.*")
	fl.StringArrayVar(&f.exclude, "exclude", nil, "exclude namespace glob (repeatable)")
	fl.BoolVar(&f.includeSystem, "include-system", false, "include admin/local/config and system.* collections")
	fl.IntVar(&f.workers, "workers", 4, "collections processed concurrently")
	fl.StringVar(&f.logLevel, "log-level", "info", "log level: debug|info|warn|error")
	fl.BoolVar(&f.logJSON, "log-json", false, "emit logs as JSON")
	fl.DurationVar(&f.timeout, "timeout", 0, "overall timeout (0 = no timeout)")
}

func (f *commonFlags) baseConfig() config.Config {
	c := config.Default()
	c.Source = config.Endpoint{URI: f.srcURI, FlavorOverride: f.srcFlavor}
	c.Dest = config.Endpoint{URI: f.dstURI, FlavorOverride: f.dstFlavor}
	c.IncludeNamespaces = f.include
	c.ExcludeNamespaces = f.exclude
	c.IncludeSystem = f.includeSystem
	c.Workers = f.workers
	c.LogLevel = f.logLevel
	c.LogJSON = f.logJSON
	return c
}

func (f *commonFlags) logger() *slog.Logger {
	return logx.New(logx.Options{Level: f.logLevel, JSON: f.logJSON})
}

// endpoints holds the two connected clients.
type endpoints struct {
	src, dst *mongox.Client
}

func (e endpoints) close(ctx context.Context) {
	if e.src != nil {
		_ = e.src.Close(ctx)
	}
	if e.dst != nil {
		_ = e.dst.Close(ctx)
	}
}

// openEndpoints connects to both sides and runs discovery.
func openEndpoints(ctx context.Context, cfg config.Config, log *slog.Logger) (endpoints, error) {
	var e endpoints
	src, err := mongox.Connect(ctx, cfg.Source.URI, mongox.ConnectOptions{
		FlavorOverride: mongox.Flavor(cfg.Source.FlavorOverride),
	})
	if err != nil {
		return e, fmt.Errorf("connect source: %w", err)
	}
	e.src = src
	dst, err := mongox.Connect(ctx, cfg.Dest.URI, mongox.ConnectOptions{
		FlavorOverride: mongox.Flavor(cfg.Dest.FlavorOverride),
	})
	if err != nil {
		e.close(ctx)
		return endpoints{}, fmt.Errorf("connect destination: %w", err)
	}
	e.dst = dst
	log.Info("connected",
		"source", src.Info.String(), "src_host", config.RedactURI(cfg.Source.URI),
		"dest", dst.Info.String(), "dst_host", config.RedactURI(cfg.Dest.URI))
	return e, nil
}

// resolveNamespaces lists source namespaces and applies the filter. It warns
// about any filter-matching namespaces that were skipped because they are not
// plain collections (views, time-series), so they are not silently unmigrated.
func resolveNamespaces(ctx context.Context, src *mongox.Client, cfg config.Config, log *slog.Logger) ([]namespace.Namespace, error) {
	filter, err := cfg.Filter()
	if err != nil {
		return nil, err
	}
	ns, skipped, err := src.ListNamespaces(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("resolve namespaces: %w", err)
	}
	for _, s := range skipped {
		log.Warn("skipping non-collection namespace (not migrated)",
			"ns", s.Namespace.String(), "type", s.Type)
	}
	if len(ns) == 0 {
		return nil, fmt.Errorf("no source namespaces matched the include/exclude filters")
	}
	return ns, nil
}

// newReporter builds the progress reporter used across the engine components.
func newReporter(log *slog.Logger) *progress.LogReporter {
	return progress.NewLogReporter(log)
}
