package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/weefer/mongomigrate/internal/cdc"
	"github.com/weefer/mongomigrate/internal/checkpoint"
	"github.com/weefer/mongomigrate/internal/config"
	"github.com/weefer/mongomigrate/internal/copier"
	"github.com/weefer/mongomigrate/internal/engine"
	"github.com/weefer/mongomigrate/internal/migrate"
	"github.com/weefer/mongomigrate/internal/verify"
)

func init() {
	subcommandRegistry = append(subcommandRegistry, newMigrateCmd)
}

func newMigrateCmd() *cobra.Command {
	var (
		cf             commonFlags
		mode           string
		drop           bool
		copyIndexes    bool
		batchSize      int
		readBatchSize  int
		sampleSize     int
		noVerify       bool
		bufferSize     int
		applyBatchSize int
		lagThreshold   time.Duration
		autoCutover    bool
		noCheckpoint   bool
		resume         bool
		runIDFlag      string
		checkpointDir  string
	)
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate data from a source MongoDB to a destination",
		Long: "migrate copies data source->destination. In snapshot mode it does a\n" +
			"one-shot parallel copy (freeze writes for consistency). In live mode it\n" +
			"opens a change stream first, copies the snapshot, then drains the change\n" +
			"buffer for a near-zero-downtime cutover, and finally verifies.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := cf.baseConfig()
			cfg.Mode = config.Mode(mode)
			cfg.DropTarget = drop
			cfg.CopyIndexes = copyIndexes
			cfg.BatchSize = batchSize
			cfg.ReadBatchSize = readBatchSize
			cfg.SampleSize = sampleSize
			cfg.BufferSize = bufferSize
			cfg.ApplyBatchSize = applyBatchSize
			cfg.LagThreshold = lagThreshold
			cfg.AutoCutover = autoCutover
			cfg.ApplyDefaults()
			if err := cfg.Validate(); err != nil {
				return err
			}

			log := cf.logger()
			ctx, cancel := contextWithTimeout(cmd.Context(), cf.timeout)
			defer cancel()

			eps, err := openEndpoints(ctx, cfg, log)
			if err != nil {
				return err
			}
			defer eps.close(context.Background())

			live := cfg.Mode == config.ModeLive
			if live && !eps.src.Info.SupportsChangeStreams {
				return fmt.Errorf("source %s does not support change streams (need a replica set or sharded cluster on 3.6+); use --mode snapshot", eps.src.Info.String())
			}

			nss, err := resolveNamespaces(ctx, eps.src, cfg, log)
			if err != nil {
				return err
			}
			log.Info("namespaces resolved", "count", len(nss))

			reporter := newReporter(log)
			cp := copier.New(eps.src, eps.dst, reporter, log)
			tl := cdc.New(eps.src, eps.dst, reporter, log)
			vf := verify.New(eps.src, eps.dst, log)
			eng := migrate.New(cp, tl, vf, reporter, log)

			var ckStore checkpoint.Store
			runID := ""
			cfgHash := ""
			if !noCheckpoint {
				ckStore = buildCheckpointStore(eps.dst, checkpointDir)
				runID = runIDFlag
				if runID == "" {
					runID = defaultRunID(cfg)
				}
				cfgHash = configHash(cfg)
				log.Info("checkpointing enabled", "runId", runID, "dir", checkpointDir, "resume", resume)
			}

			opts := migrate.Options{
				Checkpoint: ckStore,
				RunID:      runID,
				ConfigHash: cfgHash,
				Resume:     resume,
				Live:       live,
				Copy: engine.CopyOptions{
					Namespaces:    nss,
					Workers:       cfg.Workers,
					BatchSize:     cfg.BatchSize,
					ReadBatchSize: cfg.ReadBatchSize,
					DropTarget:    cfg.DropTarget,
					CopyIndexes:   cfg.CopyIndexes,
				},
				Tail: engine.TailOptions{
					Namespaces:     nss,
					BufferSize:     cfg.BufferSize,
					ApplyBatchSize: cfg.ApplyBatchSize,
				},
				Verify: engine.VerifyOptions{
					Namespaces:   nss,
					CheckCounts:  cfg.VerifyCounts,
					CheckIndexes: cfg.VerifyIndexes,
					SampleSize:   cfg.SampleSize,
					Workers:      cfg.Workers,
				},
				RunVerify:    !noVerify,
				LagThreshold: cfg.LagThreshold,
				AutoCutover:  cfg.AutoCutover,
				PollInterval: time.Second,
			}

			// Manual cutover: proceed when the operator presses Enter (after freezing
			// application writes on the source).
			if live && !cfg.AutoCutover {
				proceed := make(chan struct{})
				opts.Proceed = proceed
				go waitForEnter(ctx, cmd, proceed)
			}

			report, err := eng.Run(ctx, opts)
			printReport(cmd, report)
			if err != nil {
				return err
			}
			fmt.Fprintln(cmd.OutOrStdout(), "\nmigration complete.")
			return nil
		},
	}

	cf.register(cmd)
	fl := cmd.Flags()
	fl.StringVar(&mode, "mode", "snapshot", "snapshot | live")
	fl.BoolVar(&drop, "drop", false, "drop each destination collection before copying")
	fl.BoolVar(&copyIndexes, "copy-indexes", true, "recreate source indexes on the destination")
	fl.IntVar(&batchSize, "batch-size", 1000, "documents per destination bulk write")
	fl.IntVar(&readBatchSize, "read-batch-size", 0, "source cursor batch size (0 = driver default)")
	fl.IntVar(&sampleSize, "sample-size", 100, "documents hashed per collection during verify (0 = skip)")
	fl.BoolVar(&noVerify, "no-verify", false, "skip the post-migration verification pass")
	fl.IntVar(&bufferSize, "buffer-size", 100000, "live mode: max buffered change events")
	fl.IntVar(&applyBatchSize, "apply-batch-size", 500, "live mode: change events per bulk apply")
	fl.DurationVar(&lagThreshold, "lag-threshold", 2*time.Second, "live mode: cutover-ready lag threshold")
	fl.BoolVar(&autoCutover, "auto-cutover", true, "live mode: cut over automatically when lag is low (false = wait for Enter)")
	fl.BoolVar(&noCheckpoint, "no-checkpoint", false, "disable durable checkpointing (no resume on crash)")
	fl.BoolVar(&resume, "resume", false, "resume a prior interrupted run from its checkpoint")
	fl.StringVar(&runIDFlag, "run-id", "", "checkpoint run id (default: derived from src+dst+filter)")
	fl.StringVar(&checkpointDir, "checkpoint-dir", defaultCheckpointDir, "local directory for the checkpoint file mirror")
	return cmd
}

func waitForEnter(ctx context.Context, cmd *cobra.Command, proceed chan<- struct{}) {
	fmt.Fprintln(cmd.ErrOrStderr(), ">> when lag is low you will see 'cutover-ready'. Freeze source writes, then press Enter to cut over.")
	done := make(chan struct{})
	go func() {
		r := bufio.NewReader(os.Stdin)
		_, _ = r.ReadString('\n')
		close(done)
	}()
	select {
	case <-done:
		close(proceed)
	case <-ctx.Done():
	}
}

func printReport(cmd *cobra.Command, r migrate.Report) {
	out := cmd.OutOrStdout()
	fmt.Fprintln(out, "\n=== migration report ===")
	if len(r.Phases) > 0 {
		fmt.Fprint(out, "phases: ")
		for i, p := range r.Phases {
			if i > 0 {
				fmt.Fprint(out, " -> ")
			}
			fmt.Fprint(out, string(p))
		}
		fmt.Fprintln(out)
	}
	fmt.Fprintf(out, "copied: %d docs", r.Copy.TotalDocs)
	if r.Copy.TotalFailed > 0 {
		fmt.Fprintf(out, " (%d failed)", r.Copy.TotalFailed)
	}
	fmt.Fprintf(out, " across %d collections in %s\n", len(r.Copy.Collections), r.Copy.Duration.Round(time.Millisecond))
	for _, c := range r.Copy.Collections {
		status := "ok"
		if c.Skipped {
			status = "skipped"
		}
		if c.Err != "" {
			status = "ERROR: " + c.Err
		}
		fmt.Fprintf(out, "  %-30s %8d docs  %3d idx  %-8s\n", c.Namespace.String(), c.DocsCopied, c.IndexesCopied, status)
	}
	if len(r.Verify.Collections) > 0 {
		printVerify(cmd, r.Verify)
	}
}
