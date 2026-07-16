package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/weefer/mongomigrate/internal/cdc"
	"github.com/weefer/mongomigrate/internal/engine"
)

func init() {
	subcommandRegistry = append(subcommandRegistry, newTailCmd)
}

func newTailCmd() *cobra.Command {
	var (
		cf             commonFlags
		bufferSize     int
		applyBatchSize int
	)
	cmd := &cobra.Command{
		Use:   "tail",
		Short: "Continuously replicate change-stream events source->destination (CDC only)",
		Long: "tail opens a change stream on the source and applies events idempotently\n" +
			"to the destination until interrupted (Ctrl-C). It does NOT copy the\n" +
			"existing data — use it after a snapshot, or to keep two clusters in sync.\n" +
			"Requires a replica set or sharded source.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := cf.baseConfig()
			cfg.BufferSize = bufferSize
			cfg.ApplyBatchSize = applyBatchSize
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

			if !eps.src.Info.SupportsChangeStreams {
				return fmt.Errorf("source %s does not support change streams", eps.src.Info.String())
			}

			nss, err := resolveNamespaces(ctx, eps.src, cfg, log)
			if err != nil {
				return err
			}

			reporter := newReporter(log)
			tl := cdc.New(eps.src, eps.dst, reporter, log)
			if err := tl.Open(ctx, engine.TailOptions{
				Namespaces:     nss,
				BufferSize:     cfg.BufferSize,
				ApplyBatchSize: cfg.ApplyBatchSize,
			}); err != nil {
				return err
			}
			defer tl.Close(context.Background())

			// Report stats periodically until the context is cancelled.
			go func() {
				t := time.NewTicker(2 * time.Second)
				defer t.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-t.C:
						s := tl.Stats()
						log.Info("tail", "captured", s.Captured, "applied", s.Applied,
							"buffered", s.Buffered, "lag", s.Lag.Round(time.Millisecond))
					}
				}
			}()

			fmt.Fprintln(cmd.ErrOrStderr(), "tailing… press Ctrl-C to stop")
			err = tl.Apply(ctx)
			s := tl.Stats()
			fmt.Fprintf(cmd.OutOrStdout(), "\nstopped. captured=%d applied=%d (inserts=%d deletes=%d)\n",
				s.Captured, s.Applied, s.Inserts, s.Deletes)
			if err != nil && ctx.Err() == nil {
				return err
			}
			return nil
		},
	}
	cf.register(cmd)
	fl := cmd.Flags()
	fl.IntVar(&bufferSize, "buffer-size", 100000, "max buffered change events")
	fl.IntVar(&applyBatchSize, "apply-batch-size", 500, "change events per bulk apply")
	return cmd
}
