package cli

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/weefer/mongomigrate/internal/engine"
	"github.com/weefer/mongomigrate/internal/verify"
)

func init() {
	subcommandRegistry = append(subcommandRegistry, newVerifyCmd)
}

func newVerifyCmd() *cobra.Command {
	var (
		cf         commonFlags
		sampleSize int
		noCounts   bool
		noIndexes  bool
	)
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify a destination matches its source (counts, indexes, sampled content)",
		Long: "verify proves a migration is correct: per-collection document counts,\n" +
			"per-collection index definitions, and a sampled document content hash\n" +
			"comparison. Counts matching is necessary but not sufficient — this checks\n" +
			"indexes and content too.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg := cf.baseConfig()
			cfg.SampleSize = sampleSize
			cfg.VerifyCounts = !noCounts
			cfg.VerifyIndexes = !noIndexes
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

			nss, err := resolveNamespaces(ctx, eps.src, cfg)
			if err != nil {
				return err
			}

			vf := verify.New(eps.src, eps.dst, log)
			report, err := vf.Verify(ctx, engine.VerifyOptions{
				Namespaces:   nss,
				CheckCounts:  cfg.VerifyCounts,
				CheckIndexes: cfg.VerifyIndexes,
				SampleSize:   cfg.SampleSize,
				Workers:      cfg.Workers,
			})
			if err != nil {
				return err
			}
			printVerify(cmd, report)
			if !report.Ok {
				return fmt.Errorf("verification FAILED")
			}
			return nil
		},
	}
	cf.register(cmd)
	fl := cmd.Flags()
	fl.IntVar(&sampleSize, "sample-size", 100, "documents hashed per collection (0 = skip content sampling)")
	fl.BoolVar(&noCounts, "no-counts", false, "skip the document-count check")
	fl.BoolVar(&noIndexes, "no-indexes", false, "skip the index-definition check")
	return cmd
}

func printVerify(cmd *cobra.Command, r engine.VerifyReport) {
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "\n=== verification report: %s ===\n", okString(r.Ok))
	for _, c := range r.Collections {
		fmt.Fprintf(out, "  %-30s %s\n", c.Namespace.String(), okString(c.Ok))
		fmt.Fprintf(out, "      counts: src=%d dst=%d match=%v", c.SrcCount, c.DstCount, c.CountsMatch)
		if c.SampleChecked > 0 {
			fmt.Fprintf(out, " | sample: %d checked, %d mismatch", c.SampleChecked, c.SampleMismatches)
		}
		fmt.Fprintf(out, " | indexes match=%v\n", c.IndexesMatch)
		if len(c.MissingIndexes) > 0 {
			fmt.Fprintf(out, "      missing indexes on dst: %v\n", c.MissingIndexes)
		}
		if len(c.ExtraIndexes) > 0 {
			fmt.Fprintf(out, "      extra indexes on dst:   %v\n", c.ExtraIndexes)
		}
		for _, n := range c.Notes {
			fmt.Fprintf(out, "      note: %s\n", n)
		}
	}
}

func okString(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}
