// Package cli wires the mm command tree. main() calls Execute; commands are
// defined here so they can be constructed and exercised in tests. Subcommands
// for migrate/verify/tail are registered by their own files in this package.
package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

// Build-time version information, overridable via -ldflags.
var (
	Version   = "dev"
	Commit    = "none"
	BuildDate = "unknown"
)

// Execute runs the root command and returns a process exit code.
func Execute() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	root := NewRootCmd()
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return 1
	}
	return 0
}

// NewRootCmd builds the root command with all subcommands attached.
func NewRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "mm",
		Short:         "mongomigrate — migrate MongoDB across flavors and topologies",
		Long: "mm migrates data between any MongoDB-compatible deployments\n" +
			"(standalone, replica set, sharded, Atlas, Huawei DDS, Percona) in any\n" +
			"direction, with a native parallel copier, change-stream CDC for\n" +
			"near-zero-downtime cutover, and a verifier that proves correctness.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.AddCommand(newVersionCmd())
	root.AddCommand(newDiscoverCmd())
	// registerMigrateCmd, registerVerifyCmd, registerTailCmd attach the remaining
	// subcommands (wired during integration once their engines exist).
	for _, reg := range subcommandRegistry {
		root.AddCommand(reg())
	}
	return root
}

// subcommandRegistry lets other files in the package add subcommands without
// editing NewRootCmd. Files append their constructor in an init().
var subcommandRegistry []func() *cobra.Command

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Fprintf(cmd.OutOrStdout(), "mm %s (commit %s, built %s)\n", Version, Commit, BuildDate)
		},
	}
}
