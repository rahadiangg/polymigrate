package cli

import (
	"fmt"
	"sort"
	"time"

	"github.com/spf13/cobra"
)

func init() {
	subcommandRegistry = append(subcommandRegistry, newStatusCmd, newCheckpointCmd)
}

func newStatusCmd() *cobra.Command {
	var (
		runID, checkpointDir string
		dstURI, dstFlavor    string
	)
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the checkpoint (resume) status of a migration run",
		Long: "status reports per-collection snapshot progress, the saved change-stream\n" +
			"token, and whether the run can be resumed. Pass --dst to read the\n" +
			"authoritative checkpoint co-located with the destination; otherwise the\n" +
			"local mirror is used, which may lag.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if runID == "" {
				return fmt.Errorf("--run-id is required (see the run id printed by `poly migrate`)")
			}
			ctx, cancel := contextWithTimeout(cmd.Context(), 20*time.Second)
			defer cancel()
			store, closer, warning, err := inspectStore(ctx, dstURI, dstFlavor, checkpointDir)
			if err != nil {
				return err
			}
			defer closer()
			out := cmd.OutOrStdout()
			if warning != "" {
				fmt.Fprintf(cmd.ErrOrStderr(), "note: %s\n", warning)
			}
			st, found, err := store.Load(ctx, runID)
			if err != nil {
				return err
			}
			if !found {
				fmt.Fprintf(out, "no checkpoint found for run %q\n", runID)
				return nil
			}
			fmt.Fprintf(out, "run:        %s\n", st.RunID)
			fmt.Fprintf(out, "mode:       %s\n", st.Mode)
			fmt.Fprintf(out, "phase:      %s\n", st.Phase)
			fmt.Fprintf(out, "updated:    %s\n", st.UpdatedAt.Format("2006-01-02 15:04:05 MST"))
			fmt.Fprintf(out, "resumable:  %v\n", !st.NonResumable)
			fmt.Fprintf(out, "cdc token:  %v\n", len(st.ResumeToken) > 0)
			names := make([]string, 0, len(st.Namespaces))
			for n := range st.Namespaces {
				names = append(names, n)
			}
			sort.Strings(names)
			fmt.Fprintln(out, "collections:")
			for _, n := range names {
				p := st.Namespaces[n]
				state := "in-progress"
				if p.Done {
					state = "done"
				}
				fmt.Fprintf(out, "  %-30s %-12s %d docs\n", n, state, p.DocsCopied)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&runID, "run-id", "", "checkpoint run id (required)")
	cmd.Flags().StringVar(&checkpointDir, "checkpoint-dir", defaultCheckpointDir, "local checkpoint directory")
	cmd.Flags().StringVar(&dstURI, "dst", "", "destination URI, to read the authoritative checkpoint")
	cmd.Flags().StringVar(&dstFlavor, "dst-flavor", "", "override detected destination flavor")
	return cmd
}

func newCheckpointCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "checkpoint", Short: "Manage migration checkpoints"}
	var (
		runID, checkpointDir string
		dstURI, dstFlavor    string
	)
	clear := &cobra.Command{
		Use:   "clear",
		Short: "Delete a run's checkpoint (forces a fresh migration next time)",
		Long: "clear removes the checkpoint so the next migration starts fresh. Pass --dst\n" +
			"to also delete the authoritative checkpoint stored on the destination;\n" +
			"without it, only the local mirror is removed and a --resume could still\n" +
			"resume from the destination's copy.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if runID == "" {
				return fmt.Errorf("--run-id is required")
			}
			ctx, cancel := contextWithTimeout(cmd.Context(), 20*time.Second)
			defer cancel()
			store, closer, warning, err := inspectStore(ctx, dstURI, dstFlavor, checkpointDir)
			if err != nil {
				return err
			}
			defer closer()
			if warning != "" {
				fmt.Fprintf(cmd.ErrOrStderr(), "note: %s\n", warning)
			}
			if err := store.Delete(ctx, runID); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "cleared checkpoint %q\n", runID)
			return nil
		},
	}
	clear.Flags().StringVar(&runID, "run-id", "", "checkpoint run id (required)")
	clear.Flags().StringVar(&checkpointDir, "checkpoint-dir", defaultCheckpointDir, "local checkpoint directory")
	clear.Flags().StringVar(&dstURI, "dst", "", "destination URI, to also clear the authoritative checkpoint")
	clear.Flags().StringVar(&dstFlavor, "dst-flavor", "", "override detected destination flavor")
	cmd.AddCommand(clear)
	return cmd
}
