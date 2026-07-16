package cli

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"github.com/weefer/mongomigrate/internal/checkpoint"
)

func init() {
	subcommandRegistry = append(subcommandRegistry, newStatusCmd, newCheckpointCmd)
}

func newStatusCmd() *cobra.Command {
	var (
		runID         string
		checkpointDir string
	)
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the checkpoint (resume) status of a migration run",
		Long: "status reads the local checkpoint mirror and reports per-collection\n" +
			"snapshot progress, the saved change-stream token, and whether the run can\n" +
			"be resumed.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if runID == "" {
				return fmt.Errorf("--run-id is required (see the run id printed by `mm migrate`)")
			}
			store := checkpoint.NewFileStore(checkpointDir)
			st, found, err := store.Load(cmd.Context(), runID)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if !found {
				fmt.Fprintf(out, "no checkpoint found for run %q in %s\n", runID, checkpointDir)
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
	return cmd
}

func newCheckpointCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "checkpoint",
		Short: "Manage migration checkpoints",
	}
	var (
		runID         string
		checkpointDir string
	)
	clear := &cobra.Command{
		Use:   "clear",
		Short: "Delete a run's local checkpoint (forces a fresh migration next time)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if runID == "" {
				return fmt.Errorf("--run-id is required")
			}
			store := checkpoint.NewFileStore(checkpointDir)
			if err := store.Delete(cmd.Context(), runID); err != nil {
				return err
			}
			fmt.Fprintf(cmd.OutOrStdout(), "cleared checkpoint %q\n", runID)
			return nil
		},
	}
	clear.Flags().StringVar(&runID, "run-id", "", "checkpoint run id (required)")
	clear.Flags().StringVar(&checkpointDir, "checkpoint-dir", defaultCheckpointDir, "local checkpoint directory")
	cmd.AddCommand(clear)
	return cmd
}
