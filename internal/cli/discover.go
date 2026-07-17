package cli

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/rahadiangg/polymigrate/internal/mongox"
)

func newDiscoverCmd() *cobra.Command {
	var (
		uri            string
		flavorOverride string
		timeout        time.Duration
	)
	cmd := &cobra.Command{
		Use:   "discover",
		Short: "Connect to a MongoDB URI and report its topology and flavor",
		Long: "discover classifies an endpoint (standalone / replica set / sharded and\n" +
			"community MongoDB / Atlas / Huawei DDS / Percona) and reports whether it\n" +
			"can drive change streams for live CDC migration.",
		RunE: func(cmd *cobra.Command, _ []string) error {
			if uri == "" {
				return fmt.Errorf("--uri is required")
			}
			ctx, cancel := contextWithTimeout(cmd.Context(), timeout)
			defer cancel()

			client, err := mongox.Connect(ctx, uri, mongox.ConnectOptions{
				FlavorOverride: mongox.Flavor(flavorOverride),
			})
			if err != nil {
				return err
			}
			defer client.Close(ctx)

			info := client.Info
			out := cmd.OutOrStdout()
			fmt.Fprintf(out, "host:            %s\n", info.Host)
			fmt.Fprintf(out, "flavor:          %s\n", info.Flavor)
			fmt.Fprintf(out, "version:         %s\n", info.Version)
			fmt.Fprintf(out, "topology:        %s\n", info.Topology)
			if info.SetName != "" {
				fmt.Fprintf(out, "replica set:     %s\n", info.SetName)
			}
			fmt.Fprintf(out, "change streams:  %v\n", info.SupportsChangeStreams)
			if !info.SupportsChangeStreams {
				fmt.Fprintf(out, "                 (live CDC migration unavailable — use --mode snapshot)\n")
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&uri, "uri", "", "MongoDB connection URI (required)")
	cmd.Flags().StringVar(&flavorOverride, "flavor", "", "override detected flavor (e.g. huawei-dds)")
	cmd.Flags().DurationVar(&timeout, "timeout", 20*time.Second, "discovery timeout")
	return cmd
}
