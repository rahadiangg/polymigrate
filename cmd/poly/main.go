// Command poly is the CLI for polymigrate, a tool for migrating data between
// any MongoDB-compatible deployments — standalone, replica set, sharded, Atlas,
// Huawei DDS, and Percona — in any direction.
package main

import (
	"os"

	"github.com/rahadiangg/polymigrate/internal/cli"
)

func main() {
	os.Exit(cli.Execute())
}
