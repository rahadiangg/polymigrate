// Command mm is the CLI for mongomigrate, a MongoDB-to-MongoDB migration tool
// that works across standalone, replica set, sharded, Atlas, Huawei DDS, and
// Percona deployments in any direction.
package main

import (
	"os"

	"github.com/weefer/mongomigrate/internal/cli"
)

func main() {
	os.Exit(cli.Execute())
}
