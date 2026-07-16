package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/weefer/mongomigrate/internal/checkpoint"
	"github.com/weefer/mongomigrate/internal/config"
	"github.com/weefer/mongomigrate/internal/mongox"
)

// defaultCheckpointDir is where the local file mirror of checkpoints lives.
const defaultCheckpointDir = ".mm-checkpoints"

// buildCheckpointStore returns a composite store that writes the destination
// (authoritative, co-located with the data) and a local file mirror (fast,
// survives loss of the destination). dst may be nil, in which case only the file
// store is used (e.g. for `mm status` without connecting).
func buildCheckpointStore(dst *mongox.Client, dir string) checkpoint.Store {
	file := checkpoint.NewFileStore(dir)
	if dst == nil {
		return file
	}
	mongo := checkpoint.NewMongoStore(dst, "", "")
	return checkpoint.NewComposite(mongo, file)
}

// configHash is a stable fingerprint of the endpoints + namespace filter. It
// guards a --resume against a changed configuration. Credentials are redacted
// first so rotating a password does not invalidate a checkpoint.
func configHash(cfg config.Config) string {
	inc := append([]string(nil), cfg.IncludeNamespaces...)
	exc := append([]string(nil), cfg.ExcludeNamespaces...)
	sort.Strings(inc)
	sort.Strings(exc)
	parts := []string{
		config.RedactURI(cfg.Source.URI),
		config.RedactURI(cfg.Dest.URI),
		"inc:" + strings.Join(inc, ","),
		"exc:" + strings.Join(exc, ","),
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])
}

// defaultRunID derives a stable, human-recognisable run id from the configuration
// so a bare re-run of the same migration finds its checkpoint.
func defaultRunID(cfg config.Config) string {
	return "mm-" + configHash(cfg)[:12]
}
