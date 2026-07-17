package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/rahadiangg/polymigrate/internal/checkpoint"
	"github.com/rahadiangg/polymigrate/internal/config"
	"github.com/rahadiangg/polymigrate/internal/mongox"
)

// defaultCheckpointDir is where the local file mirror of checkpoints lives.
const defaultCheckpointDir = ".poly-checkpoints"

// buildCheckpointStore returns a composite store that writes the destination
// (authoritative, co-located with the data) and a local file mirror (fast,
// survives loss of the destination). dst may be nil, in which case only the file
// store is used (e.g. for `poly status` without connecting).
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
	return "poly-" + configHash(cfg)[:12]
}

// inspectStore builds the store used by `status` and `checkpoint clear`. When a
// destination URI is supplied it returns the same Composite that `migrate` uses
// (authoritative destination MongoStore + local file mirror), so status reflects
// — and clear removes — the authoritative checkpoint. Without a destination it
// falls back to the local mirror alone and returns a warning to surface that the
// mirror may lag, and that clear will NOT remove the destination's copy.
func inspectStore(ctx context.Context, dstURI, dstFlavor, dir string) (store checkpoint.Store, closer func(), warning string, err error) {
	file := checkpoint.NewFileStore(dir)
	if dstURI == "" {
		return file, func() {}, "using the local checkpoint mirror only; pass --dst to read/clear the authoritative destination checkpoint", nil
	}
	c, err := mongox.Connect(ctx, dstURI, mongox.ConnectOptions{FlavorOverride: mongox.Flavor(dstFlavor)})
	if err != nil {
		return nil, nil, "", fmt.Errorf("connect destination: %w", err)
	}
	store = checkpoint.NewComposite(checkpoint.NewMongoStore(c, "", ""), file)
	return store, func() { _ = c.Close(context.Background()) }, "", nil
}
