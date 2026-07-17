package cli

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/rahadiangg/polymigrate/internal/config"
)

func TestRootHasSubcommands(t *testing.T) {
	root := NewRootCmd()
	want := map[string]bool{"version": false, "discover": false, "migrate": false, "verify": false, "tail": false}
	for _, c := range root.Commands() {
		if _, ok := want[c.Name()]; ok {
			want[c.Name()] = true
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("subcommand %q not registered", name)
		}
	}
}

func TestVersionOutput(t *testing.T) {
	root := NewRootCmd()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{"version"})
	if err := root.Execute(); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("poly ")) {
		t.Errorf("version output = %q", buf.String())
	}
}

func TestContextWithTimeout(t *testing.T) {
	// Zero timeout -> no deadline.
	ctx, cancel := contextWithTimeout(context.Background(), 0)
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Error("expected no deadline for zero timeout")
	}
	// Positive timeout -> deadline set.
	ctx2, cancel2 := contextWithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel2()
	if _, ok := ctx2.Deadline(); !ok {
		t.Error("expected a deadline")
	}
}

func TestBaseConfigMapsFlags(t *testing.T) {
	cf := commonFlags{
		srcURI: "mongodb://a", dstURI: "mongodb://b",
		srcFlavor: "huawei-dds", include: []string{"shop.*"}, exclude: []string{"shop.tmp"},
		includeSystem: true, workers: 8, logLevel: "debug",
	}
	c := cf.baseConfig()
	if c.Source.URI != "mongodb://a" || c.Dest.URI != "mongodb://b" {
		t.Errorf("uris not mapped: %+v", c)
	}
	if c.Source.FlavorOverride != "huawei-dds" {
		t.Errorf("flavor override not mapped")
	}
	if c.Workers != 8 || !c.IncludeSystem || c.LogLevel != "debug" {
		t.Errorf("scalar flags not mapped: %+v", c)
	}
	if len(c.IncludeNamespaces) != 1 || len(c.ExcludeNamespaces) != 1 {
		t.Errorf("namespace filters not mapped")
	}
	// baseConfig should still validate cleanly with the defaults applied.
	c.ApplyDefaults()
	if err := c.Validate(); err != nil {
		t.Errorf("expected valid config: %v", err)
	}
	_ = config.ModeSnapshot
}

func TestOkString(t *testing.T) {
	if okString(true) != "PASS" || okString(false) != "FAIL" {
		t.Error("okString wrong")
	}
}
