package config

import (
	"strings"
	"testing"
	"time"

	"github.com/rahadiangg/polymigrate/internal/namespace"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		mut     func(*Config)
		wantErr string
	}{
		{"missing source", func(c *Config) { c.Source.URI = "" }, "source.uri is required"},
		{"missing dest", func(c *Config) { c.Dest.URI = "" }, "dest.uri is required"},
		{"same uri", func(c *Config) { c.Dest.URI = c.Source.URI }, "must differ"},
		{"bad mode", func(c *Config) { c.Mode = "teleport" }, "unknown mode"},
		{"negative workers", func(c *Config) { c.Workers = -1 }, "workers must be >= 0"},
		{"bad glob", func(c *Config) { c.IncludeNamespaces = []string{""} }, "pattern"},
		{"ok", func(c *Config) {}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Default()
			c.Source.URI = "mongodb://src:27017"
			c.Dest.URI = "mongodb://dst:27017"
			tt.mut(&c)
			err := c.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestApplyDefaults(t *testing.T) {
	var c Config
	c.ApplyDefaults()
	if c.Mode != ModeSnapshot {
		t.Errorf("mode = %q", c.Mode)
	}
	if c.Workers != 4 {
		t.Errorf("workers = %d", c.Workers)
	}
	if c.BatchSize != 1000 {
		t.Errorf("batchSize = %d", c.BatchSize)
	}
	// Explicit value survives.
	c2 := Config{Workers: 16}
	c2.ApplyDefaults()
	if c2.Workers != 16 {
		t.Errorf("workers overwritten: %d", c2.Workers)
	}
}

// TestSampleSizeZeroSurvives covers the review fix: --sample-size 0 means "skip
// content sampling" and must NOT be silently reset to the default by ApplyDefaults.
func TestSampleSizeZeroSurvives(t *testing.T) {
	c := Config{SampleSize: 0}
	c.ApplyDefaults()
	if c.SampleSize != 0 {
		t.Errorf("SampleSize = %d after ApplyDefaults, want 0 (skip) preserved", c.SampleSize)
	}
	// A non-zero explicit value also survives.
	c2 := Config{SampleSize: 42}
	c2.ApplyDefaults()
	if c2.SampleSize != 42 {
		t.Errorf("SampleSize = %d, want 42", c2.SampleSize)
	}
}

func TestParseYAML(t *testing.T) {
	yamlDoc := `
source:
  uri: mongodb://src.dds.myhuaweicloud.com:8635/appdb
  flavorOverride: huawei-dds
dest:
  uri: mongodb://psmdb-rs0:27017/?replicaSet=rs0
mode: live
include:
  - appdb.*
exclude:
  - appdb.tmp
workers: 8
lagThreshold: 5s
`
	c, err := ParseYAML([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("ParseYAML: %v", err)
	}
	if c.Mode != ModeLive {
		t.Errorf("mode = %q", c.Mode)
	}
	if c.Source.FlavorOverride != "huawei-dds" {
		t.Errorf("flavorOverride = %q", c.Source.FlavorOverride)
	}
	if c.Workers != 8 {
		t.Errorf("workers = %d", c.Workers)
	}
	if c.LagThreshold != 5*time.Second {
		t.Errorf("lagThreshold = %v", c.LagThreshold)
	}
	f, err := c.Filter()
	if err != nil {
		t.Fatalf("Filter: %v", err)
	}
	// appdb.orders included, appdb.tmp excluded.
	if !f.Match(namespace.Namespace{DB: "appdb", Collection: "orders"}) {
		t.Error("appdb.orders should be included")
	}
	if f.Match(namespace.Namespace{DB: "appdb", Collection: "tmp"}) {
		t.Error("appdb.tmp should be excluded")
	}
}

func TestParseYAMLInvalid(t *testing.T) {
	if _, err := ParseYAML([]byte("source:\n  uri: x\n")); err == nil {
		t.Error("expected validation error for missing dest")
	}
}

func TestRedactURI(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"mongodb://user:pass@host:27017/db", "mongodb://***:***@host:27017/db"},
		{"mongodb+srv://u:p@cluster.mongodb.net/test", "mongodb+srv://***:***@cluster.mongodb.net/test"},
		{"mongodb://host:27017/db", "mongodb://host:27017/db"}, // no creds
		{"mongodb://u:p@a:1,b:2/db?x=y", "mongodb://***:***@a:1,b:2/db?x=y"},
	}
	for _, tt := range tests {
		if got := RedactURI(tt.in); got != tt.want {
			t.Errorf("RedactURI(%q) = %q want %q", tt.in, got, tt.want)
		}
	}
}
