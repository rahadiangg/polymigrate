// Package config models the tool's configuration: endpoints, namespace filters,
// and per-phase tuning. It can be assembled from CLI flags or loaded from a YAML
// file, and validates itself before any connection is made.
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/rahadiangg/polymigrate/internal/namespace"
)

// Mode selects what a migration run does.
type Mode string

const (
	// ModeSnapshot is a one-shot bulk copy (writes must be frozen for consistency).
	ModeSnapshot Mode = "snapshot"
	// ModeLive is snapshot + change-stream CDC + near-zero-downtime cutover.
	ModeLive Mode = "live"
)

// Endpoint describes one side of a migration.
type Endpoint struct {
	URI            string `yaml:"uri"`
	FlavorOverride string `yaml:"flavorOverride,omitempty"`
}

// Config is the full tool configuration.
type Config struct {
	Source Endpoint `yaml:"source"`
	Dest   Endpoint `yaml:"dest"`

	Mode Mode `yaml:"mode"`

	// Namespace selection.
	IncludeNamespaces []string `yaml:"include,omitempty"`
	ExcludeNamespaces []string `yaml:"exclude,omitempty"`
	IncludeSystem     bool     `yaml:"includeSystem,omitempty"`

	// Snapshot tuning.
	Workers       int  `yaml:"workers,omitempty"`
	BatchSize     int  `yaml:"batchSize,omitempty"`
	ReadBatchSize int  `yaml:"readBatchSize,omitempty"`
	DropTarget    bool `yaml:"dropTarget,omitempty"`
	CopyIndexes   bool `yaml:"copyIndexes,omitempty"`

	// CDC tuning.
	BufferSize     int `yaml:"bufferSize,omitempty"`
	ApplyBatchSize int `yaml:"applyBatchSize,omitempty"`

	// Cutover.
	LagThreshold time.Duration `yaml:"lagThreshold,omitempty"`
	AutoCutover  bool          `yaml:"autoCutover,omitempty"`

	// Verification.
	VerifyCounts  bool `yaml:"verifyCounts,omitempty"`
	VerifyIndexes bool `yaml:"verifyIndexes,omitempty"`
	SampleSize    int  `yaml:"sampleSize,omitempty"`

	// Logging.
	LogLevel string `yaml:"logLevel,omitempty"`
	LogJSON  bool   `yaml:"logJSON,omitempty"`
}

// Default returns a Config with sensible defaults applied.
func Default() Config {
	return Config{
		Mode:           ModeSnapshot,
		Workers:        4,
		BatchSize:      1000,
		ReadBatchSize:  0,
		CopyIndexes:    true,
		BufferSize:     100000,
		ApplyBatchSize: 500,
		LagThreshold:   2 * time.Second,
		VerifyCounts:   true,
		VerifyIndexes:  true,
		SampleSize:     100,
		LogLevel:       "info",
	}
}

// ApplyDefaults fills any unset (zero) numeric/string fields with defaults.
// Booleans are left as-is because their zero value is meaningful.
func (c *Config) ApplyDefaults() {
	d := Default()
	if c.Mode == "" {
		c.Mode = d.Mode
	}
	if c.Workers == 0 {
		c.Workers = d.Workers
	}
	if c.BatchSize == 0 {
		c.BatchSize = d.BatchSize
	}
	if c.BufferSize == 0 {
		c.BufferSize = d.BufferSize
	}
	if c.ApplyBatchSize == 0 {
		c.ApplyBatchSize = d.ApplyBatchSize
	}
	if c.LagThreshold == 0 {
		c.LagThreshold = d.LagThreshold
	}
	// SampleSize is intentionally NOT defaulted here: 0 is a meaningful value that
	// means "skip content sampling". The CLI flag carries its own default (100)
	// for the unset case, so an explicit --sample-size 0 must survive.
	if c.LogLevel == "" {
		c.LogLevel = d.LogLevel
	}
}

// Validate checks the configuration for internal consistency.
func (c *Config) Validate() error {
	var errs []error
	if c.Source.URI == "" {
		errs = append(errs, errors.New("source.uri is required"))
	}
	if c.Dest.URI == "" {
		errs = append(errs, errors.New("dest.uri is required"))
	}
	if c.Source.URI != "" && c.Source.URI == c.Dest.URI {
		errs = append(errs, errors.New("source.uri and dest.uri must differ"))
	}
	switch c.Mode {
	case ModeSnapshot, ModeLive, "":
	default:
		errs = append(errs, fmt.Errorf("unknown mode %q (want snapshot or live)", c.Mode))
	}
	if c.Workers < 0 {
		errs = append(errs, errors.New("workers must be >= 0"))
	}
	if c.BatchSize < 0 {
		errs = append(errs, errors.New("batchSize must be >= 0"))
	}
	if c.SampleSize < 0 {
		errs = append(errs, errors.New("sampleSize must be >= 0"))
	}
	// Validate glob patterns compile.
	if _, err := c.Filter(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Filter builds a namespace.Filter from the include/exclude configuration.
// System databases are excluded unless IncludeSystem is set.
func (c *Config) Filter() (*namespace.Filter, error) {
	return namespace.NewFilter(c.IncludeNamespaces, c.ExcludeNamespaces, !c.IncludeSystem)
}

// LoadYAML reads, parses, defaults, and validates a YAML config file.
func LoadYAML(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}
	c, err := ParseYAML(data)
	if err != nil {
		return Config{}, fmt.Errorf("config %q: %w", path, err)
	}
	return c, nil
}

// ParseYAML parses config bytes, applies defaults, and validates.
func ParseYAML(data []byte) (Config, error) {
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return Config{}, fmt.Errorf("parse: %w", err)
	}
	c.ApplyDefaults()
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Redacted returns a copy of the config with credentials stripped from URIs, for
// safe logging.
func (c Config) Redacted() Config {
	out := c
	out.Source.URI = RedactURI(c.Source.URI)
	out.Dest.URI = RedactURI(c.Dest.URI)
	return out
}

// RedactURI removes the userinfo (user:pass@) portion of a MongoDB URI so it is
// safe to log.
func RedactURI(uri string) string {
	scheme := ""
	rest := uri
	if i := strings.Index(uri, "://"); i >= 0 {
		scheme = uri[:i+3]
		rest = uri[i+3:]
	}
	// Only the authority section (before the first '/') may contain userinfo.
	authority := rest
	tail := ""
	if slash := strings.IndexByte(rest, '/'); slash >= 0 {
		authority, tail = rest[:slash], rest[slash:]
	}
	if at := strings.LastIndexByte(authority, '@'); at >= 0 {
		return scheme + "***:***@" + authority[at+1:] + tail
	}
	return uri
}
