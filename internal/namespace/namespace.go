// Package namespace models MongoDB namespaces (db.collection) and provides
// include/exclude filtering with glob patterns. It is a leaf package with no
// dependencies on the rest of the tool, so every other package can import it
// freely.
package namespace

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
)

// Namespace identifies a collection by database and collection name.
type Namespace struct {
	DB         string
	Collection string
}

// Parse splits a "db.collection" string into a Namespace. The collection part
// may itself contain dots (MongoDB allows them), so only the first dot splits.
func Parse(s string) (Namespace, error) {
	db, coll, ok := strings.Cut(s, ".")
	if !ok {
		return Namespace{}, fmt.Errorf("namespace %q is missing a '.' separator", s)
	}
	if db == "" || coll == "" {
		return Namespace{}, fmt.Errorf("namespace %q has an empty database or collection", s)
	}
	return Namespace{DB: db, Collection: coll}, nil
}

// String renders the namespace as "db.collection".
func (n Namespace) String() string {
	return n.DB + "." + n.Collection
}

// IsZero reports whether the namespace is the empty value.
func (n Namespace) IsZero() bool {
	return n.DB == "" && n.Collection == ""
}

// SystemDatabases are the internal databases that managed MongoDB providers
// (Atlas, Huawei DDS) refuse to export and that must never be migrated as data.
// Restoring admin.system.users in particular overwrites destination auth.
var SystemDatabases = []string{"admin", "local", "config"}

// IsSystem reports whether the namespace lives in a system database or is an
// internal system collection (name begins with "system.").
func (n Namespace) IsSystem() bool {
	if slices.Contains(SystemDatabases, n.DB) {
		return true
	}
	return strings.HasPrefix(n.Collection, "system.")
}

// Sort orders namespaces by database then collection, in place.
func Sort(ns []Namespace) {
	sort.Slice(ns, func(i, j int) bool {
		if ns[i].DB != ns[j].DB {
			return ns[i].DB < ns[j].DB
		}
		return ns[i].Collection < ns[j].Collection
	})
}

// Filter selects namespaces by glob patterns. A pattern matches against the
// full "db.collection" string; the single wildcard '*' matches any run of
// characters (including dots). Empty include lists mean "include everything".
//
// Precedence: a namespace is kept when it matches at least one include pattern
// (or Include is empty) AND matches no exclude pattern. When ExcludeSystem is
// true, system databases and system.* collections are dropped regardless of the
// include patterns unless they are named by an explicit include pattern.
type Filter struct {
	Include       []string
	Exclude       []string
	ExcludeSystem bool

	includeRE []*regexp.Regexp
	excludeRE []*regexp.Regexp
	compiled  bool
}

// NewFilter compiles a Filter from include/exclude patterns. It fails fast on an
// invalid glob so misconfiguration surfaces before any data is touched.
func NewFilter(include, exclude []string, excludeSystem bool) (*Filter, error) {
	f := &Filter{Include: include, Exclude: exclude, ExcludeSystem: excludeSystem}
	if err := f.compile(); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *Filter) compile() error {
	f.includeRE = f.includeRE[:0]
	f.excludeRE = f.excludeRE[:0]
	for _, p := range f.Include {
		re, err := globToRegexp(p)
		if err != nil {
			return fmt.Errorf("include pattern %q: %w", p, err)
		}
		f.includeRE = append(f.includeRE, re)
	}
	for _, p := range f.Exclude {
		re, err := globToRegexp(p)
		if err != nil {
			return fmt.Errorf("exclude pattern %q: %w", p, err)
		}
		f.excludeRE = append(f.excludeRE, re)
	}
	f.compiled = true
	return nil
}

// Match reports whether the namespace passes the filter.
func (f *Filter) Match(n Namespace) bool {
	if !f.compiled {
		// Zero-value Filter: compile lazily so it is safe to use directly.
		if err := f.compile(); err != nil {
			return false
		}
	}

	// literallyIncluded is set only when a wildcard-free include pattern names
	// this exact namespace. Matching via a wildcard (e.g. "shop.*") does not
	// count, so system collections are not resurrected by a broad include.
	literallyIncluded := false
	if len(f.includeRE) > 0 {
		matched := false
		for i, re := range f.includeRE {
			if re.MatchString(n.String()) {
				matched = true
				if !strings.Contains(f.Include[i], "*") {
					literallyIncluded = true
				}
			}
		}
		if !matched {
			return false
		}
	}

	// System namespaces are dropped unless a literal include names them.
	if f.ExcludeSystem && n.IsSystem() && !literallyIncluded {
		return false
	}

	for _, re := range f.excludeRE {
		if re.MatchString(n.String()) {
			return false
		}
	}
	return true
}

// Apply returns the subset of ns that passes the filter, order preserved.
func (f *Filter) Apply(ns []Namespace) []Namespace {
	out := make([]Namespace, 0, len(ns))
	for _, n := range ns {
		if f.Match(n) {
			out = append(out, n)
		}
	}
	return out
}

// globToRegexp converts a glob (only '*' is special) into an anchored regexp.
// Each '*' becomes ".*"; every other character is matched literally.
func globToRegexp(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, fmt.Errorf("empty pattern")
	}
	parts := strings.Split(pattern, "*")
	for i, p := range parts {
		parts[i] = regexp.QuoteMeta(p)
	}
	return regexp.Compile("^" + strings.Join(parts, ".*") + "$")
}
