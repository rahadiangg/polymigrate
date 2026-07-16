package namespace

import (
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	tests := []struct {
		in      string
		want    Namespace
		wantErr bool
	}{
		{"mydb.users", Namespace{"mydb", "users"}, false},
		{"mydb.system.views", Namespace{"mydb", "system.views"}, false},
		{"db.a.b.c", Namespace{"db", "a.b.c"}, false},
		{"nodot", Namespace{}, true},
		{".users", Namespace{}, true},
		{"mydb.", Namespace{}, true},
		{"", Namespace{}, true},
	}
	for _, tt := range tests {
		got, err := Parse(tt.in)
		if (err != nil) != tt.wantErr {
			t.Errorf("Parse(%q) err=%v wantErr=%v", tt.in, err, tt.wantErr)
			continue
		}
		if err == nil && got != tt.want {
			t.Errorf("Parse(%q) = %+v want %+v", tt.in, got, tt.want)
		}
	}
}

func TestString(t *testing.T) {
	if got := (Namespace{"db", "coll"}).String(); got != "db.coll" {
		t.Errorf("String() = %q", got)
	}
}

func TestIsSystem(t *testing.T) {
	tests := []struct {
		ns   Namespace
		want bool
	}{
		{Namespace{"admin", "system.users"}, true},
		{Namespace{"local", "oplog.rs"}, true},
		{Namespace{"config", "shards"}, true},
		{Namespace{"mydb", "system.views"}, true},
		{Namespace{"mydb", "users"}, false},
		{Namespace{"adminx", "users"}, false}, // not exactly "admin"
	}
	for _, tt := range tests {
		if got := tt.ns.IsSystem(); got != tt.want {
			t.Errorf("%s.IsSystem() = %v want %v", tt.ns, got, tt.want)
		}
	}
}

func TestFilterGlob(t *testing.T) {
	tests := []struct {
		name          string
		include       []string
		exclude       []string
		excludeSystem bool
		ns            Namespace
		want          bool
	}{
		{"empty includes everything", nil, nil, false, Namespace{"a", "b"}, true},
		{"db wildcard", []string{"shop.*"}, nil, false, Namespace{"shop", "orders"}, true},
		{"db wildcard miss", []string{"shop.*"}, nil, false, Namespace{"other", "orders"}, false},
		{"exact match", []string{"shop.orders"}, nil, false, Namespace{"shop", "orders"}, true},
		{"exact match miss", []string{"shop.orders"}, nil, false, Namespace{"shop", "users"}, false},
		{"exclude wins", []string{"shop.*"}, []string{"shop.secret"}, false, Namespace{"shop", "secret"}, false},
		{"exclude other kept", []string{"shop.*"}, []string{"shop.secret"}, false, Namespace{"shop", "orders"}, true},
		{"system dropped", nil, nil, true, Namespace{"admin", "system.users"}, false},
		{"system.* collection dropped", nil, nil, true, Namespace{"shop", "system.views"}, false},
		{"explicit include overrides system exclusion", []string{"admin.system.users"}, nil, true, Namespace{"admin", "system.users"}, true},
		{"cross-db wildcard", []string{"*.events"}, nil, false, Namespace{"anydb", "events"}, true},
		{"star matches dots", []string{"*.system.*"}, nil, false, Namespace{"shop", "system.js"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := NewFilter(tt.include, tt.exclude, tt.excludeSystem)
			if err != nil {
				t.Fatalf("NewFilter: %v", err)
			}
			if got := f.Match(tt.ns); got != tt.want {
				t.Errorf("Match(%s) = %v want %v", tt.ns, got, tt.want)
			}
		})
	}
}

func TestFilterInvalidPattern(t *testing.T) {
	// An empty include pattern is rejected.
	if _, err := NewFilter([]string{""}, nil, false); err == nil {
		t.Error("expected error for empty pattern")
	}
}

func TestZeroValueFilterMatches(t *testing.T) {
	var f Filter // zero value should behave as "include all"
	if !f.Match(Namespace{"a", "b"}) {
		t.Error("zero-value filter should match everything")
	}
}

func TestFilterApply(t *testing.T) {
	f, _ := NewFilter([]string{"shop.*"}, []string{"shop.secret"}, true)
	in := []Namespace{
		{"shop", "orders"},
		{"shop", "secret"},
		{"shop", "system.views"},
		{"other", "x"},
	}
	got := f.Apply(in)
	want := []Namespace{{"shop", "orders"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Apply() = %v want %v", got, want)
	}
}

func TestSort(t *testing.T) {
	ns := []Namespace{{"b", "z"}, {"a", "b"}, {"a", "a"}, {"b", "a"}}
	Sort(ns)
	want := []Namespace{{"a", "a"}, {"a", "b"}, {"b", "a"}, {"b", "z"}}
	if !reflect.DeepEqual(ns, want) {
		t.Errorf("Sort() = %v want %v", ns, want)
	}
}
