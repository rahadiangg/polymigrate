package checkpoint

import (
	"bytes"
	"testing"
	"time"
)

// sampleState builds a fully-populated State for round-trip and clone tests.
func sampleState(runID string) State {
	return State{
		RunID:      runID,
		Mode:       "live",
		ConfigHash: "abc123",
		Phase:      "snapshot",
		Namespaces: map[string]NSProgress{
			"shop.orders":   {Done: true, LastID: []byte{0x01, 0x02}, DocsCopied: 42},
			"shop.products": {Done: false, LastID: []byte{0x0a}, DocsCopied: 7},
			"blog.posts":    {Done: false, DocsCopied: 0},
		},
		ResumeToken:  []byte{0xde, 0xad, 0xbe, 0xef},
		NonResumable: false,
		UpdatedAt:    time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC),
	}
}

func TestStateClone_DeepCopyIndependence(t *testing.T) {
	orig := sampleState("run-1")
	clone := orig.Clone()

	// Mutating the clone's map, per-namespace LastID, and ResumeToken must not
	// affect the original.
	clone.Namespaces["shop.orders"] = NSProgress{Done: false, DocsCopied: 999}
	clone.Namespaces["new.ns"] = NSProgress{Done: true}
	if p := clone.Namespaces["shop.products"]; len(p.LastID) > 0 {
		p.LastID[0] = 0xff
	}
	clone.ResumeToken[0] = 0x00

	if got := orig.Namespaces["shop.orders"]; !got.Done || got.DocsCopied != 42 {
		t.Errorf("original map entry mutated via clone: %+v", got)
	}
	if _, ok := orig.Namespaces["new.ns"]; ok {
		t.Error("clone added a key that leaked into the original map")
	}
	if orig.Namespaces["shop.products"].LastID[0] != 0x0a {
		t.Error("original LastID mutated via clone's shared backing array")
	}
	if !bytes.Equal(orig.ResumeToken, []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("original ResumeToken mutated via clone: %x", orig.ResumeToken)
	}
}

func TestStateClone_NilFields(t *testing.T) {
	// A zero-ish State (nil map, nil token) must clone without panicking and stay
	// equal.
	orig := State{RunID: "empty", Mode: "snapshot"}
	clone := orig.Clone()
	if clone.Namespaces != nil {
		t.Errorf("nil Namespaces became %v", clone.Namespaces)
	}
	if clone.ResumeToken != nil {
		t.Errorf("nil ResumeToken became %v", clone.ResumeToken)
	}
	if clone.RunID != "empty" || clone.Mode != "snapshot" {
		t.Errorf("scalar fields not copied: %+v", clone)
	}
}
