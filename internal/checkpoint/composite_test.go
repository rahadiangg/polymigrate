package checkpoint

import (
	"context"
	"errors"
	"testing"
)

// errStore is a Store whose operations return preconfigured errors, used to
// exercise Composite's error-handling and fallback paths.
type errStore struct {
	saveErr   error
	loadErr   error
	deleteErr error
}

func (e *errStore) Save(context.Context, State) error { return e.saveErr }
func (e *errStore) Load(context.Context, string) (State, bool, error) {
	return State{}, false, e.loadErr
}
func (e *errStore) Delete(context.Context, string) error { return e.deleteErr }

func hasState(t *testing.T, s Store, runID string) bool {
	t.Helper()
	_, found, err := s.Load(context.Background(), runID)
	if err != nil {
		t.Fatalf("Load(%q): %v", runID, err)
	}
	return found
}

func TestComposite_SaveWritesToAllStores(t *testing.T) {
	primary := NewMemStore()
	sec1 := NewMemStore()
	sec2 := NewMemStore()
	c := NewComposite(primary, sec1, sec2)

	if err := c.Save(context.Background(), sampleState("run-all")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	for name, s := range map[string]Store{"primary": primary, "sec1": sec1, "sec2": sec2} {
		if !hasState(t, s, "run-all") {
			t.Errorf("%s did not receive the save", name)
		}
	}
}

func TestComposite_LoadPrefersPrimary(t *testing.T) {
	primary := NewMemStore()
	secondary := NewMemStore()

	pState := sampleState("run-p")
	pState.Phase = "primary-phase"
	sState := sampleState("run-p")
	sState.Phase = "secondary-phase"
	_ = primary.Save(context.Background(), pState)
	_ = secondary.Save(context.Background(), sState)

	c := NewComposite(primary, secondary)
	got, found, err := c.Load(context.Background(), "run-p")
	if err != nil || !found {
		t.Fatalf("Load: found=%v err=%v", found, err)
	}
	if got.Phase != "primary-phase" {
		t.Errorf("Load returned %q, want primary-phase", got.Phase)
	}
}

func TestComposite_LoadFallsBackToSecondary(t *testing.T) {
	primary := NewMemStore() // empty
	secondary := NewMemStore()
	_ = secondary.Save(context.Background(), sampleState("run-fb"))

	c := NewComposite(primary, secondary)
	got, found, err := c.Load(context.Background(), "run-fb")
	if err != nil || !found {
		t.Fatalf("Load: found=%v err=%v", found, err)
	}
	if got.RunID != "run-fb" {
		t.Errorf("RunID = %q", got.RunID)
	}
}

func TestComposite_LoadPrimaryErrorFallsThrough(t *testing.T) {
	primary := &errStore{loadErr: errors.New("primary down")}
	secondary := NewMemStore()
	_ = secondary.Save(context.Background(), sampleState("run-err"))

	c := NewComposite(primary, secondary)
	got, found, err := c.Load(context.Background(), "run-err")
	if err != nil {
		t.Fatalf("Load: unexpected err %v (primary error should not abort fallback)", err)
	}
	if !found || got.RunID != "run-err" {
		t.Errorf("Load: found=%v runID=%q, want found via secondary", found, got.RunID)
	}
}

func TestComposite_LoadNoneFound(t *testing.T) {
	c := NewComposite(NewMemStore(), NewMemStore())
	_, found, err := c.Load(context.Background(), "missing")
	if err != nil {
		t.Fatalf("Load none: err = %v, want nil", err)
	}
	if found {
		t.Error("Load none: found = true")
	}
}

func TestComposite_LoadAllErrorReturnsJoinedError(t *testing.T) {
	pErr := errors.New("primary boom")
	sErr := errors.New("secondary boom")
	c := NewComposite(&errStore{loadErr: pErr}, &errStore{loadErr: sErr})

	_, found, err := c.Load(context.Background(), "x")
	if found {
		t.Error("found = true with all stores erroring")
	}
	if !errors.Is(err, pErr) || !errors.Is(err, sErr) {
		t.Fatalf("err = %v, want to join both store errors", err)
	}
}

func TestComposite_SavePrimaryErrorDominates(t *testing.T) {
	primary := NewMemStore()
	sentinel := errors.New("primary save failed")
	primary.SaveHook = func(State) error { return sentinel }
	sec1 := NewMemStore()
	sec2 := NewMemStore()

	c := NewComposite(primary, sec1, sec2)
	err := c.Save(context.Background(), sampleState("run-pd"))
	if !errors.Is(err, sentinel) {
		t.Fatalf("Save err = %v, want wrapping the primary sentinel", err)
	}
	// Despite the primary failure, secondaries must still have been written.
	if !hasState(t, sec1, "run-pd") || !hasState(t, sec2, "run-pd") {
		t.Error("secondaries not written when primary failed")
	}
}

func TestComposite_SaveSecondaryErrorSurfacedPrimaryOk(t *testing.T) {
	primary := NewMemStore()
	secErr := errors.New("secondary save failed")
	c := NewComposite(primary, &errStore{saveErr: secErr})

	err := c.Save(context.Background(), sampleState("run-se"))
	if !errors.Is(err, secErr) {
		t.Fatalf("Save err = %v, want to surface the secondary error", err)
	}
	// The primary still holds the data.
	if !hasState(t, primary, "run-se") {
		t.Error("primary not written")
	}
}

func TestComposite_DeleteAll(t *testing.T) {
	primary := NewMemStore()
	sec := NewMemStore()
	c := NewComposite(primary, sec)
	ctx := context.Background()

	if err := c.Save(ctx, sampleState("run-del")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := c.Delete(ctx, "run-del"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if hasState(t, primary, "run-del") || hasState(t, sec, "run-del") {
		t.Error("Delete did not remove from all stores")
	}
}

func TestComposite_DeleteErrorsJoinedPrimaryFirst(t *testing.T) {
	pErr := errors.New("primary delete boom")
	sErr := errors.New("secondary delete boom")
	c := NewComposite(&errStore{deleteErr: pErr}, &errStore{deleteErr: sErr})

	err := c.Delete(context.Background(), "x")
	if !errors.Is(err, pErr) || !errors.Is(err, sErr) {
		t.Fatalf("Delete err = %v, want to join both store errors", err)
	}
}
