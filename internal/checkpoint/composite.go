package checkpoint

import (
	"context"
	"errors"
	"fmt"
)

// Composite fans a Store's operations out across a primary store and any number
// of secondary stores. The primary is authoritative: its Save/Load result
// dominates, while the secondaries act as mirrors (e.g. a fast local file mirror
// alongside a durable destination-resident store).
//
// Save writes to every store so all mirrors stay current; Load returns the first
// store that has the checkpoint, trying the primary first; Delete removes the
// checkpoint from every store.
type Composite struct {
	primary Store
	others  []Store
}

// NewComposite builds a Composite over a primary store and zero or more
// secondaries.
func NewComposite(primary Store, others ...Store) *Composite {
	return &Composite{primary: primary, others: others}
}

// Save writes the State to every store. It returns the joined error of all
// failures with the primary's failure first, so a primary failure always
// dominates the returned error (errors.Is against the primary's error stays
// true) while secondary failures remain visible.
func (c *Composite) Save(ctx context.Context, s State) error {
	var errs []error
	if err := c.primary.Save(ctx, s); err != nil {
		errs = append(errs, fmt.Errorf("checkpoint: primary save: %w", err))
	}
	for i, o := range c.others {
		if err := o.Save(ctx, s); err != nil {
			errs = append(errs, fmt.Errorf("checkpoint: secondary %d save: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

// Load returns the checkpoint from the first store that has it, trying the
// primary first and then each secondary in order. A store that errors does not
// abort the search — the next store is tried — so a transient primary outage
// still allows loading from a mirror. When no store has the checkpoint, the
// joined errors (if any) are returned with found == false.
func (c *Composite) Load(ctx context.Context, runID string) (State, bool, error) {
	var errs []error
	if s, found, err := c.primary.Load(ctx, runID); err != nil {
		errs = append(errs, fmt.Errorf("checkpoint: primary load: %w", err))
	} else if found {
		return s, true, nil
	}
	for i, o := range c.others {
		if s, found, err := o.Load(ctx, runID); err != nil {
			errs = append(errs, fmt.Errorf("checkpoint: secondary %d load: %w", i, err))
		} else if found {
			return s, true, nil
		}
	}
	return State{}, false, errors.Join(errs...)
}

// Delete removes the checkpoint from every store, returning the joined error of
// all failures with the primary's failure first.
func (c *Composite) Delete(ctx context.Context, runID string) error {
	var errs []error
	if err := c.primary.Delete(ctx, runID); err != nil {
		errs = append(errs, fmt.Errorf("checkpoint: primary delete: %w", err))
	}
	for i, o := range c.others {
		if err := o.Delete(ctx, runID); err != nil {
			errs = append(errs, fmt.Errorf("checkpoint: secondary %d delete: %w", i, err))
		}
	}
	return errors.Join(errs...)
}

var _ Store = (*Composite)(nil)
