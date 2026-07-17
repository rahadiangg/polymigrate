package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/rahadiangg/polymigrate/internal/mongox"
)

// Default database and collection names for the checkpoint store. The store
// lives on the destination, co-located with the data it protects.
const (
	DefaultDB   = "_polymigrate"
	DefaultColl = "checkpoints"
)

// checkpointColl is the narrow set of collection operations MongoStore needs.
// The production adapter wraps a *mongo.Collection; unit tests substitute an
// in-memory fake so the store logic is testable without a live server.
type checkpointColl interface {
	// replaceOne upserts doc keyed by _id == id.
	replaceOne(ctx context.Context, id string, doc bson.Raw) error
	// findOne fetches the stored document for id. found is false with a nil error
	// when no document exists.
	findOne(ctx context.Context, id string) (doc bson.Raw, found bool, err error)
	// deleteOne removes the document for id. Deleting a missing document is not an
	// error.
	deleteOne(ctx context.Context, id string) error
}

// MongoStore persists State as a single document per run in a collection on the
// destination cluster, upserted keyed by RunID (_id).
//
// State.Namespaces is a map keyed by "db.coll"; those keys contain dots, which
// are unsafe as BSON field names across server versions, so the map is stored as
// an array of {ns,done,lastId,docsCopied} entries and reconstructed on load.
type MongoStore struct {
	coll checkpointColl
}

// NewMongoStore builds a MongoStore backed by cl's destination collection.
// Empty dbName/collName fall back to DefaultDB/DefaultColl.
func NewMongoStore(cl *mongox.Client, dbName, collName string) *MongoStore {
	if dbName == "" {
		dbName = DefaultDB
	}
	if collName == "" {
		collName = DefaultColl
	}
	return &MongoStore{
		coll: &realCheckpointColl{coll: cl.Client.Database(dbName).Collection(collName)},
	}
}

// Save upserts the State document keyed by RunID.
func (ms *MongoStore) Save(ctx context.Context, s State) error {
	raw, err := bson.Marshal(stateToDoc(s))
	if err != nil {
		return fmt.Errorf("checkpoint: marshal state %q: %w", s.RunID, err)
	}
	if err := ms.coll.replaceOne(ctx, s.RunID, raw); err != nil {
		return fmt.Errorf("checkpoint: save %q: %w", s.RunID, err)
	}
	return nil
}

// Load fetches the State for runID. A missing document yields (State{}, false, nil).
func (ms *MongoStore) Load(ctx context.Context, runID string) (State, bool, error) {
	raw, found, err := ms.coll.findOne(ctx, runID)
	if err != nil {
		return State{}, false, fmt.Errorf("checkpoint: load %q: %w", runID, err)
	}
	if !found {
		return State{}, false, nil
	}
	var doc checkpointDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return State{}, false, fmt.Errorf("checkpoint: unmarshal state %q: %w", runID, err)
	}
	return docToState(doc), true, nil
}

// Delete removes the checkpoint document for runID.
func (ms *MongoStore) Delete(ctx context.Context, runID string) error {
	if err := ms.coll.deleteOne(ctx, runID); err != nil {
		return fmt.Errorf("checkpoint: delete %q: %w", runID, err)
	}
	return nil
}

// checkpointDoc is the BSON representation of State. It mirrors State exactly
// except that the Namespaces map is flattened into an array (see nsEntry) to
// avoid dotted BSON field names.
type checkpointDoc struct {
	RunID        string        `bson:"_id"`
	Mode         string        `bson:"mode"`
	ConfigHash   string        `bson:"configHash"`
	Phase        string        `bson:"phase"`
	Namespaces   []nsEntry     `bson:"namespaces"`
	ResumeToken  []byte        `bson:"resumeToken,omitempty"`
	NonResumable bool          `bson:"nonResumable,omitempty"`
	UpdatedAt    bson.DateTime `bson:"updatedAt"`
}

// nsEntry is one namespace's snapshot progress in array form.
type nsEntry struct {
	NS         string `bson:"ns"`
	Done       bool   `bson:"done"`
	LastID     []byte `bson:"lastId,omitempty"`
	DocsCopied int64  `bson:"docsCopied"`
}

// stateToDoc converts a State into its BSON document form.
func stateToDoc(s State) checkpointDoc {
	return checkpointDoc{
		RunID:        s.RunID,
		Mode:         s.Mode,
		ConfigHash:   s.ConfigHash,
		Phase:        s.Phase,
		Namespaces:   namespacesToEntries(s.Namespaces),
		ResumeToken:  s.ResumeToken,
		NonResumable: s.NonResumable,
		UpdatedAt:    bson.NewDateTimeFromTime(s.UpdatedAt),
	}
}

// docToState converts a BSON document back into a State.
func docToState(d checkpointDoc) State {
	return State{
		RunID:        d.RunID,
		Mode:         d.Mode,
		ConfigHash:   d.ConfigHash,
		Phase:        d.Phase,
		Namespaces:   entriesToNamespaces(d.Namespaces),
		ResumeToken:  d.ResumeToken,
		NonResumable: d.NonResumable,
		UpdatedAt:    d.UpdatedAt.Time().UTC(),
	}
}

// namespacesToEntries flattens the per-namespace map into a stable, sorted
// array. Sorting by namespace makes the stored document deterministic, which
// keeps saves idempotent and eases debugging.
func namespacesToEntries(m map[string]NSProgress) []nsEntry {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]nsEntry, 0, len(keys))
	for _, k := range keys {
		p := m[k]
		out = append(out, nsEntry{
			NS:         k,
			Done:       p.Done,
			LastID:     p.LastID,
			DocsCopied: p.DocsCopied,
		})
	}
	return out
}

// entriesToNamespaces rebuilds the per-namespace map from its array form. An
// empty array yields a nil map, matching a fresh State.
func entriesToNamespaces(entries []nsEntry) map[string]NSProgress {
	if len(entries) == 0 {
		return nil
	}
	out := make(map[string]NSProgress, len(entries))
	for _, e := range entries {
		out[e.NS] = NSProgress{
			Done:       e.Done,
			LastID:     e.LastID,
			DocsCopied: e.DocsCopied,
		}
	}
	return out
}

// realCheckpointColl adapts a *mongo.Collection to checkpointColl.
type realCheckpointColl struct {
	coll *mongo.Collection
}

func (c *realCheckpointColl) replaceOne(ctx context.Context, id string, doc bson.Raw) error {
	_, err := c.coll.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: id}},
		doc,
		options.Replace().SetUpsert(true))
	return err
}

func (c *realCheckpointColl) findOne(ctx context.Context, id string) (bson.Raw, bool, error) {
	raw, err := c.coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Raw()
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return raw, true, nil
}

func (c *realCheckpointColl) deleteOne(ctx context.Context, id string) error {
	_, err := c.coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
	return err
}

var (
	_ Store          = (*MongoStore)(nil)
	_ checkpointColl = (*realCheckpointColl)(nil)
)
