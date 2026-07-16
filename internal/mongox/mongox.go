// Package mongox wraps the official MongoDB Go driver with the connection and
// discovery helpers the migration tool needs. Discovery classifies a deployment
// by topology (standalone / replica set / sharded) and flavor (community
// MongoDB, Atlas, Huawei DDS, Percona, and a few other managed API-compatible
// services), which drives compatibility decisions elsewhere: change streams
// require a replica set or sharded cluster, and managed services hide the
// admin/local/config databases.
package mongox

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/weefer/mongomigrate/internal/namespace"
)

// TopologyKind classifies the deployment shape.
type TopologyKind string

const (
	TopologyUnknown    TopologyKind = "unknown"
	TopologyStandalone TopologyKind = "standalone"
	TopologyReplicaSet TopologyKind = "replicaset"
	TopologySharded    TopologyKind = "sharded"
)

// Flavor classifies the server vendor / managed service.
type Flavor string

const (
	FlavorUnknown    Flavor = "unknown"
	FlavorMongoDB    Flavor = "mongodb"    // community / enterprise self-managed
	FlavorAtlas      Flavor = "atlas"      // MongoDB Atlas
	FlavorHuaweiDDS  Flavor = "huawei-dds" // Huawei Cloud Document Database Service
	FlavorPercona    Flavor = "percona"    // Percona Server for MongoDB
	FlavorCosmosDB   Flavor = "cosmosdb"   // Azure Cosmos DB Mongo API
	FlavorDocumentDB Flavor = "documentdb" // AWS DocumentDB
)

// ServerInfo is the result of discovery.
type ServerInfo struct {
	Version               string
	VersionArray          []int
	Topology              TopologyKind
	Flavor                Flavor
	SetName               string
	IsWritablePrimary     bool
	SupportsChangeStreams bool
	Host                  string
}

// SupportsChangeStreamsFor is exported so callers can explain the decision.
func (s ServerInfo) String() string {
	return fmt.Sprintf("%s %s (%s, changeStreams=%v)", s.Flavor, s.Version, s.Topology, s.SupportsChangeStreams)
}

// Client wraps a connected *mongo.Client along with its origin URI and, once
// discovered, its ServerInfo.
type Client struct {
	*mongo.Client
	URI  string
	Info ServerInfo
}

// ConnectOptions tune Connect.
type ConnectOptions struct {
	AppName                string
	ServerSelectionTimeout time.Duration
	ConnectTimeout         time.Duration
	// FlavorOverride, when non-empty, forces the discovered flavor (useful for
	// managed services that are indistinguishable over the wire, like DDS).
	FlavorOverride Flavor
}

// Connect dials the URI, verifies the connection with a ping, and runs discovery.
func Connect(ctx context.Context, uri string, co ConnectOptions) (*Client, error) {
	if co.ServerSelectionTimeout == 0 {
		co.ServerSelectionTimeout = 15 * time.Second
	}
	if co.ConnectTimeout == 0 {
		co.ConnectTimeout = 15 * time.Second
	}
	if co.AppName == "" {
		co.AppName = "mongomigrate"
	}

	opts := options.Client().
		ApplyURI(uri).
		SetAppName(co.AppName).
		SetServerSelectionTimeout(co.ServerSelectionTimeout).
		SetConnectTimeout(co.ConnectTimeout)

	mc, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, co.ServerSelectionTimeout)
	defer cancel()
	if err := mc.Ping(pingCtx, readpref.Primary()); err != nil {
		_ = mc.Disconnect(context.Background())
		return nil, fmt.Errorf("ping: %w", err)
	}

	c := &Client{Client: mc, URI: uri}
	info, err := c.Discover(ctx)
	if err != nil {
		_ = mc.Disconnect(context.Background())
		return nil, fmt.Errorf("discover: %w", err)
	}
	if co.FlavorOverride != "" {
		// Override the flavor AND recompute any flavor-dependent capability, so
		// forcing e.g. cosmosdb/documentdb correctly disables change streams.
		info.Flavor = co.FlavorOverride
		info.SupportsChangeStreams = supportsChangeStreams(info.Topology, info.VersionArray, info.Flavor)
	}
	c.Info = info
	return c, nil
}

// Close disconnects the client.
func (c *Client) Close(ctx context.Context) error {
	if c.Client == nil {
		return nil
	}
	return c.Client.Disconnect(ctx)
}

// Discover runs hello + buildInfo and classifies the deployment. It is safe to
// call more than once.
func (c *Client) Discover(ctx context.Context) (ServerInfo, error) {
	admin := c.Client.Database("admin")

	var hello bson.M
	if err := admin.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello); err != nil {
		// Older servers (< 5.0), including DDS 4.4, may not implement hello.
		if err2 := admin.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&hello); err2 != nil {
			return ServerInfo{}, fmt.Errorf("hello/isMaster: %w", err)
		}
	}

	var buildInfo bson.M
	if err := admin.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo); err != nil {
		// buildInfo can be blocked on locked-down managed users; degrade rather
		// than fail — topology alone is still useful.
		buildInfo = bson.M{}
	}

	host := hostFromURI(c.URI)
	return analyze(host, hello, buildInfo), nil
}

// analyze is the pure classification core; kept separate so it is unit-testable
// without a live server.
func analyze(host string, hello, buildInfo bson.M) ServerInfo {
	info := ServerInfo{Host: host}
	info.Version, _ = buildInfo["version"].(string)
	info.VersionArray = versionArray(buildInfo, info.Version)
	info.Topology = detectTopology(hello)
	info.SetName, _ = hello["setName"].(string)
	if v, ok := hello["isWritablePrimary"].(bool); ok {
		info.IsWritablePrimary = v
	} else if v, ok := hello["ismaster"].(bool); ok {
		info.IsWritablePrimary = v
	}
	info.Flavor = detectFlavor(host, buildInfo)
	info.SupportsChangeStreams = supportsChangeStreams(info.Topology, info.VersionArray, info.Flavor)
	return info
}

// detectTopology classifies the deployment from a hello/isMaster reply.
func detectTopology(hello bson.M) TopologyKind {
	if msg, _ := hello["msg"].(string); msg == "isdbgrid" {
		return TopologySharded
	}
	if setName, _ := hello["setName"].(string); setName != "" {
		return TopologyReplicaSet
	}
	// A hosts array without a setName is unusual; treat presence of setName as
	// the authoritative replica-set signal and default to standalone otherwise.
	return TopologyStandalone
}

// detectFlavor classifies the vendor. buildInfo is authoritative for Percona;
// managed services are indistinguishable in buildInfo, so the connection host
// is the primary signal for them.
func detectFlavor(host string, buildInfo bson.M) Flavor {
	// Percona exposes a dedicated field in buildInfo.
	if _, ok := buildInfo["psmdbVersion"]; ok {
		return FlavorPercona
	}
	if modules, ok := buildInfo["modules"].(bson.A); ok {
		for _, m := range modules {
			if s, ok := m.(string); ok && strings.Contains(strings.ToLower(s), "percona") {
				return FlavorPercona
			}
		}
	}
	if v, _ := buildInfo["version"].(string); strings.Contains(strings.ToLower(v), "percona") {
		return FlavorPercona
	}

	h := strings.ToLower(host)
	switch {
	case strings.HasSuffix(h, ".mongodb.net"):
		return FlavorAtlas
	case strings.Contains(h, "huaweicloud") || strings.Contains(h, ".dds."):
		return FlavorHuaweiDDS
	case strings.Contains(h, "cosmos.azure.com") || strings.Contains(h, "documents.azure.com"):
		return FlavorCosmosDB
	case strings.Contains(h, "docdb.amazonaws.com") || strings.Contains(h, "docdb-elastic.amazonaws.com"):
		return FlavorDocumentDB
	}

	if len(buildInfo) == 0 {
		return FlavorUnknown
	}
	return FlavorMongoDB
}

// supportsChangeStreams reports whether CDC via change streams is possible.
// Change streams require a replica set or sharded cluster on MongoDB 3.6+.
// Cosmos DB and DocumentDB emulate the API but have restricted or absent change
// stream support, so we do not assume it.
func supportsChangeStreams(topo TopologyKind, version []int, flavor Flavor) bool {
	if flavor == FlavorCosmosDB || flavor == FlavorDocumentDB {
		return false
	}
	if topo != TopologyReplicaSet && topo != TopologySharded {
		return false
	}
	return atLeast(version, 3, 6)
}

// versionArray extracts [major, minor, patch...] from buildInfo, falling back to
// parsing the version string.
func versionArray(buildInfo bson.M, version string) []int {
	if raw, ok := buildInfo["versionArray"].(bson.A); ok {
		out := make([]int, 0, len(raw))
		for _, e := range raw {
			out = append(out, toInt(e))
		}
		return out
	}
	return parseVersion(version)
}

// parseVersion parses "4.4.0-rc1" style strings into an int slice, stopping at
// the first non-numeric component.
func parseVersion(v string) []int {
	v = strings.TrimSpace(v)
	if v == "" {
		return nil
	}
	// Drop any pre-release / build suffix.
	if i := strings.IndexAny(v, "-+ "); i >= 0 {
		v = v[:i]
	}
	var out []int
	for part := range strings.SplitSeq(v, ".") {
		n, err := strconv.Atoi(part)
		if err != nil {
			break
		}
		out = append(out, n)
	}
	return out
}

// atLeast reports whether version >= the given major.minor.
func atLeast(version []int, major, minor int) bool {
	if len(version) == 0 {
		return false
	}
	if version[0] != major {
		return version[0] > major
	}
	if len(version) < 2 {
		return minor == 0
	}
	return version[1] >= minor
}

func toInt(v any) int {
	switch n := v.(type) {
	case int32:
		return int(n)
	case int64:
		return int(n)
	case int:
		return n
	case float64:
		return int(n)
	default:
		return 0
	}
}

// hostFromURI extracts the first host (without port) from a MongoDB connection
// string, tolerating credentials, multiple seed hosts, options, and the +srv
// scheme. Returns "" when no host can be found.
func hostFromURI(uri string) string {
	s := uri
	if i := strings.Index(s, "://"); i >= 0 {
		s = s[i+3:]
	}
	// Strip credentials.
	if i := strings.LastIndex(s, "@"); i >= 0 {
		s = s[i+1:]
	}
	// Cut at path or options.
	if i := strings.IndexAny(s, "/?"); i >= 0 {
		s = s[:i]
	}
	// First seed host.
	if i := strings.IndexByte(s, ','); i >= 0 {
		s = s[:i]
	}
	// Strip port. IPv6 hosts are bracketed: [::1]:27017.
	if strings.HasPrefix(s, "[") {
		if i := strings.IndexByte(s, ']'); i >= 0 {
			return s[1:i]
		}
		return s
	}
	if i := strings.LastIndex(s, ":"); i >= 0 {
		s = s[:i]
	}
	return s
}

// SkippedNamespace is a namespace that ListNamespaces did not return because it
// is not a plain data collection (e.g. a view or a time-series collection, whose
// data lives in a companion system.buckets.* collection the copier cannot read
// directly). Callers should surface these so an operator knows they were not
// migrated rather than assuming a clean sweep.
type SkippedNamespace struct {
	Namespace namespace.Namespace
	Type      string // the collection spec Type, e.g. "view", "timeseries"
}

// ListNamespaces enumerates every collection across every database the user can
// see, then applies the filter. It returns the matched plain collections and,
// separately, any namespaces skipped because they are not plain collections
// (views, time-series) so the caller can warn about them. System databases are
// handled by the filter's ExcludeSystem flag.
func (c *Client) ListNamespaces(ctx context.Context, filter *namespace.Filter) (matched []namespace.Namespace, skipped []SkippedNamespace, err error) {
	dbNames, err := c.Client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, nil, fmt.Errorf("list databases: %w", err)
	}

	for _, dbName := range dbNames {
		db := c.Client.Database(dbName)
		specs, err := db.ListCollectionSpecifications(ctx, bson.D{})
		if err != nil {
			return nil, nil, fmt.Errorf("list collections in %q: %w", dbName, err)
		}
		for _, sp := range specs {
			ns := namespace.Namespace{DB: dbName, Collection: sp.Name}
			// Only plain collections hold directly-copyable data. Report views and
			// time-series collections as skipped (but only if they would otherwise
			// have matched the filter, so we do not warn about irrelevant ones).
			if sp.Type != "" && sp.Type != "collection" {
				if filter == nil || filter.Match(ns) {
					skipped = append(skipped, SkippedNamespace{Namespace: ns, Type: sp.Type})
				}
				continue
			}
			if filter == nil || filter.Match(ns) {
				matched = append(matched, ns)
			}
		}
	}
	namespace.Sort(matched)
	return matched, skipped, nil
}
