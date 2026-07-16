package mongox

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestHostFromURI(t *testing.T) {
	tests := []struct {
		uri  string
		want string
	}{
		{"mongodb://localhost:27017", "localhost"},
		{"mongodb://user:pass@db.example.com:27017/mydb", "db.example.com"},
		{"mongodb+srv://user:pass@cluster0.abcde.mongodb.net/test?retryWrites=true", "cluster0.abcde.mongodb.net"},
		{"mongodb://a.host:27017,b.host:27017,c.host:27017/?replicaSet=rs0", "a.host"},
		{"mongodb://user:p@ss@host:8635/admin", "host"}, // password containing '@'
		{"mongodb://[::1]:27017/db", "::1"},
		{"mongodb://[2001:db8::1]:27017", "2001:db8::1"},
		{"mongodb://plainhost", "plainhost"},
		{"", ""},
	}
	for _, tt := range tests {
		if got := hostFromURI(tt.uri); got != tt.want {
			t.Errorf("hostFromURI(%q) = %q want %q", tt.uri, got, tt.want)
		}
	}
}

func TestDetectTopology(t *testing.T) {
	tests := []struct {
		name  string
		hello bson.M
		want  TopologyKind
	}{
		{"sharded mongos", bson.M{"msg": "isdbgrid"}, TopologySharded},
		{"replica set", bson.M{"setName": "rs0", "isWritablePrimary": true}, TopologyReplicaSet},
		{"standalone", bson.M{"isWritablePrimary": true}, TopologyStandalone},
		{"empty", bson.M{}, TopologyStandalone},
	}
	for _, tt := range tests {
		if got := detectTopology(tt.hello); got != tt.want {
			t.Errorf("%s: detectTopology = %v want %v", tt.name, got, tt.want)
		}
	}
}

func TestDetectFlavor(t *testing.T) {
	tests := []struct {
		name      string
		host      string
		buildInfo bson.M
		want      Flavor
	}{
		{"percona via psmdbVersion", "10.0.0.1", bson.M{"version": "7.0.5-3", "psmdbVersion": "7.0.5-3"}, FlavorPercona},
		{"percona via version string", "10.0.0.1", bson.M{"version": "6.0.0-percona"}, FlavorPercona},
		{"percona via modules", "10.0.0.1", bson.M{"version": "7.0.0", "modules": bson.A{"percona-server"}}, FlavorPercona},
		{"atlas via host", "cluster0.abcde.mongodb.net", bson.M{"version": "7.0.0"}, FlavorAtlas},
		{"huawei dds via host", "192.168.0.1.dds.myhuaweicloud.com", bson.M{"version": "4.4.0"}, FlavorHuaweiDDS},
		{"cosmos via host", "acct.mongo.cosmos.azure.com", bson.M{"version": "4.0.0"}, FlavorCosmosDB},
		{"documentdb via host", "docdb.cluster.docdb.amazonaws.com", bson.M{"version": "5.0.0"}, FlavorDocumentDB},
		{"plain mongodb", "10.0.0.1", bson.M{"version": "7.0.0"}, FlavorMongoDB},
		{"unknown when no buildInfo", "10.0.0.1", bson.M{}, FlavorUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := detectFlavor(tt.host, tt.buildInfo); got != tt.want {
				t.Errorf("detectFlavor(%q) = %v want %v", tt.host, got, tt.want)
			}
		})
	}
}

func TestSupportsChangeStreams(t *testing.T) {
	tests := []struct {
		name    string
		topo    TopologyKind
		version []int
		flavor  Flavor
		want    bool
	}{
		{"replicaset 4.4 mongodb", TopologyReplicaSet, []int{4, 4, 0}, FlavorMongoDB, true},
		{"replicaset 5.0 percona", TopologyReplicaSet, []int{5, 0, 29}, FlavorPercona, true},
		{"sharded 6.0", TopologySharded, []int{6, 0, 0}, FlavorMongoDB, true},
		{"replicaset 3.6 boundary", TopologyReplicaSet, []int{3, 6, 0}, FlavorMongoDB, true},
		{"replicaset 3.4 too old", TopologyReplicaSet, []int{3, 4, 0}, FlavorMongoDB, false},
		{"standalone never", TopologyStandalone, []int{7, 0, 0}, FlavorMongoDB, false},
		{"cosmos never", TopologyReplicaSet, []int{4, 0, 0}, FlavorCosmosDB, false},
		{"documentdb never", TopologyReplicaSet, []int{5, 0, 0}, FlavorDocumentDB, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := supportsChangeStreams(tt.topo, tt.version, tt.flavor); got != tt.want {
				t.Errorf("supportsChangeStreams = %v want %v", got, tt.want)
			}
		})
	}
}

func TestVersionArray(t *testing.T) {
	tests := []struct {
		name      string
		buildInfo bson.M
		version   string
		want      []int
	}{
		{"from versionArray int32", bson.M{"versionArray": bson.A{int32(4), int32(4), int32(0), int32(0)}}, "4.4.0", []int{4, 4, 0, 0}},
		{"from versionArray float", bson.M{"versionArray": bson.A{float64(7), float64(0), float64(5)}}, "7.0.5", []int{7, 0, 5}},
		{"from version string", bson.M{}, "5.0.29", []int{5, 0, 29}},
		{"version with suffix", bson.M{}, "6.0.0-rc1", []int{6, 0, 0}},
		{"empty", bson.M{}, "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := versionArray(tt.buildInfo, tt.version)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("versionArray = %v want %v", got, tt.want)
			}
		})
	}
}

func TestAtLeast(t *testing.T) {
	tests := []struct {
		v          []int
		maj, min   int
		want       bool
	}{
		{[]int{4, 4}, 3, 6, true},
		{[]int{3, 6}, 3, 6, true},
		{[]int{3, 4}, 3, 6, false},
		{[]int{5, 0}, 3, 6, true},
		{[]int{2, 6}, 3, 6, false},
		{nil, 3, 6, false},
		{[]int{4}, 4, 0, true},
	}
	for _, tt := range tests {
		if got := atLeast(tt.v, tt.maj, tt.min); got != tt.want {
			t.Errorf("atLeast(%v, %d, %d) = %v want %v", tt.v, tt.maj, tt.min, got, tt.want)
		}
	}
}

func TestAnalyzeIntegration(t *testing.T) {
	// End-to-end of the pure classifier: a Percona replica set on 7.0.
	hello := bson.M{"setName": "rs0", "isWritablePrimary": true}
	buildInfo := bson.M{"version": "7.0.5-3", "psmdbVersion": "7.0.5-3", "versionArray": bson.A{int32(7), int32(0), int32(5)}}
	info := analyze("psmdb-rs0-0.svc", hello, buildInfo)
	if info.Flavor != FlavorPercona {
		t.Errorf("flavor = %v", info.Flavor)
	}
	if info.Topology != TopologyReplicaSet {
		t.Errorf("topology = %v", info.Topology)
	}
	if !info.SupportsChangeStreams {
		t.Error("expected change streams supported")
	}
	if info.SetName != "rs0" {
		t.Errorf("setName = %q", info.SetName)
	}
}
