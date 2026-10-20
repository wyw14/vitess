/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtctld

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// TabletHealthAPIPath is the HTTP path of the tablet health aggregation API.
const TabletHealthAPIPath = "/api/tablet_health"

// TabletManagerHealthProber adapts a tmclient.TabletManagerClient to the
// discovery.TabletHealthProber interface by probing liveness through the
// tabletmanager Ping RPC.
type TabletManagerHealthProber struct {
	tmc tmclient.TabletManagerClient
}

// NewTabletManagerHealthProber returns a TabletHealthProber that pings tablets
// through the tabletmanager RPC (e.g. the tabletmanager Ping RPC).
func NewTabletManagerHealthProber(tmc tmclient.TabletManagerClient) *TabletManagerHealthProber {
	return &TabletManagerHealthProber{tmc: tmc}
}

// ProbeTabletHealth implements discovery.TabletHealthProber.
func (p *TabletManagerHealthProber) ProbeTabletHealth(ctx context.Context, tablet *topodatapb.Tablet) error {
	return p.tmc.Ping(ctx, tablet)
}

// TabletHealthAPIOption configures the tablet health HTTP API.
type TabletHealthAPIOption func(*tabletHealthAPI)

// WithTabletHealthLookup attaches a cached health lookup (typically a
// discovery.HealthCheck, as used by vtcombo). When set, the aggregation
// enriches probe results with the health version, replication lag and serving
// state observed through vttablet's StreamHealth stream.
func WithTabletHealthLookup(lookup discovery.TabletHealthLookup) TabletHealthAPIOption {
	return func(api *tabletHealthAPI) {
		api.lookup = lookup
	}
}

// WithTabletHealthProbeTimeout overrides the default per-tablet probe timeout.
// The effective timeout can still be overridden per request with the "timeout"
// query parameter.
func WithTabletHealthProbeTimeout(timeout time.Duration) TabletHealthAPIOption {
	return func(api *tabletHealthAPI) {
		api.defaultTimeout = timeout
	}
}

// RegisterTabletHealthHandler registers the tablet health aggregation API on
// the servenv HTTP mux. It returns the aggregated health of all tablets (or a
// subset selected by the keyspace, shard and cell query parameters), grouped
// by keyspace/shard and stably sorted. Each tablet is probed independently
// with its own deadline, so an unreachable tablet cannot block other shards.
func RegisterTabletHealthHandler(ts *topo.Server, prober discovery.TabletHealthProber, opts ...TabletHealthAPIOption) {
	servenv.HTTPHandle(TabletHealthAPIPath, newTabletHealthAPI(ts, prober, opts...))
}

// tabletHealthResponse is the JSON response body of the API.
type tabletHealthResponse struct {
	Shards discovery.TabletHealthReport `json:"shards"`
}

type tabletHealthAPI struct {
	ts             *topo.Server
	prober         discovery.TabletHealthProber
	lookup         discovery.TabletHealthLookup
	defaultTimeout time.Duration
}

func newTabletHealthAPI(ts *topo.Server, prober discovery.TabletHealthProber, opts ...TabletHealthAPIOption) *tabletHealthAPI {
	api := &tabletHealthAPI{
		ts:             ts,
		prober:         prober,
		defaultTimeout: discovery.DefaultTabletHealthProbeTimeout,
	}
	for _, opt := range opts {
		opt(api)
	}
	return api
}

// newTabletHealthHandler exposes the API as an http.Handler without touching
// the global servenv mux, which is what the unit tests use.
func newTabletHealthHandler(ts *topo.Server, prober discovery.TabletHealthProber, opts ...TabletHealthAPIOption) http.Handler {
	return newTabletHealthAPI(ts, prober, opts...)
}

// ServeHTTP implements http.Handler.
//
// Query parameters:
//   - keyspace: restrict the aggregation to one keyspace (default: all).
//   - shard: restrict the aggregation to one shard; requires keyspace.
//   - cell: comma-separated list of cells to consider (default: all).
//   - timeout: per-tablet probe timeout as a Go duration (default: 5s).
func (api *tabletHealthAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := acl.CheckAccessHTTP(r, acl.MONITORING); err != nil {
		acl.SendError(w, err)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keyspace := r.URL.Query().Get("keyspace")
	shard := r.URL.Query().Get("shard")
	if shard != "" && keyspace == "" {
		writeTabletHealthError(w, http.StatusBadRequest, "shard query parameter requires keyspace")
		return
	}
	cells := splitCellList(r.URL.Query().Get("cell"))

	timeout := api.defaultTimeout
	if timeoutParam := r.URL.Query().Get("timeout"); timeoutParam != "" {
		parsed, err := time.ParseDuration(timeoutParam)
		if err != nil || parsed <= 0 {
			writeTabletHealthError(w, http.StatusBadRequest, "timeout must be a positive Go duration, e.g. 5s")
			return
		}
		timeout = parsed
	}

	groups, status, err := api.tabletGroups(r.Context(), keyspace, shard, cells)
	if err != nil {
		writeTabletHealthError(w, status, err.Error())
		return
	}

	report := discovery.AggregateTabletHealth(r.Context(), groups, discovery.AggregateTabletHealthOptions{
		Prober:           api.prober,
		Lookup:           api.lookup,
		PerTabletTimeout: timeout,
	})

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(&tabletHealthResponse{Shards: report}); err != nil {
		log.Error("failed to encode tablet health response", slog.Any("error", err))
	}
}

// tabletGroups resolves the keyspace/shards to aggregate and the tablets of
// each shard. Partial results from unreachable cells are tolerated and logged.
func (api *tabletHealthAPI) tabletGroups(ctx context.Context, keyspace, shard string, cells []string) ([]discovery.TabletGroup, int, error) {
	keyspaces, err := api.targetKeyspaces(ctx, keyspace)
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return nil, http.StatusNotFound, err
		}
		return nil, http.StatusInternalServerError, err
	}

	var groups []discovery.TabletGroup
	for _, ks := range keyspaces {
		shards, err := api.targetShards(ctx, ks, shard)
		if err != nil {
			if topo.IsErrType(err, topo.NoNode) {
				return nil, http.StatusNotFound, err
			}
			return nil, http.StatusInternalServerError, err
		}
		for _, shardName := range shards {
			tabletMap, err := api.ts.GetTabletMapForShardByCell(ctx, ks, shardName, cells)
			if err != nil && !topo.IsErrType(err, topo.PartialResult) {
				return nil, http.StatusInternalServerError, err
			}
			if err != nil {
				log.Warn("tablet health API got partial tablet list",
					slog.String("keyspace", ks),
					slog.String("shard", shardName),
					slog.Any("error", err))
			}
			groups = append(groups, discovery.TabletGroup{
				Keyspace: ks,
				Shard:    shardName,
				Tablets:  sortedTablets(tabletMap),
			})
		}
	}
	return groups, http.StatusOK, nil
}

func (api *tabletHealthAPI) targetKeyspaces(ctx context.Context, keyspace string) ([]string, error) {
	if keyspace != "" {
		if _, err := api.ts.GetKeyspace(ctx, keyspace); err != nil {
			return nil, err
		}
		return []string{keyspace}, nil
	}
	return api.ts.GetKeyspaces(ctx)
}

func (api *tabletHealthAPI) targetShards(ctx context.Context, keyspace, shard string) ([]string, error) {
	if shard != "" {
		if _, err := api.ts.GetShard(ctx, keyspace, shard); err != nil {
			return nil, err
		}
		return []string{shard}, nil
	}
	return api.ts.GetShardNames(ctx, keyspace)
}

func sortedTablets(tabletMap map[string]*topo.TabletInfo) []*topodatapb.Tablet {
	tablets := make([]*topodatapb.Tablet, 0, len(tabletMap))
	for _, info := range tabletMap {
		tablets = append(tablets, info.Tablet)
	}
	sort.Slice(tablets, func(i, j int) bool {
		return topoproto.TabletAliasString(tablets[i].Alias) < topoproto.TabletAliasString(tablets[j].Alias)
	})
	return tablets
}

func splitCellList(cellParam string) []string {
	if cellParam == "" {
		return nil
	}
	cells := strings.Split(cellParam, ",")
	result := cells[:0]
	for _, cell := range cells {
		if cell = strings.TrimSpace(cell); cell != "" {
			result = append(result, cell)
		}
	}
	return result
}

func writeTabletHealthError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}
