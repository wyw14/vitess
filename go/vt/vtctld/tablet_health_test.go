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
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// scriptedTabletProber is a TabletHealthProber for API tests: it can fail or
// hang (until the probe context is canceled) specific tablet aliases.
type scriptedTabletProber struct {
	mu   sync.Mutex
	fail map[string]error
	hang map[string]bool
}

func (p *scriptedTabletProber) ProbeTabletHealth(ctx context.Context, tablet *topodatapb.Tablet) error {
	alias := topoproto.TabletAliasString(tablet.Alias)
	p.mu.Lock()
	shouldHang := p.hang[alias]
	err := p.fail[alias]
	p.mu.Unlock()
	if shouldHang {
		<-ctx.Done()
		return ctx.Err()
	}
	return err
}

func testTablet(uid uint32, keyspace, shard string) *topodatapb.Tablet {
	tablet := topo.NewTablet(uid, "cell", fmt.Sprintf("host%d", uid))
	tablet.Keyspace = keyspace
	tablet.Shard = shard
	tablet.Type = topodatapb.TabletType_REPLICA
	return tablet
}

func setupTabletHealthTopo(t *testing.T) *topo.Server {
	t.Helper()
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell")
	t.Cleanup(ts.Close)

	require.NoError(t, ts.CreateKeyspace(ctx, "ks1", &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateKeyspace(ctx, "ks2", &topodatapb.Keyspace{}))
	require.NoError(t, ts.CreateShard(ctx, "ks1", "-"))
	require.NoError(t, ts.CreateShard(ctx, "ks1", "80-c0"))
	require.NoError(t, ts.CreateShard(ctx, "ks2", "-"))

	for _, tablet := range []*topodatapb.Tablet{
		testTablet(1, "ks1", "-"),
		testTablet(2, "ks1", "-"),
		testTablet(3, "ks1", "80-c0"),
		testTablet(4, "ks2", "-"),
	} {
		require.NoError(t, ts.CreateTablet(ctx, tablet))
	}
	return ts
}

type tabletHealthAPIResponse struct {
	Shards discovery.TabletHealthReport `json:"shards"`
	Error  string                       `json:"error"`
}

func getTabletHealth(t *testing.T, handler http.Handler, target string) (int, *tabletHealthAPIResponse) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil).WithContext(t.Context())
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	resp := &tabletHealthAPIResponse{}
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), resp), "response body: %s", rec.Body.String())
	}
	return rec.Code, resp
}

// TestTabletHealthAPIAllShards verifies aggregation over every keyspace/shard
// and the stable keyspace/shard ordering.
func TestTabletHealthAPIAllShards(t *testing.T) {
	ts := setupTabletHealthTopo(t)
	handler := newTabletHealthHandler(ts, &scriptedTabletProber{}, WithTabletHealthProbeTimeout(time.Second))

	status, resp := getTabletHealth(t, handler, TabletHealthAPIPath+"?timeout=2s")
	require.Equal(t, http.StatusOK, status, resp.Error)

	require.Len(t, resp.Shards, 3)
	assert.Equal(t, [2]string{"ks1", "-"}, [2]string{resp.Shards[0].Keyspace, resp.Shards[0].Shard})
	assert.Equal(t, [2]string{"ks1", "80-c0"}, [2]string{resp.Shards[1].Keyspace, resp.Shards[1].Shard})
	assert.Equal(t, [2]string{"ks2", "-"}, [2]string{resp.Shards[2].Keyspace, resp.Shards[2].Shard})

	assert.Len(t, resp.Shards[0].Tablets, 2)
	assert.Len(t, resp.Shards[1].Tablets, 1)
	assert.Len(t, resp.Shards[2].Tablets, 1)

	for _, shard := range resp.Shards {
		for _, tablet := range shard.Tablets {
			assert.True(t, tablet.Serving, "tablet %s/%s should be serving after a successful probe", shard.Keyspace, tablet.Alias)
			assert.Empty(t, tablet.UnavailableReason)
			assert.Equal(t, discovery.LagUnknown, tablet.ReplicationLagClass, "probe-only results carry no lag data")
		}
	}
	// Tablets within a shard are sorted by alias.
	assert.Equal(t, "cell-0000000001", topoproto.TabletAliasString(resp.Shards[0].Tablets[0].Alias))
	assert.Equal(t, "cell-0000000002", topoproto.TabletAliasString(resp.Shards[0].Tablets[1].Alias))
}

// TestTabletHealthAPIFilters checks the keyspace/shard/cell query parameters.
func TestTabletHealthAPIFilters(t *testing.T) {
	ts := setupTabletHealthTopo(t)
	handler := newTabletHealthHandler(ts, &scriptedTabletProber{})

	status, resp := getTabletHealth(t, handler, TabletHealthAPIPath+"?keyspace=ks1")
	require.Equal(t, http.StatusOK, status, resp.Error)
	require.Len(t, resp.Shards, 2)
	assert.Equal(t, "ks1", resp.Shards[0].Keyspace)
	assert.Equal(t, "ks1", resp.Shards[1].Keyspace)

	status, resp = getTabletHealth(t, handler, TabletHealthAPIPath+"?keyspace=ks1&shard=80-c0")
	require.Equal(t, http.StatusOK, status, resp.Error)
	require.Len(t, resp.Shards, 1)
	assert.Equal(t, [2]string{"ks1", "80-c0"}, [2]string{resp.Shards[0].Keyspace, resp.Shards[0].Shard})
	require.Len(t, resp.Shards[0].Tablets, 1)

	// A cell filter without tablets returns the shards with no tablets.
	status, resp = getTabletHealth(t, handler, TabletHealthAPIPath+"?keyspace=ks1&cell=other-cell")
	require.Equal(t, http.StatusOK, status, resp.Error)
	require.Len(t, resp.Shards, 2)
	assert.Empty(t, resp.Shards[0].Tablets)
}

// TestTabletHealthAPIProbeTimeout verifies that a hung tablet is reported
// unavailable and cannot block the other tablet in its shard or other shards.
func TestTabletHealthAPIProbeTimeout(t *testing.T) {
	ts := setupTabletHealthTopo(t)
	prober := &scriptedTabletProber{hang: map[string]bool{"cell-0000000001": true}}
	handler := newTabletHealthHandler(ts, prober)

	start := time.Now()
	status, resp := getTabletHealth(t, handler, TabletHealthAPIPath+"?timeout=30ms")
	elapsed := time.Since(start)

	require.Equal(t, http.StatusOK, status, resp.Error)
	require.Len(t, resp.Shards, 3)
	require.Len(t, resp.Shards[0].Tablets, 2)
	assert.Less(t, elapsed, 2*time.Second)

	hung := resp.Shards[0].Tablets[0]
	healthy := resp.Shards[0].Tablets[1]
	assert.False(t, hung.Serving)
	assert.Contains(t, hung.UnavailableReason, discovery.UnavailableReasonProbeTimeout)
	assert.True(t, healthy.Serving, "the sibling tablet of the hung tablet must still be probed")
	// All other shards must be unaffected.
	assert.True(t, resp.Shards[1].Tablets[0].Serving)
	assert.True(t, resp.Shards[2].Tablets[0].Serving)
}

// TestTabletHealthAPIProbeError verifies that a non-timeout probe failure is
// surfaced with the failure reason but does not make the API call fail.
func TestTabletHealthAPIProbeError(t *testing.T) {
	ts := setupTabletHealthTopo(t)
	prober := &scriptedTabletProber{fail: map[string]error{
		"cell-0000000001": errors.New("connection refused"),
	}}
	handler := newTabletHealthHandler(ts, prober)

	status, resp := getTabletHealth(t, handler, TabletHealthAPIPath)
	require.Equal(t, http.StatusOK, status, resp.Error)
	require.Len(t, resp.Shards, 3)
	failed := resp.Shards[0].Tablets[0]
	assert.False(t, failed.Serving)
	assert.Equal(t, discovery.UnavailableReasonProbeFailed+": connection refused", failed.UnavailableReason)
}

// TestTabletHealthAPIInvalidRequests covers validation and error status codes.
func TestTabletHealthAPIInvalidRequests(t *testing.T) {
	ts := setupTabletHealthTopo(t)
	handler := newTabletHealthHandler(ts, &scriptedTabletProber{})

	cases := []struct {
		name       string
		method     string
		target     string
		wantStatus int
	}{
		{"method not allowed", http.MethodPost, TabletHealthAPIPath, http.StatusMethodNotAllowed},
		{"shard without keyspace", http.MethodGet, TabletHealthAPIPath + "?shard=-", http.StatusBadRequest},
		{"malformed timeout", http.MethodGet, TabletHealthAPIPath + "?timeout=soon", http.StatusBadRequest},
		{"non-positive timeout", http.MethodGet, TabletHealthAPIPath + "?timeout=-1s", http.StatusBadRequest},
		{"unknown keyspace", http.MethodGet, TabletHealthAPIPath + "?keyspace=missing", http.StatusNotFound},
		{"unknown shard", http.MethodGet, TabletHealthAPIPath + "?keyspace=ks1&shard=none", http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, nil).WithContext(t.Context())
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			assert.Equal(t, tc.wantStatus, rec.Code)
		})
	}
}
