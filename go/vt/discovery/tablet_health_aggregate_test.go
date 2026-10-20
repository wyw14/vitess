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

package discovery

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func cachedHealth(uid uint32, keyspace, shard string, tabletType topodatapb.TabletType, serving bool, version uint64, stats *querypb.RealtimeStats, lastErr error) *TabletHealth {
	tablet := topo.NewTablet(uid, "cell", fmt.Sprintf("host%d", uid))
	tablet.Keyspace = keyspace
	tablet.Shard = shard
	tablet.Type = tabletType
	return &TabletHealth{
		Tablet: tablet,
		Target: &querypb.Target{
			Keyspace:   keyspace,
			Shard:      shard,
			TabletType: tabletType,
		},
		Serving:       serving,
		Stats:         stats,
		LastError:     lastErr,
		HealthVersion: version,
	}
}

func reportKeys(report TabletHealthReport) [][2]string {
	keys := make([][2]string, 0, len(report))
	for _, shard := range report {
		keys = append(keys, [2]string{shard.Keyspace, shard.Shard})
	}
	return keys
}

func shardRecordByAlias(t *testing.T, shard *ShardTabletHealth, uid uint32) *TabletHealthRecord {
	t.Helper()
	wantAlias := fmt.Sprintf("cell-%010d", uid)
	for _, record := range shard.Tablets {
		if record.Alias != nil && record.Alias.Cell == "cell" && record.Alias.Uid == uid {
			assert.Equal(t, wantAlias, aliasString(record))
			return record
		}
	}
	t.Fatalf("tablet %s not found in shard %s/%s", wantAlias, shard.Keyspace, shard.Shard)
	return nil
}

func aliasString(record *TabletHealthRecord) string {
	if record.Alias == nil {
		return ""
	}
	return fmt.Sprintf("%s-%010d", record.Alias.Cell, record.Alias.Uid)
}

// TestBuildTabletHealthReport exercises the pure aggregation: grouping, lag
// bands, unavailable reasons, counts, lag ranges and stable ordering.
func TestBuildTabletHealthReport(t *testing.T) {
	// Input is deliberately shuffled: the report order must not depend on it.
	tablets := []*TabletHealth{
		// zeta/80-c0: a single replica that stopped serving without an error.
		cachedHealth(30, "zeta", "80-c0", topodatapb.TabletType_REPLICA, false, 7, nil, nil),

		// alpha/00-80: a critically lagged serving replica, a serving replica
		// whose stats are still unknown and a brand new tablet (version 0).
		cachedHealth(20, "alpha", "00-80", topodatapb.TabletType_REPLICA, true, 4, &querypb.RealtimeStats{ReplicationLagSeconds: 7201}, nil),
		cachedHealth(21, "alpha", "00-80", topodatapb.TabletType_REPLICA, true, 1, nil, nil),
		cachedHealth(22, "alpha", "00-80", topodatapb.TabletType_REPLICA, false, 0, nil, nil),

		// alpha/-: a primary, a low-lag replica, a high-lag replica and an
		// errored (unavailable) replica.
		cachedHealth(13, "alpha", "-", topodatapb.TabletType_REPLICA, false, 9, &querypb.RealtimeStats{ReplicationLagSeconds: 5}, errors.New("healthcheck timed out")),
		cachedHealth(10, "alpha", "-", topodatapb.TabletType_PRIMARY, true, 2, &querypb.RealtimeStats{}, nil),
		cachedHealth(11, "alpha", "-", topodatapb.TabletType_REPLICA, true, 5, &querypb.RealtimeStats{ReplicationLagSeconds: 5}, nil),
		cachedHealth(12, "alpha", "-", topodatapb.TabletType_REPLICA, true, 6, &querypb.RealtimeStats{ReplicationLagSeconds: 100}, nil),
	}

	report := BuildTabletHealthReport(tablets)

	require.Equal(t, [][2]string{
		{"alpha", "-"},
		{"alpha", "00-80"},
		{"zeta", "80-c0"},
	}, reportKeys(report), "report must be sorted by keyspace then shard")

	// alpha/-
	alphaDash := report[0]
	assert.Equal(t, []string{"cell-0000000010", "cell-0000000011", "cell-0000000012", "cell-0000000013"}, aliasList(alphaDash),
		"tablets within a shard must be stably sorted by alias")
	assert.Equal(t, 3, alphaDash.HealthyTabletCount)
	assert.Equal(t, 1, alphaDash.UnavailableTabletCount)
	assert.True(t, alphaDash.ReplicationLagRangeKnown)
	assert.Equal(t, uint32(5), alphaDash.ReplicationLagSecondsMin)
	assert.Equal(t, uint32(100), alphaDash.ReplicationLagSecondsMax)

	primary := shardRecordByAlias(t, alphaDash, 10)
	assert.True(t, primary.Serving)
	assert.Equal(t, LagNone, primary.ReplicationLagClass)
	assert.Equal(t, uint32(0), primary.ReplicationLagSeconds)
	assert.Equal(t, uint64(2), primary.HealthVersion)
	assert.Empty(t, primary.UnavailableReason)

	low := shardRecordByAlias(t, alphaDash, 11)
	assert.Equal(t, LagLow, low.ReplicationLagClass)
	assert.Equal(t, uint32(5), low.ReplicationLagSeconds)

	high := shardRecordByAlias(t, alphaDash, 12)
	assert.Equal(t, LagHigh, high.ReplicationLagClass)

	down := shardRecordByAlias(t, alphaDash, 13)
	assert.False(t, down.Serving)
	assert.Equal(t, "healthcheck timed out", down.UnavailableReason, "LastError must become the unavailable reason")

	// alpha/00-80
	alphaShard := report[1]
	assert.Equal(t, 2, alphaShard.HealthyTabletCount)
	assert.Equal(t, 1, alphaShard.UnavailableTabletCount)
	assert.True(t, alphaShard.ReplicationLagRangeKnown, "only the known-lag serving replica contributes to the range")
	assert.Equal(t, uint32(7201), alphaShard.ReplicationLagSecondsMin)
	assert.Equal(t, uint32(7201), alphaShard.ReplicationLagSecondsMax)

	critical := shardRecordByAlias(t, alphaShard, 20)
	assert.Equal(t, LagCritical, critical.ReplicationLagClass)
	assert.Equal(t, uint32(7201), critical.ReplicationLagSeconds)

	statsLess := shardRecordByAlias(t, alphaShard, 21)
	assert.Equal(t, LagUnknown, statsLess.ReplicationLagClass)

	fresh := shardRecordByAlias(t, alphaShard, 22)
	assert.False(t, fresh.Serving)
	assert.Equal(t, UnavailableReasonNoHealthData, fresh.UnavailableReason, "version 0 with no stats/error means no health data yet")

	// zeta/80-c0
	zetaShard := report[2]
	assert.Equal(t, 0, zetaShard.HealthyTabletCount)
	assert.Equal(t, 1, zetaShard.UnavailableTabletCount)
	assert.False(t, zetaShard.ReplicationLagRangeKnown)
	notServing := shardRecordByAlias(t, zetaShard, 30)
	assert.Equal(t, UnavailableReasonNotServing, notServing.UnavailableReason)
}

func aliasList(shard *ShardTabletHealth) []string {
	out := make([]string, 0, len(shard.Tablets))
	for _, record := range shard.Tablets {
		out = append(out, aliasString(record))
	}
	return out
}

// TestBuildTabletHealthReportStableOrder makes repeated aggregation
// deterministic regardless of input order.
func TestBuildTabletHealthReportStableOrder(t *testing.T) {
	mk := func(uid uint32, keyspace, shard string) *TabletHealth {
		return cachedHealth(uid, keyspace, shard, topodatapb.TabletType_REPLICA, true, 1, &querypb.RealtimeStats{ReplicationLagSeconds: 1}, nil)
	}
	input := []*TabletHealth{mk(3, "b", "-"), mk(1, "a", "-"), mk(2, "a", "80-c0"), mk(4, "b", "00-80")}

	first := BuildTabletHealthReport(input)
	// Reverse the input and aggregate again.
	for i, j := 0, len(input)-1; i < j; i, j = i+1, j-1 {
		input[i], input[j] = input[j], input[i]
	}
	second := BuildTabletHealthReport(input)

	require.Equal(t, reportKeys(first), reportKeys(second))
	for i := range first {
		assert.Equal(t, aliasList(first[i]), aliasList(second[i]))
	}
}

// scriptedProber is a TabletHealthProber for the fan-out tests: named tablets
// either block until their per-probe context expires, return a scripted error,
// or answer immediately.
type scriptedProber struct {
	hangAliases   map[string]struct{}
	errorAliases  map[string]error
	probeCount    atomic.Int64
	canceledCount atomic.Int64
}

func (p *scriptedProber) ProbeTabletHealth(ctx context.Context, tablet *topodatapb.Tablet) error {
	p.probeCount.Add(1)
	alias := fmt.Sprintf("%s-%010d", tablet.Alias.Cell, tablet.Alias.Uid)
	if _, ok := p.hangAliases[alias]; ok {
		<-ctx.Done()
		p.canceledCount.Add(1)
		return ctx.Err()
	}
	if err, ok := p.errorAliases[alias]; ok {
		return err
	}
	return nil
}

func groupTablet(uid uint32, keyspace, shard string) *topodatapb.Tablet {
	tablet := topo.NewTablet(uid, "cell", fmt.Sprintf("host%d", uid))
	tablet.Keyspace = keyspace
	tablet.Shard = shard
	tablet.Type = topodatapb.TabletType_REPLICA
	return tablet
}

// TestAggregateTabletHealthProbeTimeoutIsolation is the core fan-out
// guarantee: one tablet whose probe hangs must not block the other tablets of
// its shard or any other shard, and the hung tablet is reported unavailable
// with the timeout reason.
func TestAggregateTabletHealthProbeTimeoutIsolation(t *testing.T) {
	const perTabletTimeout = 50 * time.Millisecond

	groups := []TabletGroup{
		{Keyspace: "ks1", Shard: "-", Tablets: []*topodatapb.Tablet{
			groupTablet(1, "ks1", "-"), // hangs
			groupTablet(2, "ks1", "-"), // healthy, same shard as the hang
		}},
		{Keyspace: "ks2", Shard: "-", Tablets: []*topodatapb.Tablet{
			groupTablet(3, "ks2", "-"), // healthy, other shard
		}},
		{Keyspace: "ks1", Shard: "80-c0", Tablets: []*topodatapb.Tablet{
			groupTablet(4, "ks1", "80-c0"),
		}},
	}
	prober := &scriptedProber{hangAliases: map[string]struct{}{"cell-0000000001": {}}}

	start := time.Now()
	report := AggregateTabletHealth(t.Context(), groups, AggregateTabletHealthOptions{
		Prober:           prober,
		PerTabletTimeout: perTabletTimeout,
	})
	elapsed := time.Since(start)

	// All shards complete and stay sorted even though one probe hung.
	require.Equal(t, [][2]string{
		{"ks1", "-"},
		{"ks1", "80-c0"},
		{"ks2", "-"},
	}, reportKeys(report))

	require.Len(t, prober.hangAliases, 1)
	assert.Equal(t, int64(4), prober.probeCount.Load(), "every tablet must have been probed")
	assert.Equal(t, int64(1), prober.canceledCount.Load(), "the hung probe must observe its per-tablet cancellation")
	assert.Less(t, elapsed, 2*time.Second, "a 50ms hung probe must not add up to a multi-second aggregation")

	ks1dash := report[0]
	require.Len(t, ks1dash.Tablets, 2)
	byUID := map[uint32]*TabletHealthRecord{}
	for _, record := range ks1dash.Tablets {
		byUID[record.Alias.Uid] = record
	}
	assert.False(t, byUID[1].Serving)
	assert.Contains(t, byUID[1].UnavailableReason, UnavailableReasonProbeTimeout)
	assert.Contains(t, byUID[1].UnavailableReason, perTabletTimeout.String())
	assert.True(t, byUID[2].Serving, "the healthy tablet in the hanging tablet's shard must not be blocked")
	assert.Empty(t, byUID[2].UnavailableReason)

	assert.True(t, report[1].Tablets[0].Serving, "tablets of other shards must not be blocked")
	assert.True(t, report[2].Tablets[0].Serving, "tablets of other shards must not be blocked")
}

// TestAggregateTabletHealthProbeErrors checks classification of non-timeout
// probe failures, including gRPC DEADLINE_EXCEEDED errors which are not a
// plain context.DeadlineExceeded.
func TestAggregateTabletHealthProbeErrors(t *testing.T) {
	grpcDeadline := status.Error(codes.DeadlineExceeded, "rpc deadline")
	groups := []TabletGroup{{Keyspace: "ks", Shard: "-", Tablets: []*topodatapb.Tablet{
		groupTablet(1, "ks", "-"),
		groupTablet(2, "ks", "-"),
	}}}
	prober := &scriptedProber{errorAliases: map[string]error{
		"cell-0000000001": errors.New("connection refused"),
		"cell-0000000002": grpcDeadline,
	}}

	report := AggregateTabletHealth(t.Context(), groups, AggregateTabletHealthOptions{
		Prober:           prober,
		PerTabletTimeout: time.Second,
	})
	require.Len(t, report, 1)
	require.Len(t, report[0].Tablets, 2)

	assert.Equal(t, UnavailableReasonProbeFailed+": connection refused", report[0].Tablets[0].UnavailableReason)
	assert.Contains(t, report[0].Tablets[1].UnavailableReason, UnavailableReasonProbeTimeout)
}

// TestAggregateTabletHealthMergesCachedHealth verifies that cached
// StreamHealth data (version, lag band, serving state) is merged with active
// probe results: probe success keeps the cached view, a failed probe overrides
// a stale "serving" cache entry.
func TestAggregateTabletHealthMergesCachedHealth(t *testing.T) {
	groups := []TabletGroup{
		{Keyspace: "ks", Shard: "-", Tablets: []*topodatapb.Tablet{
			groupTablet(1, "ks", "-"),
			groupTablet(2, "ks", "-"),
		}},
	}
	cache := &fakeHealthLookup{
		health: map[string]*TabletHealth{
			"cell-0000000001": cachedHealth(1, "ks", "-", topodatapb.TabletType_REPLICA, true, 11, &querypb.RealtimeStats{ReplicationLagSeconds: 5}, nil),
			"cell-0000000002": cachedHealth(2, "ks", "-", topodatapb.TabletType_REPLICA, true, 12, &querypb.RealtimeStats{ReplicationLagSeconds: 5}, nil),
		},
	}
	prober := &scriptedProber{errorAliases: map[string]error{
		"cell-0000000002": errors.New("connection reset"),
	}}

	report := AggregateTabletHealth(t.Context(), groups, AggregateTabletHealthOptions{
		Prober:           prober,
		Lookup:           cache,
		PerTabletTimeout: time.Second,
	})
	require.Len(t, report, 1)
	require.Len(t, report[0].Tablets, 2)

	healthy := report[0].Tablets[0]
	assert.True(t, healthy.Serving)
	assert.Equal(t, uint64(11), healthy.HealthVersion)
	assert.Equal(t, LagLow, healthy.ReplicationLagClass)
	assert.Equal(t, uint32(5), healthy.ReplicationLagSeconds)

	stale := report[0].Tablets[1]
	assert.False(t, stale.Serving, "a failed probe overrides stale cached serving state")
	assert.Equal(t, uint64(12), stale.HealthVersion, "cached version/lag are preserved for context")
	assert.Contains(t, stale.UnavailableReason, UnavailableReasonProbeFailed)
}

type fakeHealthLookup struct {
	health map[string]*TabletHealth
}

func (f *fakeHealthLookup) GetTabletHealthByAlias(alias *topodatapb.TabletAlias) (*TabletHealth, error) {
	key := fmt.Sprintf("%s-%010d", alias.Cell, alias.Uid)
	if th, ok := f.health[key]; ok {
		return th, nil
	}
	return nil, fmt.Errorf("not found: %s", key)
}

// TestHealthCheckAggregateHealthVersion runs the real HealthCheckImpl and
// verifies that the health version recorded from vttablet StreamHealth
// responses increments and is exposed by the aggregated report.
func TestHealthCheckAggregateHealthVersion(t *testing.T) {
	ts := memorytopo.NewServer(t.Context(), "cell")
	hc := createTestHc(t.Context(), ts)
	t.Cleanup(func() { require.NoError(t, hc.Close()) })

	tablet := createTestTablet(1, "cell", "host1")
	tablet.Type = topodatapb.TabletType_REPLICA
	input := make(chan *querypb.StreamHealthResponse)
	createFakeConn(tablet, input)
	hc.AddTablet(tablet)

	resp := func(lag uint32, serving bool, healthErr string) *querypb.StreamHealthResponse {
		return &querypb.StreamHealthResponse{
			TabletAlias: tablet.Alias,
			Target:      &querypb.Target{Keyspace: "k", Shard: "s", TabletType: topodatapb.TabletType_REPLICA},
			Serving:     serving,
			RealtimeStats: &querypb.RealtimeStats{
				ReplicationLagSeconds: lag,
				HealthError:           healthErr,
			},
		}
	}

	// Two healthy responses.
	input <- resp(5, true, "")
	input <- resp(7, true, "")

	assert.Eventually(t, func() bool {
		report := hc.AggregateTabletHealthReport()
		return len(report) == 1 && len(report[0].Tablets) == 1 && report[0].Tablets[0].HealthVersion == 2
	}, 5*time.Second, 10*time.Millisecond, "health version should reach 2 after two responses")

	report := hc.AggregateTabletHealthReport()
	require.Len(t, report, 1)
	record := report[0].Tablets[0]
	assert.True(t, record.Serving)
	assert.Equal(t, uint64(2), record.HealthVersion)
	assert.Equal(t, LagLow, record.ReplicationLagClass)
	assert.Equal(t, uint32(7), record.ReplicationLagSeconds, "the latest lag value must win")
	assert.Empty(t, record.UnavailableReason)

	// A vttablet health error bumps the version and marks the tablet
	// unavailable with the vttablet error reason.
	input <- resp(7, false, "mysql is read-only")
	assert.Eventually(t, func() bool {
		aggregated := hc.AggregateTabletHealthReport()
		return len(aggregated) == 1 && aggregated[0].Tablets[0].HealthVersion == 3
	}, 5*time.Second, 10*time.Millisecond, "health version should reach 3 after the error response")

	report = hc.AggregateTabletHealthReport()
	require.Len(t, report, 1)
	record = report[0].Tablets[0]
	assert.False(t, record.Serving)
	assert.Contains(t, record.UnavailableReason, "vttablet error")
	assert.Contains(t, record.UnavailableReason, "mysql is read-only")
}
