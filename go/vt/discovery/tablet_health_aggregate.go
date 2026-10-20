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
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
)

// DefaultTabletHealthProbeTimeout is the per-tablet timeout used by
// AggregateTabletHealth when no explicit timeout is provided. It bounds how
// long a single unreachable tablet can delay the shard it belongs to; other
// shards are never blocked.
const DefaultTabletHealthProbeTimeout = 5 * time.Second

// ReplicationLagClass classifies a tablet's replication lag into one of the
// bands used by the tablet health aggregation. The bands are derived from the
// discovery low/high replication lag thresholds, which match vttablet's
// degraded/unhealthy thresholds.
type ReplicationLagClass string

const (
	// LagUnknown means the tablet's replication lag is not known, e.g. because
	// no StreamHealth response has been received yet.
	LagUnknown ReplicationLagClass = "unknown"
	// LagNone is reported by primaries (and other tablet types for which
	// replica lag does not apply): there is no replication lag to classify.
	LagNone ReplicationLagClass = "none"
	// LagLow means the lag is at or below the low replication lag threshold
	// (--discovery-low-replication-lag, default 30s): the tablet is healthy.
	LagLow ReplicationLagClass = "low"
	// LagHigh means the lag is above the low threshold but at or below the
	// high threshold (--discovery-high-replication-lag-minimum-serving,
	// default 2h): the tablet is degraded but may still serve.
	LagHigh ReplicationLagClass = "high"
	// LagCritical means the lag is above the high threshold: the tablet is
	// considered unhealthy for selection.
	LagCritical ReplicationLagClass = "critical"
)

// Unavailable reasons explain why a tablet record is not serving. They are
// stable strings so that API consumers can match on them.
const (
	// UnavailableReasonNotServing is used when a tablet reports itself as not
	// serving without a more specific error.
	UnavailableReasonNotServing = "not serving"
	// UnavailableReasonNoHealthData is used when neither the healthcheck cache
	// nor a probe could provide any health information for the tablet.
	UnavailableReasonNoHealthData = "no health data"
	// UnavailableReasonProbeTimeout is the prefix used when the per-tablet
	// health probe deadline expires.
	UnavailableReasonProbeTimeout = "health probe timed out"
	// UnavailableReasonProbeFailed is the prefix used when the per-tablet
	// health probe fails for any reason other than a timeout.
	UnavailableReasonProbeFailed = "health probe failed"
)

// TabletHealthRecord is the per-tablet entry of a tablet health aggregation.
// It combines the health record version observed via vttablet's StreamHealth
// stream, the replication lag band and the reason the tablet is unavailable.
type TabletHealthRecord struct {
	// Cell and Alias identify the tablet.
	Cell  string                `json:"cell"`
	Alias *topodata.TabletAlias `json:"alias"`
	// Keyspace and Shard are the tablet's target keyspace and shard.
	Keyspace string `json:"keyspace"`
	Shard    string `json:"shard"`
	// TabletType is the tablet type as reported by the latest health record,
	// or, before any health record is known, the type from the topo record.
	TabletType topodata.TabletType `json:"tablet_type"`
	// Serving reports whether the tablet can currently serve traffic.
	Serving bool `json:"serving"`
	// HealthVersion is the monotonically increasing version of the tablet's
	// health record (see TabletHealth.HealthVersion). It is 0 when no
	// StreamHealth response has ever been observed for the tablet.
	HealthVersion uint64 `json:"health_version"`
	// PrimaryTermStartTime is set for primaries and used as a tie breaker
	// between multiple primary candidates.
	PrimaryTermStartTime int64 `json:"primary_term_start_time,omitempty"`
	// ReplicationLagSeconds is the raw lag reported by the tablet.
	ReplicationLagSeconds uint32 `json:"replication_lag_seconds"`
	// ReplicationLagClass is the lag band derived from the discovery
	// thresholds.
	ReplicationLagClass ReplicationLagClass `json:"replication_lag_class"`
	// UnavailableReason is empty for serving tablets and otherwise explains
	// why the tablet is unavailable (timeout, stream error, not serving, ...).
	UnavailableReason string `json:"unavailable_reason,omitempty"`
}

// ShardTabletHealth is the aggregated health of all tablets in one
// keyspace/shard. Tablets are sorted stably by cell/alias.
type ShardTabletHealth struct {
	Keyspace               string                `json:"keyspace"`
	Shard                  string                `json:"shard"`
	Tablets                []*TabletHealthRecord `json:"tablets"`
	HealthyTabletCount     int                   `json:"healthy_tablet_count"`
	UnavailableTabletCount int                   `json:"unavailable_tablet_count"`
	// ReplicationLagRangeKnown is false when no serving replica reports a
	// replication lag for the shard (e.g. a shard with only a primary, or a
	// shard whose replicas never reported stats).
	ReplicationLagRangeKnown bool   `json:"replication_lag_range_known"`
	ReplicationLagSecondsMin uint32 `json:"replication_lag_seconds_min,omitempty"`
	ReplicationLagSecondsMax uint32 `json:"replication_lag_seconds_max,omitempty"`
}

// TabletHealthReport is the aggregated health of multiple shards. Its
// ordering is always stable: by keyspace, then by shard.
type TabletHealthReport []*ShardTabletHealth

// TabletGroup identifies the tablets of one keyspace/shard that should be
// probed and aggregated together.
type TabletGroup struct {
	Keyspace string
	Shard    string
	Tablets  []*topodata.Tablet
}

// TabletHealthProber actively probes a single tablet's liveness, e.g. through
// the tabletmanager Ping RPC. Implementations must honor the passed context so
// that the per-tablet deadline can cancel an in-flight probe.
type TabletHealthProber interface {
	ProbeTabletHealth(ctx context.Context, tablet *topodata.Tablet) error
}

// TabletHealthLookup provides the latest cached TabletHealth for a tablet,
// typically a *HealthCheckImpl. It is used to enrich actively probed results
// with the health version, replication lag and serving state learned from the
// vttablet StreamHealth stream.
type TabletHealthLookup interface {
	GetTabletHealthByAlias(alias *topodata.TabletAlias) (*TabletHealth, error)
}

// AggregateTabletHealthOptions controls AggregateTabletHealth.
type AggregateTabletHealthOptions struct {
	// Prober, when set, is used to actively probe every tablet.
	Prober TabletHealthProber
	// Lookup, when set, provides cached StreamHealth data per tablet.
	Lookup TabletHealthLookup
	// PerTabletTimeout bounds every individual tablet probe. When <= 0,
	// DefaultTabletHealthProbeTimeout is used. A timed-out tablet is recorded
	// as unavailable and never blocks the aggregation of other tablets or
	// other shards.
	PerTabletTimeout time.Duration
	// MaxShardConcurrency bounds the number of shards probed concurrently.
	// When <= 0, all shards are probed concurrently. Tablets within a shard
	// are always probed concurrently.
	MaxShardConcurrency int
}

// ClassifyReplicationLag maps a tablet's RealtimeStats and type onto a
// replication lag band. The returned lag seconds are 0 when the class is
// LagUnknown or LagNone.
func ClassifyReplicationLag(stats *query.RealtimeStats, tabletType topodata.TabletType) (uint32, ReplicationLagClass) {
	if tabletType == topodata.TabletType_PRIMARY {
		return 0, LagNone
	}
	if stats == nil {
		return 0, LagUnknown
	}
	lag := stats.ReplicationLagSeconds
	lowLag := uint32(lowReplicationLag.Get().Seconds())
	highLag := uint32(highReplicationLagMinServing.Get().Seconds())
	switch {
	case lag <= lowLag:
		return lag, LagLow
	case lag <= highLag:
		return lag, LagHigh
	default:
		return lag, LagCritical
	}
}

// BuildTabletHealthReport groups the given cached TabletHealth entries by
// keyspace/shard and builds a stably sorted report. The slice is not mutated.
func BuildTabletHealthReport(tablets []*TabletHealth) TabletHealthReport {
	groups := make(map[string]*ShardTabletHealth)
	order := make([]string, 0)
	for _, th := range tablets {
		if th == nil || th.Tablet == nil {
			continue
		}
		keyspace, shard := th.Tablet.Keyspace, th.Tablet.Shard
		if th.Target != nil && (th.Target.Keyspace != "" || th.Target.Shard != "") {
			keyspace, shard = th.Target.Keyspace, th.Target.Shard
		}
		key := keyspace + "\x00" + shard
		group, ok := groups[key]
		if !ok {
			group = &ShardTabletHealth{Keyspace: keyspace, Shard: shard}
			groups[key] = group
			order = append(order, key)
		}
		group.Tablets = append(group.Tablets, NewTabletHealthRecordFromCache(th))
	}

	report := make(TabletHealthReport, 0, len(order))
	for _, key := range order {
		report = append(report, finalizeShardTabletHealth(groups[key]))
	}
	sortTabletHealthReport(report)
	return report
}

// NewTabletHealthRecordFromCache converts a discovery TabletHealth snapshot
// into a TabletHealthRecord.
func NewTabletHealthRecordFromCache(th *TabletHealth) *TabletHealthRecord {
	tabletType := topodata.TabletType_UNKNOWN
	if th.Target != nil {
		tabletType = th.Target.TabletType
	} else if th.Tablet != nil {
		tabletType = th.Tablet.Type
	}
	lag, lagClass := ClassifyReplicationLag(th.Stats, tabletType)
	record := &TabletHealthRecord{
		TabletType:            tabletType,
		Serving:               th.Serving,
		HealthVersion:         th.HealthVersion,
		PrimaryTermStartTime:  th.PrimaryTermStartTime,
		ReplicationLagSeconds: lag,
		ReplicationLagClass:   lagClass,
	}
	if th.Tablet != nil {
		record.Cell = th.Tablet.Alias.GetCell()
		record.Alias = th.Tablet.Alias
		record.Keyspace = th.Tablet.Keyspace
		record.Shard = th.Tablet.Shard
		if th.Target != nil && th.Target.Keyspace != "" {
			record.Keyspace = th.Target.Keyspace
			record.Shard = th.Target.Shard
		}
	}
	if !th.Serving {
		record.UnavailableReason = cachedUnavailableReason(th)
	}
	return record
}

// cachedUnavailableReason derives a human readable reason from a cached
// non-serving TabletHealth.
func cachedUnavailableReason(th *TabletHealth) string {
	if th.LastError != nil && th.LastError.Error() != "" {
		return th.LastError.Error()
	}
	if th.Stats != nil && th.Stats.HealthError != "" {
		return "vttablet error: " + th.Stats.HealthError
	}
	// Version 0 and no stats mean the tablet was added to the healthcheck but
	// no StreamHealth response or error has ever been observed for it.
	if th.HealthVersion == 0 && th.Stats == nil {
		return UnavailableReasonNoHealthData
	}
	return UnavailableReasonNotServing
}

// NewTabletHealthRecordFromTablet builds the baseline record for a tablet that
// has no cached health data (topo record only, lag unknown).
func NewTabletHealthRecordFromTablet(tablet *topodata.Tablet) *TabletHealthRecord {
	return &TabletHealthRecord{
		Cell:                tablet.Alias.GetCell(),
		Alias:               tablet.Alias,
		Keyspace:            tablet.Keyspace,
		Shard:               tablet.Shard,
		TabletType:          tablet.Type,
		Serving:             false,
		ReplicationLagClass: LagUnknown,
		UnavailableReason:   UnavailableReasonNoHealthData,
	}
}

// AggregateTabletHealth actively aggregates the health of the given tablet
// groups (keyspace/shards).
//
// Each shard is aggregated on its own goroutine and, within the shard, every
// tablet is probed on its own goroutine with an individual per-tablet
// deadline derived from ctx. A tablet whose probe times out or errors is
// recorded as unavailable with the corresponding reason, but its shard and
// every other shard still complete. The returned report is stably sorted by
// keyspace and then shard; tablets within each shard are stably sorted by
// cell and alias.
func AggregateTabletHealth(ctx context.Context, groups []TabletGroup, opts AggregateTabletHealthOptions) TabletHealthReport {
	timeout := opts.PerTabletTimeout
	if timeout <= 0 {
		timeout = DefaultTabletHealthProbeTimeout
	}

	var sem chan struct{}
	if opts.MaxShardConcurrency > 0 {
		sem = make(chan struct{}, opts.MaxShardConcurrency)
	}

	results := make([]*ShardTabletHealth, len(groups))
	var wg sync.WaitGroup
	for i := range groups {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if sem != nil {
				select {
				case sem <- struct{}{}:
					defer func() { <-sem }()
				case <-ctx.Done():
					results[i] = finalizeShardTabletHealth(unreachableShard(groups[i], ctx.Err()))
					return
				}
			}
			results[i] = aggregateShardTabletHealth(ctx, groups[i], opts, timeout)
		}(i)
	}
	wg.Wait()

	report := make(TabletHealthReport, 0, len(results))
	for _, shard := range results {
		if shard != nil {
			report = append(report, shard)
		}
	}
	sortTabletHealthReport(report)
	return report
}

// aggregateShardTabletHealth probes the shard's tablets concurrently. Each
// probe has its own deadline so a single hung tablet cannot delay the shard.
func aggregateShardTabletHealth(ctx context.Context, group TabletGroup, opts AggregateTabletHealthOptions, timeout time.Duration) *ShardTabletHealth {
	records := make([]*TabletHealthRecord, len(group.Tablets))
	var wg sync.WaitGroup
	for i := range group.Tablets {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			records[i] = resolveTabletHealthRecord(ctx, group.Tablets[i], opts, timeout)
		}(i)
	}
	wg.Wait()
	return finalizeShardTabletHealth(&ShardTabletHealth{
		Keyspace: group.Keyspace,
		Shard:    group.Shard,
		Tablets:  records,
	})
}

// resolveTabletHealthRecord builds the record for one tablet, merging cached
// StreamHealth data with an active probe result.
func resolveTabletHealthRecord(ctx context.Context, tablet *topodata.Tablet, opts AggregateTabletHealthOptions, timeout time.Duration) *TabletHealthRecord {
	if tablet == nil {
		return nil
	}

	var cached *TabletHealth
	if opts.Lookup != nil {
		if th, err := opts.Lookup.GetTabletHealthByAlias(tablet.Alias); err == nil {
			cached = th
		}
	}

	var record *TabletHealthRecord
	if cached != nil {
		record = NewTabletHealthRecordFromCache(cached)
	} else {
		record = NewTabletHealthRecordFromTablet(tablet)
	}

	if opts.Prober == nil {
		return record
	}

	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := opts.Prober.ProbeTabletHealth(probeCtx, tablet); err != nil {
		// A fresh probe failure is authoritative about liveness even when the
		// cache still considers the tablet serving.
		record.Serving = false
		record.UnavailableReason = probeUnavailableReason(err, timeout)
	} else if cached == nil {
		// No StreamHealth data, but the tablet answered the probe: it is
		// reachable even though its serving state and lag are unknown.
		record.Serving = true
		record.UnavailableReason = ""
	}
	return record
}

// unreachableShard is returned when the parent context expires before the
// shard could even be scheduled.
func unreachableShard(group TabletGroup, err error) *ShardTabletHealth {
	records := make([]*TabletHealthRecord, 0, len(group.Tablets))
	for _, tablet := range group.Tablets {
		record := NewTabletHealthRecordFromTablet(tablet)
		record.Serving = false
		if err != nil {
			record.UnavailableReason = fmt.Sprintf("%s: %v", UnavailableReasonProbeFailed, err)
		}
		records = append(records, record)
	}
	return &ShardTabletHealth{Keyspace: group.Keyspace, Shard: group.Shard, Tablets: records}
}

func probeUnavailableReason(err error, timeout time.Duration) string {
	if isDeadlineExceeded(err) {
		return fmt.Sprintf("%s after %s", UnavailableReasonProbeTimeout, timeout)
	}
	return fmt.Sprintf("%s: %v", UnavailableReasonProbeFailed, err)
}

func isDeadlineExceeded(err error) bool {
	// tmclient may return a plain context error, a vtrpc-coded error or a
	// raw gRPC status error depending on the transport, so recognize all three.
	return errors.Is(err, context.DeadlineExceeded) ||
		vterrors.Code(err) == vtrpc.Code_DEADLINE_EXCEEDED ||
		status.Code(err) == codes.DeadlineExceeded
}

// finalizeShardTabletHealth stably sorts the shard's tablets and computes the
// counts and replication lag range.
func finalizeShardTabletHealth(shard *ShardTabletHealth) *ShardTabletHealth {
	records := make([]*TabletHealthRecord, 0, len(shard.Tablets))
	for _, record := range shard.Tablets {
		if record != nil {
			records = append(records, record)
		}
	}
	shard.Tablets = records

	sort.SliceStable(shard.Tablets, func(i, j int) bool {
		return tabletAliasLess(shard.Tablets[i], shard.Tablets[j])
	})

	var minLag, maxLag uint32
	lagKnown := false
	for _, record := range shard.Tablets {
		if record.Serving {
			shard.HealthyTabletCount++
		} else {
			shard.UnavailableTabletCount++
		}
		// The range only covers serving replicas whose lag is known; primaries
		// (LagNone) and tablets without stats (LagUnknown) are excluded.
		if !record.Serving || !lagClassKnown(record.ReplicationLagClass) {
			continue
		}
		if !lagKnown {
			minLag, maxLag, lagKnown = record.ReplicationLagSeconds, record.ReplicationLagSeconds, true
			continue
		}
		minLag = min(minLag, record.ReplicationLagSeconds)
		maxLag = max(maxLag, record.ReplicationLagSeconds)
	}
	shard.ReplicationLagRangeKnown = lagKnown
	if lagKnown {
		shard.ReplicationLagSecondsMin = minLag
		shard.ReplicationLagSecondsMax = maxLag
	}
	return shard
}

func tabletAliasLess(a, b *TabletHealthRecord) bool {
	return topoproto.TabletAliasString(a.Alias) < topoproto.TabletAliasString(b.Alias)
}

// lagClassKnown reports whether the class carries a numeric replication lag
// (i.e. a replica value), as opposed to LagNone (primary) and LagUnknown (no
// stats).
func lagClassKnown(class ReplicationLagClass) bool {
	switch class {
	case LagLow, LagHigh, LagCritical:
		return true
	default:
		return false
	}
}

// sortTabletHealthReport stably sorts the report by keyspace, then shard.
func sortTabletHealthReport(report TabletHealthReport) {
	sort.SliceStable(report, func(i, j int) bool {
		if report[i].Keyspace != report[j].Keyspace {
			return report[i].Keyspace < report[j].Keyspace
		}
		return report[i].Shard < report[j].Shard
	})
}

// GetAllTabletHealth returns a snapshot of every tablet the healthcheck knows
// about, across all keyspaces, shards and tablet types. The returned slice is
// owned by the caller; its pointers share the immutable Stats objects.
func (hc *HealthCheckImpl) GetAllTabletHealth() []*TabletHealth {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	total := 0
	for _, tablets := range hc.healthData {
		total += len(tablets)
	}
	result := make([]*TabletHealth, 0, total)
	for _, tablets := range hc.healthData {
		for _, th := range tablets {
			result = append(result, th)
		}
	}
	return result
}

// AggregateTabletHealthReport builds the aggregated health report from the
// healthcheck's current StreamHealth cache without actively probing tablets.
// The result is stably sorted by keyspace/shard.
func (hc *HealthCheckImpl) AggregateTabletHealthReport() TabletHealthReport {
	return BuildTabletHealthReport(hc.GetAllTabletHealth())
}
