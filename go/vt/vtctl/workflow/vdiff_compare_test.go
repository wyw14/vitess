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

package workflow

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"
)

const (
	cmpStartedAt   = "2026-01-15 10:00:00"
	cmpCompletedAt = "2026-01-15 10:05:00"
)

// buildCompareSummary builds a completed Summary with the given per-shard and
// per-table counters. A nil shardTables map yields a summary covering no
// shards.
func buildCompareSummary(keyspace, wf, uuid string, shardTables map[string]map[string]TableState) *Summary {
	s := &Summary{
		Workflow:       wf,
		Keyspace:       keyspace,
		UUID:           uuid,
		State:          vdiff.CompletedState,
		StartedAt:      cmpStartedAt,
		CompletedAt:    cmpCompletedAt,
		ShardSummaries: map[string]ShardSummary{},
	}
	for shard, tables := range shardTables {
		ss := ShardSummary{
			State:       vdiff.CompletedState,
			StartedAt:   cmpStartedAt,
			CompletedAt: cmpCompletedAt,
			TableStates: map[string]TableState{},
		}
		for name, ts := range tables {
			ts.TableName = name
			ts.State = vdiff.CompletedState
			ss.TableStates[name] = ts
		}
		s.ShardSummaries[shard] = ss
	}
	return s
}

func TestCompareVDiffSummaries(t *testing.T) {
	from := buildCompareSummary("customer", "c2c", "uuid-from", map[string]map[string]TableState{
		"-80": {
			"t1": {MatchingRows: 10, MismatchedRows: 2, ExtraRowsSource: 1, ExtraRowsTarget: 3},
			// Only present in the first run: disappeared in the second run.
			"gone": {MatchingRows: 4},
		},
		"80-": {
			"t1": {MatchingRows: 20},
		},
	})
	to := buildCompareSummary("customer", "c2c", "uuid-to", map[string]map[string]TableState{
		"-80": {
			"t1": {MatchingRows: 13, MismatchedRows: 0, ExtraRowsSource: 0, ExtraRowsTarget: 5},
			// Only present in the second run: new relative to the first run.
			"fresh": {MatchingRows: 7},
		},
		"80-": {
			"t1": {MatchingRows: 20},
		},
	})

	report, err := CompareVDiffSummaries(from, to)
	require.NoError(t, err)
	require.NotNil(t, report)

	require.Equal(t, "customer", report.Keyspace)
	require.Equal(t, "c2c", report.Workflow)
	require.Equal(t, "uuid-from", report.From.UUID)
	require.Equal(t, vdiff.CompletedState, report.From.State)
	require.Equal(t, cmpCompletedAt, report.From.CompletedAt)
	require.Equal(t, "uuid-to", report.To.UUID)
	require.Equal(t, cmpCompletedAt, report.To.CompletedAt)

	// Shards are sorted for stable output.
	require.Len(t, report.Shards, 2)
	require.Equal(t, "-80", report.Shards[0].Shard)
	require.Equal(t, "80-", report.Shards[1].Shard)

	firstShard := report.Shards[0].Tables
	require.Len(t, firstShard, 3)
	// Tables are sorted within a shard: gone (removed), t1 (common), fresh (new).
	require.Equal(t, "fresh", firstShard[0].Table)
	require.Equal(t, VDiffTableStatusNew, firstShard[0].Status)
	require.Equal(t, "gone", firstShard[1].Table)
	require.Equal(t, VDiffTableStatusRemoved, firstShard[1].Status)
	require.Equal(t, "t1", firstShard[2].Table)
	require.Equal(t, VDiffTableStatusCommon, firstShard[2].Status)

	// New table: zero From values, Delta equals the To values.
	fresh := firstShard[0]
	require.Equal(t, VDiffCounterDiff{From: 0, To: 7, Delta: 7}, fresh.MatchingRows)
	require.Equal(t, VDiffCounterDiff{From: 0, To: 0, Delta: 0}, fresh.MismatchedRows)

	// Removed table: zero To values, Delta is the negative of the From values.
	gone := firstShard[1]
	require.Equal(t, VDiffCounterDiff{From: 4, To: 0, Delta: -4}, gone.MatchingRows)

	// Common table: before/after/delta for every requested counter.
	t1 := firstShard[2]
	require.Equal(t, VDiffCounterDiff{From: 10, To: 13, Delta: 3}, t1.MatchingRows)
	require.Equal(t, VDiffCounterDiff{From: 2, To: 0, Delta: -2}, t1.MismatchedRows)
	require.Equal(t, VDiffCounterDiff{From: 1, To: 0, Delta: -1}, t1.ExtraRowsSource)
	require.Equal(t, VDiffCounterDiff{From: 3, To: 5, Delta: 2}, t1.ExtraRowsTarget)

	// The unchanged shard reports zero deltas and a common table.
	secondShard := report.Shards[1].Tables
	require.Len(t, secondShard, 1)
	require.Equal(t, "t1", secondShard[0].Table)
	require.Equal(t, VDiffTableStatusCommon, secondShard[0].Status)
	require.Equal(t, VDiffCounterDiff{From: 20, To: 20, Delta: 0}, secondShard[0].MatchingRows)
}

func TestCompareVDiffSummariesStableJSON(t *testing.T) {
	// Build the maps in a deliberately unsorted order to prove the report
	// sorts its output.
	from := buildCompareSummary("ks", "wf", "a", map[string]map[string]TableState{
		"zeta": {"zzz": {MatchingRows: 1}, "aaa": {MismatchedRows: 1}},
	})
	to := buildCompareSummary("ks", "wf", "b", map[string]map[string]TableState{
		"zeta": {"mmm": {MatchingRows: 2}, "aaa": {MismatchedRows: 1}, "zzz": {MatchingRows: 1}},
	})

	report, err := CompareVDiffSummaries(from, to)
	require.NoError(t, err)

	first, err := json.Marshal(report)
	require.NoError(t, err)
	second, err := json.Marshal(report)
	require.NoError(t, err)
	require.Equal(t, first, second, "marshaling the report twice must be stable")

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(first, &decoded))
	shards := decoded["Shards"].([]any)
	require.Len(t, shards, 1)
	tables := shards[0].(map[string]any)["Tables"].([]any)
	require.Equal(t, "aaa", tables[0].(map[string]any)["Table"])
	require.Equal(t, "mmm", tables[1].(map[string]any)["Table"])
	require.Equal(t, "zzz", tables[2].(map[string]any)["Table"])
}

func TestCompareVDiffSummariesRejections(t *testing.T) {
	completedFrom := buildCompareSummary("ks", "wf", "uuid-from", map[string]map[string]TableState{
		"0": {"t1": {MatchingRows: 1}},
	})
	completedTo := buildCompareSummary("ks", "wf", "uuid-to", map[string]map[string]TableState{
		"0": {"t1": {MatchingRows: 2}},
	})

	testcases := []struct {
		name        string
		from        *Summary
		to          *Summary
		errContains string
	}{
		{
			name:        "missing first report",
			from:        nil,
			to:          completedTo,
			errContains: "first (from) VDiff is missing",
		},
		{
			name:        "missing second report",
			from:        completedFrom,
			to:          nil,
			errContains: "second (to) VDiff is missing",
		},
		{
			name:        "different target keyspaces",
			from:        buildCompareSummary("ks1", "wf", "uuid-from", map[string]map[string]TableState{"0": {"t1": {}}}),
			to:          buildCompareSummary("ks2", "wf", "uuid-to", map[string]map[string]TableState{"0": {"t1": {}}}),
			errContains: "different target keyspaces",
		},
		{
			name:        "different workflows",
			from:        buildCompareSummary("ks", "wf1", "uuid-from", map[string]map[string]TableState{"0": {"t1": {}}}),
			to:          buildCompareSummary("ks", "wf2", "uuid-to", map[string]map[string]TableState{"0": {"t1": {}}}),
			errContains: "different workflows",
		},
		{
			name:        "same vdiff uuid",
			from:        buildCompareSummary("ks", "wf", "same-uuid", map[string]map[string]TableState{"0": {"t1": {}}}),
			to:          buildCompareSummary("ks", "wf", "same-uuid", map[string]map[string]TableState{"0": {"t1": {}}}),
			errContains: "with itself",
		},
		{
			name: "first vdiff not completed",
			from: func() *Summary {
				s := buildCompareSummary("ks", "wf", "uuid-from", map[string]map[string]TableState{"0": {"t1": {}}})
				s.State = vdiff.StartedState
				return s
			}(),
			to:          completedTo,
			errContains: "only completed VDiffs can be compared",
		},
		{
			name: "second vdiff not completed",
			from: completedFrom,
			to: func() *Summary {
				s := buildCompareSummary("ks", "wf", "uuid-to", map[string]map[string]TableState{"0": {"t1": {}}})
				s.State = vdiff.ErrorState
				return s
			}(),
			errContains: "only completed VDiffs can be compared",
		},
		{
			name: "second run covers fewer shards",
			from: buildCompareSummary("ks", "wf", "uuid-from", map[string]map[string]TableState{
				"-80": {"t1": {}}, "80-": {"t1": {}},
			}),
			to:          buildCompareSummary("ks", "wf", "uuid-to", map[string]map[string]TableState{"-80": {"t1": {}}}),
			errContains: "incompatible shard scope",
		},
		{
			name: "second run covers more shards",
			from: buildCompareSummary("ks", "wf", "uuid-from", map[string]map[string]TableState{
				"-80": {"t1": {}},
			}),
			to: buildCompareSummary("ks", "wf", "uuid-to", map[string]map[string]TableState{
				"-80": {"t1": {}}, "80-": {"t1": {}},
			}),
			errContains: "incompatible shard scope",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			report, err := CompareVDiffSummaries(tc.from, tc.to)
			require.Error(t, err)
			require.Nil(t, report)
			require.ErrorContains(t, err, tc.errContains)
		})
	}
}

func TestCompareVDiffSummariesShardScopeErrorNamesShards(t *testing.T) {
	from := buildCompareSummary("ks", "wf", "uuid-from", map[string]map[string]TableState{
		"-80": {"t1": {}},
	})
	to := buildCompareSummary("ks", "wf", "uuid-to", map[string]map[string]TableState{
		"80-": {"t1": {}},
	})

	_, err := CompareVDiffSummaries(from, to)
	require.Error(t, err)
	require.ErrorContains(t, err, "-80")
	require.ErrorContains(t, err, "80-")
}
