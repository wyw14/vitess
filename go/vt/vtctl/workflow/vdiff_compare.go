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
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vdiff"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// VDiffTableCompareStatus describes how a table that appears in one (or both)
// of the two compared VDiff runs is present in them.
type VDiffTableCompareStatus string

const (
	// VDiffTableStatusCommon means the table was checked on the shard in both
	// VDiff runs.
	VDiffTableStatusCommon VDiffTableCompareStatus = "common"
	// VDiffTableStatusNew means the table was only checked in the second (To)
	// VDiff run, i.e. it is new relative to the first run.
	VDiffTableStatusNew VDiffTableCompareStatus = "new"
	// VDiffTableStatusRemoved means the table was only checked in the first
	// (From) VDiff run, i.e. it has disappeared by the second run.
	VDiffTableStatusRemoved VDiffTableCompareStatus = "removed"
)

// VDiffCounterDiff captures one row-count counter for a table on a shard:
// its value in the first run (From), its value in the second run (To) and the
// difference between them (Delta = To - From).
type VDiffCounterDiff struct {
	From  int64 `json:"From"`
	To    int64 `json:"To"`
	Delta int64 `json:"Delta"`
}

// VDiffTableComparison is the aligned comparison of a single table on a single
// shard between two VDiff runs. Only the scalar row counters are included;
// sample row content is never part of the comparison.
type VDiffTableComparison struct {
	Table           string                  `json:"Table"`
	Status          VDiffTableCompareStatus `json:"Status"`
	MatchingRows    VDiffCounterDiff        `json:"MatchingRows"`
	MismatchedRows  VDiffCounterDiff        `json:"MismatchedRows"`
	ExtraRowsSource VDiffCounterDiff        `json:"ExtraRowsSource"`
	ExtraRowsTarget VDiffCounterDiff        `json:"ExtraRowsTarget"`
}

// VDiffShardComparison is the aligned per-table comparison for one target
// shard.
type VDiffShardComparison struct {
	Shard  string                 `json:"Shard"`
	Tables []VDiffTableComparison `json:"Tables"`
}

// VDiffRunRef identifies one of the two compared VDiff runs.
type VDiffRunRef struct {
	UUID        string           `json:"UUID"`
	State       vdiff.VDiffState `json:"State"`
	StartedAt   string           `json:"StartedAt,omitempty"`
	CompletedAt string           `json:"CompletedAt,omitempty"`
}

// VDiffComparisonReport is the result of aligning two completed VDiff runs of
// the same workflow. Shards and tables are always ordered by name so that the
// JSON and text renderings are stable.
type VDiffComparisonReport struct {
	Keyspace string                 `json:"Keyspace"`
	Workflow string                 `json:"Workflow"`
	From     VDiffRunRef            `json:"From"`
	To       VDiffRunRef            `json:"To"`
	Shards   []VDiffShardComparison `json:"Shards"`
}

func newVDiffRunRef(s *Summary) VDiffRunRef {
	return VDiffRunRef{
		UUID:        s.UUID,
		State:       s.State,
		StartedAt:   s.StartedAt,
		CompletedAt: s.CompletedAt,
	}
}

func makeCounterDiff(fromVal, toVal int64) VDiffCounterDiff {
	return VDiffCounterDiff{
		From:  fromVal,
		To:    toVal,
		Delta: toVal - fromVal,
	}
}

// tableCounterDiff builds the aligned counter comparison for one table on one
// shard. A table that is new in the To run has zero-valued From TableState and
// a table that has disappeared in the To run has a zero-valued To TableState,
// so the missing side naturally reports zero counts.
func tableCounterDiff(from, to TableState) []VDiffCounterDiff {
	return []VDiffCounterDiff{
		makeCounterDiff(from.MatchingRows, to.MatchingRows),
		makeCounterDiff(from.MismatchedRows, to.MismatchedRows),
		makeCounterDiff(from.ExtraRowsSource, to.ExtraRowsSource),
		makeCounterDiff(from.ExtraRowsTarget, to.ExtraRowsTarget),
	}
}

func sortedShardNames(summaries map[string]ShardSummary) []string {
	names := make([]string, 0, len(summaries))
	for name := range summaries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func sortedTableNames(tables map[string]TableState) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// CompareVDiffSummaries aligns and compares two already completed VDiff runs
// of the same workflow. It only reads the summaries built from existing VDiff
// reports: it never executes a VDiff and never touches replication.
//
// The comparison is aligned by target shard and table. For every table on a
// shard the matching, mismatched, extra-source and extra-target row counts are
// reported with their values in the first run (From), the second run (To) and
// the difference (To - From). Tables that only exist in one of the two runs
// are marked as new or removed rather than treated as an error.
//
// The comparison is rejected (and no report is produced) when the two runs
// target different keyspaces or workflows, when either run is not completed,
// or when their target shard coverage is not identical and therefore cannot
// be aligned.
func CompareVDiffSummaries(from, to *Summary) (*VDiffComparisonReport, error) {
	if from == nil {
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT,
			"cannot compare VDiffs: the report for the first (from) VDiff is missing")
	}
	if to == nil {
		return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT,
			"cannot compare VDiffs: the report for the second (to) VDiff is missing")
	}
	if from.Keyspace != to.Keyspace {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"cannot compare VDiffs for different target keyspaces %q and %q", from.Keyspace, to.Keyspace)
	}
	if from.Workflow != to.Workflow {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"cannot compare VDiffs for different workflows %q and %q in target keyspace %q",
			from.Workflow, to.Workflow, from.Keyspace)
	}
	if from.UUID != "" && from.UUID == to.UUID {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT,
			"cannot compare a VDiff with itself: %s", from.UUID)
	}
	if from.State != vdiff.CompletedState {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"the first VDiff %s for %s.%s has state %q, only completed VDiffs can be compared",
			from.UUID, from.Keyspace, from.Workflow, from.State)
	}
	if to.State != vdiff.CompletedState {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"the second VDiff %s for %s.%s has state %q, only completed VDiffs can be compared",
			to.UUID, to.Keyspace, to.Workflow, to.State)
	}

	fromShards := sortedShardNames(from.ShardSummaries)
	toShards := sortedShardNames(to.ShardSummaries)
	fromShardSet := make(map[string]struct{}, len(fromShards))
	for _, shard := range fromShards {
		fromShardSet[shard] = struct{}{}
	}
	toShardSet := make(map[string]struct{}, len(toShards))
	for _, shard := range toShards {
		toShardSet[shard] = struct{}{}
	}
	var onlyFrom, onlyTo []string
	for _, shard := range fromShards {
		if _, ok := toShardSet[shard]; !ok {
			onlyFrom = append(onlyFrom, shard)
		}
	}
	for _, shard := range toShards {
		if _, ok := fromShardSet[shard]; !ok {
			onlyTo = append(onlyTo, shard)
		}
	}
	if len(onlyFrom) > 0 || len(onlyTo) > 0 {
		var detail []string
		if len(onlyFrom) > 0 {
			detail = append(detail, "only covered by the first VDiff: "+strings.Join(onlyFrom, ","))
		}
		if len(onlyTo) > 0 {
			detail = append(detail, "only covered by the second VDiff: "+strings.Join(onlyTo, ","))
		}
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"cannot compare VDiffs %s and %s for %s.%s: incompatible shard scope (%s)",
			from.UUID, to.UUID, from.Keyspace, from.Workflow, strings.Join(detail, "; "))
	}

	report := &VDiffComparisonReport{
		Keyspace: from.Keyspace,
		Workflow: from.Workflow,
		From:     newVDiffRunRef(from),
		To:       newVDiffRunRef(to),
		Shards:   make([]VDiffShardComparison, 0, len(fromShards)),
	}

	for _, shard := range fromShards {
		fromTables := from.ShardSummaries[shard].TableStates
		toTables := to.ShardSummaries[shard].TableStates

		tableNames := sortedTableNames(mergeTableStates(fromTables, toTables))
		shardComparison := VDiffShardComparison{
			Shard:  shard,
			Tables: make([]VDiffTableComparison, 0, len(tableNames)),
		}
		for _, table := range tableNames {
			fromState, inFrom := fromTables[table]
			toState, inTo := toTables[table]

			status := VDiffTableStatusCommon
			switch {
			case inFrom && !inTo:
				status = VDiffTableStatusRemoved
			case !inFrom && inTo:
				status = VDiffTableStatusNew
			}

			counters := tableCounterDiff(fromState, toState)
			shardComparison.Tables = append(shardComparison.Tables, VDiffTableComparison{
				Table:           table,
				Status:          status,
				MatchingRows:    counters[0],
				MismatchedRows:  counters[1],
				ExtraRowsSource: counters[2],
				ExtraRowsTarget: counters[3],
			})
		}
		report.Shards = append(report.Shards, shardComparison)
	}

	return report, nil
}

func mergeTableStates(from, to map[string]TableState) map[string]TableState {
	merged := make(map[string]TableState, len(from)+len(to))
	for name, state := range from {
		merged[name] = state
	}
	for name, state := range to {
		merged[name] = state
	}
	return merged
}
