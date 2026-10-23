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

package vreplication

import "vitess.io/vitess/go/vt/topo"

// WorkflowStage is the cutover stage of a vreplication reshard (or
// MoveTables) workflow. Stages are persisted in the versioned topology
// checkpoint (see topo.ReshardCheckpoint). This package owns the stage
// semantics; the topo package only stores the value.
type WorkflowStage string

// The ordered cutover stages of a reshard workflow.
const (
	// WorkflowStageCopy: the vreplication streams have been created and
	// the initial copy is running (or done).
	WorkflowStageCopy WorkflowStage = topo.ReshardStageCopy
	// WorkflowStageCatchup: source writes are stopped and the target
	// streams are catching up to the cutover position.
	WorkflowStageCatchup WorkflowStage = topo.ReshardStageCatchup
	// WorkflowStageSwitchTraffic: the point of no return (the resharding
	// journal) has been reached and traffic is being switched to the
	// target shards.
	WorkflowStageSwitchTraffic WorkflowStage = topo.ReshardStageSwitchTraffic
	// WorkflowStageComplete: the traffic switch completed and the target
	// streams were frozen.
	WorkflowStageComplete WorkflowStage = topo.ReshardStageComplete
)

// String returns the string representation of the stage.
func (s WorkflowStage) String() string {
	return string(s)
}

// Valid returns whether the stage is one of the stages known to this
// release. A checkpoint carrying an unknown (future) stage is still
// readable, it must simply never be advanced by an older binary.
func (s WorkflowStage) Valid() bool {
	_, ok := s.rank()
	return ok
}

// Rank returns the monotonic ordering of the stage:
// copy (1) < catchup (2) < switch_traffic (3) < complete (4).
// It returns 0 and false for an unknown stage.
func (s WorkflowStage) Rank() (int, bool) {
	return s.rank()
}

func (s WorkflowStage) rank() (int, bool) {
	switch s {
	case WorkflowStageCopy:
		return 1, true
	case WorkflowStageCatchup:
		return 2, true
	case WorkflowStageSwitchTraffic:
		return 3, true
	case WorkflowStageComplete:
		return 4, true
	default:
		return 0, false
	}
}

// CanAdvanceTo reports whether a workflow may move forward from stage
// s to stage to. Rewriting the same stage is allowed so that
// checkpoint updates are idempotent. Backward transitions are rejected:
// rollback to the copy stage must use CanRollbackTo explicitly.
func (s WorkflowStage) CanAdvanceTo(to WorkflowStage) bool {
	fromRank, fromOK := s.rank()
	toRank, toOK := to.rank()
	if !fromOK || !toOK {
		return false
	}
	return toRank >= fromRank
}

// CanRollbackTo reports whether a workflow that failed or was canceled
// before the point of no return may be moved back to stage to.
// Only rolling back to the copy stage is supported, and only from a
// stage after copy.
func (s WorkflowStage) CanRollbackTo(to WorkflowStage) bool {
	fromRank, fromOK := s.rank()
	toRank, toOK := to.rank()
	if !fromOK || !toOK {
		return false
	}
	return to == WorkflowStageCopy && fromRank > toRank
}
