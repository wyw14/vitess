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

package wrangler

import (
	"context"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vttablet/tabletmanager/vreplication"
)

// This file wires the versioned topology checkpoint
// (topo.ReshardCheckpoint) into the reshard workflow. The
// checkpoint records the cutover stage (copy, catchup,
// switch_traffic, complete) so that a paused or restarted workflow
// resumes where it left off instead of switching traffic a second time.

// recordCopyCheckpoint persists the copy stage once the vreplication
// streams of a new reshard have been created.
func (rs *resharder) recordCopyCheckpoint(ctx context.Context) error {
	_, err := rs.wr.ts.UpdateReshardCheckpointFields(ctx, rs.keyspace, rs.workflow, func(cp *topo.ReshardCheckpoint) {
		advanceStageIfNeeded(cp, vreplication.WorkflowStageCopy)
	})
	return err
}

// loadOrInitCheckpoint returns the checkpoint of the workflow,
// bootstrapping it at the copy stage when no checkpoint exists yet
// (which is the case for workflows created by an older Vitess
// release).
func (ts *trafficSwitcher) loadOrInitCheckpoint(ctx context.Context) (*topo.ReshardCheckpointInfo, error) {
	return ts.TopoServer().UpdateReshardCheckpointFields(ctx, ts.TargetKeyspaceName(), ts.WorkflowName(), func(cp *topo.ReshardCheckpoint) {
		advanceStageIfNeeded(cp, vreplication.WorkflowStageCopy)
	})
}

// saveCheckpoint compare-and-swaps the checkpoint into stage,
// never moving it backwards, and applies mutate to record
// additional progress (like the switched-traffic flags).
func (ts *trafficSwitcher) saveCheckpoint(ctx context.Context, stage vreplication.WorkflowStage, mutate func(*topo.ReshardCheckpoint)) (*topo.ReshardCheckpointInfo, error) {
	return ts.TopoServer().UpdateReshardCheckpointFields(ctx, ts.TargetKeyspaceName(), ts.WorkflowName(), func(cp *topo.ReshardCheckpoint) {
		advanceStageIfNeeded(cp, stage)
		if mutate != nil {
			mutate(cp)
		}
	})
}

// rollbackCheckpointToCopy resets the checkpoint to the copy
// stage after a failed or canceled cutover, so the workflow can
// be retried. It is best effort: it must never mask the error
// that triggered the rollback. It never rolls back a checkpoint
// that already reached the point of no return (switch_traffic).
func (ts *trafficSwitcher) rollbackCheckpointToCopy(ctx context.Context) {
	_, err := ts.TopoServer().UpdateReshardCheckpointFields(ctx, ts.TargetKeyspaceName(), ts.WorkflowName(), func(cp *topo.ReshardCheckpoint) {
		rank, ok := vreplication.WorkflowStage(cp.Stage).Rank()
		switchTrafficRank, _ := vreplication.WorkflowStageSwitchTraffic.Rank()
		if !ok {
			// Unknown stage: leave it untouched rather than
			// stomping a future release's checkpoint.
			return
		}
		if rank >= switchTrafficRank {
			// The point of no return was reached, the journal
			// based recovery path must be used instead.
			return
		}
		cp.Stage = vreplication.WorkflowStageCopy.String()
		cp.JournalID = 0
		cp.WritesAllowed = false
		cp.TrafficSwitched = false
	})
	if err != nil {
		ts.Logger().Errorf("Failed to roll back the checkpoint of workflow %s.%s to the copy stage: %v",
			ts.TargetKeyspaceName(), ts.WorkflowName(), err)
	}
}

// deleteWorkflowCheckpoint removes the checkpoint when the workflow
// artifacts are dropped. A missing checkpoint is ignored so that
// workflows created by older releases can still be canceled.
func (ts *trafficSwitcher) deleteWorkflowCheckpoint(ctx context.Context) error {
	cpi, err := ts.TopoServer().GetReshardCheckpoint(ctx, ts.TargetKeyspaceName(), ts.WorkflowName())
	if err != nil {
		if topo.IsErrType(err, topo.NoNode) {
			return nil
		}
		return err
	}
	if err := ts.TopoServer().DeleteReshardCheckpoint(ctx, cpi); err != nil && !topo.IsErrType(err, topo.NoNode) {
		return err
	}
	ts.Logger().Infof("Deleted reshard workflow checkpoint for %s.%s", ts.TargetKeyspaceName(), ts.WorkflowName())
	return nil
}

// advanceStageIfNeeded moves the checkpoint to stage unless it is
// already at that stage or further ahead. An unset (empty) stage
// represents a bootstrap record that has never been persisted and is
// therefore allowed to enter any stage.
func advanceStageIfNeeded(cp *topo.ReshardCheckpoint, stage vreplication.WorkflowStage) {
	if cp.Stage == "" {
		cp.Stage = stage.String()
		return
	}
	current := vreplication.WorkflowStage(cp.Stage)
	if current.CanAdvanceTo(stage) {
		cp.Stage = stage.String()
	}
}
