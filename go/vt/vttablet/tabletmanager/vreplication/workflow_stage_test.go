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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWorkflowStageValidAndRank(t *testing.T) {
	stages := []struct {
		stage WorkflowStage
		rank  int
		valid bool
	}{
		{WorkflowStageCopy, 1, true},
		{WorkflowStageCatchup, 2, true},
		{WorkflowStageSwitchTraffic, 3, true},
		{WorkflowStageComplete, 4, true},
		{WorkflowStage("future_stage"), 0, false},
		{WorkflowStage(""), 0, false},
	}
	for _, tc := range stages {
		rank, ok := tc.stage.Rank()
		assert.Equalf(t, tc.valid, ok, "Valid() for %q", tc.stage)
		assert.Equalf(t, tc.rank, rank, "Rank() for %q", tc.stage)
		assert.Equalf(t, tc.valid, tc.stage.Valid(), "Valid() for %q", tc.stage)
	}
	assert.Equal(t, "copy", WorkflowStageCopy.String())
	assert.Equal(t, "catchup", WorkflowStageCatchup.String())
	assert.Equal(t, "switch_traffic", WorkflowStageSwitchTraffic.String())
	assert.Equal(t, "complete", WorkflowStageComplete.String())
}

func TestWorkflowStageCanAdvanceTo(t *testing.T) {
	// Forward progression is allowed, including to the same stage so that
	// checkpoint rewrites are idempotent.
	forward := [][2]WorkflowStage{
		{WorkflowStageCopy, WorkflowStageCopy},
		{WorkflowStageCopy, WorkflowStageCatchup},
		{WorkflowStageCopy, WorkflowStageSwitchTraffic},
		{WorkflowStageCopy, WorkflowStageComplete},
		{WorkflowStageCatchup, WorkflowStageCatchup},
		{WorkflowStageCatchup, WorkflowStageSwitchTraffic},
		{WorkflowStageCatchup, WorkflowStageComplete},
		{WorkflowStageSwitchTraffic, WorkflowStageSwitchTraffic},
		{WorkflowStageSwitchTraffic, WorkflowStageComplete},
		{WorkflowStageComplete, WorkflowStageComplete},
	}
	for _, pair := range forward {
		assert.Truef(t, pair[0].CanAdvanceTo(pair[1]), "%s should be able to advance to %s", pair[0], pair[1])
	}

	// Backward progression is never an "advance".
	backward := [][2]WorkflowStage{
		{WorkflowStageComplete, WorkflowStageSwitchTraffic},
		{WorkflowStageComplete, WorkflowStageCatchup},
		{WorkflowStageComplete, WorkflowStageCopy},
		{WorkflowStageSwitchTraffic, WorkflowStageCatchup},
		{WorkflowStageSwitchTraffic, WorkflowStageCopy},
		{WorkflowStageCatchup, WorkflowStageCopy},
	}
	for _, pair := range backward {
		assert.Falsef(t, pair[0].CanAdvanceTo(pair[1]), "%s must not advance backwards to %s", pair[0], pair[1])
	}

	// Unknown stages never participate in transitions.
	assert.False(t, WorkflowStageCopy.CanAdvanceTo(WorkflowStage("future_stage")))
	assert.False(t, WorkflowStage("future_stage").CanAdvanceTo(WorkflowStageComplete))
}

func TestWorkflowStageCanRollbackTo(t *testing.T) {
	// Any stage after copy may roll back to copy after a failed or
	// canceled cutover.
	assert.False(t, WorkflowStageCopy.CanRollbackTo(WorkflowStageCopy))
	assert.True(t, WorkflowStageCatchup.CanRollbackTo(WorkflowStageCopy))
	assert.True(t, WorkflowStageSwitchTraffic.CanRollbackTo(WorkflowStageCopy))
	assert.True(t, WorkflowStageComplete.CanRollbackTo(WorkflowStageCopy))

	// Rollback may never land on an intermediate stage.
	assert.False(t, WorkflowStageSwitchTraffic.CanRollbackTo(WorkflowStageCatchup))
	assert.False(t, WorkflowStageComplete.CanRollbackTo(WorkflowStageSwitchTraffic))
	assert.False(t, WorkflowStageComplete.CanRollbackTo(WorkflowStageCatchup))

	// Unknown stages never participate in rollbacks.
	assert.False(t, WorkflowStage("future_stage").CanRollbackTo(WorkflowStageCopy))
	assert.False(t, WorkflowStageCatchup.CanRollbackTo(WorkflowStage("future_stage")))
}
