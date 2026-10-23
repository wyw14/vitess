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

package topo_test

import (
	"path"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func newCheckpointTopo(t *testing.T) *topo.Server {
	t.Helper()
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "zone1")
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}))
	return ts
}

func TestReshardCheckpointLifecycle(t *testing.T) {
	ctx := t.Context()
	ts := newCheckpointTopo(t)

	// A missing checkpoint reads as NoNode and does not affect the
	// original keyspace/shard state.
	cpi, err := ts.GetReshardCheckpoint(ctx, "ks", "wf")
	require.Error(t, err)
	assert.True(t, topo.IsErrType(err, topo.NoNode))
	assert.Nil(t, cpi)

	cp := &topo.ReshardCheckpoint{
		Stage:     topo.ReshardStageCopy,
		JournalID: 42,
	}
	cpi, err = ts.CreateReshardCheckpoint(ctx, "ks", "wf", cp)
	require.NoError(t, err)
	assert.Equal(t, topo.CurrentReshardCheckpointVersion, cpi.Version)
	assert.Equal(t, "ks", cpi.Keyspace)
	assert.Equal(t, "wf", cpi.Workflow)
	assert.Equal(t, topo.ReshardStageCopy, cpi.Stage)
	assert.False(t, cpi.UpdatedAt.IsZero())

	// Creating it twice fails with NodeExists.
	_, err = ts.CreateReshardCheckpoint(ctx, "ks", "wf", &topo.ReshardCheckpoint{Stage: topo.ReshardStageCopy})
	require.Error(t, err)
	assert.True(t, topo.IsErrType(err, topo.NodeExists))

	// Advancing the stage bumps the topology version and persists
	// the additional fields.
	cpi.Stage = topo.ReshardStageCatchup
	require.NoError(t, ts.UpdateReshardCheckpoint(ctx, cpi))

	got, err := ts.GetReshardCheckpoint(ctx, "ks", "wf")
	require.NoError(t, err)
	assert.Equal(t, topo.ReshardStageCatchup, got.Stage)
	assert.Equal(t, int64(42), got.JournalID)
	assert.Equal(t, topo.CurrentReshardCheckpointVersion, got.Version)

	// Deleting the checkpoint makes subsequent reads return NoNode.
	require.NoError(t, ts.DeleteReshardCheckpoint(ctx, got))
	_, err = ts.GetReshardCheckpoint(ctx, "ks", "wf")
	assert.True(t, topo.IsErrType(err, topo.NoNode))
}

func TestReshardCheckpointBadVersion(t *testing.T) {
	ctx := t.Context()
	ts := newCheckpointTopo(t)

	cpi, err := ts.CreateReshardCheckpoint(ctx, "ks", "wf", &topo.ReshardCheckpoint{Stage: topo.ReshardStageCopy})
	require.NoError(t, err)

	// A concurrent writer advances the record...
	_, err = ts.UpdateReshardCheckpointFields(ctx, "ks", "wf", func(cp *topo.ReshardCheckpoint) {
		cp.Stage = topo.ReshardStageCatchup
	})
	require.NoError(t, err)

	// ...so the stale in-memory copy can no longer be saved.
	cpi.Stage = topo.ReshardStageSwitchTraffic
	err = ts.UpdateReshardCheckpoint(ctx, cpi)
	require.Error(t, err)
	assert.True(t, topo.IsErrType(err, topo.BadVersion))

	got, err := ts.GetReshardCheckpoint(ctx, "ks", "wf")
	require.NoError(t, err)
	assert.Equal(t, topo.ReshardStageCatchup, got.Stage)
}

// TestReshardCheckpointConcurrentUpdates makes sure two writers that
// modify different fields of the same checkpoint both win thanks to
// compare-and-swap retries.
func TestReshardCheckpointConcurrentUpdates(t *testing.T) {
	ctx := t.Context()
	ts := newCheckpointTopo(t)
	_, err := ts.CreateReshardCheckpoint(ctx, "ks", "wf", &topo.ReshardCheckpoint{Stage: topo.ReshardStageCatchup})
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, uerr := ts.UpdateReshardCheckpointFields(ctx, "ks", "wf", func(cp *topo.ReshardCheckpoint) {
			cp.Stage = topo.ReshardStageSwitchTraffic
			cp.WritesAllowed = true
		})
		assert.NoError(t, uerr)
	}()
	go func() {
		defer wg.Done()
		_, uerr := ts.UpdateReshardCheckpointFields(ctx, "ks", "wf", func(cp *topo.ReshardCheckpoint) {
			cp.TrafficSwitched = true
		})
		assert.NoError(t, uerr)
	}()
	wg.Wait()

	got, err := ts.GetReshardCheckpoint(ctx, "ks", "wf")
	require.NoError(t, err)
	assert.True(t, got.WritesAllowed)
	assert.True(t, got.TrafficSwitched)
}

// TestReshardCheckpointUpsertCreatesMissingRecord verifies that the
// field-update helper bootstraps the record when it is missing.
func TestReshardCheckpointUpsertCreatesMissingRecord(t *testing.T) {
	ctx := t.Context()
	ts := newCheckpointTopo(t)

	cpi, err := ts.UpdateReshardCheckpointFields(ctx, "ks", "wf", func(cp *topo.ReshardCheckpoint) {
		cp.Stage = topo.ReshardStageCopy
	})
	require.NoError(t, err)
	assert.Equal(t, topo.ReshardStageCopy, cpi.Stage)

	got, err := ts.GetReshardCheckpoint(ctx, "ks", "wf")
	require.NoError(t, err)
	assert.Equal(t, topo.ReshardStageCopy, got.Stage)
}

// TestReshardCheckpointWireCompatibility covers payload-level
// forward and backward compatibility:
//   - an old binary reads (but never mutates) a future-version
//     checkpoint carrying unknown fields,
//   - a payload written without an explicit version is interpreted
//     as the original v1 payload.
func TestReshardCheckpointWireCompatibility(t *testing.T) {
	ctx := t.Context()
	ts, factory := memorytopo.NewServerAndFactory(ctx, "zone1")
	t.Cleanup(ts.Close)
	require.NoError(t, ts.CreateKeyspace(ctx, "ks", &topodatapb.Keyspace{}))
	conn, err := factory.Create(topo.GlobalCell, "", "")
	require.NoError(t, err)

	// Raw payload as a future binary would write it: higher version,
	// a stage this release does not know and an extra field.
	futureRaw := []byte(`{"version":99,"keyspace":"ks","workflow":"future-wf","stage":"future_stage","future_only":"x"}`)
	_, err = conn.Create(ctx, path.Join("keyspaces", "ks", topo.ReshardCheckpointsPath, "future-wf", topo.ReshardCheckpointFile), futureRaw)
	require.NoError(t, err)

	// The record is still readable by the older server...
	got, err := ts.GetReshardCheckpoint(ctx, "ks", "future-wf")
	require.NoError(t, err)
	assert.Equal(t, uint32(99), got.Version)
	assert.Equal(t, "future_stage", got.Stage)

	// ...but it must not be mutated or downgraded.
	_, err = ts.UpdateReshardCheckpointFields(ctx, "ks", "future-wf", func(cp *topo.ReshardCheckpoint) {
		cp.Stage = topo.ReshardStageComplete
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "newer Vitess version")

	again, err := ts.GetReshardCheckpoint(ctx, "ks", "future-wf")
	require.NoError(t, err)
	assert.Equal(t, uint32(99), again.Version)
	assert.Equal(t, "future_stage", again.Stage)

	// A payload with no explicit version is treated as v1.
	legacyRaw := []byte(`{"keyspace":"ks","workflow":"legacy-wf","stage":"copy"}`)
	_, err = conn.Create(ctx, path.Join("keyspaces", "ks", topo.ReshardCheckpointsPath, "legacy-wf", topo.ReshardCheckpointFile), legacyRaw)
	require.NoError(t, err)
	legacy, err := ts.GetReshardCheckpoint(ctx, "ks", "legacy-wf")
	require.NoError(t, err)
	assert.Equal(t, topo.MinSupportedReshardCheckpointVersion, legacy.Version)
	assert.Equal(t, topo.ReshardStageCopy, legacy.Stage)
}

// TestReshardCheckpointDoesNotChangeOriginalState makes sure the
// checkpoint is additive metadata: writing it must not alter the
// keyspace and shard records that older clients keep reading.
func TestReshardCheckpointDoesNotChangeOriginalState(t *testing.T) {
	ctx := t.Context()
	ts := newCheckpointTopo(t)
	require.NoError(t, ts.CreateShard(ctx, "ks", "-80"))
	require.NoError(t, ts.CreateShard(ctx, "ks", "80-"))

	shardsBefore, err := ts.FindAllShardsInKeyspace(ctx, "ks", nil)
	require.NoError(t, err)
	keyspaceBefore, err := ts.GetKeyspace(ctx, "ks")
	require.NoError(t, err)

	for _, stage := range []string{topo.ReshardStageCopy, topo.ReshardStageCatchup, topo.ReshardStageSwitchTraffic, topo.ReshardStageComplete} {
		_, err := ts.UpdateReshardCheckpointFields(ctx, "ks", "wf", func(cp *topo.ReshardCheckpoint) {
			cp.Stage = stage
		})
		require.NoError(t, err)
	}

	keyspaceAfter, err := ts.GetKeyspace(ctx, "ks")
	require.NoError(t, err)
	assert.Equal(t, keyspaceBefore.Keyspace, keyspaceAfter.Keyspace)

	shardsAfter, err := ts.FindAllShardsInKeyspace(ctx, "ks", nil)
	require.NoError(t, err)
	assert.Len(t, shardsAfter, len(shardsBefore))
	for name := range shardsBefore {
		_, ok := shardsAfter[name]
		assert.Truef(t, ok, "shard %s must still be readable by old clients", name)
	}
}
