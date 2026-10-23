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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
)

// TestResharderCopyCheckpoint verifies that creating a reshard
// workflow persists the initial, versioned checkpoint at the copy
// stage.
func TestResharderCopyCheckpoint(t *testing.T) {
	ctx := t.Context()
	env := newTestResharderEnv(t, ctx, []string{"0"}, []string{"-80", "80-"})
	defer env.close()

	env.expectValidation()
	env.expectNoRefStream()
	env.tmc.expectVRQuery(
		200,
		insertPrefix+
			`\('resharderTest', 'keyspace:"ks" shard:"0" filter:{rules:{match:"/.*" filter:"-80"}}', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks', 4, 0, false, '{}'\)`+eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(
		210,
		insertPrefix+
			`\('resharderTest', 'keyspace:"ks" shard:"0" filter:{rules:{match:"/.*" filter:"80-"}}', '', [0-9]*, [0-9]*, '', '', [0-9]*, 0, 'Stopped', 'vt_ks', 4, 0, false, '{}'\)`+eol,
		&sqltypes.Result{},
	)
	env.tmc.expectVRQuery(200, "update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})
	env.tmc.expectVRQuery(210, "update /*vt+ ALLOW_UNSAFE_VREPLICATION_WRITE */ _vt.vreplication set state='Running' where db_name='vt_ks'", &sqltypes.Result{})

	err := env.wr.Reshard(ctx, env.keyspace, env.workflow, env.sources, env.targets, true, "", "", defaultOnDDL, true, false, false)
	require.NoError(t, err)
	env.tmc.verifyQueries(t)

	cpi, err := env.topoServ.GetReshardCheckpoint(ctx, env.keyspace, env.workflow)
	require.NoError(t, err)
	assert.Equal(t, topo.CurrentReshardCheckpointVersion, cpi.Version)
	assert.Equal(t, env.keyspace, cpi.Keyspace)
	assert.Equal(t, env.workflow, cpi.Workflow)
	assert.Equal(t, topo.ReshardStageCopy, cpi.Stage)
	assert.False(t, cpi.WritesAllowed)
	assert.False(t, cpi.TrafficSwitched)
}

// TestShardMigrateCheckpointLockConflict verifies that a
// SwitchWrites call fails with a lock error while another process
// holds the source keyspace lock, without touching the workflow.
func TestShardMigrateCheckpointLockConflict(t *testing.T) {
	ctx := t.Context()
	tme := newTestShardMigrater(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer tme.close(t)

	currentLockTimeout := topo.LockTimeout
	topo.LockTimeout = 500 * time.Millisecond
	t.Cleanup(func() { topo.LockTimeout = currentLockTimeout })

	// Another process holds the source keyspace lock.
	lockCtx, unlock, err := tme.ts.LockKeyspace(context.Background(), "ks", "concurrent-operation")
	require.NoError(t, err)
	t.Cleanup(func() {
		var unlockErr error
		unlock(&unlockErr)
	})
	_ = lockCtx

	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", time.Second, false, false, false, false, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to lock the ks keyspace")

	// No checkpoint may have been written while waiting for the lock.
	_, err = tme.ts.GetReshardCheckpoint(ctx, "ks", "test")
	assert.True(t, topo.IsErrType(err, topo.NoNode))
	verifyQueries(t, tme.allDBClients)
}

// topoWriteFailError injects failures for every topology write the
// cutover performs: shard serving-graph writes and per-cell
// SrvKeyspace writes. Checkpoint writes use a different path and
// therefore still succeed.
func topoWriteFailError(tme *testShardMigraterEnv) {
	tme.topoFactory.AddOperationError(memorytopo.Update, `keyspaces/ks/shards/.+/Shard`, errors.New("injected shard write failure"))
	// Cell-local connections see keyspace-relative paths (no cells/<cell>
	// prefix), so the SrvKeyspace write is matched the same way.
	tme.topoFactory.AddOperationError(memorytopo.Update, `keyspaces/ks/SrvKeyspace`, errors.New("injected srv keyspace write failure"))
}

// seedExistingJournals makes the resharding journal check report
// existing (empty) journals on every source shard, so that
// createJournals does not need to insert anything.
func seedExistingJournals(tme *testShardMigraterEnv) {
	for _, dbclient := range tme.dbSourceClients {
		dbclient.addQuery("select val from _vt.resharding_journal where id=6432976123657117097",
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("val", "varbinary"), ""), nil)
	}
}

// TestShardMigrateResumeSkipsCutover is the restart guarantee:
// a checkpoint left at switch_traffic with the switched-traffic
// flags set (as if the previous process died after the routing
// change but before freeze) makes the resumed SwitchWrites skip
// allowTargetWrites/changeRouting entirely and simply finalize the
// workflow. The injected shard-write failure proves the skip: a
// repeated cutover would have errored out.
func TestShardMigrateResumeSkipsCutover(t *testing.T) {
	ctx := t.Context()
	tme := newTestShardMigrater(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer tme.close(t)

	_, err := tme.ts.CreateReshardCheckpoint(ctx, "ks", "test", &topo.ReshardCheckpoint{
		Stage:           topo.ReshardStageSwitchTraffic,
		JournalID:       6432976123657117097,
		WritesAllowed:   true,
		TrafficSwitched: true,
	})
	require.NoError(t, err)

	topoWriteFailError(tme)
	seedExistingJournals(tme)
	tme.expectFrozenTargetVReplication()

	journalID, _, err := tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", time.Second, false, false, false, false, true)
	require.NoError(t, err)
	assert.Equal(t, int64(6432976123657117097), journalID)
	verifyQueries(t, tme.allDBClients)

	cpi, err := tme.ts.GetReshardCheckpoint(ctx, "ks", "test")
	require.NoError(t, err)
	assert.Equal(t, topo.ReshardStageComplete, cpi.Stage)
	assert.True(t, cpi.WritesAllowed)
	assert.True(t, cpi.TrafficSwitched)

	// The serving graph is exactly as it was before the resumed
	// call: traffic was not switched a second time.
	checkServedTypes(t, tme.ts, "ks:-40", 3)
	checkServedTypes(t, tme.ts, "ks:40-", 3)
	checkServedTypes(t, tme.ts, "ks:-80", 0)
	checkServedTypes(t, tme.ts, "ks:80-", 0)
	checkIfPrimaryServing(t, tme.ts, "ks:-40", true)
	checkIfPrimaryServing(t, tme.ts, "ks:40-", true)
	checkIfPrimaryServing(t, tme.ts, "ks:-80", false)
	checkIfPrimaryServing(t, tme.ts, "ks:80-", false)

	// A second resume is a full no-op (the checkpoint already
	// records completion): no journal or freeze queries happen.
	_, _, err = tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", time.Second, false, false, false, false, true)
	require.NoError(t, err)
	verifyQueries(t, tme.allDBClients)
}

// TestShardMigrateResumeWithoutCheckpointPerformsCutover is the
// control for TestShardMigrateResumeSkipsCutover: without the
// checkpoint flags the resumed cutover really performs (and here
// hits the injected shard write failure).
func TestShardMigrateResumeWithoutCheckpointPerformsCutover(t *testing.T) {
	ctx := t.Context()
	tme := newTestShardMigrater(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer tme.close(t)

	topoWriteFailError(tme)
	seedExistingJournals(tme)

	_, _, err := tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", time.Second, false, false, false, false, true)
	require.Error(t, err)
	// The cutover really ran and hit the injected failure while
	// allowing target writes (per-cell SrvKeyspace writes).
	assert.Contains(t, err.Error(), "failed to allow writes")
	verifyQueries(t, tme.allDBClients)

	// The checkpoint advanced to the cutover stage but did not
	// record the writes-allowed flag.
	cpi, err := tme.ts.GetReshardCheckpoint(ctx, "ks", "test")
	require.NoError(t, err)
	assert.Equal(t, topo.ReshardStageSwitchTraffic, cpi.Stage)
	assert.False(t, cpi.WritesAllowed)
	assert.False(t, cpi.TrafficSwitched)
}

// TestShardMigrateCheckpointRollbackOnFailure verifies that a
// failed cutover before the point of no return rolls the
// checkpoint back to the copy stage and restores the original
// serving state, so the workflow can be retried.
func TestShardMigrateCheckpointRollbackOnFailure(t *testing.T) {
	ctx := t.Context()
	tme := newTestShardMigrater(ctx, t, []string{"-40", "40-"}, []string{"-80", "80-"})
	defer tme.close(t)

	// No journals yet.
	tme.expectNoPreviousJournals()

	// stopStreams: read source streams.
	tme.dbSourceClients[0].addQuery("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse' and state = 'Stopped' and message != 'FROZEN'", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse' and state = 'Stopped' and message != 'FROZEN'", &sqltypes.Result{}, nil)
	tme.dbSourceClients[0].addQuery("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("select id, workflow, source, pos, workflow_type, workflow_sub_type, defer_secondary_keys from _vt.vreplication where db_name='vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)

	// cancelMigration: restart target streams and remove reverse streams.
	tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow != 'test_reverse'", &sqltypes.Result{}, nil)
	tme.dbTargetClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid12, nil)
	tme.dbTargetClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test'", resultid2, nil)
	tme.dbTargetClients[0].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (1, 2)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[1].addQuery("update _vt.vreplication set state = 'Running', message = '' where id in (2)", &sqltypes.Result{}, nil)
	tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 1", runningResult(1), nil)
	tme.dbTargetClients[0].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)
	tme.dbTargetClients[1].addQuery("select * from _vt.vreplication where id = 2", runningResult(2), nil)
	tme.dbSourceClients[0].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test_reverse'", resultid3, nil)
	tme.dbSourceClients[1].addQuery("select id from _vt.vreplication where db_name = 'vt_ks' and workflow = 'test_reverse'", resultid34, nil)
	tme.dbSourceClients[0].addQuery("delete from _vt.vreplication where id in (3)", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("delete from _vt.vreplication where id in (3, 4)", &sqltypes.Result{}, nil)
	tme.dbSourceClients[0].addQuery("delete from _vt.copy_state where vrepl_id in (3)", &sqltypes.Result{}, nil)
	tme.dbSourceClients[0].addQuery("delete from _vt.post_copy_action where vrepl_id in (3)", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("delete from _vt.copy_state where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)
	tme.dbSourceClients[1].addQuery("delete from _vt.post_copy_action where vrepl_id in (3, 4)", &sqltypes.Result{}, nil)

	_, _, err := tme.wr.SwitchWrites(ctx, tme.targetKeyspace, "test", 0*time.Second, false, false, false, false, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DeadlineExceeded")
	verifyQueries(t, tme.allDBClients)

	// The checkpoint was rolled back to the copy stage.
	cpi, err := tme.ts.GetReshardCheckpoint(ctx, "ks", "test")
	require.NoError(t, err)
	assert.Equal(t, topo.ReshardStageCopy, cpi.Stage)
	assert.Equal(t, int64(0), cpi.JournalID)
	assert.False(t, cpi.WritesAllowed)
	assert.False(t, cpi.TrafficSwitched)

	// Source writes were re-enabled by the cancel/rollback path.
	checkIfPrimaryServing(t, tme.ts, "ks:-40", true)
	checkIfPrimaryServing(t, tme.ts, "ks:40-", true)
	checkIfPrimaryServing(t, tme.ts, "ks:-80", false)
	checkIfPrimaryServing(t, tme.ts, "ks:80-", false)
}
