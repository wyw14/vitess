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

package topo

import (
	"context"
	"encoding/json"
	"path"
	"time"

	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// This file provides the utility methods to save / retrieve versioned
// reshard workflow checkpoints in the global topology cell.
//
// The checkpoint is additive metadata: it lives in its own subtree
// (keyspaces/<keyspace>/reshardworkflows/<workflow>/Checkpoint) and never
// replaces the existing sources of workflow state (shard serving graph, routing
// rules, resharding journals, vreplication stream records). Older clients
// that do not know about the file simply ignore it and keep deriving the
// workflow state from those records, so mixed-version clusters keep working.

const (
	// ReshardCheckpointsPath is the directory, relative to a keyspace,
	// that holds the checkpoints of the keyspace's reshard workflows.
	ReshardCheckpointsPath = "reshardworkflows"

	// ReshardCheckpointFile is the filename of a reshard workflow checkpoint.
	ReshardCheckpointFile = "Checkpoint"

	// CurrentReshardCheckpointVersion is the on-wire payload version written
	// by this server. It must be bumped whenever the JSON payload gains
	// or loses semantics.
	CurrentReshardCheckpointVersion uint32 = 1

	// MinSupportedReshardCheckpointVersion is the oldest payload version this
	// server is able to parse and modify.
	MinSupportedReshardCheckpointVersion uint32 = 1
)

// Reshard workflow stages, persisted as stable string values so that
// unknown stages written by future versions survive JSON decoding.
const (
	// ReshardStageCopy means the vreplication streams have been created and
	// the initial row copy is in progress (or done).
	ReshardStageCopy = "copy"
	// ReshardStageCatchup means source writes have been stopped and the
	// target streams are catching up to the cutover position.
	ReshardStageCatchup = "catchup"
	// ReshardStageSwitchTraffic means the point of no return (the
	// resharding journal) has been reached and traffic is being switched.
	ReshardStageSwitchTraffic = "switch_traffic"
	// ReshardStageComplete means the traffic switch finished and the target
	// streams were frozen.
	ReshardStageComplete = "complete"
)

// reshardStageRank defines the monotonic ordering of the stages. A stage
// that is absent from the map (for instance one written by a newer
// release) has no known rank and is never mutated by this server.
var reshardStageRank = map[string]int{
	ReshardStageCopy:          1,
	ReshardStageCatchup:       2,
	ReshardStageSwitchTraffic: 3,
	ReshardStageComplete:      4,
}

// maxReshardCheckpointRetries bounds the compare-and-swap retries performed
// while updating a checkpoint under contention.
const maxReshardCheckpointRetries = 10

// ReshardCheckpoint is the versioned, JSON-serialized checkpoint of a
// reshard (or MoveTables) workflow.
type ReshardCheckpoint struct {
	// Version is the on-wire payload version. It is used to keep
	// forward and backward compatibility across Vitess releases.
	Version uint32 `json:"version"`

	// Keyspace and Workflow identify the workflow the checkpoint belongs to.
	Keyspace string `json:"keyspace"`
	Workflow string `json:"workflow"`

	// Stage is the current cutover stage of the workflow.
	Stage string `json:"stage"`

	// JournalID is the id of the resharding journal once the point of
	// no return has been reached.
	JournalID int64 `json:"journal_id,omitempty"`

	// WritesAllowed records that writes were re-enabled on the target
	// shards/tables. A resumed workflow must not perform this step again.
	WritesAllowed bool `json:"writes_allowed,omitempty"`

	// TrafficSwitched records that the routing rules / shard serving graph
	// were switched to the target. A resumed workflow must not perform
	// this step again.
	TrafficSwitched bool `json:"traffic_switched,omitempty"`

	// UpdatedAt is the last time the checkpoint was written.
	UpdatedAt time.Time `json:"updated_at"`
}

// ReshardCheckpointInfo is a meta struct that contains a checkpoint and
// the opaque topology Version of the record that stores it.
type ReshardCheckpointInfo struct {
	version Version
	*ReshardCheckpoint
}

func pathForReshardCheckpoint(keyspace, workflow string) string {
	return path.Join(KeyspacesPath, keyspace, ReshardCheckpointsPath, workflow, ReshardCheckpointFile)
}

// StageRank returns the monotonic rank of the checkpoint's stage and
// whether the stage is known to this server.
func (cp *ReshardCheckpoint) StageRank() (int, bool) {
	rank, ok := reshardStageRank[cp.Stage]
	return rank, ok
}

// validateForWrite makes sure a checkpoint is well formed before it is
// persisted.
func (cp *ReshardCheckpoint) validateForWrite() error {
	if cp.Version < MinSupportedReshardCheckpointVersion || cp.Version > CurrentReshardCheckpointVersion {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"unsupported reshard checkpoint version %d for workflow %s.%s (supported versions: %d-%d)",
			cp.Version, cp.Keyspace, cp.Workflow, MinSupportedReshardCheckpointVersion, CurrentReshardCheckpointVersion)
	}
	if cp.Keyspace == "" || cp.Workflow == "" {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "reshard checkpoint is missing its keyspace or workflow name")
	}
	if _, ok := reshardStageRank[cp.Stage]; !ok {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unknown reshard checkpoint stage %q for workflow %s.%s",
			cp.Stage, cp.Keyspace, cp.Workflow)
	}
	return nil
}

// GetReshardCheckpoint reads the checkpoint of a workflow from the
// global cell. It returns a NoNode error if no checkpoint exists yet.
func (ts *Server) GetReshardCheckpoint(ctx context.Context, keyspace, workflow string) (*ReshardCheckpointInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	contents, version, err := ts.globalCell.Get(ctx, pathForReshardCheckpoint(keyspace, workflow))
	if err != nil {
		return nil, err
	}
	cp := &ReshardCheckpoint{}
	if err := json.Unmarshal(contents, cp); err != nil {
		return nil, vterrors.Wrapf(err, "failed to unmarshal reshard checkpoint for workflow %s.%s", keyspace, workflow)
	}
	// A payload without an explicit version is treated as the original v1.
	if cp.Version == 0 {
		cp.Version = MinSupportedReshardCheckpointVersion
	}
	return &ReshardCheckpointInfo{
		version:           version,
		ReshardCheckpoint: cp,
	}, nil
}

// CreateReshardCheckpoint persists the initial checkpoint. It returns a
// NodeExists error if the checkpoint already exists.
func (ts *Server) CreateReshardCheckpoint(ctx context.Context, keyspace, workflow string, cp *ReshardCheckpoint) (*ReshardCheckpointInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cp == nil {
		cp = &ReshardCheckpoint{}
	}
	cp.Keyspace = keyspace
	cp.Workflow = workflow
	if cp.Version == 0 {
		cp.Version = CurrentReshardCheckpointVersion
	}
	if cp.UpdatedAt.IsZero() {
		cp.UpdatedAt = time.Now().UTC()
	}
	if err := cp.validateForWrite(); err != nil {
		return nil, err
	}
	contents, err := json.Marshal(cp)
	if err != nil {
		return nil, err
	}
	version, err := ts.globalCell.Create(ctx, pathForReshardCheckpoint(keyspace, workflow), contents)
	if err != nil {
		return nil, err
	}
	return &ReshardCheckpointInfo{
		version:           version,
		ReshardCheckpoint: cp,
	}, nil
}

// UpdateReshardCheckpoint saves the checkpoint using compare-and-swap:
// it returns a BadVersion error if the record was concurrently modified.
func (ts *Server) UpdateReshardCheckpoint(ctx context.Context, cpi *ReshardCheckpointInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cpi == nil || cpi.ReshardCheckpoint == nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot update a nil reshard checkpoint")
	}
	if err := cpi.validateForWrite(); err != nil {
		return err
	}
	contents, err := json.Marshal(cpi.ReshardCheckpoint)
	if err != nil {
		return err
	}
	version, err := ts.globalCell.Update(ctx, pathForReshardCheckpoint(cpi.Keyspace, cpi.Workflow), contents, cpi.version)
	if err != nil {
		return err
	}
	cpi.version = version
	return nil
}

// DeleteReshardCheckpoint removes the checkpoint. A nil version is an
// unconditional delete.
func (ts *Server) DeleteReshardCheckpoint(ctx context.Context, cpi *ReshardCheckpointInfo) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if cpi == nil || cpi.ReshardCheckpoint == nil {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot delete a nil reshard checkpoint")
	}
	return ts.globalCell.Delete(ctx, pathForReshardCheckpoint(cpi.Keyspace, cpi.Workflow), cpi.version)
}

// UpdateReshardCheckpointFields applies fn to the current checkpoint of a
// workflow and compare-and-swaps the result into the topology, retrying on
// version conflicts. The checkpoint is created first if it does not exist
// yet.
//
// The record stored in the topology is always the one fn was applied to,
// so concurrent updates do not overwrite each other. A checkpoint
// written by a newer, incompatible Vitess release is never mutated: in
// that case a FAILED_PRECONDITION error is returned.
func (ts *Server) UpdateReshardCheckpointFields(ctx context.Context, keyspace, workflow string, fn func(*ReshardCheckpoint)) (*ReshardCheckpointInfo, error) {
	if fn == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "no checkpoint update function provided")
	}
	for range maxReshardCheckpointRetries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cpi, err := ts.GetReshardCheckpoint(ctx, keyspace, workflow)
		switch {
		case err == nil:
			// Refuse to stomp a payload written by a newer release.
			if cpi.Version > CurrentReshardCheckpointVersion {
				return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
					"reshard checkpoint for workflow %s.%s was written by a newer Vitess version (checkpoint version %d, supported version %d)",
					keyspace, workflow, cpi.Version, CurrentReshardCheckpointVersion)
			}
		case IsErrType(err, NoNode):
			cpi = &ReshardCheckpointInfo{
				ReshardCheckpoint: &ReshardCheckpoint{
					Version:   CurrentReshardCheckpointVersion,
					Keyspace:  keyspace,
					Workflow:  workflow,
					UpdatedAt: time.Now().UTC(),
				},
			}
		default:
			return nil, err
		}

		fn(cpi.ReshardCheckpoint)
		cpi.Keyspace = keyspace
		cpi.Workflow = workflow
		cpi.Version = CurrentReshardCheckpointVersion
		cpi.UpdatedAt = time.Now().UTC()
		if err := cpi.validateForWrite(); err != nil {
			return nil, err
		}
		contents, err := json.Marshal(cpi.ReshardCheckpoint)
		if err != nil {
			return nil, err
		}

		if cpi.version == nil {
			// The record was missing on read: create it, retrying if
			// somebody else won the race.
			version, cerr := ts.globalCell.Create(ctx, pathForReshardCheckpoint(keyspace, workflow), contents)
			if IsErrType(cerr, NodeExists) {
				continue
			}
			if cerr != nil {
				return nil, cerr
			}
			cpi.version = version
			return cpi, nil
		}

		version, uerr := ts.globalCell.Update(ctx, pathForReshardCheckpoint(keyspace, workflow), contents, cpi.version)
		if IsErrType(uerr, BadVersion) {
			// Concurrent update: re-read and apply again.
			continue
		}
		if uerr != nil {
			return nil, uerr
		}
		cpi.version = version
		return cpi, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE,
		"failed to update the reshard checkpoint for workflow %s.%s after %d compare-and-swap retries",
		keyspace, workflow, maxReshardCheckpointRetries)
}
