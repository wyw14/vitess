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

package vdiff

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// cannedVDiffClient serves pre-built VDiffShow responses keyed by the
// requested VDiff argument (UUID). It records the requests so tests can
// verify that compare never requests sample rows.
type cannedVDiffClient struct {
	vtctldclient.VtctldClient
	shows    map[string]*vtctldatapb.VDiffShowResponse
	requests []*vtctldatapb.VDiffShowRequest
}

func (c *cannedVDiffClient) VDiffShow(_ context.Context, in *vtctldatapb.VDiffShowRequest, _ ...grpc.CallOption) (*vtctldatapb.VDiffShowResponse, error) {
	c.requests = append(c.requests, in)
	if resp, ok := c.shows[in.Arg]; ok {
		return resp, nil
	}
	return nil, fmt.Errorf("no vdiff found for UUID %s", in.Arg)
}

func showResponseForShard(rows ...string) *vtctldatapb.VDiffShowResponse {
	return &vtctldatapb.VDiffShowResponse{
		TabletResponses: map[string]*tabletmanagerdatapb.VDiffResponse{
			"0": {
				Id:     1,
				Output: sqltypes.ResultToProto3(sqltypes.MakeTestResult(fields, rows...)),
			},
		},
	}
}

func vdiffRow(state, table, vdiffUUID, tableState, tableRows, startedAt, rowsCompared, completedAt, hasMismatch, report string) string {
	return strings.Join([]string{
		state, "", table, vdiffUUID, tableState, tableRows, startedAt, rowsCompared, completedAt, hasMismatch, report,
	}, "|")
}

func TestCommandCompare(t *testing.T) {
	ctx := t.Context()
	fromUUID := uuid.NewString()
	toUUID := uuid.NewString()
	start1, comp1 := "2026-01-01 10:00:00", "2026-01-01 10:05:00"
	start2, comp2 := "2026-01-02 10:00:00", "2026-01-02 10:05:00"

	client := &cannedVDiffClient{
		shows: map[string]*vtctldatapb.VDiffShowResponse{
			// First run: t1 has one mismatched and one extra source row, and a
			// "gone" table that no longer exists in the second run.
			fromUUID: showResponseForShard(
				vdiffRow("completed", "t1", fromUUID, "completed", "12", start1, "12", comp1, "1",
					`{"TableName": "t1", "ProcessedRows": 12, "MatchingRows": 10, "MismatchedRows": 1, "ExtraRowsSource": 1, "ExtraRowsTarget": 0}`),
				vdiffRow("completed", "gone", fromUUID, "completed", "4", start1, "4", comp1, "0",
					`{"TableName": "gone", "ProcessedRows": 4, "MatchingRows": 4, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`),
			),
			// Second run: t1 is fully in sync, and a new "fresh" table appears.
			toUUID: showResponseForShard(
				vdiffRow("completed", "t1", toUUID, "completed", "12", start2, "12", comp2, "0",
					`{"TableName": "t1", "ProcessedRows": 12, "MatchingRows": 12, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`),
				vdiffRow("completed", "fresh", toUUID, "completed", "3", start2, "3", comp2, "0",
					`{"TableName": "fresh", "ProcessedRows": 3, "MatchingRows": 3, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`),
			),
		},
	}

	// Save and restore the global command state.
	prevKS, prevWF, prevFormat := common.BaseOptions.TargetKeyspace, common.BaseOptions.Workflow, common.BaseOptions.Format
	prevClient := common.GetClient()
	t.Cleanup(func() {
		common.BaseOptions.TargetKeyspace = prevKS
		common.BaseOptions.Workflow = prevWF
		common.BaseOptions.Format = prevFormat
		common.SetClient(prevClient)
	})
	common.BaseOptions.TargetKeyspace = "targetks"
	common.BaseOptions.Workflow = "vdiffTest"
	common.SetClient(client)
	common.SetCommandCtx(ctx)
	compareOptions.FromUUID = uuid.MustParse(fromUUID)
	compareOptions.ToUUID = uuid.MustParse(toUUID)

	t.Run("json output", func(t *testing.T) {
		common.BaseOptions.Format = "json"
		cmd := &cobra.Command{}
		var out bytes.Buffer
		cmd.SetOut(&out)

		require.NoError(t, commandCompare(cmd, nil))
		rendered := out.String()

		require.Contains(t, rendered, `"Keyspace": "targetks"`)
		require.Contains(t, rendered, `"Workflow": "vdiffTest"`)
		require.Contains(t, rendered, `"UUID": "`+fromUUID+`"`)
		require.Contains(t, rendered, `"UUID": "`+toUUID+`"`)
		require.Contains(t, rendered, `"Shard": "0"`)
		// Tables are sorted: fresh, gone, t1.
		require.Contains(t, rendered, `"Table": "fresh"`)
		require.Contains(t, rendered, `"Table": "gone"`)
		require.Contains(t, rendered, `"Table": "t1"`)
		// New and removed tables are marked.
		require.Contains(t, rendered, `"Status": "new"`)
		require.Contains(t, rendered, `"Status": "removed"`)
		// Before/after/delta counters for the common table t1.
		require.Contains(t, rendered, `"From": 10,`)
		require.Contains(t, rendered, `"To": 12,`)
		require.Contains(t, rendered, `"Delta": 2`)
		require.Contains(t, rendered, `"From": 1,`)
		require.Contains(t, rendered, `"Delta": -1`)
		// Removed table contributes a negative delta on the From value.
		require.Contains(t, rendered, `"Delta": -4`)
		// No sample row content ever appears in compare output.
		require.NotContains(t, rendered, "Sample")
		require.NotContains(t, rendered, "RowDiff")
	})

	t.Run("table output", func(t *testing.T) {
		common.BaseOptions.Format = "text"
		cmd := &cobra.Command{}
		var out bytes.Buffer
		cmd.SetOut(&out)

		require.NoError(t, commandCompare(cmd, nil))
		rendered := out.String()

		require.Contains(t, rendered, "VDiff Comparison for targetks.vdiffTest")
		require.Contains(t, rendered, "From: "+fromUUID)
		require.Contains(t, rendered, "To:   "+toUUID)
		require.Contains(t, rendered, "Matching Rows")
		require.Contains(t, rendered, "Mismatched Rows")
		require.Contains(t, rendered, "Extra Rows Source")
		require.Contains(t, rendered, "Extra Rows Target")
		require.Contains(t, rendered, "new")
		require.Contains(t, rendered, "removed")
		require.Contains(t, rendered, "+2")
		require.Contains(t, rendered, "-1")
		require.Contains(t, rendered, "-4")
		require.Contains(t, rendered, "+3")
	})

	// Both sub-commands above ran one comparison each, fetching the two runs
	// in order. Every request must read an existing report without samples.
	require.Len(t, client.requests, 4)
	for i, req := range client.requests {
		require.Equal(t, "targetks", req.TargetKeyspace)
		require.Equal(t, "vdiffTest", req.Workflow)
		require.True(t, req.NoSamples, "compare must always read reports without sample rows")
		require.Equal(t, map[int]string{0: fromUUID, 1: toUUID, 2: fromUUID, 3: toUUID}[i], req.Arg)
	}
}

func TestCommandCompareRejectsIncompleteVDiff(t *testing.T) {
	ctx := t.Context()
	fromUUID := uuid.New().String()
	toUUID := uuid.New().String()
	start := "2026-01-01 10:00:00"

	client := &cannedVDiffClient{
		shows: map[string]*vtctldatapb.VDiffShowResponse{
			// The first run is still running, so the comparison must be refused.
			fromUUID: showResponseForShard(
				vdiffRow("started", "t1", fromUUID, "started", "12", start, "0", "", "0",
					`{"TableName": "t1", "ProcessedRows": 0, "MatchingRows": 0, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`),
			),
			toUUID: showResponseForShard(
				vdiffRow("completed", "t1", toUUID, "completed", "12", start, "12", "2026-01-02 10:05:00", "0",
					`{"TableName": "t1", "ProcessedRows": 12, "MatchingRows": 12, "MismatchedRows": 0, "ExtraRowsSource": 0, "ExtraRowsTarget": 0}`),
			),
		},
	}

	prevKS, prevWF, prevFormat, prevClient := common.BaseOptions.TargetKeyspace, common.BaseOptions.Workflow, common.BaseOptions.Format, common.GetClient()
	t.Cleanup(func() {
		common.BaseOptions.TargetKeyspace = prevKS
		common.BaseOptions.Workflow = prevWF
		common.BaseOptions.Format = prevFormat
		common.SetClient(prevClient)
	})
	common.BaseOptions.TargetKeyspace = "targetks"
	common.BaseOptions.Workflow = "vdiffTest"
	common.BaseOptions.Format = "text"
	common.SetClient(client)
	common.SetCommandCtx(ctx)
	compareOptions.FromUUID = uuid.MustParse(fromUUID)
	compareOptions.ToUUID = uuid.MustParse(toUUID)

	cmd := &cobra.Command{}
	cmd.SetOut(&bytes.Buffer{})
	err := commandCompare(cmd, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "only completed VDiffs can be compared")
}

// TestCompareCommandDefinition exercises the compare command's positional
// arguments and pre-run validation directly. (The package-level command
// variables can only be registered once per test process, so invoking
// registerCommands here would clash with TestShowNoSamplesFlag.)
func TestCompareCommandDefinition(t *testing.T) {
	require.Equal(t, "compare <from-uuid> <to-uuid>", compare.Use)
	require.Contains(t, compare.Aliases, "Compare")
	require.NotNil(t, compare.RunE)
	require.NotNil(t, compare.PreRunE)

	// Exactly two VDiff UUIDs are required.
	require.Error(t, compare.Args(compare, nil))
	require.Error(t, compare.Args(compare, []string{uuid.NewString()}))
	require.NoError(t, compare.Args(compare, []string{uuid.NewString(), uuid.NewString()}))

	// Both arguments must be valid UUIDs.
	require.Error(t, compare.PreRunE(compare, []string{"not-a-uuid", uuid.NewString()}))
	require.Error(t, compare.PreRunE(compare, []string{uuid.NewString(), "not-a-uuid"}))

	// The same run cannot be compared with itself.
	same := uuid.NewString()
	require.ErrorContains(t, compare.PreRunE(compare, []string{same, same}), "must be different")

	// Two distinct UUIDs are accepted and stored for the RunE function.
	fromUUID, toUUID := uuid.NewString(), uuid.NewString()
	require.NoError(t, compare.PreRunE(compare, []string{fromUUID, toUUID}))
	require.Equal(t, fromUUID, compareOptions.FromUUID.String())
	require.Equal(t, toUUID, compareOptions.ToUUID.String())
}
