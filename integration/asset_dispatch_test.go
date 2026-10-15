package integration

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	releaseactivities "github.com/mindreon/orbit-control/internal/activities/assetrelease"
	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/tenancy"
	"github.com/mindreon/orbit-control/internal/workflows/assetrelease"
)

type dispatchClient struct {
	client.Client
	failures map[string]error
	started  chan string
}

func (c *dispatchClient) DescribeWorkflowExecution(
	context.Context,
	string,
	string,
) (*workflowservice.DescribeWorkflowExecutionResponse, error) {
	return &workflowservice.DescribeWorkflowExecutionResponse{
		WorkflowExecutionInfo: &workflow.WorkflowExecutionInfo{Status: enums.WORKFLOW_EXECUTION_STATUS_FAILED},
	}, nil
}

func (c *dispatchClient) ExecuteWorkflow(
	_ context.Context,
	options client.StartWorkflowOptions,
	_ any,
	_ ...any,
) (client.WorkflowRun, error) {
	if err := c.failures[options.ID]; err != nil {
		return nil, err
	}
	select {
	case c.started <- options.ID:
	default:
	}
	return nil, nil
}

func testDispatchIsolation(
	t *testing.T,
	d *db.DB,
	s *assets.Service,
	a *releaseactivities.Activities,
	tenant, owner, workspace string,
) {
	t.Helper()
	ctx := t.Context()
	inputs := []assetrelease.Input{}
	raw, err := bundle.Pack(map[string][]byte{"AGENTS.md": []byte("dispatcher")})
	if err != nil {
		t.Fatal(err)
	}
	staged, err := s.PrepareUpload(auth.Context(ctx, auth.Principal{ID: owner, Kind: "user"}, tenant), "1.0.0", raw)
	if err != nil {
		t.Fatal(err)
	}
	otherTenant, err := tenancy.CreateTenant(
		ctx,
		d,
		auth.Principal{ID: owner, Kind: "user"},
		"dispatch-"+ids.New(),
		"Dispatch isolation",
	)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		targetTenant, targetWorkspace := tenant, workspace
		if i == 2 {
			targetTenant, targetWorkspace = otherTenant.ID, ""
			staged, err = s.PrepareUpload(
				auth.Context(ctx, auth.Principal{ID: owner, Kind: "user"}, targetTenant),
				"1.0.0",
				raw,
			)
			if err != nil {
				t.Fatal(err)
			}
		}
		if err = d.WithTenant(ctx, targetTenant, func(tx pgx.Tx) error {
			ctx := auth.Context(db.Context(ctx, tx), auth.Principal{ID: owner, Kind: "user"}, targetTenant)
			asset, err := s.Create(ctx, "agent", fmt.Sprintf("dispatch-%d", i), "private", targetWorkspace)
			if err != nil {
				return err
			}
			version, err := s.Upload(ctx, asset, staged)
			if err != nil {
				return err
			}
			if err = s.Submit(ctx, asset, version.ID, "stable"); err != nil {
				return err
			}
			inputs = append(inputs, assetrelease.Input{Tenant: targetTenant, VersionID: version.ID, Generation: 1})
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	// A failed Workflow may have left an approved version; restart must reach it.
	snap, err := a.SnapshotAssetRelease(ctx, inputs[2])
	if err != nil {
		t.Fatal(err)
	}
	reports := []contract.AssetCheckReport{}
	for _, kind := range []contract.AssetCheckKind{"structure", "compat", "secrets", "static", "injection", "permission_diff"} {
		reports = append(reports, contract.AssetCheckReport{Check: kind, CheckerVersion: "recovery-test"})
	}
	if snap.State != "submitted" {
		t.Fatal(snap.State)
	}
	if err = a.AggregateAssetChecks(ctx, assetrelease.Aggregate{Input: inputs[2], Reports: reports, State: "approved"}); err != nil {
		t.Fatal(err)
	}
	c := &dispatchClient{
		failures: map[string]error{
			assetrelease.ID(inputs[0]): serviceerror.NewInvalidArgument("permanent test error"),
			assetrelease.ID(inputs[1]): serviceerror.NewUnavailable("transient test error"),
		},
		started: make(chan string, 10),
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	run := func(want string) {
		t.Helper()
		dispatchCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = releaseactivities.Dispatch(dispatchCtx, d, c, logger)
		}()
		defer func() {
			cancel()
			<-done
		}()
		timeout := time.After(5 * time.Second)
		for {
			select {
			case id := <-c.started:
				if id == want {
					return
				}
			case <-timeout:
				t.Fatal("poison row starved another release")
			}
		}
	}
	run(assetrelease.ID(inputs[2]))
	for i, in := range inputs[:2] {
		var retryable bool
		var message string
		if err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx,
				"SELECT release_retryable,release_error FROM asset_versions WHERE id=$1",
				in.VersionID).Scan(&retryable,
				&message)
		}); err != nil || retryable != (i == 1) || message == "" {
			t.Fatal("dispatch classification", i, retryable, message, err)
		}
		if !strings.Contains(logs.String(), in.Tenant) || !strings.Contains(logs.String(), in.VersionID) ||
			!strings.Contains(logs.String(), "error") {
			t.Fatal("dispatch log lacks row identity")
		}
	}
	if err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		return s.RetryRelease(auth.Context(db.Context(ctx, tx), auth.Principal{ID: owner, Kind: "user"}, tenant), inputs[0].VersionID)
	}); err != nil {
		t.Fatal(err)
	}
	delete(c.failures, assetrelease.ID(inputs[0]))
	run(assetrelease.ID(inputs[0]))
	for _, in := range inputs[:2] {
		if _, err = a.AbortAssetRelease(ctx, assetrelease.Failure{Input: in, Withdrawn: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err = a.Sign(ctx, inputs[2]); err != nil {
		t.Fatal(err)
	}
	if err = a.Publish(ctx, inputs[2]); err != nil {
		t.Fatal(err)
	}

}
