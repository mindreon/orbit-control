package integration

import (
	"context"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/workflows/assetrelease"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
)

// Branch histories use deterministic Activity fixtures on a real server in a
// separate namespace. J5 separately exercises the real SQL/S3 Activities.
func testReplayBranches(t *testing.T, tenant string) {
	t.Helper()
	ctx := t.Context()
	namespace := os.Getenv("ORBIT_TEST_TEMPORAL_NAMESPACE") + "-replay"
	c, err := client.Dial(client.Options{HostPort: os.Getenv("ORBIT_TEST_TEMPORAL_ADDRESS"), Namespace: namespace})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err = c.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{Namespace: namespace, WorkflowExecutionRetentionPeriod: durationOneDay()}); err != nil {
		if _, ok := err.(*serviceerror.NamespaceAlreadyExists); !ok {
			t.Fatal(err)
		}
	}
	inputs := map[string]assetrelease.Input{}
	paths := []string{"reject", "multiple-reviewers", "withdraw", "compensation"}
	for _, name := range paths {
		inputs[name] = assetrelease.Input{Tenant: tenant, VersionID: ids.New(), Generation: 1}
	}
	w := worker.New(c, contract.TaskQueueCtl, worker.Options{})
	w.RegisterWorkflowWithOptions(assetrelease.Run, workflowRegisterOptions())
	w.RegisterActivityWithOptions(func(_ context.Context, in assetrelease.Input) (assetrelease.Snapshot, error) {
		snap := assetrelease.Snapshot{Author: "author", Owner: "owner", Visibility: "tenant", Required: 1, Check: contract.AssetCheckInput{V: 1, TenantId: in.Tenant, AssetId: ids.New(), AssetVersionId: in.VersionID, Kind: "agent", BlobRef: "replay-fixture", ContentDigest: "sha256:" + strings.Repeat("0", 64)}}
		if in == inputs["multiple-reviewers"] {
			snap.Required = 2
		}
		if in == inputs["compensation"] {
			snap.Visibility = "private"
		}
		return snap, nil
	}, activity.RegisterOptions{Name: "SnapshotAssetRelease"})
	w.RegisterActivityWithOptions(func(context.Context, assetrelease.Aggregate) error { return nil }, activity.RegisterOptions{Name: "AggregateAssetChecks"})
	for _, name := range []string{"CreateReviewTask", "RemindAssetReview", "Sign", "NotifyAuthor"} {
		w.RegisterActivityWithOptions(func(context.Context, assetrelease.Input) error { return nil }, activity.RegisterOptions{Name: name})
	}
	w.RegisterActivityWithOptions(func(_ context.Context, review assetrelease.Review) (assetrelease.Review, error) { return review, nil }, activity.RegisterOptions{Name: "LoadAssetReview"})
	w.RegisterActivityWithOptions(func(_ context.Context, in assetrelease.Aggregate) (assetrelease.Result, error) {
		return assetrelease.Result{State: in.State}, nil
	}, activity.RegisterOptions{Name: "DecideAssetReview"})
	w.RegisterActivityWithOptions(func(_ context.Context, in assetrelease.Input) error {
		if in == inputs["compensation"] {
			return temporal.NewNonRetryableApplicationError("fixture publish failure", "PublishFixtureFailure", nil)
		}
		return nil
	}, activity.RegisterOptions{Name: "Publish"})
	var compensated atomic.Bool
	w.RegisterActivityWithOptions(func(context.Context, assetrelease.Input) error { compensated.Store(true); return nil }, activity.RegisterOptions{Name: "CompensateAssetPublish"})
	w.RegisterActivityWithOptions(func(_ context.Context, f assetrelease.Failure) (assetrelease.Result, error) {
		state := "approved"
		if f.Withdrawn {
			state = "draft"
		}
		return assetrelease.Result{State: state}, nil
	}, activity.RegisterOptions{Name: "AbortAssetRelease"})
	fake := worker.New(c, contract.TaskQueueAssetCheck, worker.Options{})
	checks := []string{contract.ActivityAssetCheckStructure, contract.ActivityAssetCheckCompat, contract.ActivityAssetCheckSecrets, contract.ActivityAssetCheckStatic, contract.ActivityAssetCheckInjection, contract.ActivityAssetCheckPermissionDiff}
	for i, name := range checks {
		kind := contract.AssetCheckKind([]string{"structure", "compat", "secrets", "static", "injection", "permission_diff"}[i])
		fn := func(context.Context, contract.AssetCheckInput) (contract.AssetCheckReport, error) {
			return contract.AssetCheckReport{Check: kind, CheckerVersion: "replay-fixture/1"}, nil
		}
		if i == 5 {
			w.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
		} else {
			fake.RegisterActivityWithOptions(fn, activity.RegisterOptions{Name: name})
		}
	}
	if err = w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if err = fake.Start(); err != nil {
		t.Fatal(err)
	}
	defer fake.Stop()
	for _, name := range paths {
		in := inputs[name]
		run, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: assetrelease.ID(in), TaskQueue: contract.TaskQueueCtl}, assetrelease.Name, in)
		if err != nil {
			t.Fatal(err)
		}
		switch name {
		case "withdraw":
			if err = c.SignalWorkflow(ctx, run.GetID(), run.GetRunID(), assetrelease.Withdraw, nil); err != nil {
				t.Fatal(err)
			}
		case "reject", "multiple-reviewers":
			count := 1
			if name == "multiple-reviewers" {
				count = 2
			}
			for range count {
				decision := "approve"
				if name == "reject" {
					decision = "reject"
				}
				review := assetrelease.Review{ID: ids.New(), Reviewer: ids.New(), Input: in, Decision: decision}
				updateReviewFixture(t, c, run.GetID(), review)
			}
		}
		getCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		var result assetrelease.Result
		err = run.Get(getCtx, &result)
		cancel()
		if name == "compensation" {
			if err == nil || !compensated.Load() {
				t.Fatal("compensation branch missing", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
		if name == "withdraw" && result.State != "draft" || name == "reject" && result.State != "rejected" || name == "multiple-reviewers" && result.State != "published" {
			t.Fatal(name, result)
		}
		saveReplay(t, c, run.GetID(), name)
	}
}
func updateReviewFixture(t *testing.T, c client.Client, workflowID string, review assetrelease.Review) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: workflowID, UpdateName: assetrelease.SubmitReview, UpdateID: review.ID, WaitForStage: client.WorkflowUpdateStageCompleted, Args: []any{review}})
		if err == nil {
			err = handle.Get(ctx, nil)
		}
		cancel()
		if err == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("review fixture did not complete")
}
