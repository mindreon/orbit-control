package assetrelease

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func TestReleasePaths(t *testing.T) {
	for _, tc := range []struct {
		name, visibility, decision, want     string
		required                             int
		withdraw, fail, expanded, sla, block bool
	}{
		{name: "private exempt", visibility: "private", want: "published", required: 1},
		{name: "workspace exempt", visibility: "workspace", want: "published", required: 1},
		{name: "tenant approve", visibility: "tenant", decision: "approve", want: "published", required: 1},
		{name: "reject", visibility: "tenant", decision: "reject", want: "rejected", required: 1},
		{name: "changes", visibility: "tenant", decision: "request_changes", want: "changes_requested", required: 1},
		{name: "multiple independent reviewers", visibility: "tenant", decision: "approve", want: "published", required: 2},
		{name: "withdraw", visibility: "tenant", want: "rejected", required: 1, withdraw: true},
		{name: "publish compensation", visibility: "private", required: 1, fail: true},
		{name: "sla", visibility: "tenant", decision: "approve", want: "published", required: 1, sla: true},
		{name: "permission expansion", visibility: "private", decision: "approve", want: "published", required: 1, expanded: true},
		{name: "block", visibility: "private", want: "check_failed", required: 1, block: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			in := Input{"tenant", "version", 1}
			snap := Snapshot{Author: "author", Owner: "owner", Visibility: tc.visibility, Required: tc.required, Check: contract.AssetCheckInput{V: 1, TenantId: "tenant", AssetId: "asset", AssetVersionId: "version", Kind: "agent", ContentDigest: "sha256:" + strings.Repeat("0", 64), BlobRef: "blob"}}
			env.RegisterActivityWithOptions(func(context.Context, Input) (Snapshot, error) { return snap, nil }, activity.RegisterOptions{Name: "SnapshotAssetRelease"})
			checks := []string{contract.ActivityAssetCheckStructure, contract.ActivityAssetCheckCompat, contract.ActivityAssetCheckSecrets, contract.ActivityAssetCheckStatic, contract.ActivityAssetCheckInjection, contract.ActivityAssetCheckPermissionDiff}
			for i, name := range checks {
				report := contract.AssetCheckReport{Check: contract.AssetCheckKind([]string{"structure", "compat", "secrets", "static", "injection", "permission_diff"}[i]), CheckerVersion: "test"}
				if tc.expanded && i == 5 {
					report.Findings = []contract.AssetCheckFinding{{Check: report.Check, RuleId: "expanded", Severity: "review", Message: "expanded"}}
				}
				if tc.block && i == 0 {
					report.Findings = []contract.AssetCheckFinding{{Check: report.Check, RuleId: "block", Severity: "block", Message: "block"}}
				}
				env.RegisterActivityWithOptions(func(context.Context, contract.AssetCheckInput) (contract.AssetCheckReport, error) { return report, nil }, activity.RegisterOptions{Name: name})
			}
			states := []string{}
			env.RegisterActivityWithOptions(func(_ context.Context, a Aggregate) error { states = append(states, a.State); return nil }, activity.RegisterOptions{Name: "AggregateAssetChecks"})
			env.RegisterActivityWithOptions(func(context.Context, Input) error { return nil }, activity.RegisterOptions{Name: "CreateReviewTask"})
			env.RegisterActivityWithOptions(func(context.Context, Input) error { return nil }, activity.RegisterOptions{Name: "RemindAssetReview"})
			env.RegisterActivityWithOptions(func(_ context.Context, r Review) (Review, error) { return r, nil }, activity.RegisterOptions{Name: "LoadAssetReview"})
			env.RegisterActivityWithOptions(func(_ context.Context, a Aggregate) (Result, error) {
				states = append(states, a.State)
				return Result{a.State}, nil
			}, activity.RegisterOptions{Name: "DecideAssetReview"})
			for _, name := range []string{"Sign", "Publish", "CompensateAssetPublish", "NotifyAuthor"} {
				env.RegisterActivityWithOptions(func(context.Context, Input) error { return nil }, activity.RegisterOptions{Name: name})
			}
			env.RegisterActivityWithOptions(func(context.Context, Input) (Result, error) { return Result{"rejected"}, nil }, activity.RegisterOptions{Name: "AbortAssetRelease"})
			if tc.fail {
				env.OnActivity("Publish", mock.Anything, in).Return(temporal.NewNonRetryableApplicationError("publish failed", "PublishFailure", nil)).Once()
				env.OnActivity("CompensateAssetPublish", mock.Anything, in).Return(nil).Once()
			}
			if tc.sla {
				env.OnActivity("RemindAssetReview", mock.Anything, in).Return(nil).Once()
			}
			if tc.withdraw {
				env.RegisterDelayedCallback(func() { env.SignalWorkflow(Withdraw, nil) }, time.Second)
			}
			if tc.decision != "" {
				env.RegisterDelayedCallback(func() {
					review := Review{ID: "self-review", Reviewer: "author", Decision: "approve", Input: in}
					env.UpdateWorkflow(SubmitReview, review.ID, &testsuite.TestUpdateCallback{OnComplete: func(_ any, e error) {
						if e == nil {
							t.Error("author review accepted")
						}
					}}, review)
				}, 100*time.Millisecond)
				env.RegisterDelayedCallback(func() {
					review := Review{ID: "wrong-release", Reviewer: "outside", Decision: "approve", Input: Input{"other", "version", 1}}
					env.UpdateWorkflow(SubmitReview, review.ID, &testsuite.TestUpdateCallback{OnReject: func(e error) {
						if e == nil {
							t.Error("wrong release accepted")
						}
					}}, review)
				}, 200*time.Millisecond)
			}
			if tc.required == 2 {
				env.RegisterDelayedCallback(func() {
					review := Review{ID: "duplicate-voter", Reviewer: "x", Decision: "approve", Input: in}
					env.UpdateWorkflow(SubmitReview, review.ID, &testsuite.TestUpdateCallback{OnComplete: func(_ any, e error) {
						if e == nil {
							t.Error("duplicate reviewer counted twice")
						}
					}}, review)
				}, 1500*time.Millisecond)
			}
			if tc.decision != "" {
				delay := time.Second
				if tc.sla {
					delay = 49 * time.Hour
				}
				for i := range tc.required {
					env.RegisterDelayedCallback(func() {
						review := Review{ID: string(rune('a' + i)), Reviewer: string(rune('x' + i)), Decision: tc.decision, Input: in}
						env.UpdateWorkflow(SubmitReview, review.ID, &testsuite.TestUpdateCallback{OnReject: func(e error) { t.Errorf("update rejected: %v", e) }, OnComplete: func(_ any, e error) {
							if e != nil {
								t.Errorf("update failed: %v", e)
							}
						}}, review)
					}, delay+time.Duration(i)*time.Second)
				}
			}
			env.ExecuteWorkflow(Run, in)
			if !env.IsWorkflowCompleted() {
				t.Fatal("not complete")
			}
			if tc.fail {
				if env.GetWorkflowError() == nil {
					t.Fatal("publish failure ignored")
				}
			} else {
				if e := env.GetWorkflowError(); e != nil {
					t.Fatal(e)
				}
				var result Result
				if e := env.GetWorkflowResult(&result); e != nil {
					t.Fatal(e)
				}
				if result.State != tc.want {
					t.Fatal(result)
				}
			}
			if tc.expanded && (len(states) == 0 || states[0] != "in_review") {
				t.Fatal("permission expansion did not force review")
			}
			if tc.block && (len(states) != 1 || states[0] != "check_failed") {
				t.Fatal("blocking check did not stop publication")
			}
			env.AssertExpectations(t)
		})
	}
}
func TestPermissionExpansion(t *testing.T) {
	empty := contract.EffectivePermissions{}
	for _, p := range []contract.EffectivePermissions{{Network: contract.NetworkPermissions{Egress: []string{"api.test"}}}, {Secrets: []contract.SecretRequirement{{Name: "KEY"}}}, {EffectClasses: []contract.EffectClass{"external_write"}}} {
		if !Expanded(empty, p) {
			t.Fatal("missed expansion")
		}
	}
	before := contract.EffectivePermissions{Network: contract.NetworkPermissions{Egress: []string{"*.example.com"}}, EffectClasses: []contract.EffectClass{"privileged"}}
	after := contract.EffectivePermissions{Network: contract.NetworkPermissions{Egress: []string{"api.example.com"}}, EffectClasses: []contract.EffectClass{"read_only"}}
	if Expanded(before, after) {
		t.Fatal("narrowing marked expanded")
	}
}
