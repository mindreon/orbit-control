package assetrelease

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/mindreon/orbit-control/internal/contract"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const Name = "AssetReleaseWorkflow"
const SubmitReview = "SubmitReview"
const Withdraw = "Withdraw"

type Input struct {
	Tenant     string `json:"tenant"`
	VersionID  string `json:"version_id"`
	Generation int    `json:"generation"`
}
type Snapshot struct {
	State           string                   `json:"state,omitempty"`
	Reviews         []Review                 `json:"reviews,omitempty"`
	Uploader        string                   `json:"uploader,omitempty"`
	Check           contract.AssetCheckInput `json:"check"`
	Author          string                   `json:"author"`
	Owner           string                   `json:"owner"`
	Visibility      string                   `json:"visibility"`
	WorkspaceReview bool                     `json:"workspace_review"`
	Required        int                      `json:"required"`
}
type Review struct {
	ID       string `json:"id"`
	Reviewer string `json:"reviewer"`
	Decision string `json:"decision"`
	Comment  string `json:"comment"`
	Input    Input  `json:"input"`
}
type Aggregate struct {
	Input   Input                       `json:"input"`
	Reports []contract.AssetCheckReport `json:"reports"`
	State   string                      `json:"state"`
}
type Result struct {
	State string `json:"state"`
}

func ID(in Input) string { return "asset:" + in.Tenant + ":" + in.VersionID }

func Run(ctx workflow.Context, in Input) (result Result, err error) {
	ctl := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{TaskQueue: contract.TaskQueueCtl, StartToCloseTimeout: 30 * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 5}})
	active, cancel := workflow.WithCancel(ctl)
	defer cancel()
	withdrawn := false
	workflow.Go(ctx, func(ctx workflow.Context) {
		workflow.GetSignalChannel(ctx, Withdraw).Receive(ctx, nil)
		withdrawn = true
		cancel()
	})
	defer func() {
		if err != nil || withdrawn {
			cleanup, _ := workflow.NewDisconnectedContext(ctl)
			var out Result
			e := workflow.ExecuteActivity(cleanup, "AbortAssetRelease", releaseFailure(in, err, withdrawn)).Get(cleanup, &out)
			if e != nil {
				err = e
			} else if withdrawn {
				result = out
				err = nil
			}
		}
	}()
	var snap Snapshot
	if err = workflow.ExecuteActivity(active, "SnapshotAssetRelease", in).Get(active, &snap); err != nil {
		return
	}
	state := snap.State
	if state != "approved" && state != "in_review" {
		state, err = checkRelease(active, in, snap)
		if err != nil {
			return
		}
		if state == "check_failed" {
			return Result{state}, nil
		}
	}
	if state == "in_review" {
		state, err = reviewRelease(ctx, ctl, active, in, snap, &withdrawn)
		if err != nil {
			return
		}
		if state != "approved" {
			return Result{state}, nil
		}
	}
	if withdrawn {
		return Result{}, temporal.NewCanceledError()
	}
	if err = workflow.ExecuteActivity(active, "Sign", in).Get(active, nil); err != nil {
		return
	}
	if err = workflow.ExecuteActivity(active, "Publish", in).Get(active, nil); err != nil {
		cleanup, _ := workflow.NewDisconnectedContext(ctl)
		if compensation := workflow.ExecuteActivity(cleanup, "CompensateAssetPublish", in).Get(cleanup, nil); compensation != nil {
			return Result{}, compensation
		}
		return
	}
	if err = workflow.ExecuteActivity(active, "NotifyAuthor", in).Get(active, nil); err != nil {
		return
	}
	return Result{"published"}, nil
}

func checkRelease(active workflow.Context, in Input, snap Snapshot) (string, error) {
	var err error
	checks := []string{contract.ActivityAssetCheckStructure, contract.ActivityAssetCheckCompat, contract.ActivityAssetCheckSecrets, contract.ActivityAssetCheckStatic, contract.ActivityAssetCheckInjection, contract.ActivityAssetCheckPermissionDiff}
	futures := make([]workflow.Future, 0, len(checks))
	for _, name := range checks {
		target := active
		if name != contract.ActivityAssetCheckPermissionDiff {
			target = workflow.WithActivityOptions(active, workflow.ActivityOptions{TaskQueue: contract.TaskQueueAssetCheck, StartToCloseTimeout: 5 * time.Minute, HeartbeatTimeout: 30 * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 2}})
		}
		futures = append(futures, workflow.ExecuteActivity(target, name, snap.Check))
	}
	reports := make([]contract.AssetCheckReport, len(futures))
	block := false
	manual := snap.Visibility == "tenant" || snap.Visibility == "workspace" && snap.WorkspaceReview
	for i, f := range futures {
		if err = f.Get(active, &reports[i]); err != nil {
			return "", err
		}
		for _, finding := range reports[i].Findings {
			block = block || finding.Severity == "block"
			manual = manual || finding.Severity == "review"
		}
	}
	state := "approved"
	if manual {
		state = "in_review"
	}
	if block {
		state = "check_failed"
	}
	if err = workflow.ExecuteActivity(active, "AggregateAssetChecks", Aggregate{in, reports, state}).Get(active, nil); err != nil {
		return "", err
	}
	return state, nil

}
func reviewRelease(ctx, ctl, active workflow.Context, in Input, snap Snapshot, withdrawn *bool) (string, error) {
	var err error
	if err = workflow.ExecuteActivity(active, "CreateReviewTask", in).Get(active, nil); err != nil {
		return "", err
	}
	reviewers := []string{}
	reviewIDs := []string{}
	decision := ""
	for _, review := range snap.Reviews {
		if review.Reviewer == snap.Author || review.Reviewer == snap.Owner || review.Reviewer == snap.Uploader {
			return "", fmt.Errorf("author cannot review")
		}
		reviewIDs = append(reviewIDs, review.ID)
		reviewers = append(reviewers, review.Reviewer)
		if review.Decision == "reject" {
			decision = "rejected"
		} else if review.Decision == "request_changes" && decision != "rejected" {
			decision = "changes_requested"
		}
	}
	if decision == "" && len(reviewers) >= snap.Required {
		decision = "approved"
	}
	processing := false
	var updateFailure error
	err = workflow.SetUpdateHandlerWithOptions(ctx, SubmitReview, func(updateCtx workflow.Context, review Review) error {
		if slices.Contains(reviewIDs, review.ID) {
			return nil
		}
		processing = true
		defer func() { processing = false }()
		var verified Review
		if e := workflow.ExecuteActivity(ctl, "LoadAssetReview", review).Get(updateCtx, &verified); e != nil {
			updateFailure = e
			return e
		}
		if verified.Reviewer == snap.Author || verified.Reviewer == snap.Owner || verified.Reviewer == snap.Uploader {
			return fmt.Errorf("author cannot review")
		}
		if slices.Contains(reviewers, verified.Reviewer) {
			return fmt.Errorf("reviewer already voted")
		}
		reviewIDs = append(reviewIDs, verified.ID)
		reviewers = append(reviewers, verified.Reviewer)
		switch verified.Decision {
		case "reject":
			decision = "rejected"
		case "request_changes":
			decision = "changes_requested"
		case "approve":
			if len(reviewers) >= snap.Required {
				decision = "approved"
			}
		}
		return nil
	}, workflow.UpdateHandlerOptions{Validator: func(review Review) error {
		if decision != "" || *withdrawn || processing {
			return fmt.Errorf("review is closed or busy")
		}
		if review.Input != in || review.ID == "" {
			return fmt.Errorf("invalid review identity")
		}
		return nil
	}})
	if err != nil {
		return "", err
	}
	timerCtx, stopTimer := workflow.WithCancel(active)
	timer := workflow.NewTimer(timerCtx, 48*time.Hour)
	reminded := false
	workflow.Go(active, func(ctx workflow.Context) {
		if timer.Get(ctx, nil) == nil {
			reminded = true
		}
	})
	if err = workflow.Await(active, func() bool { return decision != "" || *withdrawn || reminded || updateFailure != nil }); err != nil {
		stopTimer()
		return "", err
	}
	if reminded && decision == "" && !*withdrawn && updateFailure == nil {
		if err = workflow.ExecuteActivity(active, "RemindAssetReview", in).Get(active, nil); err != nil {
			stopTimer()
			return "", err
		}
		if err = workflow.Await(active, func() bool { return decision != "" || *withdrawn || updateFailure != nil }); err != nil {
			stopTimer()
			return "", err
		}
	}
	stopTimer()
	if updateFailure != nil {
		return "", updateFailure
	}
	if *withdrawn {
		return "", temporal.NewCanceledError()
	}
	if err = workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) }); err != nil {
		return "", err
	}
	var finalDecision Result
	if err = workflow.ExecuteActivity(active, "DecideAssetReview", Aggregate{Input: in, State: decision}).Get(active, &finalDecision); err != nil {
		return "", err
	}
	return finalDecision.State, nil

}

// Failure codes carry no Activity payloads, credentials or free-form error text.
type Failure struct {
	Input     Input  `json:"input"`
	Withdrawn bool   `json:"withdrawn"`
	Code      string `json:"code"`
	Retryable bool   `json:"retryable"`
}

func releaseFailure(in Input, err error, withdrawn bool) Failure {
	out := Failure{Input: in, Withdrawn: withdrawn, Code: "WorkflowFailure", Retryable: true}
	if e, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		out.Code = e.Type()
		out.Retryable = !e.NonRetryable()
	}
	if e, ok := errors.AsType[*temporal.TimeoutError](err); ok {
		out.Code = "ActivityTimeout:" + e.TimeoutType().String()
	}
	return out
}
