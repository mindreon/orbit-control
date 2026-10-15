package assetrelease

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/temporalx"
	flow "github.com/mindreon/orbit-control/internal/workflows/assetrelease"
)

func Dial(c config.Config) (client.Client, error) { return temporalx.Dial(c) }

func NewWorker(c client.Client, a *Activities) worker.Worker {
	w := worker.New(c, contract.TaskQueueCtl, worker.Options{})
	Register(w, a)
	return w
}
func Register(w worker.Worker, a *Activities) {
	w.RegisterWorkflowWithOptions(flow.Run, workflow.RegisterOptions{Name: Name})
	w.RegisterActivityWithOptions(a.SnapshotAssetRelease, activity.RegisterOptions{Name: "SnapshotAssetRelease"})
	w.RegisterActivityWithOptions(a.AssetCheckPermissionDiff, activity.RegisterOptions{Name: contract.ActivityAssetCheckPermissionDiff})
	w.RegisterActivity(a.AggregateAssetChecks)
	w.RegisterActivity(a.CreateReviewTask)
	w.RegisterActivity(a.RemindAssetReview)
	w.RegisterActivity(a.LoadAssetReview)
	w.RegisterActivity(a.DecideAssetReview)
	w.RegisterActivity(a.Sign)
	w.RegisterActivity(a.Publish)
	w.RegisterActivity(a.CompensateAssetPublish)
	w.RegisterActivity(a.NotifyAuthor)
	w.RegisterActivity(a.AbortAssetRelease)
}

// Dispatch scans only tenant-local outbox fields through WithTenant. The global
// query discovers tenant IDs, never business rows. Committed requests survive
// API crashes; Temporal workflow/update IDs make redelivery safe.
func Dispatch(ctx context.Context, d *db.DB, c client.Client, l *slog.Logger) error {
	for {
		if err := dispatchOnce(ctx, d, c, l); err != nil && ctx.Err() == nil {
			l.Error("asset release dispatch failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// dispatchOnce isolates every tenant and every delivery. A poison row cannot
// starve unrelated releases, withdrawals or reviewer updates.
func dispatchOnce(ctx context.Context, d *db.DB, c client.Client, l *slog.Logger) error {
	tenants, err := releaseTenants(ctx, d)
	if err != nil {
		return err
	}
	for _, tenant := range tenants {
		releases, reviews, err := releaseDeliveries(ctx, d, tenant)
		if err != nil {
			l.Error("asset dispatch tenant failed", "tenant", tenant, "error", err)
			continue
		}
		for _, row := range releases {
			if err = dispatchRelease(ctx, d, c, row); err != nil {
				recordDispatchError(ctx, d, l, row.Input, err)
			}
		}
		for _, review := range reviews {
			if err = dispatchReview(ctx, d, c, review); err != nil {
				recordDispatchError(ctx, d, l, review.Input, err)
			}
		}
	}
	return nil
}

type releaseDelivery struct {
	Input
	Withdraw  bool
	Delivered bool
}

func releaseTenants(ctx context.Context, d *db.DB) ([]string, error) {
	tenants := []string{}
	err := d.Global(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text FROM tenants WHERE status='active' ORDER BY id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				return err
			}
			tenants = append(tenants, id)
		}
		return rows.Err()
	})
	return tenants, err
}
func releaseDeliveries(ctx context.Context, d *db.DB, tenant string) ([]releaseDelivery, []Review, error) {
	releases := []releaseDelivery{}
	reviews := []Review{}
	err := d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text,release_generation,withdraw_requested,withdraw_delivered
 FROM asset_versions
 WHERE (release_retryable OR withdraw_requested)
 AND state IN ('submitted','checking','in_review','approved')
 ORDER BY id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			row := releaseDelivery{Input: Input{Tenant: tenant}}
			if err = rows.Scan(&row.VersionID, &row.Generation, &row.Withdraw, &row.Delivered); err != nil {
				rows.Close()
				return err
			}
			releases = append(releases, row)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		rows, err = tx.Query(ctx, `SELECT r.id::text,r.version_id::text,r.generation
 FROM asset_reviews r
 JOIN asset_versions v ON v.id=r.version_id
 WHERE v.release_retryable
 AND NOT r.delivered
 AND r.generation=v.release_generation
 AND v.state='in_review'
 ORDER BY r.id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			review := Review{Input: Input{Tenant: tenant}}
			if err = rows.Scan(&review.ID, &review.Input.VersionID, &review.Input.Generation); err != nil {
				return err
			}
			reviews = append(reviews, review)
		}
		return rows.Err()
	})
	return releases, reviews, err
}
func dispatchRelease(ctx context.Context, d *db.DB, c client.Client, row releaseDelivery) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	in := row.Input
	execution, err := c.DescribeWorkflowExecution(ctx, flow.ID(in), "")
	running := false
	runID := ""
	if err == nil {
		info := execution.WorkflowExecutionInfo
		if info == nil {
			return temporal.NewNonRetryableApplicationError("workflow info missing", "InvalidWorkflow", nil)
		}
		running = info.Status == enums.WORKFLOW_EXECUTION_STATUS_RUNNING
		if running {
			var generation int
			if info.Memo == nil {
				return temporal.NewNonRetryableApplicationError("workflow memo missing", "InvalidWorkflow", nil)
			}
			if err = converter.GetDefaultDataConverter().FromPayload(info.Memo.Fields["generation"], &generation); err != nil {
				return temporal.NewNonRetryableApplicationError("invalid workflow memo", "InvalidWorkflow", err)
			}
			// An older generation may still be closing after withdrawal. Never signal it.
			if generation != in.Generation {
				return nil
			}
			runID = info.Execution.RunId
		}
	} else if _, missing := errors.AsType[*serviceerror.NotFound](err); !missing {
		return err
	}
	if row.Withdraw && !running {
		return (&Activities{DB: d, Assets: &assets.Service{}}).AbortWithdraw(ctx, in)
	}
	if !running {
		return startRelease(ctx, d, c, in)
	}

	if row.Withdraw && !row.Delivered {
		if err = c.SignalWorkflow(ctx, flow.ID(in), runID, Withdraw, nil); err != nil {
			return err
		}
		return d.WithTenant(ctx, in.Tenant, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx,
				`UPDATE asset_versions SET withdraw_delivered=true WHERE tenant_id=$1 AND id=$2 AND release_generation=$3`,
				in.Tenant, in.VersionID, in.Generation)
			return err
		})
	}
	return nil
}
func startRelease(ctx context.Context, d *db.DB, c client.Client, in Input) error {
	eligible := false
	if err := d.WithTenant(ctx, in.Tenant, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, `SELECT release_generation=$3
 AND release_retryable
 AND NOT withdraw_requested
 AND state IN ('submitted','checking','in_review','approved')
 FROM asset_versions
 WHERE tenant_id=$1
 AND id=$2`, in.Tenant, in.VersionID, in.Generation).Scan(&eligible)
	}); err != nil {
		return err
	}
	if !eligible {
		return nil
	}
	_, err := c.ExecuteWorkflow(
		ctx,
		client.StartWorkflowOptions{
			ID:                                       flow.ID(in),
			TaskQueue:                                contract.TaskQueueCtl,
			WorkflowIDReusePolicy:                    enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowExecutionErrorWhenAlreadyStarted: true,
			Memo:                                     map[string]any{"generation": in.Generation},
		},
		Name,
		in,
	)
	if err != nil {
		if _, already := errors.AsType[*serviceerror.WorkflowExecutionAlreadyStarted](err); !already {
			return err
		}
	}
	return d.WithTenant(ctx, in.Tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE asset_versions SET release_started=true WHERE tenant_id=$1 AND id=$2 AND release_generation=$3`,
			in.Tenant, in.VersionID, in.Generation)
		return err
	})
}
func (a *Activities) AbortWithdraw(ctx context.Context, in Input) error {
	_, err := a.AbortAssetRelease(ctx, Failure{Input: in, Withdrawn: true})
	return err
}
func dispatchReview(ctx context.Context, d *db.DB, c client.Client, review Review) error {
	ctx, cancelDelivery := context.WithTimeout(ctx, 3*time.Second)
	defer cancelDelivery()
	// Do not wait for an unregistered Update handler: it can stall this dispatcher
	// for minutes. Accepted delivery is durable; SQL votes remain the authority.
	handle, err := c.UpdateWorkflow(
		ctx,
		client.UpdateWorkflowOptions{
			WorkflowID:   flow.ID(review.Input),
			UpdateName:   SubmitReview,
			UpdateID:     review.ID,
			Args:         []any{review},
			WaitForStage: client.WorkflowUpdateStageAccepted,
		},
	)
	if err != nil {
		return err
	}
	// Get is deliberately bounded so a stalled Activity cannot starve later rows.
	getCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err = handle.Get(getCtx, nil); err != nil {
		return err
	}
	return d.WithTenant(ctx, review.Input.Tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`UPDATE asset_reviews SET delivered=true WHERE tenant_id=$1 AND id=$2 AND generation=$3`,
			review.Input.Tenant, review.ID, review.Input.Generation)
		return err
	})
}
func dispatchPermanent(err error) bool {
	if _, ok := errors.AsType[*serviceerror.InvalidArgument](err); ok {
		return true
	}
	if _, ok := errors.AsType[*serviceerror.PermissionDenied](err); ok {
		return true
	}
	if _, ok := errors.AsType[*serviceerror.NamespaceNotFound](err); ok {
		return true
	}
	if e, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		return e.NonRetryable()
	}
	return false
}
func recordDispatchError(ctx context.Context, d *db.DB, l *slog.Logger, in Input, err error) {
	permanent := dispatchPermanent(err)
	l.Error(
		"asset release delivery failed",
		"tenant",
		in.Tenant,
		"version",
		in.VersionID,
		"generation",
		in.Generation,
		"permanent",
		permanent,
		"error",
		err,
	)
	code := fmt.Sprintf("Dispatch:%T", err)
	if saveErr := d.WithTenant(ctx, in.Tenant, func(tx pgx.Tx) error {
		_, e := tx.Exec(ctx, `UPDATE asset_versions
 SET release_error=$3,release_retryable=release_retryable
 AND $4
 WHERE tenant_id=$1
 AND id=$2
 AND release_generation=$5`, in.Tenant, in.VersionID, code, !permanent, in.Generation)
		return e
	}); saveErr != nil {
		l.Error("asset dispatch error recording failed", "tenant", in.Tenant, "version", in.VersionID, "error", saveErr)
	}
}
