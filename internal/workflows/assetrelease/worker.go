package assetrelease

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

func Dial(c config.Config) (client.Client, error) {
	options := client.Options{HostPort: c.TemporalAddress, Namespace: c.TemporalNamespace}
	if c.TemporalTLS {
		tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.TemporalServerName}
		if c.TemporalCAFile != "" {
			b, e := os.ReadFile(c.TemporalCAFile)
			if e != nil {
				return nil, fmt.Errorf("cannot read ORBIT_TEMPORAL_TLS_CA_FILE")
			}
			tlsConfig.RootCAs = x509.NewCertPool()
			if !tlsConfig.RootCAs.AppendCertsFromPEM(b) {
				return nil, fmt.Errorf("invalid Temporal CA")
			}
		}
		if c.TemporalCertFile != "" || c.TemporalKeyFile != "" {
			cert, e := tls.LoadX509KeyPair(c.TemporalCertFile, c.TemporalKeyFile)
			if e != nil {
				return nil, fmt.Errorf("invalid Temporal client certificate/key")
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
		options.ConnectionOptions.TLS = tlsConfig
	}
	return client.Dial(options)
}
func NewWorker(c client.Client, a *Activities) worker.Worker {
	w := worker.New(c, contract.TaskQueueCtl, worker.Options{})
	w.RegisterWorkflowWithOptions(Run, workflow.RegisterOptions{Name: Name})
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
	return w
}

// Dispatch scans only tenant-local outbox fields through WithTenant. The global
// query discovers tenant IDs, never business rows. Committed requests survive
// API crashes; Temporal workflow/update IDs make redelivery safe.
func Dispatch(ctx context.Context, d *db.DB, c client.Client, l *slog.Logger) error {
	for {
		if err := dispatchOnce(ctx, d, c); err != nil && ctx.Err() == nil {
			l.Error("asset release dispatch failed", "error_type", fmt.Sprintf("%T", err))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(500 * time.Millisecond):
		}
	}
}
func dispatchOnce(ctx context.Context, d *db.DB, c client.Client) error {
	tenants := []string{}
	if err := d.Global(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text FROM tenants WHERE status='active'`)
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
	}); err != nil {
		return err
	}
	for _, tenant := range tenants {
		starts := []Input{}
		withdrawals := []Input{}
		reviews := []Review{}
		err := d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			rows, err := tx.Query(ctx, `SELECT id::text,release_generation,release_started,withdraw_requested AND NOT withdraw_delivered FROM asset_versions WHERE (state='submitted' AND NOT release_started) OR (withdraw_requested AND NOT withdraw_delivered)`)
			if err != nil {
				return err
			}
			for rows.Next() {
				var id string
				var generation int
				var started, withdraw bool
				if err = rows.Scan(&id, &generation, &started, &withdraw); err != nil {
					rows.Close()
					return err
				}
				in := Input{tenant, id, generation}
				if !started {
					starts = append(starts, in)
				}
				if withdraw {
					if started {
						withdrawals = append(withdrawals, in)
					}
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			rows, err = tx.Query(ctx, `SELECT r.id::text,r.version_id::text,r.generation FROM asset_reviews r JOIN asset_versions v ON v.id=r.version_id WHERE NOT r.delivered AND r.generation=v.release_generation AND v.state='in_review' ORDER BY r.id`)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var review Review
				review.Input.Tenant = tenant
				if err = rows.Scan(&review.ID, &review.Input.VersionID, &review.Input.Generation); err != nil {
					return err
				}
				reviews = append(reviews, review)
			}
			return rows.Err()
		})
		if err != nil {
			return err
		}
		for _, in := range starts {
			_, err = c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{ID: ID(in), TaskQueue: contract.TaskQueueCtl, WorkflowIDReusePolicy: enums.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, Memo: map[string]any{"generation": in.Generation}}, Name, in)
			if err != nil {
				if _, already := errors.AsType[*serviceerror.WorkflowExecutionAlreadyStarted](err); !already {
					return err
				}
				execution, e := c.DescribeWorkflowExecution(ctx, ID(in), "")
				if e != nil {
					return e
				}
				var generation int
				if execution.WorkflowExecutionInfo.Memo == nil {
					return fmt.Errorf("release workflow memo missing")
				}
				if e = converter.GetDefaultDataConverter().FromPayload(execution.WorkflowExecutionInfo.Memo.Fields["generation"], &generation); e != nil {
					return e
				}
				if generation != in.Generation {
					continue
				}
			}
			if err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `UPDATE asset_versions SET release_started=true WHERE id=$1 AND release_generation=$2`, in.VersionID, in.Generation)
				return err
			}); err != nil {
				return err
			}
		}
		for _, in := range withdrawals {
			if err = c.SignalWorkflow(ctx, ID(in), "", Withdraw, nil); err != nil {
				if _, ok := errors.AsType[*serviceerror.NotFound](err); !ok {
					return err
				}
			}
			if err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `UPDATE asset_versions SET withdraw_delivered=true WHERE id=$1 AND release_generation=$2`, in.VersionID, in.Generation)
				return err
			}); err != nil {
				return err
			}
		}
		for _, review := range reviews {
			handle, e := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: ID(review.Input), UpdateName: SubmitReview, UpdateID: review.ID, Args: []any{review}, WaitForStage: client.WorkflowUpdateStageCompleted})
			if e != nil {
				return e
			}
			if e = handle.Get(ctx, nil); e != nil {
				return e
			}
			if err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, `UPDATE asset_reviews SET delivered=true WHERE id=$1`, review.ID)
				return err
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
