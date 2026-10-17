package schedules

import (
	"context"
	"encoding/json/v2"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

// Sync takes the row identity, not a snapshot: reading after the session lock
// prevents an old caller from overwriting a newer Temporal configuration.
func (s Service) Sync(ctx context.Context, tenant, id string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	pending := true
	locked, err := s.DB.WithSessionLock(ctx, id, 10, func(conn *db.Session) error {
		for range 3 {
			var row Schedule
			if err := conn.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
				var raw []byte
				if err := tx.QueryRow(ctx, selectSchedule, tenant, id).Scan(&raw); err != nil {
					return err
				}
				return json.Unmarshal(raw, &row)
			}); err != nil {
				return err
			}
			if err := s.syncTemporal(ctx, row); err != nil {
				return err
			}
			var version int64
			if err := conn.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
				if _, err := tx.Exec(ctx, `UPDATE schedules SET synced_version=$3
 WHERE tenant_id=$1 AND id=$2 AND synced_version<$3`, tenant, id, row.Version); err != nil {
					return err
				}
				return tx.QueryRow(ctx, `SELECT version FROM schedules WHERE tenant_id=$1 AND id=$2`, tenant, id).Scan(&version)
			}); err != nil {
				return err
			}
			if version <= row.Version {
				pending = false
				return nil
			}
		}
		return nil
	})
	return pending || !locked, err
}
func (s Service) syncTemporal(ctx context.Context, row Schedule) error {
	if s.Temporal == nil {
		return errors.New("schedule client unavailable")
	}
	var err error
	for attempt := range 3 {
		if err = s.syncAttempt(ctx, row); err == nil {
			return nil
		}
		if attempt == 2 {
			break
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}
func (s Service) syncAttempt(ctx context.Context, row Schedule) error {
	handle := s.Temporal.GetHandle(ctx, w.ScheduleID(row.TenantID, row.ID))
	if row.Status == "deleted" {
		err := handle.Delete(ctx)
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			return nil
		}
		return err
	}
	spec := client.ScheduleSpec{CronExpressions: []string{row.Cron}, TimeZoneName: row.Timezone, Jitter: time.Minute}
	overlap := enums.SCHEDULE_OVERLAP_POLICY_SKIP
	if row.OverlapPolicy == "buffer_one" {
		overlap = enums.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE
	}
	action := &client.ScheduleWorkflowAction{ID: w.ScheduledRunWorkflowIDBase(row.TenantID, row.ID),
		Workflow: w.ScheduledRunWorkflow, TaskQueue: contract.TaskQueueCtl,
		Args: []any{w.ScheduledRunInput{TenantID: row.TenantID, ScheduleID: row.ID}},
		TypedSearchAttributes: temporal.NewSearchAttributes(
			temporal.NewSearchAttributeKeyKeyword("TenantId").ValueSet(row.TenantID),
			temporal.NewSearchAttributeKeyKeyword("WorkspaceId").ValueSet(row.WorkspaceID),
			temporal.NewSearchAttributeKeyKeyword("RunKind").ValueSet("schedule"))}
	catchup := time.Duration(row.CatchupWindowSeconds) * time.Second
	_, err := handle.Describe(ctx)
	if _, missing := errors.AsType[*serviceerror.NotFound](err); missing {
		_, err = s.Temporal.Create(ctx, client.ScheduleOptions{ID: w.ScheduleID(row.TenantID, row.ID), Spec: spec,
			Action: action, Overlap: overlap, CatchupWindow: catchup, Paused: row.Status == "paused"})
		if err == nil {
			return nil
		}
		if !errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
			if _, exists := errors.AsType[*serviceerror.AlreadyExists](err); !exists {
				return err
			}
		}
	} else if err != nil {
		return err
	}
	if err := handle.Update(ctx, client.ScheduleUpdateOptions{DoUpdate: func(client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
		return &client.ScheduleUpdate{Schedule: &client.Schedule{Spec: &spec, Action: action,
			Policy: &client.SchedulePolicies{Overlap: overlap, CatchupWindow: catchup},
			State:  &client.ScheduleState{Paused: row.Status == "paused"}}}, nil
	}}); err != nil {
		return err
	}
	return nil
}

// SweepOnce isolates tenants and bounds every tenant's pending batch.
func (s Service) SweepOnce(ctx context.Context) error {
	tenants := []string{}
	if err := s.DB.Global(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT id::text FROM tenants WHERE status='active' ORDER BY id`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			tenants = append(tenants, id)
		}
		return rows.Err()
	}); err != nil {
		return err
	}
	var failures []error
	for _, tenant := range tenants {
		identities := []string{}
		err := s.DB.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			rows, err := tx.Query(ctx, `SELECT id::text FROM schedules WHERE tenant_id=$1
 AND synced_version<version ORDER BY id LIMIT 100`, tenant)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var id string
				if err := rows.Scan(&id); err != nil {
					return err
				}
				identities = append(identities, id)
			}
			return rows.Err()
		})
		if err != nil {
			failures = append(failures, err)
			continue
		}
		for _, id := range identities {
			if _, err := s.Sync(ctx, tenant, id); err != nil {
				failures = append(failures, err)
			}
		}
	}
	return errors.Join(failures...)
}
func (s Service) Sweep(ctx context.Context, onError func(error)) {
	interval := s.SweepInterval
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := s.SweepOnce(ctx); err != nil && ctx.Err() == nil {
			onError(err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
