package sink

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
)

type Outbox struct {
	DB        *db.DB
	Publisher natsx.Publisher
	Logger    *slog.Logger
	Interval  time.Duration
	Meter     metric.Meter
	// Test seam for a corrected validator; production always uses validateOutbox.
	// Tests shorten only the publication budget, preserving time to commit.
	publishBudget time.Duration
	validate      func(outboxEvent) (string, string, contract.RunEvent)
}

func validateOutbox(event outboxEvent) (string, string, contract.RunEvent) {
	var envelope contract.RunEvent
	if len(event.payload) > natsx.MaxMessageSize {
		return "", "invalid_envelope", envelope
	}
	if err := json.Unmarshal(event.payload, &envelope); err != nil {
		return "", "invalid_envelope", envelope
	}
	if envelope.TenantId != event.tenant || envelope.RunId != event.run || envelope.Id != event.id {
		return "", "identity_mismatch", envelope
	}
	if !ids.Valid(envelope.SessionId) {
		return "", "subject_unresolvable", envelope
	}
	if contract.Validate("RunEvent", event.payload) != nil {
		return "", "invalid_envelope", envelope
	}
	return "run." + event.tenant + "." + envelope.SessionId + "." + event.run, "", envelope
}

type outboxEvent struct {
	tenant, run, id string
	payload         []byte
}

func (o *Outbox) Run(ctx context.Context) {
	for {
		if err := o.Republish(ctx); err != nil && ctx.Err() == nil {
			o.Logger.Warn("outbox scan failed", "error_type", fmt.Sprintf("%T", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(o.Interval):
		}
	}
}

func (o *Outbox) Republish(ctx context.Context) error {
	// Parent cancellation stops publication; reserve a fresh, bounded transaction
	// context to commit its successful prefix. DB/transaction failures still rollback.
	parent := ctx
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 30*time.Second)
	defer cancel()
	budget := o.publishBudget
	if budget <= 0 {
		budget = 20 * time.Second
	}
	publishCtx, stopPublish := context.WithTimeout(parent, budget)
	defer stopPublish()
	var publishErr error
	// The same app-role transaction scans through the SECURITY DEFINER function
	// and marks each tenant under RLS. No second pool connection is acquired.
	// A rollback may republish an acknowledged prefix; stable msg IDs deduplicate it.
	validate := o.validate
	if validate == nil {
		validate = validateOutbox
	}
	meter := o.Meter
	if meter == nil {
		meter = otel.Meter("github.com/mindreon/orbit-control/internal/sink")
	}
	quarantined, err := meter.Int64Counter("outbox_quarantined_total")
	if err != nil {
		return err
	}
	var isolated []outboxEvent
	var reasons []string
	err = o.DB.Global(ctx, func(tx pgx.Tx) error {
		var acquired bool
		if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended('control_events_outbox',0))`).Scan(&acquired); err != nil {
			return err
		}
		if !acquired {
			return nil
		}
		rows, err := tx.Query(ctx, `SELECT tenant_id::text,run_id::text,event_id,payload FROM control_events_unpublished(500)`)
		if err != nil {
			return err
		}
		var events []outboxEvent
		for rows.Next() {
			var event outboxEvent
			if err := rows.Scan(&event.tenant, &event.run, &event.id, &event.payload); err != nil {
				rows.Close()
				return err
			}
			events = append(events, event)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, event := range events {
			if publishCtx.Err() != nil {
				publishErr = publishCtx.Err()
				break
			}
			if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id',$1,true)`, event.tenant); err != nil {
				return err
			}
			subject, reason, envelope := validate(event)
			if reason != "" {
				if _, err := tx.Exec(ctx, `UPDATE control_events SET quarantined_at=now(),quarantine_reason=$4
 WHERE tenant_id=$1 AND run_id=$2 AND event_id=$3 AND published_at IS NULL AND quarantined_at IS NULL`,
					event.tenant, event.run, event.id, reason); err != nil {
					return err
				}
				isolated = append(isolated, event)
				reasons = append(reasons, reason)
				continue
			}
			if err := o.Publisher.Publish(publishCtx, subject, event.id, event.payload); err != nil {
				o.Logger.Warn("outbox publish failed", "run_id", event.run, "event_id", event.id,
					"error_type", fmt.Sprintf("%T", err))
				publishErr = err
				break
			}
			if envelope.Type == contract.EventTypeRunCompleted || envelope.Type == contract.EventTypeRunFailed {
				markerSubject, markerID, markerBody, err := natsx.EndMarker(event.tenant, envelope.SessionId, event.run, event.id)
				if err == nil {
					err = o.Publisher.Publish(publishCtx, markerSubject, markerID, markerBody)
				}
				if err != nil {
					o.Logger.Warn("outbox end marker publish failed", "run_id", event.run, "event_id", event.id, "error_type", fmt.Sprintf("%T", err))
					publishErr = err
					break
				}
			}
			if _, err := tx.Exec(ctx, `UPDATE control_events SET published_at=now()
 WHERE tenant_id=$1 AND run_id=$2 AND event_id=$3 AND published_at IS NULL AND quarantined_at IS NULL`,
				event.tenant, event.run, event.id); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		for i, event := range isolated {
			o.Logger.Warn("outbox.quarantine", "tenant_id", event.tenant, "run_id", event.run,
				"event_id", event.id, "reason", reasons[i])
			count(quarantined, "reason", reasons[i])
		}
		return publishErr
	}
	return err
}
