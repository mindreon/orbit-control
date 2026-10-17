// Package sink projects durable run events and republishes the control outbox.
package sink

import (
	"context"
	"encoding/json/v2"
	"errors"
	"math"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
)

type rejection struct {
	tenant, subject, eventID, reason string
	seq, delivered                   uint64
	payload                          []byte
}

type eventStore interface {
	write(context.Context, contract.RunEvent, uint64, []byte) (bool, error)
	reject(context.Context, rejection) error
}

type postgresStore struct{ db *db.DB }

func (s postgresStore) write(ctx context.Context, e contract.RunEvent, seq uint64, raw []byte) (bool, error) {
	written := false
	err := s.db.WithTenant(ctx, e.TenantId, func(tx pgx.Tx) error {
		result, err := tx.Exec(ctx, `INSERT INTO run_events
 (tenant_id,run_id,session_id,member,segment,attempt,event_id,stream_seq,type,payload,ts)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT DO NOTHING`,
			e.TenantId, e.RunId, e.SessionId, e.Member, e.Segment, e.Attempt, e.Id, int64(seq), e.Type, raw, e.Ts)
		written = err == nil && result.RowsAffected() == 1
		return err
	})
	return written, err
}

func (s postgresStore) reject(ctx context.Context, r rejection) error {
	return s.db.WithTenant(ctx, r.tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO run_event_rejects
 (tenant_id,stream_seq,subject,event_id,reason,delivered,payload) VALUES($1,$2,$3,NULLIF($4,''),$5,$6,$7)
 ON CONFLICT DO NOTHING`, r.tenant, int64(r.seq), r.subject, r.eventID, r.reason, int64(r.delivered), r.payload)
		return err
	})
}

func subjectTenant(subject string) string {
	parts := strings.Split(subject, ".")
	if len(parts) >= 2 && parts[0] == "run" && ids.Valid(parts[1]) {
		return parts[1]
	}
	return ""
}

func classify(subject string, seq uint64, raw []byte) (contract.RunEvent, string) {
	var event contract.RunEvent
	if seq == 0 || seq > math.MaxInt64 {
		return event, "invalid_sequence"
	}
	if len(raw) > natsx.MaxMessageSize || contract.Validate("RunEvent", raw) != nil {
		return event, "invalid_contract"
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return event, "invalid_payload"
	}
	if subject != "run."+event.TenantId+"."+event.SessionId+"."+event.RunId {
		return event, "subject_mismatch"
	}
	return event, ""
}

func persistent(kind contract.EventType) bool {
	switch kind {
	case contract.EventTypeMessageDelta, contract.EventTypeReasoningDelta, contract.EventTypeUsageUpdate, contract.EventTypeSandboxOutput:
		return false
	default:
		return true
	}
}

func poisonReason(err error) string {
	if e, ok := errors.AsType[*pgconn.PgError](err); ok {
		switch e.Code {
		case "23503":
			return "foreign_key"
		case "23514":
			return "check_constraint"
		case "22P02", "22P05", "22003", "22008":
			return "invalid_payload"
		}
	}
	// Network, timeout, SQLSTATE 08/40/53/57 and unclassified failures retain
	// the message. Unknown infrastructure errors must not silently lose data.
	return ""
}
