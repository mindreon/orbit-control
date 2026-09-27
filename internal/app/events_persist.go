package app

import (
	"context"
	"encoding/json"

	"github.com/mindreon/orbit-control/internal/store"
)

// emitDurable stores a durable event under the per-task seq when Postgres is
// configured, and otherwise keeps the in-memory global id. allowFinished lets
// control record the closing session.status of a room that is already closed
// or failed. Worker events after that return ErrNotFound (FM-62).
func (a *App) emitDurable(ctx context.Context, tenantID string, env Envelope, allowFinished bool) error {
	es, ok := a.Repo.(store.EventStore)
	if !ok {
		a.publishDurable(env)
		return nil
	}
	payload := env.Payload
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	seq, err := es.AppendEvent(ctx, tenantID, store.EventRecord{
		EventUID: id("ev_"),
		TaskID:   env.TaskID,
		Type:     env.Type,
		Source:   env.Source,
		Payload:  payload,
	}, allowFinished)
	if err != nil {
		return err
	}
	env.ID = uint64(seq)
	raw, err := EncodeEnvelope(env)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.broadcastLocked(env.TaskID, StreamFrame{Seq: env.ID, Durable: true, Data: raw})
	a.mu.Unlock()
	a.persistActivity(env.TaskID, env)
	return nil
}

func envelopeFromEvent(row store.EventRecord) Envelope {
	ts := stamp(row.CreatedAt)
	var head struct {
		OccurredAt string `json:"occurredAt"`
	}
	if json.Unmarshal(row.Payload, &head) == nil && head.OccurredAt != "" {
		ts = head.OccurredAt
	}
	return Envelope{
		ID: uint64(row.Seq), Type: row.Type, TaskID: row.TaskID,
		TS: ts, Source: row.Source, Payload: row.Payload,
	}
}

// headLocked reads the room head. The caller holds a.mu. The database call
// does not take a.mu.
func (a *App) headLocked(roomID string) (uint64, error) {
	if es, ok := a.Repo.(store.EventStore); ok {
		tenantID := a.live[roomID].TenantID
		if tenantID == "" {
			tenantID = a.DefaultTenant
		}
		head, err := es.RoomEventHead(context.Background(), tenantID, roomID)
		if err != nil {
			return 0, err
		}
		return uint64(head), nil
	}
	return a.Events.Head(roomID)
}
