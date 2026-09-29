package task

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

type OutboxRecord struct {
	ID       int64
	TenantID string
	TaskID   string
	EventID  string
	Body     []byte
}

type OutboxStore interface {
	ClaimRuntimeOutbox(context.Context, int) ([]OutboxRecord, error)
	MarkRuntimeOutboxProjected(context.Context, []int64) error
}

// RuntimeOutboxWakeup is implemented by PostgreSQL stores. Notifications are
// only a latency optimization; the polling loop remains the recovery path.
type RuntimeOutboxWakeup interface {
	ListenRuntimeOutbox(context.Context, string) (<-chan struct{}, error)
}

// ProjectorLeadership is implemented by PostgreSQL stores: at most one control replica projects the outbox at a time
// (09 §3). The returned release gives the leadership up; the store also releases it if the connection dies.
type ProjectorLeadership interface {
	AcquireProjectorLeadership(context.Context) (release func(), ok bool, err error)
}

type Projector struct {
	Store OutboxStore
	Tasks *Service
	// OnDrop is told about an outbox row that can never be projected. Such a row is marked done: leaving it would
	// make every later row wait behind it.
	OnDrop func(OutboxRecord, error)
	Poll   time.Duration
	Batch  int
}

// Run projects until ctx ends. It never gives up on an error: a failed batch is retried, and with a store that supports
// leadership a replica that is not the leader waits for the leader to go away.
func (p *Projector) Run(ctx context.Context) error {
	leadership, elected := p.Store.(ProjectorLeadership)
	for ctx.Err() == nil {
		release := func() {}
		if elected {
			rel, ok, err := leadership.AcquireProjectorLeadership(ctx)
			if err != nil || !ok {
				sleep(ctx, 2*time.Second)
				continue
			}
			release = rel
		}
		_ = p.run(ctx)
		release()
		sleep(ctx, time.Second)
	}
	return ctx.Err()
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func (p *Projector) run(parent context.Context) error {
	// The wakeup listener holds a connection until this context ends, so each run gets its own.
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	if p.Poll <= 0 {
		p.Poll = 250 * time.Millisecond
	}
	if p.Batch <= 0 {
		p.Batch = 100
	}
	ticker := time.NewTicker(p.Poll)
	defer ticker.Stop()
	var wake <-chan struct{}
	if listener, ok := p.Store.(RuntimeOutboxWakeup); ok {
		if notifications, err := listener.ListenRuntimeOutbox(ctx, ""); err == nil {
			wake = notifications
		}
	}
	for {
		if err := p.project(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-wake:
			// The notification identifies a task, but the outbox query is the
			// source of truth and safely catches every pending task.
		}
	}
}

func (p *Projector) project(ctx context.Context) error {
	rows, err := p.Store.ClaimRuntimeOutbox(ctx, p.Batch)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		var body struct {
			TenantID   string          `json:"tenant_id"`
			EventID    string          `json:"event_id"`
			TaskID     string          `json:"task_id"`
			Type       string          `json:"type"`
			Source     json.RawMessage `json:"source"`
			Payload    json.RawMessage `json:"payload"`
			OccurredAt time.Time       `json:"occurred_at"`
			Retention  string          `json:"retention"`
			Entity     struct {
				Kind    string `json:"kind"`
				ID      string `json:"id"`
				Version int64  `json:"version"`
			} `json:"entity"`
		}
		if err := json.Unmarshal(row.Body, &body); err != nil {
			p.drop(row, err)
			ids = append(ids, row.ID)
			continue
		}
		if body.TenantID == "" {
			body.TenantID = row.TenantID // the outbox row carries the tenant the worker wrote it under
		}
		if body.EventID == "" {
			body.EventID = row.EventID
		}
		if body.TaskID == "" {
			body.TaskID = row.TaskID
		}
		source := "workflow"
		var sourceString string
		if json.Unmarshal(body.Source, &sourceString) == nil {
			source = sourceString
		} else {
			var sourceObject struct {
				Kind string `json:"kind"`
			}
			if json.Unmarshal(body.Source, &sourceObject) == nil && sourceObject.Kind != "" {
				source = sourceObject.Kind
			}
		}
		if err := p.Tasks.AppendEvent(Event{EventID: body.EventID, TenantID: body.TenantID, TaskID: body.TaskID, Type: body.Type, Source: source, Payload: body.Payload, Occurred: body.OccurredAt, Durable: body.Retention != "ephemeral", EntityKind: body.Entity.Kind, EntityID: body.Entity.ID, EntityVersion: body.Entity.Version}); err != nil {
			if !errors.Is(err, ErrMalformedEvent) {
				return err
			}
			p.drop(row, err)
		}
		ids = append(ids, row.ID)
	}
	return p.Store.MarkRuntimeOutboxProjected(ctx, ids)
}

func (p *Projector) drop(row OutboxRecord, err error) {
	if p.OnDrop != nil {
		p.OnDrop(row, err)
	}
}
