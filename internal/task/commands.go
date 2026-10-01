package task

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mindreon/orbit-control/internal/orch"
)

func (s *Service) Update(ctx context.Context, p Principal, id, name, commandID string, payload any) (json.RawMessage, error) {
	t, err := s.Get(ctx, p, id)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if isClosed(t.Status) && (name == "sendMessage" || name == "updateTaskConfig") {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	s.mu.Unlock()
	requestBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	// The update name is part of the hash: one command_id cannot mean two different commands.
	sum := sha256.Sum256(append([]byte(name+"\x00"), requestBytes...))
	hash := fmt.Sprintf("%x", sum)
	key, owner := commandKey(p, id, commandID), mustUUIDv7()
	stored, claimed, err := s.ledger.ClaimCommand(ctx, p.TenantID, key, hash, owner, commandLease)
	if err != nil {
		return nil, err
	}
	if !claimed {
		return stored, nil
	}
	raw, err := s.runUpdate(ctx, p, id, name, commandID, payload)
	if err != nil {
		s.releaseCommand(ctx, p.TenantID, key, owner)
		return nil, mapOrchErr(err)
	}
	if err := s.ledger.CompleteCommand(ctx, p.TenantID, key, owner, raw); err != nil {
		// The workflow accepted the command. Give the claim up so a retry with the same command_id does not have to
		// wait for the lease; the Update ID makes the second send a no-op.
		s.releaseCommand(ctx, p.TenantID, key, owner)
		return nil, err
	}
	return raw, nil
}

// runUpdate sends the command to the workflow. Without an orchestrator the local projection stays usable in dev and
// during an orchestrator restart; the durable event projector replaces it when connected.
func (s *Service) runUpdate(ctx context.Context, p Principal, id, name, commandID string, payload any) (json.RawMessage, error) {
	if config, ok := payload.(map[string]any); ok && name == "updateTaskConfig" {
		return s.runConfigUpdate(ctx, p, id, commandID, config)
	}
	if s.orch != nil {
		raw, err := s.orch.UpdateTask(ctx, p.TenantID, id, name, commandID, payload)
		if err != nil {
			return nil, err
		}
		s.applyUpdateProjection(id, name, raw)
		return raw, nil
	}
	s.applyLocalUpdate(id, name, payload)
	return json.RawMessage(`{"accepted":true}`), nil
}

// releaseCommand runs on a context that outlives a cancelled request, so a client that hung up does not leave the
// claim held until the lease passes. A failure here is bounded by that lease.
func (s *Service) releaseCommand(ctx context.Context, tenantID, key, owner string) {
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = s.ledger.ReleaseCommand(releaseCtx, tenantID, key, owner)
}

func (s *Service) Signal(ctx context.Context, p Principal, id, name string, payload any) error {
	if _, err := s.Get(ctx, p, id); err != nil {
		return err
	}
	if s.orch != nil {
		return s.orch.SignalTask(ctx, p.TenantID, id, name, payload)
	}
	return nil
}

// mapOrchErr turns what the workflow's validators refuse with into the errors the API answers by.
func mapOrchErr(err error) error {
	if errors.Is(err, orch.ErrTaskClosed) {
		return ErrClosed
	}
	return err
}
