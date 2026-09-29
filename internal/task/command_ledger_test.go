package task

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

var ledgerPrincipal = Principal{TenantID: "tenant", UserID: "user"}

// fakeOrch counts UpdateTask calls, fails the first `failures` of them, and can block on `gate`.
type fakeOrch struct {
	TaskClient
	mu       sync.Mutex
	calls    int
	failures int
	entered  chan struct{}
	gate     chan struct{}
}

func (f *fakeOrch) UpdateTask(_ context.Context, _, _, _, commandID string, _ any) (json.RawMessage, error) {
	f.mu.Lock()
	f.calls++
	fail := f.calls <= f.failures
	f.mu.Unlock()
	if f.entered != nil {
		f.entered <- struct{}{}
	}
	if f.gate != nil {
		<-f.gate
	}
	if fail {
		return nil, errors.New("orchestrator unavailable")
	}
	return json.RawMessage(`{"accepted":true,"command_id":"` + commandID + `"}`), nil
}

func (f *fakeOrch) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func newServiceWith(t *testing.T, client TaskClient, ledger CommandLedger) (*Service, string) {
	t.Helper()
	service := New(nil)
	created, err := service.Create(context.Background(), ledgerPrincipal, CreateInput{Title: "ledger", Goal: "idempotent"})
	if err != nil {
		t.Fatal(err)
	}
	service.orch = client
	if ledger != nil {
		service.ledger = ledger
	}
	return service, created.ID
}

func pause() map[string]any { return map[string]any{"action": "pause"} }

func TestFailedUpdateCanBeRetriedWithTheSameCommandID(t *testing.T) {
	client := &fakeOrch{failures: 1}
	service, taskID := newServiceWith(t, client, nil)
	ctx := context.Background()
	if _, err := service.Update(ctx, ledgerPrincipal, taskID, "control", "cmd-1", pause()); err == nil {
		t.Fatal("first call must fail")
	}
	raw, err := service.Update(ctx, ledgerPrincipal, taskID, "control", "cmd-1", pause())
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if len(raw) == 0 {
		t.Fatal("retry returned an empty result")
	}
	if client.callCount() != 2 {
		t.Fatalf("orchestrator calls = %d, want 2 (the retry must reach the workflow)", client.callCount())
	}
	replay, err := service.Update(ctx, ledgerPrincipal, taskID, "control", "cmd-1", pause())
	if err != nil || string(replay) != string(raw) || client.callCount() != 2 {
		t.Fatalf("replay = %s %v, calls = %d", replay, err, client.callCount())
	}
}

func TestConcurrentDuplicateGetsInProgressThenTheStoredResult(t *testing.T) {
	client := &fakeOrch{entered: make(chan struct{}, 1), gate: make(chan struct{})}
	service, taskID := newServiceWith(t, client, nil)
	ctx := context.Background()
	done := make(chan json.RawMessage, 1)
	go func() {
		raw, err := service.Update(ctx, ledgerPrincipal, taskID, "control", "cmd-1", pause())
		if err != nil {
			t.Error(err)
		}
		done <- raw
	}()
	<-client.entered
	if _, err := service.Update(ctx, ledgerPrincipal, taskID, "control", "cmd-1", pause()); !errors.Is(err, ErrCommandInProgress) {
		t.Fatalf("concurrent duplicate err = %v, want ErrCommandInProgress", err)
	}
	if _, err := service.Update(ctx, ledgerPrincipal, taskID, "control", "cmd-1", map[string]any{"action": "cancel"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("different body while in progress err = %v, want ErrIdempotencyConflict", err)
	}
	close(client.gate)
	first := <-done
	again, err := service.Update(ctx, ledgerPrincipal, taskID, "control", "cmd-1", pause())
	if err != nil || string(again) != string(first) || len(again) == 0 {
		t.Fatalf("after completion: %s %v, first %s", again, err, first)
	}
	if client.callCount() != 1 {
		t.Fatalf("orchestrator calls = %d, want 1", client.callCount())
	}
}

func TestSameCommandIDForAnotherUpdateNameIsAConflict(t *testing.T) {
	service, taskID := newServiceWith(t, &fakeOrch{}, nil)
	ctx := context.Background()
	if _, err := service.Update(ctx, ledgerPrincipal, taskID, "control", "cmd-1", pause()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Update(ctx, ledgerPrincipal, taskID, "grantBudget", "cmd-1", pause()); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestTwoServicesShareOneLedger(t *testing.T) {
	ledger := newMemoryLedger(16, time.Now)
	a, taskID := newServiceWith(t, &fakeOrch{}, ledger)
	b := New(nil)
	b.orch, b.ledger = &fakeOrch{}, ledger
	b.tasks[taskID] = a.tasks[taskID]
	ctx := context.Background()
	if _, err := a.Update(ctx, ledgerPrincipal, taskID, "control", "cmd-1", pause()); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Update(ctx, ledgerPrincipal, taskID, "control", "cmd-1", map[string]any{"action": "cancel"}); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("second replica err = %v, want ErrIdempotencyConflict", err)
	}
}

func TestAbandonedCommandIsTakenOverAfterTheLease(t *testing.T) {
	now := time.Now()
	clock := func() time.Time { return now }
	ledger := newMemoryLedger(16, clock)
	if _, claimed, err := ledger.ClaimCommand(context.Background(), ledgerPrincipal.TenantID, "k", "h", "dead-owner", commandLease); err != nil || !claimed {
		t.Fatalf("first claim: %v %v", claimed, err)
	}
	if _, _, err := ledger.ClaimCommand(context.Background(), ledgerPrincipal.TenantID, "k", "h", "other", commandLease); !errors.Is(err, ErrCommandInProgress) {
		t.Fatalf("within the lease err = %v, want ErrCommandInProgress", err)
	}
	now = now.Add(commandLease + time.Second)
	if _, claimed, err := ledger.ClaimCommand(context.Background(), ledgerPrincipal.TenantID, "k", "h", "other", commandLease); err != nil || !claimed {
		t.Fatalf("after the lease: claimed=%v err=%v", claimed, err)
	}
	// The dead owner waking up must not overwrite the new owner's row.
	if err := ledger.CompleteCommand(context.Background(), ledgerPrincipal.TenantID, "k", "dead-owner", json.RawMessage(`{"stale":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := ledger.CompleteCommand(context.Background(), ledgerPrincipal.TenantID, "k", "other", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	result, claimed, err := ledger.ClaimCommand(context.Background(), ledgerPrincipal.TenantID, "k", "h", "third", commandLease)
	if err != nil || claimed || string(result) != `{"ok":true}` {
		t.Fatalf("replay = %s claimed=%v err=%v", result, claimed, err)
	}
}

func TestMemoryLedgerStaysBounded(t *testing.T) {
	ledger := newMemoryLedger(8, time.Now)
	for i := range 32 {
		key := "k" + strconv.Itoa(i)
		if _, claimed, err := ledger.ClaimCommand(context.Background(), ledgerPrincipal.TenantID, key, "h", "o", commandLease); err != nil || !claimed {
			t.Fatal(claimed, err)
		}
		if err := ledger.CompleteCommand(context.Background(), ledgerPrincipal.TenantID, key, "o", json.RawMessage(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if got := ledger.len(); got > 8 {
		t.Fatalf("ledger len = %d, limit 8", got)
	}
}
