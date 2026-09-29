package task

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewIDUsesUUIDv7(t *testing.T) {
	// Version nibble is 7; the variant nibble is 8, 9, a, or b.
	pattern := regexp.MustCompile(`^task_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
	var previous time.Time
	for range 20 {
		id := newID("task")
		if !pattern.MatchString(id) {
			t.Fatalf("invalid task id %q", id)
		}
		parsed, err := uuid.Parse(strings.TrimPrefix(id, "task_"))
		if err != nil {
			t.Fatalf("parse %q: %v", id, err)
		}
		if parsed.Version() != 7 {
			t.Fatalf("version = %d, want 7", parsed.Version())
		}
		sec, nsec := parsed.Time().UnixTime()
		stamp := time.Unix(sec, nsec)
		if !previous.IsZero() && stamp.Before(previous) {
			t.Fatalf("ids went backwards in time: %s then %s", previous, stamp)
		}
		previous = stamp
	}
}

func TestCloseAllSubscribersSignalsReconnect(t *testing.T) {
	service := New(nil)
	principal := Principal{TenantID: "tenant", UserID: "user"}
	created, err := service.Create(context.Background(), principal, CreateInput{
		Title: "close stream",
		Goal:  "reconnect",
	})
	if err != nil {
		t.Fatal(err)
	}
	sub, err := service.Subscribe(context.Background(), principal, created.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	service.CloseAllSubscribers()
	select {
	case <-sub.Closed:
	case <-time.After(time.Second):
		t.Fatal("subscriber was not closed")
	}
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-sub.Events:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("subscriber event channel was not closed")
		}
	}
}
