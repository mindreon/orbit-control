package task

import (
	"context"
	"regexp"
	"testing"
	"time"
)

func TestNewIDUsesCrockfordTaskAlphabet(t *testing.T) {
	pattern := regexp.MustCompile(`^task_[0-9A-HJKMNP-TV-Z]{26}$`)
	for range 100 {
		id, err := newID("task")
		if err != nil {
			t.Fatal(err)
		}
		if !pattern.MatchString(id) {
			t.Fatalf("invalid task id %q", id)
		}
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
