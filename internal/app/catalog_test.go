package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/worker"
)

func TestPersonaGrantCompositionAndAuditPersist(t *testing.T) {
	dir := t.TempDir()
	a := New(worker.New(""))
	a.Store = store.New(dir)

	persona, err := a.CreatePersona("Reviewer", "Be careful", nil)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := a.MintGrant(map[string]string{"DOCS_TOKEN": "secret-value"}, 60)
	if err != nil {
		t.Fatal(err)
	}
	if len(grant.EnvNames) != 1 || grant.EnvNames[0] != "DOCS_TOKEN" {
		t.Fatalf("grant public view = %+v", grant)
	}
	p, connectors, env, err := a.CompositionForRoom(persona.ID, grant.ID)
	if err != nil {
		t.Fatal(err)
	}
	if p == nil || p.Name != "Reviewer" {
		t.Fatalf("persona = %+v", p)
	}
	if len(connectors) != 0 {
		t.Fatalf("connectors = %+v", connectors)
	}
	if env["DOCS_TOKEN"] != "secret-value" {
		t.Fatalf("grant env = %#v", env)
	}
	// Persist an audit line without a live worker session: openSession fails
	// (no worker URL) and the room is recorded as closed.
	room, _, err := a.CreateRoom(context.Background(), Principal{TenantID: DefaultTenantID, UserID: "u-test"}, CreateRoomInput{})
	if room == nil {
		t.Fatalf("create room: %v", err)
	}
	roomID := room.ID
	a.Publish(roomID, Event{"type": "session.status", "roomId": roomID, "status": "idle"})
	auditPath := filepath.Join(dir, "audit", roomID+".jsonl")
	raw, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) == 0 {
		t.Fatal("expected audit jsonl bytes")
	}
}
