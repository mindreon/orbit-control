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

	ctx := context.Background()
	persona, err := a.CreatePersona(ctx, DefaultTenantID, "Reviewer", "Be careful", nil)
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

func TestCatalogSurvivesNewProcessAndStaysInTenant(t *testing.T) {
	a := New(worker.New(""))
	ctx := context.Background()
	persona, err := a.CreatePersona(ctx, DefaultTenantID, "Reviewer", "Be careful", nil)
	if err != nil {
		t.Fatal(err)
	}
	connector, err := a.CreateMcpConnector(ctx, DefaultTenantID, McpConnectorInput{
		Name: "Docs", Command: "npx", Args: []string{"--header=demo"}, EnvRefs: []string{"DOCS_TOKEN"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if connector.Transport != "stdio" {
		t.Fatalf("transport = %s", connector.Transport)
	}
	if _, err := a.CreateMcpConnector(ctx, DefaultTenantID, McpConnectorInput{
		Name: "Bad", Command: "true", EnvRefs: []string{"TOKEN=secret"},
	}); err == nil {
		t.Fatal("env ref with a value was accepted")
	}
	if _, err := a.CreateMcpConnector(ctx, DefaultTenantID, McpConnectorInput{
		Name: "Public", Transport: "streamable_http", URL: "http://mcp.example.com/mcp",
	}); err == nil {
		t.Fatal("public http url was accepted")
	}
	remote, err := a.CreateMcpConnector(ctx, DefaultTenantID, McpConnectorInput{
		Name: "Remote", Transport: "streamable_http", URL: "https://mcp.example.com/mcp",
		HeaderRefs: []HeaderRef{{Name: "Authorization", Env: "DOCS_TOKEN"}}, DefaultOpen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.CreateMcpConnector(ctx, DefaultTenantID, McpConnectorInput{
		Name: "Leaked", Transport: "streamable_http", URL: "https://mcp.example.com/mcp?token=hidden",
	}); err == nil {
		t.Fatal("url secret was accepted")
	}
	if _, err := a.CreatePersona(ctx, "other", "Hidden", "no", nil); err != nil {
		t.Fatal(err)
	}

	b := NewWithOptions(Options{Repo: a.Repo, Worker: worker.New(""), DefaultTenant: DefaultTenantID})
	items, err := b.ListPersonas(ctx, DefaultTenantID)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ID != persona.ID || items[0].Name != "Reviewer" {
		t.Fatalf("default personas = %+v", items)
	}
	hidden, err := b.ListPersonas(ctx, "other")
	if err != nil || len(hidden) != 1 || hidden[0].Name != "Hidden" {
		t.Fatalf("other tenant = %+v err=%v", hidden, err)
	}
	connectors, err := b.ListMcpConnectors(ctx, DefaultTenantID)
	if err != nil || len(connectors) != 2 {
		t.Fatalf("connectors = %+v err=%v", connectors, err)
	}
	foundDocs := false
	for _, item := range connectors {
		if item.ID == connector.ID && len(item.Args) == 1 && item.Args[0] == "--header=demo" {
			foundDocs = true
		}
	}
	if !foundDocs {
		t.Fatalf("stdio connector missing: %+v", connectors)
	}
	selected, err := b.ConnectorsForRoom(ctx, DefaultTenantID, "")
	if err != nil || len(selected) != 1 || selected[0].ID != remote.ID || selected[0].URL != "https://mcp.example.com/mcp" {
		t.Fatalf("default open = %+v err=%v", selected, err)
	}
	if selected[0].HeaderRefs[0].Env != "DOCS_TOKEN" || selected[0].HeaderRefs[0].Name != "Authorization" {
		t.Fatalf("header ref = %+v", selected[0].HeaderRefs)
	}
	linked, err := b.ConnectorsForRoom(ctx, DefaultTenantID, persona.ID)
	if err != nil || len(linked) != 1 {
		t.Fatalf("persona without links should still get default open, got %+v err=%v", linked, err)
	}
	loaded, _, _, err := b.CompositionForRoom(persona.ID, "")
	if err != nil || loaded == nil || loaded.Name != "Reviewer" {
		t.Fatalf("composition = %+v err=%v", loaded, err)
	}
}
