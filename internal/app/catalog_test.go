package app

import (
	"context"
	"testing"
)

func TestCatalogSurvivesNewProcessAndStaysInTenant(t *testing.T) {
	a := NewWithOptions(Options{})
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
	if _, err := a.CreateMcpConnector(ctx, DefaultTenantID, McpConnectorInput{
		Name: "Remote", Transport: "streamable_http", URL: "https://mcp.example.com/mcp",
		HeaderRefs: []HeaderRef{{Name: "Authorization", Env: "DOCS_TOKEN"}}, DefaultOpen: true,
	}); err != nil {
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

	b := NewWithOptions(Options{Repo: a.Repo, DefaultTenant: DefaultTenantID})
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
}
