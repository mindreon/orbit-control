//go:build e2e

package persistence

import (
	"context"
	"testing"
	"time"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/store/pgstore"
)

// The persona and MCP connector tables through the gorm store: rows land in the caller's tenant, arrays that were left
// out come back as [] and the newest row is first.
func TestCatalogStoreRoundTripsPersonasAndConnectors(t *testing.T) {
	const tenant = "t-catalog-store"
	opsEnsureTenant(t, tenant)
	repo := pgstore.New(newPool(t, appURL, 2))
	ctx := context.Background()
	older := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	newer := older.Add(time.Minute)

	if err := repo.CreatePersona(ctx, tenant, store.PersonaRecord{ID: "p-old-" + tenant, Name: "old", CreatedAt: older}); err != nil {
		t.Fatalf("create persona: %v", err)
	}
	if err := repo.CreatePersona(ctx, tenant, store.PersonaRecord{ID: "p-new-" + tenant, Name: "new", Instructions: "be brief", McpConnectorIDs: []string{"c1", "c2"}, CreatedAt: newer}); err != nil {
		t.Fatalf("create persona: %v", err)
	}
	personas, err := repo.ListPersonas(ctx, tenant)
	if err != nil || len(personas) != 2 {
		t.Fatalf("list personas: %v %+v", err, personas)
	}
	if personas[0].Name != "new" || len(personas[0].McpConnectorIDs) != 2 || personas[0].Instructions != "be brief" {
		t.Fatalf("newest persona first with its fields: %+v", personas[0])
	}
	if personas[1].McpConnectorIDs == nil || len(personas[1].McpConnectorIDs) != 0 {
		t.Fatalf("a persona without connectors lists [], got %#v", personas[1].McpConnectorIDs)
	}

	if err := repo.CreateMcpConnector(ctx, tenant, store.McpConnectorRecord{ID: "m1-" + tenant, Name: "files", Command: "npx", Args: []string{"-y", "srv"}, EnvRefs: []string{"TOKEN"}, CreatedAt: older}); err != nil {
		t.Fatalf("create connector: %v", err)
	}
	if err := repo.CreateMcpConnector(ctx, tenant, store.McpConnectorRecord{ID: "m2-" + tenant, Name: "web", Transport: "streamable_http", URL: "https://example.test/mcp", HeaderRefs: []string{"Authorization:TOKEN"}, DefaultOpen: true, CreatedAt: newer}); err != nil {
		t.Fatalf("create connector: %v", err)
	}
	connectors, err := repo.ListMcpConnectors(ctx, tenant)
	if err != nil || len(connectors) != 2 {
		t.Fatalf("list connectors: %v %+v", err, connectors)
	}
	if connectors[0].Name != "web" || connectors[0].Transport != "streamable_http" || !connectors[0].DefaultOpen || len(connectors[0].HeaderRefs) != 1 {
		t.Fatalf("newest connector first with its fields: %+v", connectors[0])
	}
	if connectors[1].Transport != "stdio" || len(connectors[1].Args) != 2 || connectors[1].HeaderRefs == nil || len(connectors[1].HeaderRefs) != 0 {
		t.Fatalf("stdio is the default transport and unset arrays list []: %+v", connectors[1])
	}

	// Another tenant sees none of it.
	opsEnsureTenant(t, tenant+"-other")
	other, err := repo.ListPersonas(ctx, tenant+"-other")
	if err != nil || len(other) != 0 {
		t.Fatalf("a foreign tenant's personas: %v %+v", err, other)
	}
}
