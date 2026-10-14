package integration

import (
	"testing"

	"github.com/jackc/pgx/v5"
)

func testContractColumns(t *testing.T, admin *pgx.Conn) {
	expected := map[string]string{
		"runs.budget_millicredits": "bigint", "runs.used_millicredits": "bigint",
		"budget_reservations.millicredits": "bigint", "usage_ledger.millicredits": "bigint", "usage_ledger_default.millicredits": "bigint", "usage_rollups.millicredits": "bigint",
		"runs.created_by": "uuid", "runs.message_id": "uuid", "run_events.event_id": "text",
		"interactions.seq": "integer", "interactions.session_id": "uuid", "interactions.remind_at": "timestamp with time zone", "interactions.expires_at": "timestamp with time zone", "interactions.resolved_at": "timestamp with time zone",
		"permission_grants.interaction_id": "uuid", "permission_grants.arg_pattern": "text",
		"artifacts.updated_at": "timestamp with time zone", "sandboxes.session_id": "uuid", "sandboxes.snapshot_id": "text", "sandboxes.expires_at": "timestamp with time zone", "sandboxes.lease_gen": "integer",
		"model_profiles.pricing": "jsonb", "model_profiles.context_window": "integer",
	}
	rows, err := admin.Query(t.Context(), `SELECT table_name||'.'||column_name,data_type FROM information_schema.columns WHERE table_schema='public'`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	actual := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatal(err)
		}
		actual[name] = typ
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for name, typ := range expected {
		if actual[name] != typ {
			t.Errorf("%s: got %q want %q", name, actual[name], typ)
		}
	}
	for _, name := range []string{"runs.budget_credits", "runs.used_credits", "usage_ledger.credits", "budget_reservations.credits", "usage_rollups.credits"} {
		if actual[name] != "" {
			t.Errorf("obsolete credit column %s", name)
		}
	}
}
