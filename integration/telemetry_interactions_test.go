package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func testTelemetryInteractions(t *testing.T, d *db.DB, admin *pgx.Conn, reader *sdkmetric.ManualReader) {
	for _, outcome := range []string{"resolved", "expired", "cancelled"} {
		f := newRunFixture(t, d, admin)
		f.start(t)
		req := contract.InteractionRequest{Kind: "ask_user", InteractionId: ids.New(), Question: &contract.Question{Text: "Continue?"}}
		op := f.op()
		_, err := f.createInteraction(t, w.InteractionInput{RunOp: op, Request: req, RemindAt: op.EventAt,
			EscalateAt: op.EventAt.Add(time.Hour), ExpiresAt: op.EventAt.Add(2 * time.Hour)})
		require.NoError(t, err)
		before := metricTotal(t, reader, "orbit.interaction.wait.duration")
		for range 2 {
			if outcome == "cancelled" {
				require.NoError(t, f.activities.CancelInteraction(t.Context(), w.NotifyInput{RunOp: f.op(), InteractionID: req.InteractionId}))
			} else {
				require.NoError(t, f.activities.ApplyDecision(t.Context(), w.DecisionInput{RunOp: f.op(), InteractionID: req.InteractionId,
					Kind: req.Kind, Actor: f.actor, Expired: outcome == "expired", Decision: contract.Decision{Kind: "ask_user", Answer: new("yes")}}))
			}
		}
		require.Equal(t, before+1, metricTotal(t, reader, "orbit.interaction.wait.duration"), outcome)
	}
}

func testTelemetryRollbacks(t *testing.T, d *db.DB, admin *pgx.Conn, reader *sdkmetric.ManualReader) {
	for _, mode := range []string{"team-prepare", "start", "agent-finish", "team-finish", "cancel", "decision"} {
		t.Run(mode, func(t *testing.T) {
			f := newRunFixture(t, d, admin)
			if mode != "team-prepare" {
				f.start(t)
			}
			id := ids.New()
			_, err := admin.Exec(t.Context(), `INSERT INTO interactions(id,tenant_id,run_id,session_id,member,workflow_id,
 kind,request,approvers,status,seq) VALUES($1,$2,$3,$4,'main','fixture','ask_user',$5,$6,'pending',1)`,
				id, f.tenant, f.run, f.session, []byte(`{"kind":"ask_user","interaction_id":"`+id+`","question":{"text":"Continue?"}}`),
				[]byte(`["`+f.actor+`"]`))
			require.NoError(t, err)
			_, err = admin.Exec(t.Context(), `CREATE FUNCTION telemetry_abort() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN RAISE EXCEPTION 'telemetry rollback fixture'; END $$;
 CREATE TRIGGER telemetry_abort BEFORE UPDATE ON runs FOR EACH ROW EXECUTE FUNCTION telemetry_abort()`)
			require.NoError(t, err)
			defer func() {
				_, err := admin.Exec(context.Background(), "DROP TRIGGER telemetry_abort ON runs; DROP FUNCTION telemetry_abort()")
				require.NoError(t, err)
			}()
			names := []string{"orbit.runs.started", "orbit.runs.finished", "orbit.run.duration", "orbit.run.start.latency",
				"orbit.interaction.wait.duration"}
			before := map[string]float64{}
			for _, name := range names {
				before[name] = metricTotal(t, reader, name)
			}
			switch mode {
			case "team-prepare":
				err = f.activities.PrepareTeamRun(t.Context(), w.PrepareTeamInput{RunOp: f.op(), Budget: 100})
			case "start":
				op := f.op()
				op.Seq = 2
				err = f.activities.StartSegment(t.Context(), w.StartSegmentInput{RunOp: op})
			case "agent-finish":
				err = f.activities.FinalizeRun(t.Context(), w.FinalizeInput{RunOp: f.op(), Status: "succeeded"})
			case "team-finish":
				err = f.activities.FinalizeTeamRun(t.Context(), w.FinalizeTeamInput{RunOp: f.op(), Status: "succeeded"})
			case "cancel":
				// CancelInteraction has no later runs update, so abort its own
				// transition. The transaction must not record a wait sample.
				_, err = admin.Exec(t.Context(), `CREATE TRIGGER telemetry_abort_interaction BEFORE UPDATE ON interactions
 FOR EACH ROW EXECUTE FUNCTION telemetry_abort()`)
				require.NoError(t, err)
				defer func() {
					_, err := admin.Exec(context.Background(), "DROP TRIGGER telemetry_abort_interaction ON interactions")
					require.NoError(t, err)
				}()
				err = f.activities.CancelInteraction(t.Context(), w.NotifyInput{RunOp: f.op(), InteractionID: id})
			case "decision":
				err = f.activities.ApplyDecision(t.Context(), w.DecisionInput{RunOp: f.op(), InteractionID: id, Kind: "ask_user",
					Expired: true, Decision: contract.Decision{Kind: "ask_user", Answer: new("yes")}})
			}
			require.Error(t, err)
			for _, name := range names {
				require.Equal(t, before[name], metricTotal(t, reader, name), name)
			}
		})
	}
}
