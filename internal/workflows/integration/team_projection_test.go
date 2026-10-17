//go:build temporal_integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func (f *projectionFixture) teamLock(t *testing.T) contract.RunLock {
	t.Helper()
	examples := filepath.Join("..", "..", "..", "contract", "v2", "examples")
	raw, err := os.ReadFile(filepath.Join(examples, "RunLock.team.json"))
	require.NoError(t, err)
	var lock contract.RunLock
	require.NoError(t, json.Unmarshal(raw, &lock))
	manifest, err := os.ReadFile(filepath.Join(examples, "TeamManifest.research_squad.json"))
	require.NoError(t, err)
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(manifest))
	_, err = f.objects.Put(t.Context(), f.tenant, digest, manifest)
	require.NoError(t, err)
	// Only the team manifest is stored; member bundles are not read by submission.
	for i, entry := range lock.Entries {
		files := entry.Files[:0:0]
		if entry.Kind == "team" {
			for _, file := range entry.Files {
				if file.Path == "team.json" {
					file.Sha256, file.SizeBytes = digest, len(manifest)
					files = append(files, file)
				}
			}
		}
		entry.Files = files
		lock.Entries[i] = entry
	}
	return lock
}

// teamSegment scripts the runtime: the leader creates one task, the researcher completes it, the reviewer
// approves it and the leader finishes the team. Every instruction reaches the TeamRun as a real Update.
func (f *projectionFixture) teamSegment(ctx context.Context, in contract.SegmentInput) (contract.SegmentResult, error) {
	result := contract.SegmentResult{V: 1, Status: "completed", CheckpointVersion: in.ExpectedCheckpointVersion + 1,
		Progress: contract.SegmentProgress{NewOutput: true},
		Final:    &contract.FinalMessage{MessageId: "summary", TextPreview: "Team report ready"}}
	caller := new(contract.MemberName(in.Member))
	key := fmt.Sprintf("%s-%d", in.Member, in.Seq)
	update := func(name string, input any) error {
		handle, err := f.temporal.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: w.TeamRunWorkflowID(in.TenantId, in.RunId),
			UpdateName: name, UpdateID: key, WaitForStage: client.WorkflowUpdateStageCompleted, Args: []any{input}})
		if err != nil {
			return err
		}
		return handle.Get(ctx, nil)
	}
	switch in.Team.Role {
	case "leader":
		if in.Seq == 1 {
			return result, update("CreateTasks", contract.CreateTasksInput{Caller: caller, RequestId: key,
				Tasks: []contract.NewTask{{Title: "Research", Description: "Research the topic", Assignee: new(contract.MemberName("researcher"))}}})
		}
		return result, update("FinishTeam", contract.FinishTeamInput{Caller: caller, RequestId: key, Summary: "Team report ready"})
	case "teammate":
		return result, update("CompleteTask", contract.CompleteTaskInput{Caller: caller, RequestId: key, TaskId: in.Team.Task.TaskId, ResultText: "findings"})
	default:
		return result, update("SubmitVerdict", contract.SubmitVerdictInput{Caller: caller, RequestId: key, TaskId: in.Team.Task.TaskId,
			Verdict: "approve", Comment: "accepted"})
	}
}

func TestAPIToTeamRunProjection(t *testing.T) {
	f := newProjectionFixture(t)
	bus, err := natsx.Connect(testEnv(t, "ORBIT_TEST_NATS_URL"), "orbit-control-test")
	require.NoError(t, err)
	t.Cleanup(bus.Close)
	require.NoError(t, bus.EnsureRunEventsStream(t.Context(), 0))
	sub, err := bus.Conn.SubscribeSync("run." + f.tenant + ".>")
	require.NoError(t, err)
	defer sub.Unsubscribe()
	require.NoError(t, bus.Conn.Flush())
	lock := f.teamLock(t)
	f.lock = &lock
	f.agentSegment = f.teamSegment
	f.start(t, bus)
	f.configureAPI(t)
	var session struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(f.request(t, "POST", "/v1/sessions", map[string]any{
		"workspace_id": f.workspace, "entry_ref": "acme/research-squad", "entry_kind": "team", "title": "Team"}), &session))
	var submission struct {
		RunID string `json:"run_id"`
	}
	require.NoError(t, json.Unmarshal(f.request(t, "POST", "/v1/sessions/"+session.ID+"/messages", map[string]string{"text": "research it"}), &submission))
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	require.NoError(t, f.temporal.GetWorkflow(ctx, w.TeamRunWorkflowID(f.tenant, submission.RunID), "").Get(ctx, nil))
	require.Contains(t, string(f.request(t, "GET", "/v1/runs/"+submission.RunID, nil)), `"status":"succeeded"`)
	require.Contains(t, string(f.request(t, "GET", "/v1/sessions/"+session.ID+"/messages", nil)), "Team report ready")
	require.NoError(t, f.db.WithTenant(t.Context(), f.tenant, func(tx pgx.Tx) error {
		var status, entry string
		var turns int
		require.NoError(t, tx.QueryRow(t.Context(), `SELECT status FROM team_tasks WHERE run_id=$1`, submission.RunID).Scan(&status))
		require.Equal(t, "completed", status)
		require.NoError(t, tx.QueryRow(t.Context(), `SELECT count(*) FROM team_turns WHERE run_id=$1 AND ended_at IS NOT NULL`, submission.RunID).Scan(&turns))
		require.GreaterOrEqual(t, turns, 3)
		require.NoError(t, tx.QueryRow(t.Context(), `SELECT entry_kind FROM sessions WHERE id=$1`, session.ID).Scan(&entry))
		require.Equal(t, "team", entry)
		return nil
	}))
	var types []contract.EventType
	for {
		msg, err := sub.NextMsg(5 * time.Second)
		require.NoError(t, err, "run.completed must be published through real NATS")
		var event contract.RunEvent
		if json.Unmarshal(msg.Data, &event) != nil {
			continue
		}
		types = append(types, event.Type)
		if event.Type == "run.completed" {
			break
		}
	}
	require.Contains(t, types, contract.EventType("team.task.created"))
}
