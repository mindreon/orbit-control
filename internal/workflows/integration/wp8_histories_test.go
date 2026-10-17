//go:build temporal_integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
	"github.com/mindreon/orbit-control/internal/workflows/membertask"
	"github.com/mindreon/orbit-control/internal/workflows/scheduledrun"
	"github.com/mindreon/orbit-control/internal/workflows/teamrun"
)

func teamHistoryInput(name string) w.TeamRunInput {
	base := runInput(name)
	return w.TeamRunInput{V: 1, TenantID: base.TenantID, WorkspaceID: base.WorkspaceID, SessionID: base.SessionID,
		RunID: name, Lock: base.Lock, InputRef: base.InputRef, EventSubject: "run.tenant.session." + name,
		BudgetMillicredits: base.BudgetMillicredits, Limits: base.Limits, Sandbox: base.Sandbox, Tier: "standard",
		Team: w.TeamSpec{Leader: "leader", MaxParallel: 2, ReviewMaxRounds: 1, Roster: []contract.RosterEntry{
			{Name: "leader", Role: "leader", Status: "idle", Description: "lead"},
			{Name: "alice", Role: "teammate", Status: "idle", Description: "work"},
			{Name: "reviewer", Role: "reviewer", Status: "idle", Description: "review"},
		}}}
}
func registerWP8(reg worker.Worker, c client.Client) {
	reg.RegisterWorkflowWithOptions(teamrun.Run, workflow.RegisterOptions{Name: w.TeamRunWorkflow})
	reg.RegisterWorkflowWithOptions(membertask.Run, workflow.RegisterOptions{Name: w.MemberTaskWorkflow})
	reg.RegisterWorkflowWithOptions(scheduledrun.Run, workflow.RegisterOptions{Name: w.ScheduledRunWorkflow})
	reg.RegisterActivityWithOptions(func(context.Context, w.PrepareTeamInput) error { return nil },
		activity.RegisterOptions{Name: "PrepareTeamRun"})
	reg.RegisterActivityWithOptions(func(context.Context, w.RecordBoardInput) error { return nil },
		activity.RegisterOptions{Name: "RecordBoard"})
	reg.RegisterActivityWithOptions(func(context.Context, w.FinalizeTeamInput) error { return nil },
		activity.RegisterOptions{Name: "FinalizeTeamRun"})
	reg.RegisterActivityWithOptions(func(context.Context, w.NotifyInput) error { return nil },
		activity.RegisterOptions{Name: "CancelInteraction"})
	reg.RegisterActivityWithOptions(func(ctx context.Context, in w.TurnOp) (w.TurnResult, error) {
		var result w.TurnResult
		err := c.GetWorkflow(ctx, w.MemberTaskWorkflowID(in.TenantID, in.RunID, in.Member, in.Turn), "").Get(ctx, &result)
		return result, err
	}, activity.RegisterOptions{Name: "WaitForTurn"})
	reg.RegisterActivityWithOptions(func(_ context.Context, in w.ScheduledSessionInput) (w.ScheduledStart, error) {
		team := teamHistoryInput("scheduled-" + in.ScheduleID)
		team.Unattended = true
		return w.ScheduledStart{Kind: "team", Team: &team}, nil
	}, activity.RegisterOptions{Name: "CreateScheduledSession"})
	reg.RegisterActivityWithOptions(func(context.Context, w.RecordScheduleInput) error { return nil },
		activity.RegisterOptions{Name: "RecordScheduleRun"})
}
func historyUpdate(ctx context.Context, c client.Client, run, name, key string, input any) error {
	handle, err := c.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{WorkflowID: w.TeamRunWorkflowID("tenant", run),
		UpdateName: name, UpdateID: key, WaitForStage: client.WorkflowUpdateStageCompleted, Args: []any{input}})
	if err != nil {
		return err
	}
	return handle.Get(ctx, nil)
}
func wp8Segment(c client.Client) func(context.Context, contract.SegmentInput) (contract.SegmentResult, error) {
	return func(ctx context.Context, in contract.SegmentInput) (contract.SegmentResult, error) {
		result := contract.SegmentResult{V: 1, Status: "completed", CheckpointVersion: in.ExpectedCheckpointVersion + 1,
			Progress: contract.SegmentProgress{NewOutput: true}, Final: &contract.FinalMessage{MessageId: "summary",
				TextPreview: "Recorded team summary"}}
		if in.RunId == "agentrun-unattended" {
			result.Status = "interrupted"
			result.Interaction = &contract.InteractionRequest{InteractionId: "unattended", Kind: "effect_uncertain"}
			return result, nil
		}
		if in.RunId == "teamrun-cancel" {
			activity.RecordHeartbeat(ctx)
			ticker := time.NewTicker(50 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return result, ctx.Err()
				case <-ticker.C:
					activity.RecordHeartbeat(ctx)
				}
			}
		}
		if in.RunId == "membertask-deliver-endturn" {
			timer := time.NewTimer(200 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return result, ctx.Err()
			}
			return result, nil
		}
		if in.Team == nil {
			return result, nil
		}
		caller := new(contract.MemberName(in.Member))
		key := fmt.Sprintf("%s-%d", in.Member, in.Seq)
		switch in.Team.Role {
		case "leader":
			if in.Seq == 1 {
				err := historyUpdate(ctx, c, in.RunId, "CreateTasks", key, contract.CreateTasksInput{Caller: caller, RequestId: key,
					Tasks: []contract.NewTask{{Title: "Recorded task", Description: "Write and review", Assignee: new(contract.MemberName("alice"))}}})
				return result, err
			}
			return result, historyUpdate(ctx, c, in.RunId, "FinishTeam", key,
				contract.FinishTeamInput{Caller: caller, RequestId: key, Summary: "Recorded team summary"})
		case "teammate":
			if strings.Contains(in.RunId, "can-midflight") {
				timer := time.NewTimer(250 * time.Millisecond)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-ctx.Done():
					return result, ctx.Err()
				}
			}
			return result, historyUpdate(ctx, c, in.RunId, "CompleteTask", key,
				contract.CompleteTaskInput{Caller: caller, RequestId: key, TaskId: in.Team.Task.TaskId, ResultText: "ready"})
		case "reviewer":
			verdict := contract.Verdict("approve")
			if strings.Contains(in.RunId, "reject-escalate") {
				verdict = "reject"
			}
			return result, historyUpdate(ctx, c, in.RunId, "SubmitVerdict", key,
				contract.SubmitVerdictInput{Caller: caller, RequestId: key, TaskId: in.Team.Task.TaskId, Verdict: verdict, Comment: "recorded"})
		}
		return result, nil
	}
}
func TestRecordWP8Histories(t *testing.T) {
	c := devClient(t)
	f := &fixtures{calls: map[string]int{}}
	queue := "wp8-record-" + time.Now().Format("150405.000000")
	ctl := worker.New(c, contract.TaskQueueCtl, worker.Options{})
	registerFixtures(ctl, f)
	registerWP8(ctl, c)
	wait := worker.New(c, w.TaskQueueCtlWait, worker.Options{})
	registerWP8(wait, c)
	agent := worker.New(c, contract.TaskQueueAgentStandard, worker.Options{})
	agent.RegisterActivityWithOptions(wp8Segment(c), activity.RegisterOptions{Name: contract.ActivityRunAgentSegment})
	root := worker.New(c, queue, worker.Options{})
	registerFixtures(root, f)
	registerWP8(root, c)
	for _, reg := range []worker.Worker{ctl, wait, agent, root} {
		require.NoError(t, reg.Start())
		t.Cleanup(reg.Stop)
	}
	for _, name := range []string{"teamrun-happy", "teamrun-reject-escalate", "teamrun-can-midflight", "teamrun-cancel"} {
		t.Run(name, func(t *testing.T) {
			in := teamHistoryInput(name)
			run, err := c.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{ID: w.TeamRunWorkflowID("tenant", name), TaskQueue: queue},
				w.TeamRunWorkflow, in)
			require.NoError(t, err)
			first := run.GetRunID()
			if name == "teamrun-cancel" {
				time.AfterFunc(300*time.Millisecond, func() {
					_ = c.SignalWorkflow(context.Background(), run.GetID(), "", w.Cancel, w.CancelInput{Reason: "recorded"})
				})
			}
			// A cancelled in-flight segment is only observed on the next throttled heartbeat flush (0.8 of
			// the 1m heartbeat timeout), so the cancelled run closes about 48s after the signal.
			ctx, cancel := context.WithTimeout(t.Context(), 100*time.Second)
			defer cancel()
			require.NoError(t, run.Get(ctx, nil))
			// can-midflight keeps the first run, which ends in Continue-As-New while a member is active; the
			// others keep the final run of the chain, which carries the scenario through to FinalizeTeamRun.
			if name != "teamrun-can-midflight" {
				first = ""
			}
			saveWP8(t, c, run.GetID(), first, name)
		})
	}
	t.Run("membertask-deliver-endturn", func(t *testing.T) {
		name := "membertask-deliver-endturn"
		base := teamHistoryInput(name)
		in := w.MemberTaskInput{V: 1, TenantID: base.TenantID, WorkspaceID: base.WorkspaceID, SessionID: base.SessionID, RunID: name,
			Member: "alice", Role: "teammate", Turn: 1, Kind: "task", Lock: base.Lock, EventSubject: base.EventSubject,
			Input: contract.SegmentTurn{Kind: "follow_up"}, BudgetMillicredits: base.BudgetMillicredits, Limits: base.Limits,
			Sandbox: base.Sandbox, Lease: contract.SandboxLease{Provider: "local", SandboxId: "fixture"}, Tier: "standard"}
		run, err := c.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{ID: queue + name, TaskQueue: queue}, w.MemberTaskWorkflow, in)
		require.NoError(t, err)
		require.NoError(t, c.SignalWorkflow(t.Context(), run.GetID(), "", w.Deliver,
			w.DeliverInput{Mailbox: []contract.MailboxMessage{{MessageId: "m1", From: "leader", Text: new("hello"), SentAt: time.Now()}}}))
		require.NoError(t, c.SignalWorkflow(t.Context(), run.GetID(), "", w.EndTurn, struct{}{}))
		require.NoError(t, run.Get(t.Context(), nil))
		saveWP8(t, c, run.GetID(), run.GetRunID(), name)
	})
	t.Run("scheduledrun-team", func(t *testing.T) {
		id := w.ScheduledRunWorkflowIDBase("tenant", queue) + "-" + time.Now().UTC().Truncate(time.Second).Format(time.RFC3339)
		run, err := c.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{ID: id, TaskQueue: queue}, w.ScheduledRunWorkflow,
			w.ScheduledRunInput{TenantID: "tenant", ScheduleID: queue})
		require.NoError(t, err)
		require.NoError(t, run.Get(t.Context(), nil))
		saveWP8(t, c, run.GetID(), run.GetRunID(), "scheduledrun-team")
	})
	t.Run("agentrun-unattended", func(t *testing.T) {
		in := runInput("agentrun-unattended")
		in.Unattended = true
		run, err := c.ExecuteWorkflow(t.Context(), client.StartWorkflowOptions{ID: queue + "unattended", TaskQueue: queue}, w.AgentRunWorkflow,
			in)
		require.NoError(t, err)
		require.NoError(t, run.Get(t.Context(), nil))
		saveWP8(t, c, run.GetID(), run.GetRunID(), "agentrun-unattended")
	})
}

func saveWP8(t *testing.T, c client.Client, id, runID, name string) { save(t, c, id, runID, name) }
