package sessions

import (
	"context"
	"encoding/json/v2"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

// BuildStart persists the complete admission before any Temporal delivery.
// RunID and MessageID are caller-chosen stable identities, shared by API replay
// and scheduled firing replay. An existing run reads its immutable saved lock.
func (svc Service) BuildStart(ctx context.Context, tx pgx.Tx, session Session, principal auth.Principal,
	runID, messageID, prompt string, budget *int64) (w.SubmitMessageInput, error) {
	ctx = auth.Context(db.Context(ctx, tx), principal, auth.Tenant(ctx))
	lock, storedBudget, err := svc.PrepareSubmission(ctx, session, runID, messageID, prompt, budget)
	if err != nil {
		return w.SubmitMessageInput{}, err
	}
	return svc.StartInput(ctx, session, runID, messageID, principal.ID, lock, storedBudget)
}

func (svc Service) StartInput(ctx context.Context, session Session, runID, messageID, principal string,
	lock contract.RunLock, budget int64) (w.SubmitMessageInput, error) {
	permissions := lock.EffectivePermissions
	msg := w.SubmitMessageInput{V: 1, RunID: runID, MessageID: messageID, PrincipalID: principal, Trigger: "user_message",
		Lock:               contract.SegmentLockRef{Id: lock.LockId, Digest: lock.Digest},
		InputRef:           "t/" + auth.Tenant(ctx) + "/runs/" + runID + "/input/" + messageID + ".json",
		BudgetMillicredits: budget, Limits: w.DefaultLimits(budget),
		Sandbox: w.SandboxSpec{IsolationKey: w.IsolationKey(auth.Tenant(ctx), session.ID), Profile: permissions.SandboxProfile,
			Egress: permissions.Network.Egress, TTLSeconds: 1800}}
	if lock.Entry.Kind != "team" {
		return msg, nil
	}
	spec, err := svc.teamSpec(ctx, lock)
	if err != nil {
		return msg, err
	}
	msg.EntryKind, msg.Team = "team", &spec
	return msg, nil
}
func (svc Service) teamSpec(ctx context.Context, lock contract.RunLock) (w.TeamSpec, error) {
	spec := w.TeamSpec{MaxParallel: 2, ReviewMaxRounds: 3}
	for _, member := range lock.Members {
		spec.Roster = append(spec.Roster,
			contract.RosterEntry{Name: member.Name, Role: contract.TeamRole(member.Role), Status: "idle", Description: member.Description})
		if member.Role == "leader" {
			spec.Leader = string(member.Name)
		}
	}
	for _, entry := range lock.Entries {
		if entry.Kind != "team" || entry.Ref != lock.Entry.Ref || entry.Version != lock.Entry.Version {
			continue
		}
		for _, file := range entry.Files {
			if file.Path != "team.json" {
				continue
			}
			raw, err := svc.Objects.Get(ctx, auth.Tenant(ctx), file.Sha256, 256<<10)
			if err != nil {
				return spec, err
			}
			var manifest contract.TeamManifest
			if err = contract.Validate("TeamManifest", raw); err != nil {
				return spec, httpx.E(422, "LOCK_RESOLUTION_FAILED", "Invalid team manifest")
			}
			if err = json.Unmarshal(raw, &manifest); err != nil {
				return spec, httpx.E(422, "LOCK_RESOLUTION_FAILED", "Invalid team manifest")
			}
			if manifest.MaxParallelMembers != nil {
				spec.MaxParallel = *manifest.MaxParallelMembers
			}
			if manifest.Review != nil {
				spec.ReviewMaxRounds = manifest.Review.MaxRounds
			}
			if spec.Leader == "" || len(spec.Roster) < 2 {
				return spec, httpx.E(422, "LOCK_RESOLUTION_FAILED", "Invalid team roster")
			}
			spec.MaxParallel = max(1, min(spec.MaxParallel, len(spec.Roster)-1))
			return spec, nil
		}
	}
	return spec, httpx.E(422, "LOCK_RESOLUTION_FAILED", "Team manifest missing from run lock")
}

// ScheduledIDs derives the single session/run pair belonging to a firing.
func ScheduledIDs(schedule, fireAt string) (string, string, string) {
	session := ids.Derive(schedule, "session:"+fireAt)
	run := ids.Derive(session, "run")
	return session, run, ids.Derive(run, "input")
}
