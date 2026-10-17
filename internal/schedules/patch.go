package schedules

import (
	"context"
	"slices"
	"time"

	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/policy"
	"github.com/mindreon/orbit-control/internal/sessions"
)

func (s Service) Patch(ctx context.Context, id string, in PatchInput) (Schedule, error) {
	out, err := s.Manage(ctx, id)
	if err != nil {
		return out, err
	}
	if out.Status == "deleted" {
		return out, httpx.E(409, "SCHEDULE_NOT_FOUND", "Schedule is deleted")
	}
	next := CreateInput{WorkspaceID: out.WorkspaceID, Name: out.Name, Description: out.Description, Cron: out.Cron, Timezone: out.Timezone,
		EntryKind: out.EntryKind, EntryRef: out.EntryRef, Prompt: out.Prompt, BudgetMillicredits: out.BudgetMillicredits,
		OverlapPolicy: out.OverlapPolicy, CatchupWindowSeconds: out.CatchupWindowSeconds}
	fields := []string{}
	if in.Name != nil {
		next.Name = *in.Name
	}
	if in.Description != nil {
		next.Description = in.Description
	}
	if in.Cron != nil && *in.Cron != out.Cron {
		next.Cron = *in.Cron
		fields = append(fields, "cron")
	}
	if in.Timezone != nil && *in.Timezone != out.Timezone {
		next.Timezone = *in.Timezone
		fields = append(fields, "timezone")
	}
	if in.Prompt != nil && *in.Prompt != out.Prompt {
		next.Prompt = *in.Prompt
		fields = append(fields, "prompt")
	}
	if in.BudgetMillicredits != nil && *in.BudgetMillicredits != out.BudgetMillicredits {
		next.BudgetMillicredits = *in.BudgetMillicredits
		fields = append(fields, "budget_millicredits")
	}
	if in.OverlapPolicy != nil && *in.OverlapPolicy != out.OverlapPolicy {
		next.OverlapPolicy = *in.OverlapPolicy
		fields = append(fields, "overlap_policy")
	}
	if in.CatchupWindowSeconds != nil && *in.CatchupWindowSeconds != out.CatchupWindowSeconds {
		next.CatchupWindowSeconds = *in.CatchupWindowSeconds
		fields = append(fields, "catchup_window_seconds")
	}
	if in.Status != nil && !slices.Contains([]string{"active", "paused"}, *in.Status) {
		return out, httpx.E(422, "INVALID_INPUT", "Invalid schedule status")
	}
	if err = validate(next, time.Now()); err != nil {
		return out, err
	}
	owner := out.OwnerID
	transfer := owner != auth.From(ctx).ID && len(fields) > 0
	if transfer {
		if err = (policy.Authorizer{}).Require(ctx, "session.create", resource(out.WorkspaceID)); err != nil {
			return out, err
		}
		if _, err = sessions.ResolveEntry(ctx, s.Locks, out.WorkspaceID, out.EntryRef, out.EntryKind); err != nil {
			return out, err
		}
		if err = s.quota(ctx, out.WorkspaceID, auth.From(ctx).ID, true); err != nil {
			return out, err
		}
		owner = auth.From(ctx).ID
	}
	_, err = db.Tx(ctx).Exec(ctx, `UPDATE schedules SET name=$3,description=$4,cron=$5,timezone=$6,prompt=$7,
 budget_millicredits=$8,overlap_policy=$9,catchup_window=$10*interval '1 second',status=COALESCE($11,status),owner_id=$12,
 paused_reason=CASE WHEN $13 THEN NULL ELSE paused_reason END,version=version+1,updated_at=now()
 WHERE tenant_id=$1 AND id=$2`, auth.Tenant(ctx), id, next.Name, next.Description, next.Cron, next.Timezone, next.Prompt,
		next.BudgetMillicredits, next.OverlapPolicy, next.CatchupWindowSeconds, in.Status, owner, transfer)
	if err != nil {
		return out, err
	}
	if transfer {
		err = audit.Append(ctx, db.Tx(ctx), auth.From(ctx), auth.Tenant(ctx), "schedule.owner_transferred", "schedule", id,
			map[string]any{"from": out.OwnerID, "to": owner, "fields": fields})
		if err != nil {
			return out, err
		}
	}
	if err = audit.Append(ctx, db.Tx(ctx), auth.From(ctx), auth.Tenant(ctx), "schedule.updated", "schedule", id, map[string]any{}); err != nil {
		return out, err
	}
	return get(ctx, id)
}
