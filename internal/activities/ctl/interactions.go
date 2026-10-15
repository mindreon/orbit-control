package ctl

import (
	"context"
	"encoding/json/v2"
	"slices"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/interactions"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func (a *Activities) CreateInteraction(ctx context.Context, in w.InteractionInput) (w.InteractionOutput, error) {
	out := w.InteractionOutput{CreatedAt: in.EventAt}
	if err := validate("InteractionRequest", in.Request); err != nil {
		return out, err
	}
	err := a.change(ctx, in.RunOp, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
		var owner string
		if err := tx.QueryRow(
			ctx,
			`SELECT owner_id::text FROM sessions WHERE tenant_id=$1 AND id=$2`,
			in.TenantID,
			in.SessionID,
		).Scan(
			&owner,
		); err != nil {
			return nil, err
		}
		out.Approvers = []string{owner}
		approvers, err := json.Marshal(out.Approvers)
		if err != nil {
			return nil, err
		}
		req, err := json.Marshal(in.Request)
		if err != nil {
			return nil, err
		}
		n, err := tx.Exec(
			ctx,
			`INSERT INTO interactions(id,tenant_id,run_id,session_id,member,seq,kind,request,approvers,status,remind_at,due_at,
 expires_at,created_at)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending',$10,$11,$12,$13)
 ON CONFLICT DO NOTHING`,
			in.Request.InteractionId,
			in.TenantID,
			in.RunID,
			in.SessionID,
			in.Member,
			in.Seq,
			in.Request.Kind,
			req,
			approvers,
			in.RemindAt,
			in.EscalateAt,
			in.ExpiresAt,
			in.EventAt,
		)
		if err != nil {
			return nil, err
		}
		if n.RowsAffected() == 0 {
			return nil, nil
		}
		status := "waiting_approval"
		if in.Request.Kind == "ask_user" {
			status = "waiting_input"
		}
		if _, err = tx.Exec(
			ctx,
			`UPDATE runs SET status=$3 WHERE tenant_id=$1 AND id=$2 AND ended_at IS NULL`,
			in.TenantID,
			in.RunID,
			status,
		); err != nil {
			return nil, err
		}
		return []event{
			{
				typ:  "interaction.requested",
				data: contract.InteractionRequested{Interaction: in.Request, ExpiresAt: &in.ExpiresAt},
			},
		}, nil
	})
	return out, err
}
func (a *Activities) CancelInteraction(ctx context.Context, in w.NotifyInput) error {
	return a.change(ctx, in.RunOp, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
		_, err := tx.Exec(
			ctx,
			`UPDATE interactions
 SET status='cancelled',resolved_at=now()
 WHERE tenant_id=$1
 AND id=$2
 AND status IN ('pending','escalated')`,
			in.TenantID,
			in.InteractionID,
		)
		return nil, err
	})
}
func (a *Activities) NotifyInteraction(ctx context.Context, in w.NotifyInput) error {
	return a.change(ctx, in.RunOp, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
		i, err := interactions.Get(ctx, in.TenantID, in.InteractionID)
		if err != nil {
			return nil, err
		}
		if i.Status != "pending" && i.Status != "escalated" {
			return nil, nil
		}
		if in.Kind == "escalate" {
			var workspace string
			if err = tx.QueryRow(
				ctx,
				`SELECT workspace_id::text FROM sessions WHERE tenant_id=$1 AND id=$2`,
				in.TenantID,
				i.SessionID,
			).Scan(
				&workspace,
			); err != nil {
				return nil, err
			}
			admins, e := interactions.WorkspaceAdmins(ctx, workspace)
			if e != nil {
				return nil, e
			}
			i.Approvers = append(i.Approvers, admins...)
			slices.Sort(i.Approvers)
			i.Approvers = slices.Compact(i.Approvers)
			raw, e := json.Marshal(i.Approvers)
			if e != nil {
				return nil, e
			}
			if _, err = tx.Exec(
				ctx,
				`UPDATE interactions
 SET approvers=$3,status='escalated',escalated_at=COALESCE(escalated_at,now())
 WHERE tenant_id=$1
 AND id=$2`,
				in.TenantID,
				i.ID,
				raw,
			); err != nil {
				return nil, err
			}
		}
		return []event{
			{
				typ:  "interaction.requested",
				data: contract.InteractionRequested{Interaction: i.Request, ExpiresAt: &i.ExpiresAt},
			},
		}, nil
	})
}
func (a *Activities) ApplyDecision(ctx context.Context, in w.DecisionInput) error {
	if err := validate("Decision", in.Decision); err != nil {
		return err
	}
	return a.change(ctx, in.RunOp, func(ctx context.Context, tx pgx.Tx) ([]event, error) {
		i, err := interactions.Get(ctx, in.TenantID, in.InteractionID)
		if err != nil {
			return nil, err
		}
		if i.Status != "pending" && i.Status != "escalated" {
			return nil, nil
		}
		if i.Kind != in.Decision.Kind {
			return nil, temporalInvalid("Decision kind mismatch")
		}
		var workspace string
		if err = tx.QueryRow(
			ctx,
			`SELECT workspace_id::text FROM sessions WHERE tenant_id=$1 AND id=$2`,
			in.TenantID,
			in.SessionID,
		).Scan(
			&workspace,
		); err != nil {
			return nil, err
		}
		scope := ""
		if !in.Expired {
			if !slices.Contains(i.Approvers, in.Actor) {
				return nil, temporalInvalid("Actor is not an approver")
			}
			scope, err = interactions.Grant(ctx, in.TenantID, workspace, in.SessionID, in.Actor, i, in.Decision)
			if err != nil {
				return nil, err
			}
		}
		status := "resolved"
		var actor *string
		if in.Expired {
			status = "expired"
		} else {
			actor = &in.Actor
		}
		raw, err := json.Marshal(in.Decision)
		if err != nil {
			return nil, err
		}
		if _, err = tx.Exec(
			ctx,
			`UPDATE interactions
 SET status=$3,decision=$4,decided_by=$5,resolved_at=now()
 WHERE tenant_id=$1
 AND id=$2`,
			in.TenantID,
			i.ID,
			status,
			raw,
			actor,
		); err != nil {
			return nil, err
		}
		if err = auditDecision(ctx, tx, in, i.ID, scope); err != nil {
			return nil, err
		}
		if _, err = tx.Exec(
			ctx,
			`UPDATE runs SET status='running' WHERE tenant_id=$1 AND id=$2 AND ended_at IS NULL`,
			in.TenantID,
			in.RunID,
		); err != nil {
			return nil, err
		}
		return []event{
			{
				typ:  "interaction.resolved",
				data: contract.InteractionResolved{InteractionId: i.ID, Decision: in.Decision, Actor: in.Actor},
			},
		}, nil
	})
}
func temporalInvalid(message string) error {
	return temporal.NewNonRetryableApplicationError(message, contract.ErrorInvalidSpec, nil)
}

func auditDecision(ctx context.Context, tx pgx.Tx, in w.DecisionInput, interactionID, scope string) error {
	p, err := runActor(ctx, tx, in.RunOp)
	if err != nil {
		return err
	}
	if !in.Expired {
		p = auth.Principal{ID: in.Actor, Kind: "user"}
	}
	if err = audit.Append(
		ctx,
		tx,
		p,
		in.TenantID,
		"interaction.resolve",
		"interaction",
		interactionID,
		map[string]any{
			"expired":         in.Expired,
			"kind":            in.Kind,
			"grant_scope":     scope,
			"requested_scope": in.Decision.Scope,
		},
	); err != nil {
		return err
	}
	return nil
}
