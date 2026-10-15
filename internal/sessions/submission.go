package sessions

import (
	"context"
	"encoding/json/v2"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
	"github.com/mindreon/orbit-control/internal/usage"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

type Service struct {
	Objects          objstore.ObjectStore
	Locks            locks.Resolver
	DefaultRunBudget int64
}

func logWrite(ctx context.Context, action, kind, id string, detail any) error {
	return audit.Append(ctx, db.Tx(ctx), auth.From(ctx), auth.Tenant(ctx), action, kind, id, detail)
}
func (svc Service) PrepareSubmission(
	ctx context.Context, s Session, runID, messageID, text string, requestedBudget *int64,
) (contract.RunLock, int64, error) {
	tenant := auth.Tenant(ctx)
	tx := db.Tx(ctx)
	if _, err := tx.Exec(ctx, `SELECT id FROM sessions WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, tenant, s.ID); err != nil {
		return contract.RunLock{}, 0, err
	}
	var raw []byte
	var budget int64
	err := tx.QueryRow(ctx, `SELECT l.lock,r.budget_millicredits
 FROM runs r
 JOIN run_locks l ON l.id=r.lock_id
 WHERE r.tenant_id=$1
 AND r.id=$2`, tenant, runID).Scan(&raw, &budget)
	if err == nil {
		var lock contract.RunLock
		err = json.Unmarshal(raw, &lock)
		return lock, budget, err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return contract.RunLock{}, 0, err
	}
	lock, pin, err := svc.submissionLock(ctx, s, ids.Derive(runID, "lock"))
	if err != nil {
		return lock, 0, err
	}
	budget, err = svc.submissionBudget(ctx, lock, requestedBudget)
	if err != nil {
		return lock, 0, err
	}
	if err = usage.Admit(ctx, tenant, s.WorkspaceID, auth.From(ctx).ID, budget); err != nil {
		return lock, 0, err
	}
	if err = saveSubmissionRun(ctx, s, runID, messageID, lock, budget, pin); err != nil {
		return lock, 0, err
	}
	if err = svc.saveSubmissionMessage(ctx, s.ID, runID, messageID, text); err != nil {
		return lock, 0, err
	}
	err = logWrite(ctx, "run.created", "run", runID, map[string]any{"budget_millicredits": budget})
	return lock, budget, err
}
func (svc Service) submissionLock(ctx context.Context, s Session, lockID string) (contract.RunLock, bool, error) {
	var pinned *string
	var pin bool
	tx, tenant := db.Tx(ctx), auth.Tenant(ctx)
	err := tx.QueryRow(ctx, `SELECT pinned_lock_id::text,pinned FROM sessions WHERE tenant_id=$1 AND id=$2`, tenant, s.ID).
		Scan(&pinned, &pin)
	var lock contract.RunLock
	if err != nil {
		return lock, false, err
	}
	if pinned != nil {
		var raw []byte
		if err = tx.QueryRow(ctx, `SELECT lock FROM run_locks WHERE tenant_id=$1 AND id=$2`, tenant, *pinned).Scan(&raw); err != nil {
			return lock, false, err
		}
		err = json.Unmarshal(raw, &lock)
		lock.LockId = lockID
		return lock, false, err
	}
	lock, err = svc.Locks.Resolve(ctx, locks.Request{TenantID: tenant, LockID: lockID,
		Entry: locks.Entry{Kind: "agent", Ref: s.EntryRef}, Scope: locks.Scope{WorkspaceID: s.WorkspaceID}})
	if err != nil {
		return lock, false, httpx.E(422, "LOCK_RESOLUTION_FAILED", "Agent dependencies could not be resolved")
	}
	return lock, pin, nil
}
func (svc Service) submissionBudget(ctx context.Context, lock contract.RunLock, requested *int64) (int64, error) {
	budget := svc.DefaultRunBudget
	if budget == 0 {
		budget = 200000
	}
	// Defaults come from immutable locked agent bytes.
	for _, file := range lock.Entries[lock.Members[0].AgentEntry].Files {
		if file.Path != "agent.json" {
			continue
		}
		raw, err := svc.Objects.Get(ctx, auth.Tenant(ctx), file.Sha256, 256<<10)
		if err != nil {
			return 0, err
		}
		var manifest contract.AgentManifest
		if err = json.Unmarshal(raw, &manifest); err != nil {
			return 0, err
		}
		if manifest.Limits != nil && manifest.Limits.DefaultRunBudgetMillicredits != nil {
			budget = *manifest.Limits.DefaultRunBudgetMillicredits
		}
	}
	if requested != nil {
		budget = *requested
	}
	if budget < 1 {
		return 0, httpx.E(422, "INVALID_INPUT", "Run budget must be positive")
	}
	return budget, nil
}
func saveSubmissionRun(ctx context.Context, s Session, runID, messageID string, lock contract.RunLock, budget int64, pin bool) error {
	tx, tenant := db.Tx(ctx), auth.Tenant(ctx)
	_, err := tx.Exec(ctx, `INSERT INTO runs(id,tenant_id,session_id,trigger,status,created_by,message_id,
 budget_millicredits,temporal_workflow_id)
 VALUES($1,$2,$3,'user_message','queued',$4,$5,$6,$7)`,
		runID, tenant, s.ID, auth.From(ctx).ID, messageID, budget, w.AgentRunWorkflowID(tenant, runID))
	if err != nil {
		return err
	}
	if err = locks.Save(ctx, tx, runID, lock); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE runs SET lock_id=$3 WHERE tenant_id=$1 AND id=$2`, tenant, runID, lock.LockId); err != nil {
		return err
	}
	if pin {
		_, err = tx.Exec(ctx, `UPDATE sessions SET pinned_lock_id=$3 WHERE tenant_id=$1 AND id=$2`, tenant, s.ID, lock.LockId)
	}
	return err
}
func (svc Service) saveSubmissionMessage(ctx context.Context, session, run, message, text string) error {
	tenant := auth.Tenant(ctx)
	content, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return err
	}
	ref := "t/" + tenant + "/runs/" + run + "/input/" + message + ".json"
	if err = svc.Objects.PutObject(ctx, tenant, ref, content, "application/json"); err != nil {
		return err
	}
	_, err = db.Tx(ctx).Exec(ctx, `INSERT INTO messages(id,tenant_id,session_id,run_id,role,author_id,content,content_ref)
 VALUES($1,$2,$3,$4,'user',$5,$6,$7)`, message, tenant, session, run, auth.From(ctx).ID, content, ref)
	return err
}
