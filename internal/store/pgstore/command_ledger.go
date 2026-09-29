package pgstore

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// The API command idempotency record (10 §1): rows of idempotency_ledger with scope api_command. The database clock
// decides lease expiry, so replicas with skewed clocks agree.

var _ taskruntime.ProjectionStore = (*Store)(nil)

const scopeAPICommand = "api_command"

type ledgerRow struct {
	Scope       string `gorm:"primaryKey"`
	Key         string `gorm:"primaryKey"`
	TenantID    string
	RequestHash string
	Status      string
	ResultRef   []byte `gorm:"type:jsonb"`
	Owner       *string
}

func (ledgerRow) TableName() string { return "idempotency_ledger" }

// ledgerUpdatable are the only columns of idempotency_ledger that orbit_app may UPDATE.
var ledgerUpdatable = []string{"status", "result_ref", "owner", "last_seen"}

func ledgerRowScope(tx *gorm.DB, tenantID, key string) *gorm.DB {
	return tx.Model(&ledgerRow{}).Where("scope = ? AND key = ? AND tenant_id = ?", scopeAPICommand, key, tenantID)
}

func (s *Store) ClaimCommand(ctx context.Context, tenantID, key, hash, owner string, lease time.Duration) (json.RawMessage, bool, error) {
	var result json.RawMessage
	claimed := false
	err := s.inTenant(ctx, tenantID, func(tx *gorm.DB) error {
		var err error
		result, claimed, err = claimLedgerRow(tx, tenantID, key, hash, owner, lease)
		return err
	})
	if err != nil {
		return nil, false, projectionErr("claim command", err)
	}
	return result, claimed, nil
}

func claimLedgerRow(tx *gorm.DB, tenantID, key, hash, owner string, lease time.Duration) (json.RawMessage, bool, error) {
	insert := ledgerRow{Scope: scopeAPICommand, Key: key, TenantID: tenantID, RequestHash: hash, Status: "started", Owner: &owner}
	created := tx.Clauses(clause.OnConflict{DoNothing: true}).
		Select("Scope", "Key", "TenantID", "RequestHash", "Status", "Owner").Create(&insert)
	if created.Error != nil {
		return nil, false, created.Error
	}
	if created.RowsAffected == 1 {
		return nil, true, nil
	}
	var row ledgerRow
	if err := ledgerRowScope(tx, tenantID, key).Take(&row).Error; err != nil {
		return nil, false, err
	}
	switch {
	case row.RequestHash != hash:
		return nil, false, taskruntime.ErrIdempotencyConflict
	case row.Status == "succeeded":
		return json.RawMessage(row.ResultRef), false, nil
	case row.Status != "started":
		return nil, false, errors.New("api command ledger row in unexpected status " + row.Status)
	}
	// Take over a released claim or one whose owner stopped renewing it. The condition is part of the UPDATE, so of
	// two replicas racing for the same row exactly one sees it match.
	taken := ledgerRowScope(tx, tenantID, key).
		Where("status = 'started' AND (owner IS NULL OR last_seen < now() - make_interval(secs => ?))", lease.Seconds()).
		Select(ledgerUpdatable).
		Updates(map[string]any{"owner": owner, "last_seen": gorm.Expr("now()")})
	if taken.Error != nil {
		return nil, false, taken.Error
	}
	if taken.RowsAffected == 0 {
		return nil, false, taskruntime.ErrCommandInProgress
	}
	return nil, true, nil
}

func (s *Store) CompleteCommand(ctx context.Context, tenantID, key, owner string, result json.RawMessage) error {
	err := s.inTenant(ctx, tenantID, func(tx *gorm.DB) error {
		// No row matches when the claim was taken over after the lease; the new owner completes it instead.
		return ledgerRowScope(tx, tenantID, key).Where("status = 'started' AND owner = ?", owner).
			Select(ledgerUpdatable).
			Updates(map[string]any{"status": "succeeded", "result_ref": []byte(result), "last_seen": gorm.Expr("now()")}).Error
	})
	return projectionErr("complete command", err)
}

func (s *Store) ReleaseCommand(ctx context.Context, tenantID, key, owner string) error {
	err := s.inTenant(ctx, tenantID, func(tx *gorm.DB) error {
		return ledgerRowScope(tx, tenantID, key).Where("status = 'started' AND owner = ?", owner).
			Select(ledgerUpdatable).
			Updates(map[string]any{"owner": nil}).Error
	})
	return projectionErr("release command", err)
}
