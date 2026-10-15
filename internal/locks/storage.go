package locks

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
)

// Save joins the caller's transaction after INSERT runs; Resolve never persists.
func Save(ctx context.Context, tx pgx.Tx, runID string, lock contract.RunLock) error {
	raw, err := json.Marshal(lock)
	if err != nil {
		return err
	}
	if err = contract.ValidateStrict("RunLock", raw); err != nil {
		return err
	}
	digest, err := contract.RunLockDigest(raw)
	if err != nil || digest != lock.Digest {
		return assets.Invalid("Lock digest mismatch")
	}
	entries, err := json.Marshal(lock.Entries)
	if err != nil {
		return err
	}
	permissions, err := json.Marshal(lock.EffectivePermissions)
	if err != nil {
		return err
	}
	n, err := tx.Exec(ctx,
		`INSERT INTO run_locks(id,tenant_id,run_id,digest,entries,effective_permissions,lock) SELECT $1,$2,id,$4,$5,$6,$7
 FROM runs
 WHERE tenant_id=$2
 AND id=$3`, lock.LockId, auth.Tenant(ctx), runID, lock.Digest, entries, permissions, raw)
	if err != nil {
		return err
	}
	if n.RowsAffected() != 1 {
		return assets.Invalid("Run does not belong to the lock tenant")
	}
	return audit.Append(
		ctx,
		tx,
		auth.From(ctx),
		auth.Tenant(ctx),
		"run.lock_created",
		"run_lock",
		lock.LockId,
		map[string]string{"digest": lock.Digest},
	)
}

// Files reads every entry, including derived embedded bundles, by its immutable
// tenant-local blob digest. Entry refs are identities, not asset_files lookup keys.
// The caller must authorize access to the run/lock before calling Files.
func (r *BundleResolver) Files(ctx context.Context, lock contract.RunLock, entryIndex int) ([]bundle.File, error) {
	if entryIndex < 0 || entryIndex >= len(lock.Entries) {
		return nil, assets.Invalid("Invalid lock entry")
	}
	entry := lock.Entries[entryIndex]
	hashes := map[string]string{}
	for _, f := range entry.Files {
		hashes[f.Path] = f.Sha256
	}
	digest, err := contract.BundleDigest(hashes)
	if err != nil || digest != entry.Digest {
		return nil, assets.Invalid("Lock file-set digest mismatch")
	}
	result := make([]bundle.File, 0, len(entry.Files))
	for _, f := range entry.Files {
		if f.SizeBytes < 0 || f.SizeBytes > 20<<20 {
			return nil, assets.Invalid("Lock file exceeds size limit")
		}
		data, err := r.Assets.Store.Get(ctx, auth.Tenant(ctx), f.Sha256, 20<<20)
		if err != nil {
			return nil, err
		}
		if int64(len(data)) != int64(f.SizeBytes) || fmt.Sprintf("sha256:%x", sha256.Sum256(data)) != f.Sha256 {
			return nil, assets.Invalid("Lock file blob digest mismatch")
		}
		result = append(result, bundle.File{Path: f.Path, Digest: f.Sha256, Data: data})
	}
	return result, nil
}
