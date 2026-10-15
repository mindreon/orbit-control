package assets

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"fmt"

	"github.com/Masterminds/semver/v3"

	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/jcs"
)

// Upload contains validated immutable metadata; all blob writes precede the DB transaction.
type Upload struct {
	bundle               bundle.Bundle
	manifest             Manifest
	permissions          contract.EffectivePermissions
	version, tenant, ref string
	fileRefs             []string
	sizes                []int
}

func (s *Service) PrepareUpload(ctx context.Context, version string, raw []byte) (*Upload, error) {
	if _, err := semver.StrictNewVersion(version); err != nil {
		return nil, Invalid("version must be an exact semantic version")
	}
	b, err := bundle.Parse(raw, bundle.DefaultLimits)
	if err != nil {
		return nil, Invalid(err.Error())
	}
	m, p, err := ParseManifest(b.Files)
	if err != nil {
		return nil, Invalid(err.Error())
	}
	if m.Agent != nil && m.Agent.Version != nil && *m.Agent.Version != version ||
		m.Team != nil && m.Team.Version != nil && *m.Team.Version != version {
		return nil, Invalid("Manifest version differs from upload version")
	}
	for _, d := range m.Dependencies {
		if _, err := semver.NewConstraint(d.Constraint); err != nil {
			return nil, Invalid("Invalid dependency version constraint")
		}
	}
	if s.Store == nil {
		return nil, httpx.E(503, "STORAGE_UNAVAILABLE", "Asset storage is unavailable")
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(b.Archive))
	ref, err := s.Store.Put(ctx, auth.Tenant(ctx), digest, b.Archive)
	if err != nil {
		return nil, err
	}

	refs := make([]string, 0, len(b.Files))
	sizes := make([]int, 0, len(b.Files))
	for _, f := range b.Files {
		fileRef, err := s.Store.Put(ctx, auth.Tenant(ctx), f.Digest, f.Data)
		if err != nil {
			return nil, err
		}
		refs = append(refs, fileRef)
		sizes = append(sizes, len(f.Data))
	}
	// Only metadata survives staging; release the archive and decoded file bytes.
	b.Archive = nil
	for i := range b.Files {
		b.Files[i].Data = nil
	}
	return &Upload{
		bundle:      b,
		manifest:    m,
		permissions: p,
		version:     version,
		tenant:      auth.Tenant(ctx),
		ref:         ref,
		fileRefs:    refs,
		sizes:       sizes,
	}, nil
}
func (s *Service) Upload(ctx context.Context, a Asset, u *Upload) (Version, error) {
	if u.tenant != auth.Tenant(ctx) {
		return Version{}, Invalid("Upload tenant mismatch")
	}
	// Serialize with status/visibility updates; do not trust a stale preflight read.
	if err := db.Tx(ctx).QueryRow(ctx, ("SELECT status FROM assets WHERE tenant_id=$1 AND id=$2 FOR SHARE"),
		auth.Tenant(ctx), a.ID).Scan(&a.Status); err != nil {
		return Version{}, err
	}
	if a.Status != "active" {
		return Version{}, Invalid("Asset is unavailable")
	}
	b, m, p, version, ref := u.bundle, u.manifest, u.permissions, u.version, u.ref
	mb, err := json.Marshal(m)
	if err != nil {
		return Version{}, err
	}
	pb, err := json.Marshal(p)
	if err != nil {
		return Version{}, err
	}
	permissionsRaw, err := json.Marshal(struct {
		Effective contract.EffectivePermissions            `json:"effective_permissions"`
		Members   map[string]contract.EffectivePermissions `json:"members,omitempty"`
	}{Effective: p, Members: m.MemberPermissions})
	if err != nil {
		return Version{}, err
	}
	permissionsJCS, err := jcs.Canonicalize(permissionsRaw)
	if err != nil {
		return Version{}, err
	}
	identityJCS, err := SigningIdentity(a, version, b.Digest, permissionsJCS)
	if err != nil {
		return Version{}, err
	}
	id := ids.New()
	_, err = db.Tx(ctx).
		Exec(ctx,
			`INSERT INTO asset_versions(id,tenant_id,asset_id,version,manifest,content_digest,blob_index_ref,permissions,compat,
 state,uploaded_by,permissions_jcs,identity_jcs)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,'{}','draft',$9,$10,$11)`, id, auth.Tenant(ctx), a.ID, version, mb, b.Digest,
			ref, pb, auth.From(ctx).ID, permissionsJCS, identityJCS)
	if err != nil {
		return Version{}, err
	}
	if err = saveUploadContents(ctx, id, u); err != nil {
		return Version{}, err
	}
	if err = Log(
		ctx, "asset.version_uploaded", "asset_version", id,
		map[string]any{"content_digest": b.Digest, "excluded": b.Excluded},
	); err != nil {
		return Version{}, err
	}
	return s.Version(ctx, a.ID, id)
}

func saveUploadContents(ctx context.Context, id string, u *Upload) error {
	b, m := u.bundle, u.manifest
	var err error
	for i, f := range b.Files {
		_, err = db.Tx(ctx).
			Exec(ctx, `INSERT INTO asset_files(tenant_id,version_id,path,sha256,size_bytes,is_text,blob_ref)
 VALUES($1,$2,$3,$4,$5,$6,$7)`, auth.Tenant(ctx), id, f.Path, f.Digest, u.sizes[i], f.Text, u.fileRefs[i])
		if err != nil {
			return err
		}
	}
	// Dependencies with the same reference but incompatible constraints are retained
	// in manifest; SQL projection is only an index, not the resolver's authority.
	for _, d := range m.Dependencies {
		_, err = db.Tx(ctx).
			Exec(ctx, `INSERT INTO asset_dependencies(tenant_id,version_id,dep_kind,dep_ref,"constraint")
 VALUES($1,$2,$3,$4,$5)
 ON CONFLICT DO NOTHING`, auth.Tenant(ctx), id, d.Kind, d.Ref, d.Constraint)
		if err != nil {
			return err
		}
	}
	return nil
}
