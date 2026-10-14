package assets

import (
	"context"
	"encoding/json/v2"
	"mime"
	"net/http"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/jackc/pgx/v5"
	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
	"github.com/mindreon/orbit-control/internal/policy"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

type Service struct {
	Store  objstore.Store
	Signer *Signer
	Authz  policy.Authorizer
}
type Asset struct {
	ID          string             `json:"id"`
	Publisher   string             `json:"publisher"`
	Kind        contract.AssetKind `json:"kind"`
	Name        string             `json:"name"`
	Visibility  string             `json:"visibility"`
	WorkspaceID string             `json:"workspace_id,omitempty"`
	OwnerID     string             `json:"owner_id"`
	Status      string             `json:"status"`
	CreatedAt   time.Time          `json:"created_at"`
}
type Version struct {
	ID             string                        `json:"id"`
	AssetID        string                        `json:"asset_id"`
	Version        string                        `json:"version"`
	Manifest       Manifest                      `json:"manifest"`
	Digest         string                        `json:"content_digest"`
	BlobRef        string                        `json:"blob_ref"`
	Permissions    contract.EffectivePermissions `json:"permissions"`
	State          string                        `json:"state"`
	Signature      string                        `json:"signature,omitempty"`
	CreatedAt      time.Time                     `json:"created_at"`
	ReleaseError   *string                       `json:"release_error,omitempty"`
	UploadedBy     string                        `json:"uploaded_by,omitempty"`
	PermissionsJCS []byte                        `json:"-"`
	IdentityJCS    []byte                        `json:"-"`
}

var Slug = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{2,63}$`)
var Channel = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

func Invalid(message string) error { return httpx.E(422, "INVALID_INPUT", message) }
func Log(ctx context.Context, action, typ, id string, detail any) error {
	return audit.Append(ctx, db.Tx(ctx), auth.From(ctx), auth.Tenant(ctx), action, typ, id, detail)
}
func scanAsset(row pgx.Row) (Asset, error) {
	var a Asset
	err := row.Scan(&a.ID, &a.Publisher, &a.Kind, &a.Name, &a.Visibility, &a.WorkspaceID, &a.OwnerID, &a.Status, &a.CreatedAt)
	return a, err
}

const assetColumns = `id::text,publisher,kind,name,visibility,COALESCE(workspace_id::text,''),owner_id::text,status,created_at`

func (s *Service) Get(ctx context.Context, id, action string) (Asset, error) {
	if !ids.Valid(id) {
		return Asset{}, httpx.E(404, "NOT_FOUND", "Asset not found")
	}
	a, err := scanAsset(db.Tx(ctx).QueryRow(ctx, `SELECT `+assetColumns+` FROM assets WHERE tenant_id=$1 AND id=$2`, auth.Tenant(ctx), id))
	if err != nil {
		return a, err
	}
	var reviewing bool
	err = db.Tx(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM asset_versions WHERE asset_id=$1 AND state='in_review')`, id).Scan(&reviewing)
	if err != nil {
		return a, err
	}
	return a, s.Authz.RequireAsset(ctx, action, policy.Asset{ID: a.ID, WorkspaceID: a.WorkspaceID, OwnerID: a.OwnerID, Visibility: a.Visibility, Reviewing: reviewing})
}
func (s *Service) Create(ctx context.Context, kind contract.AssetKind, name, visibility, workspace string) (Asset, error) {
	if kind != "agent" && kind != "skill" && kind != "team" && kind != "mcp_server" || !Slug.MatchString(name) {
		return Asset{}, Invalid("Invalid asset kind or name")
	}
	if visibility != "private" && visibility != "workspace" && visibility != "tenant" || visibility == "workspace" && workspace == "" {
		return Asset{}, Invalid("Invalid asset visibility or workspace")
	}
	resource := policy.Resource{Type: "tenant", ID: auth.Tenant(ctx)}
	if workspace != "" {
		if !ids.Valid(workspace) {
			return Asset{}, Invalid("Invalid workspace")
		}
		resource = policy.Resource{Type: "workspace", ID: workspace}
	}
	if err := s.Authz.Require(ctx, "asset.create", resource); err != nil {
		return Asset{}, err
	}
	var publisher string
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT slug FROM tenants WHERE id=$1`, auth.Tenant(ctx)).Scan(&publisher); err != nil {
		return Asset{}, err
	}
	id := ids.New()
	p := auth.From(ctx)
	_, err := db.Tx(ctx).Exec(ctx, `INSERT INTO assets(id,tenant_id,publisher,kind,name,visibility,workspace_id,owner_id,status) VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,'')::uuid,$8,'active')`, id, auth.Tenant(ctx), publisher, kind, name, visibility, workspace, p.ID)
	if err != nil {
		return Asset{}, err
	}
	if err = tenancy.Bind(ctx, db.Tx(ctx), p, auth.Tenant(ctx), p.Kind, p.ID, "asset.owner", "asset", id); err != nil {
		return Asset{}, err
	}
	if err = Log(ctx, "asset.created", "asset", id, map[string]any{"kind": kind, "visibility": visibility}); err != nil {
		return Asset{}, err
	}
	return s.Get(ctx, id, "asset.read")
}
func (s *Service) Version(ctx context.Context, asset, id string) (Version, error) {
	var v Version
	if !ids.Valid(id) {
		return v, httpx.E(404, "NOT_FOUND", "Version not found")
	}
	var m, p []byte
	err := db.Tx(ctx).QueryRow(ctx, `SELECT id::text,asset_id::text,version,manifest,content_digest,blob_index_ref,permissions,state,COALESCE(signature,''),created_at,release_error,COALESCE(uploaded_by::text,''),permissions_jcs,identity_jcs FROM asset_versions WHERE tenant_id=$1 AND asset_id=$2 AND id=$3`, auth.Tenant(ctx), asset, id).Scan(&v.ID, &v.AssetID, &v.Version, &m, &v.Digest, &v.BlobRef, &p, &v.State, &v.Signature, &v.CreatedAt, &v.ReleaseError, &v.UploadedBy, &v.PermissionsJCS, &v.IdentityJCS)
	if err != nil {
		return v, err
	}
	if err = json.Unmarshal(m, &v.Manifest); err != nil {
		return v, err
	}
	err = json.Unmarshal(p, &v.Permissions)
	return v, err
}
func SetState(ctx context.Context, id, to string) error {
	var from string
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT state FROM asset_versions WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, auth.Tenant(ctx), id).Scan(&from); err != nil {
		return err
	}
	if err := Transition(from, to); err != nil {
		return err
	}
	if _, err := db.Tx(ctx).Exec(ctx, `UPDATE asset_versions SET state=$2 WHERE id=$1`, id, to); err != nil {
		return err
	}
	return Log(ctx, "asset.version_state", "asset_version", id, map[string]string{"from": from, "to": to})
}
func (s *Service) Submit(ctx context.Context, a Asset, id, channel string) error {
	var visibility string
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT visibility FROM assets WHERE tenant_id=$1 AND id=$2 FOR UPDATE`, auth.Tenant(ctx), a.ID).Scan(&visibility); err != nil {
		return err
	}
	if visibility != a.Visibility {
		return httpx.E(409, "ASSET_CHANGED", "Asset visibility changed; retry submission")
	}
	if channel == "" {
		channel = "stable"
	}
	if !Channel.MatchString(channel) {
		return Invalid("Invalid channel")
	}
	if _, err := s.Version(ctx, a.ID, id); err != nil {
		return err
	}
	if err := SetState(ctx, id, "submitted"); err != nil {
		return err
	}
	review := true
	required := 1
	rows, err := db.Tx(ctx).Query(ctx, `SELECT body FROM policies WHERE tenant_id=$1 AND status='active' AND kind='tool_rules' AND scope_type='tenant' AND scope_id=$1 ORDER BY version,id`, auth.Tenant(ctx))
	if err != nil {
		return err
	}
	for rows.Next() {
		var raw []byte
		if err = rows.Scan(&raw); err != nil {
			rows.Close()
			return err
		}
		var policy struct {
			Review struct {
				Approvals int `json:"approvals"`
			} `json:"asset_review"`
		}
		if err = json.Unmarshal(raw, &policy); err != nil {
			rows.Close()
			return err
		}
		if policy.Review.Approvals != 0 {
			required = policy.Review.Approvals
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	// Workspace policies are edited through workspace settings, not request input.
	if a.WorkspaceID != "" && a.Visibility != "private" {
		var raw []byte
		if err := db.Tx(ctx).QueryRow(ctx, `SELECT settings FROM workspaces WHERE id=$1`, a.WorkspaceID).Scan(&raw); err != nil {
			return err
		}
		var settings struct {
			Assets struct {
				ReviewRequired *bool `json:"review_required"`
				Approvals      int   `json:"approvals"`
			} `json:"assets"`
		}
		if err := json.Unmarshal(raw, &settings); err != nil {
			return err
		}
		if a.Visibility == "workspace" && settings.Assets.ReviewRequired != nil {
			review = *settings.Assets.ReviewRequired
		}
		if settings.Assets.Approvals != 0 {
			if a.Visibility == "tenant" {
				required = max(required, settings.Assets.Approvals)
			} else {
				required = settings.Assets.Approvals
			}
		}
	}
	if required < 1 || required > 20 {
		return Invalid("Asset approvals policy must be between 1 and 20")
	}
	_, err = db.Tx(ctx).Exec(ctx, `UPDATE asset_versions SET submitted_by=$2,release_channel=$3,release_started=false,withdraw_requested=false,withdraw_delivered=false,review_required=$4,workspace_review=$5,release_generation=release_generation+1,release_error=NULL,release_retryable=true,previous_version_id=(SELECT id FROM asset_versions WHERE asset_id=$6 AND state IN ('published','deprecated') ORDER BY published_at DESC,id DESC LIMIT 1),previous_channel_id=(SELECT version_id FROM asset_channels WHERE asset_id=$6 AND channel=$3) WHERE id=$1`, id, auth.From(ctx).ID, channel, required, review, a.ID)
	return err
}
func (s *Service) SetChannel(ctx context.Context, a Asset, channel, id string) error {
	if !Channel.MatchString(channel) {
		return Invalid("Invalid channel")
	}
	v, err := s.Version(ctx, a.ID, id)
	if err != nil {
		return err
	}
	if !Usable(v.State) || a.Status != "active" {
		return Invalid("Channel target is unavailable")
	}
	if err = s.Signer.Verify(a, v); err != nil {
		return Invalid("Channel target signature is invalid")
	}
	var previous *string
	err = db.Tx(ctx).QueryRow(ctx, `SELECT version_id::text FROM asset_channels WHERE asset_id=$1 AND channel=$2 FOR UPDATE`, a.ID, channel).Scan(&previous)
	if err != nil && err != pgx.ErrNoRows {
		return err
	}
	_, err = db.Tx(ctx).Exec(ctx, `INSERT INTO asset_channels(tenant_id,asset_id,channel,version_id,updated_by) VALUES($1,$2,$3,$4,$5) ON CONFLICT(tenant_id,asset_id,channel) DO UPDATE SET version_id=EXCLUDED.version_id,updated_by=EXCLUDED.updated_by,updated_at=now()`, auth.Tenant(ctx), a.ID, channel, id, auth.From(ctx).ID)
	if err != nil {
		return err
	}
	return Log(ctx, "asset.channel_changed", "asset", a.ID, map[string]any{"channel": channel, "previous_version_id": previous, "version_id": id})
}
func (s *Service) Files(ctx context.Context, id string) ([]contract.LockFile, error) {
	rows, err := db.Tx(ctx).Query(ctx, `SELECT path,sha256,size_bytes FROM asset_files WHERE tenant_id=$1 AND version_id=$2 ORDER BY path`, auth.Tenant(ctx), id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []contract.LockFile{}
	for rows.Next() {
		var f contract.LockFile
		if err = rows.Scan(&f.Path, &f.Sha256, &f.SizeBytes); err != nil {
			return nil, err
		}
		result = append(result, f)
	}
	return result, rows.Err()
}
func (s *Service) File(ctx context.Context, id, name string) ([]byte, string, error) {
	if _, err := bundle.SafePath(name); err != nil {
		return nil, "", Invalid("Invalid file path")
	}
	var digest string
	var size int64
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT sha256,size_bytes FROM asset_files WHERE version_id=$1 AND path=$2`, id, name).Scan(&digest, &size); err != nil {
		return nil, "", err
	}
	if size > 20<<20 {
		return nil, "", httpx.E(413, "FILE_TOO_LARGE", "File exceeds browsing size limit")
	}
	data, err := s.Store.Get(ctx, auth.Tenant(ctx), digest, 20<<20)
	if err != nil {
		return nil, "", err
	}
	typ := mime.TypeByExtension(path.Ext(name))
	if typ == "" {
		typ = http.DetectContentType(data)
	}
	return data, typ, nil
}
func (s *Service) Install(ctx context.Context, a Asset, scope, scopeID, constraint, channel, pinned string) (string, error) {
	if err := s.requireInstall(ctx, scope, scopeID); err != nil {
		return "", err
	}
	// Lock against concurrent metadata changes and use the current ceiling.
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT visibility,status FROM assets WHERE tenant_id=$1 AND id=$2 FOR SHARE`, auth.Tenant(ctx), a.ID).Scan(&a.Visibility, &a.Status); err != nil {
		return "", err
	}
	if a.Visibility != "tenant" && (scope == "tenant" || a.Visibility != "workspace" || a.WorkspaceID != scopeID) {
		return "", httpx.E(403, "VISIBILITY_BOUNDARY", "Asset must be visible to every member of the installation scope")
	}
	if a.Status != "active" {
		return "", Invalid("Asset is unavailable")
	}
	if constraint == "" {
		constraint = "*"
	}
	if _, err := semver.NewConstraint(constraint); err != nil {
		return "", Invalid("Invalid semantic version constraint")
	}
	if channel != "" && !Channel.MatchString(channel) {
		return "", Invalid("Invalid channel")
	}
	if pinned != "" && !ids.Valid(pinned) {
		return "", Invalid("Invalid pinned version")
	}
	if _, err := s.SelectVersion(ctx, a, []string{constraint}, pinned, channel); err != nil {
		return "", err
	}
	id := ids.New()
	err := db.Tx(ctx).QueryRow(ctx, `INSERT INTO asset_installs(id,tenant_id,scope_type,scope_id,asset_id,"constraint",channel,pinned_version_id,enabled) VALUES($1,$2,$3,$4,$5,$6,NULLIF($7,''),NULLIF($8,'')::uuid,true) ON CONFLICT(tenant_id,scope_type,scope_id,asset_id) DO UPDATE SET "constraint"=EXCLUDED."constraint",channel=EXCLUDED.channel,pinned_version_id=EXCLUDED.pinned_version_id,enabled=true RETURNING id::text`, id, auth.Tenant(ctx), scope, scopeID, a.ID, constraint, channel, pinned).Scan(&id)
	if err != nil {
		return "", err
	}
	return id, Log(ctx, "asset.installed", "asset_install", id, map[string]any{"asset_id": a.ID, "scope_type": scope, "scope_id": scopeID})
}
func Ref(a Asset) string      { return a.Publisher + "/" + a.Name }
func IsLocal(ref string) bool { return ref == "." || strings.HasPrefix(ref, "./") }

// SelectVersion is shared by installation validation and recursive lock
// resolution. A pin or channel is authoritative; an unavailable pointer never
// silently falls back to a different version.
func (s *Service) SelectVersion(ctx context.Context, a Asset, ranges []string, pinned, channel string) (Version, error) {
	if a.Status != "active" {
		return Version{}, Invalid("Asset is unavailable")
	}
	constraints := []*semver.Constraints{}
	for _, text := range ranges {
		if text == "" {
			text = "*"
		}
		c, err := semver.NewConstraint(text)
		if err != nil {
			return Version{}, Invalid("Invalid semantic version constraint")
		}
		constraints = append(constraints, c)
	}
	if pinned == "" && channel != "" {
		if err := db.Tx(ctx).QueryRow(ctx, `SELECT version_id::text FROM asset_channels WHERE asset_id=$1 AND channel=$2`, a.ID, channel).Scan(&pinned); err != nil {
			return Version{}, err
		}
	}
	rows, err := db.Tx(ctx).Query(ctx, `SELECT id::text,version FROM asset_versions WHERE asset_id=$1 AND state IN ('published','deprecated') AND ($2='' OR id=NULLIF($2,'')::uuid)`, a.ID, pinned)
	if err != nil {
		return Version{}, err
	}
	bestID := ""
	var best *semver.Version
	for rows.Next() {
		var id, text string
		if err = rows.Scan(&id, &text); err != nil {
			rows.Close()
			return Version{}, err
		}
		v, e := semver.StrictNewVersion(text)
		if e != nil {
			rows.Close()
			return Version{}, e
		}
		match := true
		for _, c := range constraints {
			match = match && c.Check(v)
		}
		if match && (best == nil || v.GreaterThan(best)) {
			best = v
			bestID = id
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return Version{}, err
	}
	if bestID == "" {
		return Version{}, Invalid("No available version satisfies asset constraints")
	}
	v, err := s.Version(ctx, a.ID, bestID)
	if err != nil {
		return v, err
	}
	if !Usable(v.State) {
		return v, Invalid("Asset version became unavailable")
	}
	if err = s.Signer.Verify(a, v); err != nil {
		return v, Invalid("Asset signature verification failed")
	}
	return v, nil
}
