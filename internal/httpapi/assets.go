package httpapi

import (
	"encoding/base64"
	"encoding/json/jsontext"
	"errors"
	"io"
	"mime"
	"net/http"
	"slices"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
)

func (h *Handler) assetService() (*assets.Service, error) {
	if h.Assets == nil {
		return nil, httpx.E(503, "ASSETS_UNAVAILABLE", "Asset service is unavailable")
	}
	return h.Assets, nil
}
func (h *Handler) createAsset(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	var in struct {
		Kind        contract.AssetKind `json:"kind"`
		Name        string             `json:"name"`
		Visibility  string             `json:"visibility"`
		WorkspaceID string             `json:"workspace_id"`
	}
	if err = httpx.Decode(r, &in); err != nil {
		return err
	}
	a, err := s.Create(r.Context(), in.Kind, in.Name, in.Visibility, in.WorkspaceID)
	if err != nil {
		return err
	}
	httpx.JSON(w, 201, a)
	return nil
}
func (h *Handler) getAsset(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	a, err := s.Get(r.Context(), r.PathValue("a"), "asset.read")
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, a)
	return nil
}
func (h *Handler) listAssets(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	page, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	ctx := r.Context()
	result := []assets.Asset{}
	cursor := page.Cursor
	for len(result) <= page.Limit {
		rows, e := db.Tx(ctx).Query(ctx, `SELECT id::text FROM assets WHERE ($1='' OR id>NULLIF($1,'')::uuid) AND ($2='' OR kind=$2) AND ($3='' OR visibility=$3) AND ($4='' OR name ILIKE '%'||$4||'%') AND ($5='' OR workspace_id=NULLIF($5,'')::uuid) ORDER BY id LIMIT 100`, cursor, r.URL.Query().Get("kind"), r.URL.Query().Get("visibility"), r.URL.Query().Get("q"), r.URL.Query().Get("workspace_id"))
		if e != nil {
			return e
		}
		batch := []string{}
		for rows.Next() {
			var id string
			if e = rows.Scan(&id); e != nil {
				rows.Close()
				return e
			}
			batch = append(batch, id)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		if len(batch) == 0 {
			break
		}
		for _, id := range batch {
			cursor = id
			a, e := s.Get(ctx, id, "asset.read")
			if status, ok := errors.AsType[*httpx.Error](e); ok && status.Status == 404 {
				continue
			}
			if e != nil {
				return e
			}
			result = append(result, a)
			if len(result) > page.Limit {
				break
			}
		}
	}
	next := ""
	if len(result) > page.Limit {
		result = result[:page.Limit]
		next = httpx.Cursor(result[len(result)-1].ID)
	}
	httpx.JSON(w, 200, map[string]any{"items": result, "next_cursor": next})
	return nil
}
func (h *Handler) patchAsset(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	ctx := r.Context()
	a, err := s.Get(ctx, r.PathValue("a"), "asset.share")
	if err != nil {
		return err
	}
	var in struct {
		Visibility string `json:"visibility"`
	}
	if err = httpx.Decode(r, &in); err != nil {
		return err
	}
	if !slices.Contains([]string{"private", "workspace", "tenant"}, in.Visibility) || in.Visibility == "workspace" && a.WorkspaceID == "" {
		return assets.Invalid("Invalid visibility")
	}
	if _, err = db.Tx(ctx).Exec(ctx, `SELECT id FROM assets WHERE id=$1 FOR UPDATE`, a.ID); err != nil {
		return err
	}
	// Visibility is part of the release decision. Widening a published or pending
	// asset requires a new asset/release; a metadata PATCH cannot bypass review.
	if in.Visibility != a.Visibility {
		var frozen bool
		if err = db.Tx(ctx).QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM asset_versions WHERE asset_id=$1 AND state NOT IN ('draft','check_failed','changes_requested','rejected'))`, a.ID).Scan(&frozen); err != nil {
			return err
		}
		if frozen {
			return httpx.E(409, "RELEASE_VISIBILITY_FROZEN", "Visibility is frozen once a version is submitted")
		}
	}
	if _, err = db.Tx(ctx).Exec(ctx, `UPDATE assets SET visibility=$2 WHERE id=$1`, a.ID, in.Visibility); err != nil {
		return err
	}
	if err = assets.Log(ctx, "asset.visibility_changed", "asset", a.ID, in); err != nil {
		return err
	}
	a.Visibility = in.Visibility
	httpx.JSON(w, 200, a)
	return nil
}
func (h *Handler) archiveAsset(w http.ResponseWriter, r *http.Request) error {
	return h.assetStatus(w, r, "archived")
}
func (h *Handler) suspendAsset(w http.ResponseWriter, r *http.Request) error {
	return h.assetStatus(w, r, "suspended")
}
func (h *Handler) assetStatus(w http.ResponseWriter, r *http.Request, status string) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	ctx := r.Context()
	a, err := s.Get(ctx, r.PathValue("a"), "asset.update")
	if err != nil {
		return err
	}
	if _, err = db.Tx(ctx).Exec(ctx, `UPDATE assets SET status=$2 WHERE id=$1`, a.ID, status); err != nil {
		return err
	}
	if err = assets.Log(ctx, "asset."+status, "asset", a.ID, map[string]any{}); err != nil {
		return err
	}
	if status == "archived" {
		w.WriteHeader(204)
	} else {
		httpx.JSON(w, 202, map[string]string{"id": a.ID, "status": status})
	}
	return nil
}
func (h *Handler) uploadAssetVersion(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	if r.Header.Get("Idempotency-Key") == "" {
		return httpx.E(400, "IDEMPOTENCY_REQUIRED", "Version upload requires Idempotency-Key")
	}
	a, err := s.Get(r.Context(), r.PathValue("a"), "asset.update")
	if err != nil {
		return err
	}
	var source io.Reader = r.Body
	var nextPart func() error
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaType == "multipart/form-data" {
		reader, e := r.MultipartReader()
		if e != nil {
			return assets.Invalid("Invalid multipart archive")
		}
		part, e := reader.NextPart()
		if e != nil || part.FormName() != "archive" || part.FileName() == "" {
			return assets.Invalid("Multipart upload requires one archive file")
		}
		source = part
		nextPart = func() error {
			_, e := reader.NextPart()
			if e != io.EOF {
				return assets.Invalid("Multipart upload requires exactly one archive file")
			}
			return nil
		}
	}
	raw, err := io.ReadAll(io.LimitReader(source, bundle.MaxArchive+1))
	if err != nil {
		return httpx.E(413, "BODY_TOO_LARGE", "Archive exceeds upload limit")
	}
	if nextPart != nil {
		if err = nextPart(); err != nil {
			return err
		}
	}
	v, err := s.Upload(r.Context(), a, r.URL.Query().Get("version"), raw)
	if err != nil {
		return err
	}
	httpx.JSON(w, 201, v)
	return nil
}
func (h *Handler) getAssetVersion(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	a, err := s.Get(r.Context(), r.PathValue("a"), "asset.read")
	if err != nil {
		return err
	}
	v, err := s.Version(r.Context(), a.ID, r.PathValue("v"))
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, v)
	return nil
}
func (h *Handler) listAssetVersions(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	a, err := s.Get(r.Context(), r.PathValue("a"), "asset.read")
	if err != nil {
		return err
	}
	return h.versionPage(w, r, `asset_id=$1`, a.ID, false)
}
func (h *Handler) versionPage(w http.ResponseWriter, r *http.Request, filter string, value string, review bool) error {
	ctx := r.Context()
	p, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	s := h.Assets
	items := []assets.Version{}
	cursor := p.Cursor
	for len(items) <= p.Limit {
		rows, e := db.Tx(ctx).Query(ctx, `SELECT id::text,asset_id::text FROM asset_versions WHERE `+filter+` AND ($2='' OR id>NULLIF($2,'')::uuid) ORDER BY id LIMIT 100`, value, cursor)
		if e != nil {
			return e
		}
		batch := [][2]string{}
		for rows.Next() {
			var v [2]string
			if e = rows.Scan(&v[0], &v[1]); e != nil {
				rows.Close()
				return e
			}
			batch = append(batch, v)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		if len(batch) == 0 {
			break
		}
		for _, row := range batch {
			cursor = row[0]
			if review {
				if _, e = s.Get(ctx, row[1], "asset.review"); e != nil {
					if v, ok := errors.AsType[*httpx.Error](e); ok && (v.Status == 403 || v.Status == 404) {
						continue
					}
					return e
				}
			}
			v, e := s.Version(ctx, row[1], row[0])
			if e != nil {
				return e
			}
			items = append(items, v)
			if len(items) > p.Limit {
				break
			}
		}
	}
	next := ""
	if len(items) > p.Limit {
		items = items[:p.Limit]
		next = httpx.Cursor(items[len(items)-1].ID)
	}
	httpx.JSON(w, 200, map[string]any{"items": items, "next_cursor": next})
	return nil
}
func (h *Handler) versionFor(r *http.Request, action string) (*assets.Service, assets.Asset, assets.Version, error) {
	s, err := h.assetService()
	if err != nil {
		return nil, assets.Asset{}, assets.Version{}, err
	}
	a, err := s.Get(r.Context(), r.PathValue("a"), action)
	if err != nil {
		return s, a, assets.Version{}, err
	}
	v, err := s.Version(r.Context(), a.ID, r.PathValue("v"))
	return s, a, v, err
}
func (h *Handler) submitAssetVersion(w http.ResponseWriter, r *http.Request) error {
	s, a, v, err := h.versionFor(r, "asset.publish")
	if err != nil {
		return err
	}
	var in struct {
		Channel string `json:"channel"`
	}
	if err = httpx.Decode(r, &in); err != nil {
		return err
	}
	if err = s.Submit(r.Context(), a, v.ID, in.Channel); err != nil {
		return err
	}
	httpx.JSON(w, 202, map[string]string{"id": v.ID, "status": "submitted"})
	return nil
}
func (h *Handler) withdrawAssetVersion(w http.ResponseWriter, r *http.Request) error {
	_, _, v, err := h.versionFor(r, "asset.publish")
	if err != nil {
		return err
	}
	ctx := r.Context()
	n, err := db.Tx(ctx).Exec(ctx, `UPDATE asset_versions SET withdraw_requested=true WHERE id=$1 AND state IN ('submitted','checking','in_review')`, v.ID)
	if err != nil {
		return err
	}
	if n.RowsAffected() == 0 {
		return httpx.E(409, "INVALID_ASSET_STATE", "Release cannot be withdrawn at this stage")
	}
	if err = assets.Log(ctx, "asset.withdraw_requested", "asset_version", v.ID, map[string]any{}); err != nil {
		return err
	}
	httpx.JSON(w, 202, map[string]string{"id": v.ID, "status": "withdraw_requested"})
	return nil
}
func (h *Handler) transitionAssetVersion(w http.ResponseWriter, r *http.Request) error {
	s, a, v, err := h.versionFor(r, "asset.update")
	if err != nil {
		return err
	}
	var in struct {
		State string `json:"state"`
	}
	if err = httpx.Decode(r, &in); err != nil {
		return err
	}
	if !slices.Contains([]string{"draft", "deprecated", "suspended", "yanked", "published"}, in.State) {
		return assets.Invalid("Invalid requested state")
	}
	if in.State == "published" {
		if v.State != "suspended" {
			return httpx.E(409, "INVALID_ASSET_STATE", "Only suspended versions can be restored through this operation")
		}
		if err = h.Authz.Require(r.Context(), "asset.review", tenantResource(r.Context())); err != nil {
			return err
		}
		if err = s.Signer.Verify(a, v); err != nil {
			return err
		}
	}
	if err = assets.SetState(r.Context(), v.ID, in.State); err != nil {
		return err
	}
	v.State = in.State
	httpx.JSON(w, 200, v)
	return nil
}
func (h *Handler) listAssetFiles(w http.ResponseWriter, r *http.Request) error {
	s, _, v, err := h.versionFor(r, "asset.read")
	if err != nil {
		return err
	}
	files, err := s.Files(r.Context(), v.ID)
	if err != nil {
		return err
	}
	return textPage(w, r, files, func(f contract.LockFile) string { return f.Path })
}
func (h *Handler) getAssetFile(w http.ResponseWriter, r *http.Request) error {
	s, _, v, err := h.versionFor(r, "asset.read")
	if err != nil {
		return err
	}
	data, typ, err := s.File(r.Context(), v.ID, r.URL.Query().Get("path"))
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", typ)
	w.Header().Set("Content-Disposition", "attachment")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(200)
	_, err = w.Write(data)
	return err
}
func textPage[T any](w http.ResponseWriter, r *http.Request, items []T, key func(T) string) error {
	// File names and composite keys use opaque base64 cursors rather than UUIDs.
	clone := r.Clone(r.Context())
	q := clone.URL.Query()
	q.Del("cursor")
	u := *clone.URL
	clone.URL = &u
	clone.URL.RawQuery = q.Encode()
	p, err := httpx.Pagination(clone)
	if err != nil {
		return err
	}
	cursor := ""
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		b, e := base64.RawURLEncoding.DecodeString(raw)
		if e != nil {
			return httpx.E(400, "INVALID_CURSOR", "Invalid cursor")
		}
		cursor = string(b)
	}
	result := []T{}
	for _, item := range items {
		if key(item) > cursor {
			result = append(result, item)
		}
	}
	next := ""
	if len(result) > p.Limit {
		result = result[:p.Limit]
		next = base64.RawURLEncoding.EncodeToString([]byte(key(result[len(result)-1])))
	}
	httpx.JSON(w, 200, map[string]any{"items": result, "next_cursor": next})
	return nil
}
func (h *Handler) listAssetChecks(w http.ResponseWriter, r *http.Request) error {
	_, _, v, err := h.versionFor(r, "asset.read")
	if err != nil {
		return err
	}
	p, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	return h.jsonPage(w, r, p, `SELECT id::text,jsonb_build_object('id',id,'report',findings,'status',status) FROM asset_check_reports WHERE version_id=$1 AND ($2='' OR id>NULLIF($2,'')::uuid) ORDER BY id LIMIT $3`, v.ID)
}
func (h *Handler) jsonPage(w http.ResponseWriter, r *http.Request, p httpx.Page, query string, arg any) error {
	rows, err := db.Tx(r.Context()).Query(r.Context(), query, arg, p.Cursor, p.Limit+1)
	if err != nil {
		return err
	}
	defer rows.Close()
	items := []jsontext.Value{}
	keys := []string{}
	for rows.Next() {
		var id string
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			return err
		}
		keys = append(keys, id)
		items = append(items, jsontext.Value(raw))
	}
	if err = rows.Err(); err != nil {
		return err
	}
	next := ""
	if len(items) > p.Limit {
		items = items[:p.Limit]
		next = httpx.Cursor(keys[p.Limit-1])
	}
	httpx.JSON(w, 200, map[string]any{"items": items, "next_cursor": next})
	return nil
}
func (h *Handler) listAssetChannels(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	a, err := s.Get(r.Context(), r.PathValue("a"), "asset.read")
	if err != nil {
		return err
	}
	rows, err := db.Tx(r.Context()).Query(r.Context(), `SELECT channel,version_id::text FROM asset_channels WHERE asset_id=$1 ORDER BY channel`, a.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	type channel struct {
		Name    string `json:"channel"`
		Version string `json:"version_id"`
	}
	items := []channel{}
	for rows.Next() {
		var c channel
		if err = rows.Scan(&c.Name, &c.Version); err != nil {
			return err
		}
		items = append(items, c)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	return textPage(w, r, items, func(c channel) string { return c.Name })
}
func (h *Handler) setAssetChannel(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	a, err := s.Get(r.Context(), r.PathValue("a"), "asset.publish")
	if err != nil {
		return err
	}
	var in struct {
		ID string `json:"version_id"`
	}
	if err = httpx.Decode(r, &in); err != nil {
		return err
	}
	if err = s.SetChannel(r.Context(), a, r.PathValue("c"), in.ID); err != nil {
		return err
	}
	httpx.JSON(w, 202, map[string]string{"id": in.ID, "status": "channel_updated"})
	return nil
}
func (h *Handler) listAssetReviews(w http.ResponseWriter, r *http.Request) error {
	if _, err := h.assetService(); err != nil {
		return err
	}
	return h.versionPage(w, r, `state='in_review' AND tenant_id=$1`, auth.Tenant(r.Context()), true)
}
func (h *Handler) decideAssetReview(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	ctx := r.Context()
	id := r.PathValue("v")
	if !ids.Valid(id) {
		return assets.Invalid("Invalid version")
	}
	var assetID, submitter, state string
	var generation int
	if err = db.Tx(ctx).QueryRow(ctx, `SELECT asset_id::text,COALESCE(submitted_by::text,''),state,release_generation FROM asset_versions WHERE id=$1 FOR UPDATE`, id).Scan(&assetID, &submitter, &state, &generation); err != nil {
		return err
	}
	a, err := s.Get(ctx, assetID, "asset.review")
	if err != nil {
		return err
	}
	if err = h.Authz.RequireAsset(ctx, "asset.review", policy.Asset{ID: a.ID, WorkspaceID: a.WorkspaceID, OwnerID: a.OwnerID, Visibility: a.Visibility, Reviewing: true, SubmitterID: submitter}); err != nil {
		return err
	}
	if state != "in_review" {
		return httpx.E(409, "INVALID_ASSET_STATE", "Version is not awaiting review")
	}
	var in struct {
		Decision string `json:"decision"`
		Comment  string `json:"comment"`
	}
	if err = httpx.Decode(r, &in); err != nil {
		return err
	}
	if !slices.Contains([]string{"approve", "reject", "request_changes"}, in.Decision) || len(in.Comment) > 4000 {
		return assets.Invalid("Invalid review decision or comment")
	}
	reviewID := ids.New()
	if _, err = db.Tx(ctx).Exec(ctx, `INSERT INTO asset_reviews(id,tenant_id,version_id,reviewer_id,decision,comment,generation) VALUES($1,$2,$3,$4,$5,$6,$7)`, reviewID, auth.Tenant(ctx), id, auth.From(ctx).ID, in.Decision, in.Comment, generation); err != nil {
		return err
	}
	if err = assets.Log(ctx, "asset.review_submitted", "asset_version", id, map[string]string{"review_id": reviewID, "decision": in.Decision}); err != nil {
		return err
	}
	httpx.JSON(w, 202, map[string]string{"id": reviewID, "status": "queued"})
	return nil
}
func (h *Handler) previewAssetLock(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	if _, err = h.workspace(r.Context(), r.PathValue("w"), "workspace.read"); err != nil {
		return err
	}
	var entry locks.Entry
	if err = httpx.Decode(r, &entry); err != nil {
		return err
	}
	resolver := locks.Resolver{Assets: s}
	lock, err := resolver.Resolve(r.Context(), auth.Tenant(r.Context()), entry, locks.Scope{WorkspaceID: r.PathValue("w")})
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, lock)
	return nil
}

type grant struct {
	Type       string `json:"grantee_type"`
	ID         string `json:"grantee_id"`
	Permission string `json:"permission"`
}

func (g grant) key() string { return g.Type + ":" + g.ID + ":" + g.Permission }
func (h *Handler) listAssetGrants(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	a, err := s.Get(r.Context(), r.PathValue("a"), "asset.share")
	if err != nil {
		return err
	}
	rows, err := db.Tx(r.Context()).Query(r.Context(), `SELECT grantee_type,grantee_id::text,permission FROM asset_grants WHERE asset_id=$1 ORDER BY grantee_type,grantee_id,permission`, a.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	items := []grant{}
	for rows.Next() {
		var g grant
		if err = rows.Scan(&g.Type, &g.ID, &g.Permission); err != nil {
			return err
		}
		items = append(items, g)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	return textPage(w, r, items, grant.key)
}
func (h *Handler) grantAsset(w http.ResponseWriter, r *http.Request) error {
	return h.assetGrant(w, r, false)
}
func (h *Handler) revokeAssetGrant(w http.ResponseWriter, r *http.Request) error {
	return h.assetGrant(w, r, true)
}
func (h *Handler) assetGrant(w http.ResponseWriter, r *http.Request, revoke bool) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	ctx := r.Context()
	a, err := s.Get(ctx, r.PathValue("a"), "asset.share")
	if err != nil {
		return err
	}
	var g grant
	if err = httpx.Decode(r, &g); err != nil {
		return err
	}
	if !ids.Valid(g.ID) || !slices.Contains([]string{"user", "service", "group", "workspace", "tenant"}, g.Type) || !slices.Contains([]string{"use", "maintain"}, g.Permission) {
		return assets.Invalid("Invalid asset grant")
	}
	if !revoke {
		if err = h.Authz.RequireAssetGrantee(ctx, policy.Asset{ID: a.ID, WorkspaceID: a.WorkspaceID, OwnerID: a.OwnerID, Visibility: a.Visibility}, g.Type, g.ID); err != nil {
			return err
		}
	}
	if revoke {
		_, err = db.Tx(ctx).Exec(ctx, `DELETE FROM asset_grants WHERE asset_id=$1 AND grantee_type=$2 AND grantee_id=$3 AND permission=$4`, a.ID, g.Type, g.ID, g.Permission)
	} else {
		_, err = db.Tx(ctx).Exec(ctx, `INSERT INTO asset_grants(tenant_id,asset_id,grantee_type,grantee_id,permission) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, auth.Tenant(ctx), a.ID, g.Type, g.ID, g.Permission)
	}
	if err != nil {
		return err
	}
	action := "asset.granted"
	if revoke {
		action = "asset.grant_revoked"
	}
	if err = assets.Log(ctx, action, "asset", a.ID, g); err != nil {
		return err
	}
	if revoke {
		w.WriteHeader(204)
	} else {
		httpx.JSON(w, 202, map[string]string{"id": a.ID, "status": "granted"})
	}
	return nil
}
func (h *Handler) installWorkspaceAsset(w http.ResponseWriter, r *http.Request) error {
	return h.installAsset(w, r, "workspace", r.PathValue("w"))
}
func (h *Handler) installTenantAsset(w http.ResponseWriter, r *http.Request) error {
	return h.installAsset(w, r, "tenant", auth.Tenant(r.Context()))
}
func (h *Handler) installAsset(w http.ResponseWriter, r *http.Request, scope, id string) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	var in struct {
		AssetID    string `json:"asset_id"`
		Constraint string `json:"version_constraint"`
		Channel    string `json:"channel"`
		Pinned     string `json:"pinned_version_id"`
	}
	if err = httpx.Decode(r, &in); err != nil {
		return err
	}
	a, err := s.Get(r.Context(), in.AssetID, "asset.use")
	if err != nil {
		return err
	}
	install, err := s.Install(r.Context(), a, scope, id, in.Constraint, in.Channel, in.Pinned)
	if err != nil {
		return err
	}
	httpx.JSON(w, 202, map[string]string{"id": install, "status": "installed"})
	return nil
}
func (h *Handler) listWorkspaceInstalls(w http.ResponseWriter, r *http.Request) error {
	return h.listInstalls(w, r, "workspace", r.PathValue("w"))
}
func (h *Handler) listTenantInstalls(w http.ResponseWriter, r *http.Request) error {
	return h.listInstalls(w, r, "tenant", auth.Tenant(r.Context()))
}
func (h *Handler) listInstalls(w http.ResponseWriter, r *http.Request, scope, id string) error {
	if scope == "workspace" {
		if _, err := h.workspace(r.Context(), id, "workspace.read"); err != nil {
			return err
		}
	} else if err := h.Authz.Require(r.Context(), "tenant.read", tenantResource(r.Context())); err != nil {
		return err
	}
	p, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	return h.jsonPage(w, r, p, `SELECT id::text,to_jsonb(i)-'tenant_id' FROM asset_installs i WHERE scope_id=$1 AND scope_type='`+scope+`' AND ($2='' OR id>NULLIF($2,'')::uuid) ORDER BY id LIMIT $3`, id)
}
func (h *Handler) uninstallAsset(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	id := r.PathValue("i")
	if !ids.Valid(id) {
		return assets.Invalid("Invalid install")
	}
	var scope, scopeID string
	if err := db.Tx(ctx).QueryRow(ctx, `SELECT scope_type,scope_id::text FROM asset_installs WHERE id=$1`, id).Scan(&scope, &scopeID); err != nil {
		return err
	}
	action := "asset.use"
	if scope == "tenant" {
		action = "policy.manage"
	}
	if err := h.Authz.Require(ctx, action, policy.Resource{Type: scope, ID: scopeID}); err != nil {
		return err
	}
	if _, err := db.Tx(ctx).Exec(ctx, `DELETE FROM asset_installs WHERE id=$1`, id); err != nil {
		return err
	}
	if err := assets.Log(ctx, "asset.uninstalled", "asset_install", id, map[string]any{}); err != nil {
		return err
	}
	w.WriteHeader(204)
	return nil
}
