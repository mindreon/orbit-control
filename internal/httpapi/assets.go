package httpapi

import (
	"encoding/base64"
	"net/http"
	"strings"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
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
	filter, err := assetFilter(r)
	if err != nil {
		return err
	}
	result, err := s.List(r.Context(), page, filter)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, result)
	return nil
}

func assetFilter(r *http.Request) (assets.Filter, error) {
	q := r.URL.Query()
	f := assets.Filter{Kind: q.Get("kind"), Visibility: q.Get("visibility"), Query: q.Get("q"), WorkspaceID: q.Get("workspace_id")}
	if q.Has("owner") {
		if q.Get("owner") != "me" {
			return f, httpx.E(422, "INVALID_INPUT", "owner must be me")
		}
		f.Owner = auth.From(r.Context()).ID
	}
	if q.Has("ref") {
		publisher, name, ok := strings.Cut(q.Get("ref"), "/")
		if !ok || !referencePart(publisher) || !referencePart(name) {
			return f, httpx.E(422, "INVALID_INPUT", "Invalid asset reference")
		}
		f.Publisher, f.Name = publisher, name
	}
	return f, nil
}
func referencePart(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return value != "." && value != ".."
}
func (h *Handler) assetDependents(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	p, err := dependentPage(r)
	if err != nil {
		return err
	}
	kind := r.URL.Query().Get("kind")
	if kind != "" && kind != "agent" && kind != "team" {
		return httpx.E(422, "INVALID_INPUT", "Invalid dependent kind")
	}
	result, err := s.Dependents(r.Context(), r.PathValue("a"), kind, p)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, result)
	return nil
}
func dependentPage(r *http.Request) (httpx.Page, error) {
	p, err := httpx.Pagination(r)
	if err == nil && p.Limit > 50 {
		return p, httpx.E(400, "INVALID_LIMIT", "limit must be between 1 and 50")
	}
	return p, err
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
	a, err = s.ChangeVisibility(ctx, a.ID, in.Visibility)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, a)
	return nil
}
func (h *Handler) archiveAsset(w http.ResponseWriter, r *http.Request) error {
	return h.assetStatus(w, r, "archived")
}
func (h *Handler) restoreAsset(w http.ResponseWriter, r *http.Request) error {
	return h.assetStatus(w, r, "active")
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
	if _, err = s.ChangeStatus(ctx, a.ID, status); err != nil {
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
	u := r.Context().Value(uploadKey{}).(*assets.Upload)
	v, err := s.Upload(r.Context(), a, u)
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
	return h.versionPage(w, r, a.ID, false)
}
func (h *Handler) versionPage(w http.ResponseWriter, r *http.Request, value string, review bool) error {
	p, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	result, err := h.Assets.Versions(r.Context(), p, value, review)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, result)
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
	s, _, v, err := h.versionFor(r, "asset.publish")
	if err != nil {
		return err
	}
	if err = s.RequestWithdraw(r.Context(), v.ID); err != nil {
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
	v, err = s.ChangeVersionState(r.Context(), a, v, in.State)
	if err != nil {
		return err
	}
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
	result, err := h.Assets.Checks(r.Context(), p, v.ID)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, result)
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
	items, err := s.Channels(r.Context(), a.ID)
	if err != nil {
		return err
	}
	return textPage(w, r, items, func(c assets.AssetChannel) string { return c.Name })
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
	return h.versionPage(w, r, auth.Tenant(r.Context()), true)
}
func (h *Handler) decideAssetReview(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	var in struct {
		Decision string `json:"decision"`
		Comment  string `json:"comment"`
	}
	if err = httpx.Decode(r, &in); err != nil {
		return err
	}
	reviewID, err := s.Review(r.Context(), r.PathValue("v"), in.Decision, in.Comment)
	if err != nil {
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
	resolver := locks.NewResolver(s)
	lock, err := resolver.Resolve(
		r.Context(),
		locks.Request{
			TenantID: auth.Tenant(r.Context()),
			LockID:   ids.New(),
			Entry:    entry,
			Scope:    locks.Scope{WorkspaceID: r.PathValue("w")},
		},
	)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, lock)
	return nil
}

func (h *Handler) listAssetGrants(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	a, err := s.Get(r.Context(), r.PathValue("a"), "asset.share")
	if err != nil {
		return err
	}
	items, err := s.Grants(r.Context(), a.ID)
	if err != nil {
		return err
	}
	return textPage(w, r, items, assets.Grant.Key)
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
	var g assets.Grant
	if err = httpx.Decode(r, &g); err != nil {
		return err
	}
	if err = s.ChangeGrant(ctx, a, g, revoke); err != nil {
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
	p, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	service, err := h.assetService()
	if err != nil {
		return err
	}
	result, err := service.Installs(r.Context(), p, scope, id)
	if err != nil {
		return err
	}
	httpx.JSON(w, 200, result)
	return nil
}
func (h *Handler) uninstallAsset(w http.ResponseWriter, r *http.Request) error {
	s, err := h.assetService()
	if err != nil {
		return err
	}
	if err = s.Uninstall(r.Context(), r.PathValue("i")); err != nil {
		return err
	}
	w.WriteHeader(204)
	return nil
}
