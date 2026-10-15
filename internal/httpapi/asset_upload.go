package httpapi

import (
	"bytes"
	"context"
	"io"
	"mime"
	"net/http"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

type uploadKey struct{}

func readArchive(r *http.Request) ([]byte, error) {
	var source io.Reader = r.Body
	var nextPart func() error
	mediaType, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if mediaType == "multipart/form-data" {
		reader, e := r.MultipartReader()
		if e != nil {
			return nil, assets.Invalid("Invalid multipart archive")
		}
		part, e := reader.NextPart()
		if e != nil || part.FormName() != "archive" || part.FileName() == "" {
			return nil, assets.Invalid("Multipart upload requires one archive file")
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
		return nil, httpx.E(413, "BODY_TOO_LARGE", "Archive exceeds upload limit")
	}
	if nextPart != nil {
		if err = nextPart(); err != nil {
			return nil, err
		}
	}
	return raw, nil
}

// Stage before opening the mutation/idempotency transaction. Content-addressed
// writes are idempotent; failed metadata commits can leave unreferenced blobs.
func (h *Handler) stageUpload(r *http.Request) (*http.Request, error) {
	if r.Header.Get("Idempotency-Key") == "" {
		return nil, httpx.E(400, "IDEMPOTENCY_REQUIRED", "Version upload requires Idempotency-Key")
	}
	s, err := h.assetService()
	if err != nil {
		return nil, err
	}
	ctx := r.Context()
	err = h.DB.WithTenant(ctx, auth.Tenant(ctx), func(tx pgx.Tx) error {
		if err := tenancy.Active(ctx, tx, auth.From(ctx).ID); err != nil {
			return err
		}
		return s.RequireUpload(db.Context(ctx, tx), r.PathValue("a"))
	})
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, httpx.E(413, "BODY_TOO_LARGE", "Request body exceeds limit")
	}
	ctx = httpx.WithRequestBody(ctx, raw)
	r = r.WithContext(ctx)
	r.Body = io.NopCloser(bytes.NewReader(raw))
	archive := raw
	if typ, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); typ == "multipart/form-data" {
		archive, err = readArchive(r)
		if err != nil {
			return nil, err
		}
	}
	u, err := s.PrepareUpload(ctx, r.URL.Query().Get("version"), archive)
	if err != nil {
		return nil, err
	}
	return r.WithContext(context.WithValue(ctx, uploadKey{}, u)), nil
}
func (h *Handler) uploadSlots() chan struct{} {
	h.uploadOnce.Do(func() {
		n := h.Config.AssetUploadConcurrency
		if n == 0 {
			n = 2
		}
		h.uploadSemaphore = make(chan struct{}, n)
	})
	return h.uploadSemaphore
}
