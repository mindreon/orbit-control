package httpapi

import (
	"net/http"

	"github.com/mindreon/orbit-control/internal/interactions"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/usage"
)

func (h *Handler) runEffects(out http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	run, _, err := sessions.GetRun(ctx, r.PathValue("r"), "session.read")
	if err != nil {
		return err
	}
	page, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	result, err := sessions.Effects(ctx, run.ID, page)
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, result)
	return nil
}

func (h *Handler) runInteractions(out http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	run, _, err := sessions.GetRun(ctx, r.PathValue("r"), "session.read")
	if err != nil {
		return err
	}
	page, err := httpx.Pagination(r)
	if err != nil {
		return err
	}
	result, err := interactions.ListByRun(ctx, run.ID, page)
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, result)
	return nil
}

func (h *Handler) runCost(out http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	run, _, err := sessions.GetRun(ctx, r.PathValue("r"), "session.read")
	if err != nil {
		return err
	}
	result, err := usage.RunCost(ctx, run.ID)
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, result)
	return nil
}
