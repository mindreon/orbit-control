package httpapi

import (
	"net/http"

	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/sessions"
)

func (h *Handler) history(out http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()
	query, err := sessions.ParseEventQuery(r.URL.Query())
	if err != nil {
		return err
	}
	session, run := r.PathValue("s"), r.PathValue("r")
	if run != "" {
		_, s, err := sessions.GetRun(ctx, run, "session.read")
		if err != nil {
			return err
		}
		session = s.ID
	} else if _, err := sessions.Get(ctx, session, "session.read"); err != nil {
		return err
	}
	page, err := sessions.Events(ctx, session, run, query)
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, page)
	return nil
}
