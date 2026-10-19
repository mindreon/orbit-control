package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/usage"
)

func (h *Handler) usage(out http.ResponseWriter, r *http.Request) error {
	q := r.URL.Query()
	o := usage.ReportOptions{Scope: q.Get("scope"), ID: q.Get("id"), Granularity: q.Get("granularity")}
	for _, field := range []struct {
		name   string
		target *time.Time
	}{{name: "from", target: &o.From}, {name: "to", target: &o.To}} {
		if raw := q.Get(field.name); raw != "" {
			value, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				return httpx.E(422, "INVALID_INPUT", "Invalid usage report time")
			}
			*field.target = value
		}
	}
	if raw := q.Get("group_by"); raw != "" {
		o.GroupBy = strings.Split(raw, ",")
	}
	if err := o.Validate(time.Now()); err != nil {
		return err
	}
	result, err := usage.Report(r.Context(), o)
	if err != nil {
		return err
	}
	httpx.JSON(out, 200, result)
	return nil
}
