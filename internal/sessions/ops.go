package sessions

import (
	"net/url"
	"strings"
)

type RunOps struct {
	TraceID            *string `json:"trace_id"`
	TemporalWorkflowID string  `json:"temporal_workflow_id"`
	TemporalURL        *string `json:"temporal_url"`
	TraceURL           *string `json:"trace_url"`
}

func (r Run) OperatorView(namespace, temporalTemplate, traceTemplate string) *RunOps {
	link := func(template string) *string {
		if template == "" {
			return nil
		}
		traceID := ""
		if r.TraceID != nil {
			traceID = *r.TraceID
		}
		value := strings.NewReplacer("{namespace}", url.PathEscape(namespace),
			"{workflow_id}", url.PathEscape(r.TemporalWorkflowID), "{trace_id}", url.PathEscape(traceID)).Replace(template)
		return &value
	}
	return &RunOps{TraceID: r.TraceID, TemporalWorkflowID: r.TemporalWorkflowID,
		TemporalURL: link(temporalTemplate), TraceURL: link(traceTemplate)}
}
