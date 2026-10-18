// Package wfctx contains the IO-free context bridge used by Workflow code.
package wfctx

import "go.temporal.io/sdk/workflow"

type spanKey struct{}

// SpanContextKey is shared with the Temporal tracing interceptor.
var SpanContextKey = spanKey{}

func WorkflowSpan(ctx workflow.Context) any { return ctx.Value(SpanContextKey) }
