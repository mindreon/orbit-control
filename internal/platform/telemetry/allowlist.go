package telemetry

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// SpanAttributes is shared with the contract fixture and the runtime exporter.
var SpanAttributes = []string{
	"orbit.tenant_id", "orbit.run_id", "orbit.session_id", "orbit.member", "orbit.segment", "orbit.attempt",
	"orbit.asset_ref", "orbit.turn_kind", "orbit.segment.status", "orbit.segment.end_reason", "orbit.iterations",
	"orbit.effect_class", "orbit.checkpoint.bytes", "orbit.checkpoint.storage", "orbit.checkpoint.conflict",
	"orbit.listener", "orbit.request_id", "orbit.task_queue",
	"http.request.method", "http.route", "http.response.status_code",
	"temporalWorkflowID", "temporalRunID", "temporalActivityID", "temporalUpdateID",
	"gen_ai.operation.name", "gen_ai.provider.name", "gen_ai.request.model", "gen_ai.response.model",
	"gen_ai.request.stream", "gen_ai.response.finish_reasons", "gen_ai.usage.input_tokens", "gen_ai.usage.output_tokens",
	"gen_ai.usage.reasoning.output_tokens", "gen_ai.usage.cache_read.input_tokens", "gen_ai.usage.cache_creation.input_tokens",
	"gen_ai.response.time_to_first_chunk", "gen_ai.tool.name", "error.type",
	"openjiuwen.request.number", "openjiuwen.request.retry_count", "openjiuwen.trajectory.record.kind",
	"openjiuwen.gen_ai.response.total_latency_ms",
}

var spanName = regexp.MustCompile(`^[A-Za-z0-9_.:/{} <>\-]{1,128}$`)

var allowedSpanKeys = func() map[attribute.Key]bool {
	out := make(map[attribute.Key]bool, len(SpanAttributes))
	for _, key := range SpanAttributes {
		out[attribute.Key(key)] = true
	}
	return out
}()

type allowlistExporter struct{ next sdktrace.SpanExporter }

// AllowlistExporter is the sole content boundary before OTLP export. A snapshot
// avoids mutating SDK spans while other readers may still be using them.
func AllowlistExporter(next sdktrace.SpanExporter) sdktrace.SpanExporter {
	return &allowlistExporter{next: next}
}

func (e *allowlistExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	clean := make([]sdktrace.ReadOnlySpan, 0, len(spans))
	for _, span := range spans {
		filtered := &filteredSpan{ReadOnlySpan: span, name: span.Name(), status: span.Status()}
		filtered.attrs = filterAttributes(span.Attributes(), func(key string) bool { return allowedSpanKeys[attribute.Key(key)] })
		filtered.status.Description = ""
		if !spanName.MatchString(filtered.name) {
			filtered.name = "redacted"
			Counter("orbit.telemetry.spans_redacted").Add(ctx, 1, attribute.String("orbit.reason", "name"))
		}
		filtered.links = slices.Clone(span.Links())
		for i := range filtered.links {
			filtered.links[i].Attributes = nil
			filtered.links[i].SpanContext = filtered.links[i].SpanContext.WithTraceState(trace.TraceState{})
		}
		filtered.resource = resource.NewSchemaless(filterAttributes(span.Resource().Attributes(), resourceAttribute)...)
		clean = append(clean, filtered)
	}
	return e.next.ExportSpans(ctx, clean)
}

func (e *allowlistExporter) Shutdown(ctx context.Context) error { return e.next.Shutdown(ctx) }

func resourceAttribute(key string) bool {
	return key == "service.name" || key == "service.version" || key == "service.instance.id" ||
		key == "deployment.environment.name" || strings.HasPrefix(key, "telemetry.sdk.")
}

func filterAttributes(attrs []attribute.KeyValue, allowed func(string) bool) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		if !allowed(string(attr.Key)) {
			continue
		}
		valid := true
		switch attr.Value.Type() {
		case attribute.STRING:
			valid = len(attr.Value.AsString()) <= 256
		case attribute.STRINGSLICE:
			for _, value := range attr.Value.AsStringSlice() {
				valid = valid && len(value) <= 256
			}
		}
		if valid {
			out = append(out, attr)
		}
	}
	return out
}

// Embed the SDK interface to preserve its private implementation marker; all
// content-bearing fields are overridden without altering the original span.
type filteredSpan struct {
	sdktrace.ReadOnlySpan
	name     string
	attrs    []attribute.KeyValue
	links    []sdktrace.Link
	status   sdktrace.Status
	resource *resource.Resource
}

func (s *filteredSpan) Name() string                     { return s.name }
func (s *filteredSpan) Attributes() []attribute.KeyValue { return s.attrs }
func (s *filteredSpan) Events() []sdktrace.Event         { return nil }
func (s *filteredSpan) Links() []sdktrace.Link           { return s.links }
func (s *filteredSpan) Status() sdktrace.Status          { return s.status }
func (s *filteredSpan) Resource() *resource.Resource     { return s.resource }

func (s *filteredSpan) SpanContext() trace.SpanContext {
	return s.ReadOnlySpan.SpanContext().WithTraceState(trace.TraceState{})
}
func (s *filteredSpan) Parent() trace.SpanContext {
	return s.ReadOnlySpan.Parent().WithTraceState(trace.TraceState{})
}
