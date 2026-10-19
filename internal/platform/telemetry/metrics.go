package telemetry

import (
	"context"
	"slices"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
)

type Spec struct {
	Meter         string    `json:"meter"`
	Name          string    `json:"name"`
	Kind          string    `json:"kind"`
	Unit          string    `json:"unit"`
	Labels        []string  `json:"labels"`
	TenantAllowed bool      `json:"tenant_allowed"`
	Buckets       []float64 `json:"buckets,omitempty"`
}

var Metrics = []Spec{
	{Meter: "orbit-control", Name: "orbit.usage.rollup_backlog", Kind: "gauge", Unit: "{hour}", Labels: []string{}},
	{Meter: "orbit-control", Name: "orbit.usage.rollup_oldest_dirty_age", Kind: "gauge", Unit: "s", Labels: []string{}},
	{Meter: "orbit-control", Name: "orbit.usage.rollup_conflicts", Kind: "counter", Labels: []string{}},
	{Meter: "orbit-control", Name: "sink_messages_total", Kind: "counter", Labels: []string{"result"}},
	{Meter: "orbit-control", Name: "sink_rejected_total", Kind: "counter", Labels: []string{"reason"}},
	{Meter: "orbit-control", Name: "sink_skipped_total", Kind: "counter", Labels: []string{"type"}},
	{Meter: "orbit-control", Name: "sink_lag_seconds", Kind: "histogram", Unit: "s", Labels: []string{}},
	{Meter: "orbit-control", Name: "sink_batch_duration_seconds", Kind: "histogram", Unit: "s", Labels: []string{}},
	{Meter: "orbit-control", Name: "sink_pending", Kind: "gauge", Labels: []string{}},
	{Meter: "orbit-control", Name: "outbox_quarantined_total", Kind: "counter", Labels: []string{"reason"}},
	{Meter: "orbit-control", Name: "control_outbox_failures_total", Kind: "counter", Labels: []string{"stage"}},
	{Meter: "stream-gateway", Name: "streamgw_connections", Kind: "updowncounter", Labels: []string{"scope"}},
	{Meter: "stream-gateway", Name: "streamgw_frames_sent_total", Kind: "counter", Labels: []string{"type"}},
	{Meter: "stream-gateway", Name: "streamgw_resumes_total", Kind: "counter", Labels: []string{}},
	{Meter: "stream-gateway", Name: "streamgw_cursor_expired_total", Kind: "counter", Labels: []string{}},
	{Meter: "stream-gateway", Name: "streamgw_slow_client_closed_total", Kind: "counter", Labels: []string{}},
	{Meter: "stream-gateway", Name: "streamgw_auth_failures_total", Kind: "counter", Labels: []string{"reason"}},
	{Meter: "stream-gateway", Name: "streamgw_invalid_frames_total", Kind: "counter", Labels: []string{}},
	{Meter: "stream-gateway", Name: "streamgw_order_violation_total", Kind: "counter", Labels: []string{}},
	{Meter: "stream-gateway", Name: "streamgw_rejected_total", Kind: "counter", Labels: []string{"reason"}},
	{Meter: "orbit-control", Name: "orbit.http.server.request.duration", Kind: "histogram", Unit: "s",
		Labels:  []string{"http.request.method", "http.route", "http.response.status_code", "orbit.listener"},
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}},
	{Meter: "stream-gateway", Name: "orbit.http.server.request.duration", Kind: "histogram", Unit: "s",
		Labels:  []string{"http.request.method", "http.route", "http.response.status_code", "orbit.listener"},
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}},
	{Meter: "orbit-control", Name: "orbit.temporal.task_queue.backlog", Kind: "gauge", Unit: "{task}", Labels: []string{"orbit.task_queue"}},
	{Meter: "orbit-control", Name: "orbit.runs.active", Kind: "gauge", Unit: "{run}",
		Labels: []string{"orbit.tenant_id", "orbit.run.status"}, TenantAllowed: true},
	{Meter: "orbit-control", Name: "orbit.interactions.pending", Kind: "gauge", Unit: "{interaction}",
		Labels: []string{"orbit.interaction.kind"}},
	{Meter: "orbit-control", Name: "orbit.interactions.oldest_pending.age", Kind: "gauge", Unit: "s",
		Labels: []string{"orbit.interaction.kind"}},
	{Meter: "orbit-control", Name: "orbit.interaction.wait.duration",
		Kind: "histogram", Unit: "s",
		Labels:  []string{"orbit.interaction.kind", "orbit.interaction.outcome"},
		Buckets: []float64{1, 10, 30, 60, 300, 900, 3600, 14400, 86400, 259200}},
	{Meter: "orbit-control", Name: "orbit.quota.rejections", Kind: "counter", Unit: "{request}",
		Labels: []string{"orbit.quota.metric", "orbit.quota.scope"}},
	{Meter: "orbit-control", Name: "orbit.runs.started", Kind: "counter", Unit: "{run}",
		Labels: []string{"orbit.run.trigger", "orbit.entry_kind"}},
	{Meter: "orbit-control", Name: "orbit.runs.finished", Kind: "counter", Unit: "{run}",
		Labels: []string{"orbit.run.status", "orbit.entry_kind"}},
	{Meter: "orbit-control", Name: "orbit.run.duration", Kind: "histogram", Unit: "s",
		Labels:  []string{"orbit.run.status", "orbit.entry_kind"},
		Buckets: []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 14400}},
	{Meter: "orbit-control", Name: "orbit.run.start.latency", Kind: "histogram", Unit: "s", Labels: []string{"orbit.run.trigger"},
		Buckets: []float64{.1, .25, .5, 1, 2, 3, 5, 10, 30}},
	{Meter: "orbit-control", Name: "orbit.usage.tokens", Kind: "counter", Unit: "{token}", TenantAllowed: true,
		Labels: []string{"orbit.tenant_id", "gen_ai.request.model", "orbit.token.type"}},
	{Meter: "orbit-control", Name: "orbit.quota.policies_over_threshold", Kind: "gauge", Unit: "{policy}",
		Labels: []string{"orbit.quota.metric", "orbit.threshold"}},
	{Meter: "orbit-control", Name: "orbit.telemetry.label_overflow",
		Kind: "counter", Unit: "{measurement}", Labels: []string{"orbit.label"}},
	{Meter: "orbit-control", Name: "orbit.telemetry.label_violations", Kind: "counter", Unit: "{measurement}",
		Labels: []string{"orbit.metric"}},
	{Meter: "orbit-control", Name: "orbit.telemetry.label_registered", Kind: "gauge", Unit: "{label}", Labels: []string{"orbit.label"}},
	{Meter: "orbit-control", Name: "orbit.telemetry.spans_redacted", Kind: "counter", Unit: "{span}", Labels: []string{"orbit.reason"}},
}

func metricSpec(name string) Spec { return specFor("orbit-control", name) }
func specFor(meter, name string) Spec {
	for _, spec := range Metrics {
		if spec.Meter == meter && spec.Name == name {
			return spec
		}
	}
	panic("unregistered metric: " + meter + "/" + name)
}

type counter struct {
	spec Spec
	next metric.Int64Counter
}
type histogram struct {
	spec Spec
	next metric.Float64Histogram
}
type gauge struct {
	spec Spec
	next metric.Int64Gauge
}
type instrumentKey struct {
	provider    metric.MeterProvider
	meter, name string
}
type instruments struct {
	counter   metric.Int64Counter
	histogram metric.Float64Histogram
	gauge     metric.Int64Gauge
}

var instrumentCache = struct {
	sync.Mutex
	values map[instrumentKey]instruments
}{values: map[instrumentKey]instruments{}}

func cached(spec Spec) instruments {
	provider := otel.GetMeterProvider()
	key := instrumentKey{provider: provider, meter: spec.Meter, name: spec.Name}
	instrumentCache.Lock()
	defer instrumentCache.Unlock()
	if found, ok := instrumentCache.values[key]; ok {
		return found
	}
	var out instruments
	var err error
	meter := provider.Meter(spec.Meter)
	switch spec.Kind {
	case "counter":
		out.counter, err = meter.Int64Counter(spec.Name, metric.WithUnit(spec.Unit))
	case "histogram":
		out.histogram, err = meter.Float64Histogram(spec.Name, metric.WithUnit(spec.Unit))
	case "gauge":
		out.gauge, err = meter.Int64Gauge(spec.Name, metric.WithUnit(spec.Unit))
	default:
		panic("unsupported metric kind: " + spec.Kind)
	}
	if err != nil {
		panic(err)
	}
	instrumentCache.values[key] = out
	return out
}
func Counter(name string) counter {
	spec := metricSpec(name)
	return counter{spec: spec, next: cached(spec).counter}
}
func Histogram(name string) histogram { return HistogramFor("orbit-control", name) }
func HistogramFor(meter, name string) histogram {
	spec := specFor(meter, name)
	return histogram{spec: spec, next: cached(spec).histogram}
}
func Gauge(name string) gauge {
	spec := metricSpec(name)
	return gauge{spec: spec, next: cached(spec).gauge}
}
func (g gauge) Record(ctx context.Context, n int64, labels ...attribute.KeyValue) {
	if attrs, ok := metricLabels(ctx, g.spec, labels); ok {
		g.next.Record(ctx, n, metric.WithAttributes(attrs...))
	}
}
func Labels(ctx context.Context, name string, attrs ...attribute.KeyValue) []attribute.KeyValue {
	out, _ := metricLabels(ctx, metricSpec(name), attrs)
	return out
}
func (c counter) Add(ctx context.Context, n int64, labels ...attribute.KeyValue) {
	if attrs, ok := metricLabels(ctx, c.spec, labels); ok {
		c.next.Add(ctx, n, metric.WithAttributes(attrs...))
	}
}
func (h histogram) Record(ctx context.Context, value float64, labels ...attribute.KeyValue) {
	if attrs, ok := metricLabels(ctx, h.spec, labels); ok {
		h.next.Record(ctx, value, metric.WithAttributes(attrs...))
	}
}
func metricLabels(ctx context.Context, spec Spec, attrs []attribute.KeyValue) ([]attribute.KeyValue, bool) {
	keys := make([]string, 0, len(attrs))
	for _, attr := range attrs {
		keys = append(keys, string(attr.Key))
	}
	slices.Sort(keys)
	want := slices.Clone(spec.Labels)
	slices.Sort(want)
	if !slices.Equal(keys, want) {
		if testing.Testing() {
			panic("metric label mismatch: " + spec.Name)
		}
		Counter("orbit.telemetry.label_violations").Add(ctx, 1, attribute.String("orbit.metric", spec.Name))
		return nil, false
	}
	out := slices.Clone(attrs)
	for i, attr := range out {
		if limiter := labelLimiters[string(attr.Key)]; limiter != nil {
			out[i] = attribute.String(string(attr.Key), limiter.Value(ctx, attr.Value.AsString()))
		}
	}
	return out, true
}

type Limiter struct {
	mu    sync.Mutex
	label string
	limit int
	seen  map[string]bool
}

func NewLimiter(label string, limit int) *Limiter {
	return &Limiter{label: label, limit: limit, seen: map[string]bool{}}
}
func (l *Limiter) Value(ctx context.Context, value string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.seen[value] {
		return value
	}
	if len(l.seen) < l.limit {
		l.seen[value] = true
		Gauge("orbit.telemetry.label_registered").Record(ctx, int64(len(l.seen)), attribute.String("orbit.label", l.label))
		return value
	}
	Counter("orbit.telemetry.label_overflow").Add(ctx, 1, attribute.String("orbit.label", l.label))
	return "_other"
}

var labelLimiters = map[string]*Limiter{
	"orbit.tenant_id":      NewLimiter("tenant_id", 500),
	"gen_ai.request.model": NewLimiter("model", 50),
}

// ConfigureLimits runs once during process startup, before requests or workers.
func ConfigureLimits(tenant, model int) {
	if tenant > 0 {
		labelLimiters["orbit.tenant_id"] = NewLimiter("tenant_id", tenant)
	}
	if model > 0 {
		labelLimiters["gen_ai.request.model"] = NewLimiter("model", model)
	}
}

// MeterOptions is shared by Setup and ManualReader tests, including all privacy guards.
func MeterOptions() []sdkmetric.Option {
	options := []sdkmetric.Option{sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter)}
	for _, spec := range Metrics {
		keys := make([]attribute.Key, 0, len(spec.Labels))
		for _, label := range spec.Labels {
			keys = append(keys, attribute.Key(label))
		}
		stream := sdkmetric.Stream{AttributeFilter: attribute.NewAllowKeysFilter(keys...)}
		if len(spec.Buckets) > 0 {
			stream.Aggregation = sdkmetric.AggregationExplicitBucketHistogram{Boundaries: spec.Buckets}
		}
		criteria := sdkmetric.Instrument{Name: spec.Name}
		if spec.Name == "orbit.http.server.request.duration" {
			criteria.Scope = instrumentation.Scope{Name: spec.Meter}
		}
		options = append(options, sdkmetric.WithView(sdkmetric.NewView(criteria, stream)))
	}
	return options
}
