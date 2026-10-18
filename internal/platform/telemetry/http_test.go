package telemetry

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
)

func TestHTTPMiddleware(t *testing.T) {
	_, exporter, reader := testProviders(t)
	const parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	for _, listener := range []string{"public", "internal", "stream"} {
		for _, status := range []int{200, 404, 503} {
			mux := http.NewServeMux()
			mux.HandleFunc("POST /invitations/{token}", func(w http.ResponseWriter, r *http.Request) {
				sc := trace.SpanContextFromContext(r.Context())
				require.Empty(t, sc.TraceState().String())
				out := propagation.MapCarrier{}
				TraceparentOnly{}.Inject(r.Context(), out)
				require.NotContains(t, out, "tracestate")
				SetRoute(r.Context(), "POST /invitations/{token}:accept")
				SetTenant(r.Context(), "tenant")
				w.WriteHeader(status)
			})
			r := httptest.NewRequest("POST", "/invitations/ORBIT-CANARY-PATH?token=ORBIT-CANARY-QUERY", nil)
			r.Header.Set("traceparent", parent)
			r.Header.Set("tracestate", "vendor=ORBIT-CANARY-STATE")
			r.Header.Set("Authorization", "Bearer ORBIT-CANARY-AUTH")
			HTTPMiddleware(listener, mux).ServeHTTP(httptest.NewRecorder(), r)
			spans := exporter.GetSpans()
			span := spans[len(spans)-1]
			if listener == "public" {
				require.Equal(t, "POST /invitations/{token}:accept", span.Name)
			} else {
				require.Equal(t, "POST /invitations/{token}", span.Name)
			}
			if listener == "internal" {
				require.Equal(t, "0123456789abcdef0123456789abcdef", span.SpanContext.TraceID().String())
				require.Equal(t, "0123456789abcdef", span.Parent.SpanID().String())
			} else {
				require.NotEqual(t, "0123456789abcdef0123456789abcdef", span.SpanContext.TraceID().String())
			}
			if status >= 500 {
				require.Equal(t, codes.Error, span.Status.Code)
			} else {
				require.Equal(t, codes.Unset, span.Status.Code)
			}
			raw, err := json.Marshal(span)
			require.NoError(t, err)
			require.NotContains(t, string(raw), "ORBIT-CANARY")
		}
	}
	HTTPMiddleware("public", http.NotFoundHandler()).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/missing", nil))
	spans := exporter.GetSpans()
	require.Equal(t, "GET _unmatched", spans[len(spans)-1].Name)
	var data metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(t.Context(), &data))
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "orbit.http.server.request.duration" {
				continue
			}
			for _, point := range m.Data.(metricdata.Histogram[float64]).DataPoints {
				require.Equal(t, 4, point.Attributes.Len())
				require.Empty(t, point.Exemplars)
			}
		}
	}
}
