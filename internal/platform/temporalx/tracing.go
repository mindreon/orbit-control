package temporalx

import (
	"go.opentelemetry.io/otel"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"

	"github.com/mindreon/orbit-control/internal/platform/telemetry"
	"github.com/mindreon/orbit-control/internal/platform/telemetry/wfctx"
)

func TemporalTracing() interceptor.Interceptor {
	tracing, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{
		Tracer: otel.Tracer("orbit-control/temporal"), TextMapPropagator: telemetry.TraceparentOnly{},
		SpanContextKey: wfctx.SpanContextKey, HeaderKey: "_tracer-data", DisableQueryTracing: true, DisableBaggage: true,
	})
	if err != nil {
		panic(err) // The fixed options above cannot fail validation.
	}
	return tracing
}

func TemporalMetrics() client.MetricsHandler {
	return opentelemetry.NewMetricsHandler(opentelemetry.MetricsHandlerOptions{Meter: otel.Meter("temporal-sdk")})
}
