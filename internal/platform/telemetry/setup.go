package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/url"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type Config struct {
	Service                 string
	Environment             string
	Endpoint                string
	SampleRatio             float64
	MetricInterval          time.Duration
	TenantLimit, ModelLimit int
}

func FromEnv(service, environment string) (Config, error) {
	c := Config{Service: service, Environment: environment, Endpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		SampleRatio: 1, MetricInterval: 15 * time.Second}
	if value := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); value != "" {
		ratio, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(ratio) || ratio < 0 || ratio > 1 {
			return c, fmt.Errorf("OTEL_TRACES_SAMPLER_ARG must be in [0,1]")
		}
		c.SampleRatio = ratio
	}
	if value := os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"); value != "" {
		ms, err := strconv.ParseInt(value, 10, 64)
		if err != nil || ms <= 0 || ms > math.MaxInt64/int64(time.Millisecond) {
			return c, fmt.Errorf("OTEL_METRIC_EXPORT_INTERVAL must be positive milliseconds")
		}
		c.MetricInterval = time.Duration(ms) * time.Millisecond
	}
	return c, nil
}

func Setup(ctx context.Context, c Config, log *slog.Logger) (func(context.Context) error, error) {
	ConfigureLimits(c.TenantLimit, c.ModelLimit)
	otel.SetTextMapPropagator(TraceparentOnly{})
	if c.Endpoint == "" {
		log.InfoContext(ctx, "telemetry.disabled")
		return func(context.Context) error { return nil }, nil
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil {
		return nil, fmt.Errorf("OTEL_EXPORTER_OTLP_ENDPOINT must be an HTTP(S) URL without credentials")
	}
	traceOptions := []otlptracegrpc.Option{otlptracegrpc.WithEndpointURL(c.Endpoint)}
	metricOptions := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpointURL(c.Endpoint)}
	if u.Scheme == "http" {
		traceOptions = append(traceOptions, otlptracegrpc.WithInsecure())
		metricOptions = append(metricOptions, otlpmetricgrpc.WithInsecure())
	}
	exporter, err := otlptracegrpc.New(ctx, traceOptions...)
	if err != nil {
		return nil, fmt.Errorf("telemetry trace exporter: %T", err)
	}
	metricExporter, err := otlpmetricgrpc.New(ctx, metricOptions...)
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return nil, fmt.Errorf("telemetry metric exporter: %T", err)
	}
	hostname, _ := os.Hostname()
	version := "unknown"
	if build, ok := debug.ReadBuildInfo(); ok {
		version = build.Main.Version
	}
	res := resource.NewSchemaless(attribute.String("service.name", c.Service), attribute.String("service.version", version),
		attribute.String("service.instance.id", hostname), attribute.String("deployment.environment.name", c.Environment))
	limits := sdktrace.NewSpanLimits()
	limits.AttributeCountLimit = 64
	tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(c.SampleRatio))),
		sdktrace.WithRawSpanLimits(limits),
		sdktrace.WithBatcher(AllowlistExporter(exporter), sdktrace.WithMaxQueueSize(2048), sdktrace.WithMaxExportBatchSize(512),
			sdktrace.WithBatchTimeout(5*time.Second), sdktrace.WithExportTimeout(10*time.Second)))
	interval := c.MetricInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	options := append(MeterOptions(), sdkmetric.WithResource(res), sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
		sdkmetric.WithInterval(interval), sdkmetric.WithTimeout(10*time.Second))))
	mp := sdkmetric.NewMeterProvider(options...)
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	var mu sync.Mutex
	var last time.Time
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		mu.Lock()
		defer mu.Unlock()
		if time.Since(last) < time.Minute {
			return
		}
		last = time.Now()
		log.Warn("telemetry.export_failed", "service", c.Service, "endpoint_host", u.Host, "transport", "otlp_grpc",
			"operation", "export", "error_type", fmt.Sprintf("%T", err))
	}))
	return func(ctx context.Context) error { return errors.Join(mp.Shutdown(ctx), tp.Shutdown(ctx)) }, nil
}
