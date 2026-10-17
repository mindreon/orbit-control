package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"go.temporal.io/sdk/client"

	releaseactivities "github.com/mindreon/orbit-control/internal/activities/assetrelease"
	"github.com/mindreon/orbit-control/internal/activities/ctl"
	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/kernelapi"
	"github.com/mindreon/orbit-control/internal/locks"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	orbitlog "github.com/mindreon/orbit-control/internal/platform/log"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
	"github.com/mindreon/orbit-control/internal/platform/temporalx"
	"github.com/mindreon/orbit-control/internal/runworker"
	"github.com/mindreon/orbit-control/internal/schedules"
	"github.com/mindreon/orbit-control/internal/secrets"
	"github.com/mindreon/orbit-control/internal/sessions"
	"github.com/mindreon/orbit-control/internal/sink"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) != 2 || (os.Args[1] != "api" && os.Args[1] != "worker" && os.Args[1] != "all") {
		return fmt.Errorf("usage: orbit-control api|worker|all")
	}
	c, e := config.Load()
	if e != nil {
		return e
	}
	if os.Args[1] != "worker" {
		if e = c.ValidateAPI(); e != nil {
			return e
		}
	}
	if os.Args[1] != "api" {
		if e = c.ValidateWorker(); e != nil {
			return e
		}
	}
	if e = c.ValidateAssets(); e != nil {
		return e
	}
	if e = c.ValidateRuns(os.Args[1] != "worker"); e != nil {
		return e
	}
	l := orbitlog.New(os.Stdout, c.LogFormat)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	startup, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	d, e := db.OpenApp(startup, c.DatabaseURL)
	if e != nil {
		return e
	}
	defer d.Close()
	store, e := objstore.New(c.ObjectEndpoint, c.ObjectBucket, c.ObjectAccessKey, c.ObjectSecretKey, c.ObjectTLS)
	if e != nil {
		return e
	}
	signer, e := assets.LoadSigner(c.AssetKeyFile, c.AssetKeyID, c.AuthMode == "dev", c.AssetPublicKeys)
	if e != nil {
		return e
	}
	assetService := &assets.Service{Store: store, Signer: signer}
	tc, err := temporalx.Dial(c)
	if err != nil {
		return err
	}
	defer tc.Close()
	bus, err := natsx.Connect(c.NATSURL, "orbit-control")
	if err != nil {
		return err
	}
	defer bus.Close()
	metrics, err := sink.NewMetrics(nil)
	if err != nil {
		return err
	}
	if os.Args[1] != "api" {
		workerCtx, cancelWorker := context.WithCancel(context.WithoutCancel(ctx))
		defer cancelWorker()
		cleanup, err := startWorkers(workerCtx, d, c, l, assetService, store, tc, bus, metrics)
		if err != nil {
			return err
		}
		defer cleanup()
		if os.Args[1] == "worker" {
			<-ctx.Done()
			return nil
		}
	}
	return serveAPI(ctx, startup, d, c, l, assetService, store, tc, bus)
}

func serveAPI(
	ctx, startup context.Context,
	d *db.DB,
	c config.Config,
	l *slog.Logger,
	assetService *assets.Service,
	store *objstore.S3,
	tc client.Client,
	bus *natsx.Bus,
) error {
	floor, e := natsx.NewEventFloor(bus)
	if e != nil {
		return e
	}
	a, e := auth.New(startup, d, c, l)
	if e != nil {
		return e
	}
	defer a.Close()
	h := &httpapi.Handler{
		Schedules: &schedules.Service{DB: d, Locks: locks.NewResolver(assetService), Temporal: tc.ScheduleClient(),
			DefaultLimit: c.SchedulesDefaultLimit, SweepInterval: c.ScheduleSweepInterval},
		EventFloor: floor,
		DB:         d,
		Auth:       a,
		Config:     c,
		Logger:     l,
		Assets:     assetService,
		Objects:    store,
		Temporal:   temporalx.Service{Client: tc},
	}
	server := &http.Server{
		Addr:              c.Addr,
		Handler:           h.Router(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	internal := &http.Server{
		Addr: c.InternalAddr,
		Handler: (&kernelapi.Handler{DB: d,
			Temporal: temporalx.Service{Client: tc}, Schedules: h.Schedules,
			Signer:  a.Capability,
			Secrets: secrets.Keys{Current: c.SecretsKeyVersion, KEKs: c.SecretsKEKs},
			Obj:     store, ServiceTokenSHA256: c.WorkerServiceTokenSHA256}).Router(),

		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = internal.Shutdown(shutdown)
		_ = server.Shutdown(shutdown)
	}()
	done := make(chan error, 2)
	go func() { done <- internal.ListenAndServe() }()
	go func() { done <- server.ListenAndServe() }()
	l.Info("API listening", "address", c.Addr)
	select {
	case e := <-done:
		if e == http.ErrServerClosed {
			return nil
		}
		return e
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return errors.Join(server.Shutdown(shutdown), internal.Shutdown(shutdown))
	}
}

func startWorkers(
	ctx context.Context,
	d *db.DB,
	c config.Config,
	l *slog.Logger,
	assetService *assets.Service,
	store *objstore.S3,
	tc client.Client,
	bus *natsx.Bus,
	metrics *sink.Metrics,
) (func(), error) {

	if err := bus.EnsureRunEventsStream(ctx, c.NATSStreamMaxBytes); err != nil {
		return nil, err
	}
	var consumer jetstream.Consumer
	if c.SinkEnabled {
		var err error
		consumer, err = bus.EnsureSinkConsumer(ctx)
		if err != nil {
			return nil, err
		}
	}
	scheduleService := &schedules.Service{DB: d, Locks: locks.NewResolver(assetService), Temporal: tc.ScheduleClient(),
		DefaultLimit: c.SchedulesDefaultLimit, SweepInterval: c.ScheduleSweepInterval}
	activities := &ctl.Activities{DB: d, Obj: store, NATS: bus, Temporal: tc, Environment: c.Environment, Logger: l,
		SandboxProvider: contract.SandboxProvider(c.SandboxProvider), Schedules: scheduleService,
		Sessions: sessions.Service{Objects: store, Locks: locks.NewResolver(assetService), DefaultRunBudget: c.DefaultRunBudget}}
	worker := runworker.New(tc, activities)
	waitWorker := runworker.NewWait(tc, activities, c.CtlWaitConcurrency)
	if err := waitWorker.Start(); err != nil {
		return nil, err
	}

	releaseactivities.Register(worker, &releaseactivities.Activities{DB: d, Assets: assetService})
	if err := worker.Start(); err != nil {
		waitWorker.Stop()
		return nil, err
	}
	dispatchCtx, cancelDispatch := context.WithCancel(ctx)
	dispatchDone := make(chan struct{})
	var projections sync.WaitGroup
	projections.Go(func() {
		scheduleService.Sweep(dispatchCtx, func(err error) { l.Warn("schedule synchronization failed", "error_type", fmt.Sprintf("%T", err)) })
	})
	if c.SinkEnabled {
		projection := sink.New(d, consumer, l, metrics)
		projections.Go(func() {
			if err := projection.Run(dispatchCtx); err != nil {
				l.Error("sink stopped", "error_type", fmt.Sprintf("%T", err))
			}
		})
		projections.Go(func() { projection.SamplePending(dispatchCtx) })
		outbox := &sink.Outbox{DB: d, Publisher: bus, Logger: l, Interval: c.OutboxInterval}
		projections.Go(func() { outbox.Run(dispatchCtx) })
	}
	cleanup := func() {
		cancelDispatch()
		<-dispatchDone
		projections.Wait()
		worker.Stop()
		waitWorker.Stop()
	}
	go func() {
		defer close(dispatchDone)
		if err := releaseactivities.Dispatch(dispatchCtx, d, tc, l); err != nil {
			l.Error("asset dispatcher stopped", "error_type", fmt.Sprintf("%T", err))
		}
	}()
	return cleanup, nil
}
