package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"

	releaseactivities "github.com/mindreon/orbit-control/internal/activities/assetrelease"
	"github.com/mindreon/orbit-control/internal/activities/ctl"
	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/kernelapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	orbitlog "github.com/mindreon/orbit-control/internal/platform/log"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
	"github.com/mindreon/orbit-control/internal/platform/temporalx"
	"github.com/mindreon/orbit-control/internal/runworker"
	"github.com/mindreon/orbit-control/internal/secrets"
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
	bus, err := natsx.Connect(c.NATSURL)
	if err != nil {
		return err
	}
	defer bus.Close()
	if os.Args[1] != "api" {
		cleanup, err := startWorkers(ctx, d, c, l, assetService, store, tc, bus)
		if err != nil {
			return err
		}
		defer cleanup()
		if os.Args[1] == "worker" {
			<-ctx.Done()
			return nil
		}
	}
	return serveAPI(ctx, startup, d, c, l, assetService, store, tc)
}

func serveAPI(
	ctx, startup context.Context,
	d *db.DB,
	c config.Config,
	l *slog.Logger,
	assetService *assets.Service,
	store *objstore.S3,
	tc client.Client,
) error {
	a, e := auth.New(startup, d, c, l)
	if e != nil {
		return e
	}
	defer a.Close()
	h := &httpapi.Handler{
		DB:       d,
		Auth:     a,
		Config:   c,
		Logger:   l,
		Assets:   assetService,
		Objects:  store,
		Temporal: temporalx.Service{Client: tc},
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
			Signer: a.Capability,
			Secrets: secrets.Keys{Current: c.SecretsKeyVersion,
				KEKs: c.SecretsKEKs},
			Obj:                store,
			ServiceTokenSHA256: c.WorkerServiceTokenSHA256}).Router(),

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
) (func(), error) {

	if err := bus.EnsureRunEventsStream(); err != nil {
		return nil, err
	}
	worker := runworker.New(
		tc,
		&ctl.Activities{
			DB: d, Obj: store, NATS: bus, Temporal: tc, Environment: c.Environment,
			SandboxProvider: contract.SandboxProvider(c.SandboxProvider),
		},
	)
	releaseactivities.Register(worker, &releaseactivities.Activities{DB: d, Assets: assetService})
	if err := worker.Start(); err != nil {
		return nil, err
	}
	dispatchCtx, cancelDispatch := context.WithCancel(ctx)
	dispatchDone := make(chan struct{})
	cleanup := func() {
		cancelDispatch()
		<-dispatchDone
		worker.Stop()
	}
	go func() {
		defer close(dispatchDone)
		if err := releaseactivities.Dispatch(dispatchCtx, d, tc, l); err != nil {
			l.Error("asset dispatcher stopped", "error_type", fmt.Sprintf("%T", err))
		}
	}()
	return cleanup, nil
}
