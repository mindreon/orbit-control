package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	orbitlog "github.com/mindreon/orbit-control/internal/platform/log"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
	"github.com/mindreon/orbit-control/internal/workflows/assetrelease"
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
	if os.Args[1] == "worker" || os.Args[1] == "all" {
		tc, err := assetrelease.Dial(c)
		if err != nil {
			return err
		}
		defer tc.Close()
		worker := assetrelease.NewWorker(tc, &assetrelease.Activities{DB: d, Assets: assetService})
		if err = worker.Start(); err != nil {
			return err
		}
		dispatchCtx, cancelDispatch := context.WithCancel(ctx)
		dispatchDone := make(chan struct{})
		defer func() { cancelDispatch(); <-dispatchDone; worker.Stop() }()
		go func() {
			defer close(dispatchDone)
			if err := assetrelease.Dispatch(dispatchCtx, d, tc, l); err != nil {
				l.Error("asset dispatcher stopped", "error_type", fmt.Sprintf("%T", err))
			}
		}()
		if os.Args[1] == "worker" {
			<-ctx.Done()
			return nil
		}
	}
	a, e := auth.New(startup, d, c, l)
	if e != nil {
		return e
	}
	defer a.Close()
	h := &httpapi.Handler{DB: d, Auth: a, Config: c, Logger: l, Assets: assetService}
	server := &http.Server{Addr: c.Addr, Handler: h.Router(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	l.Info("API listening", "address", c.Addr)
	select {
	case e = <-done:
		if e == http.ErrServerClosed {
			return nil
		}
		return e
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}
