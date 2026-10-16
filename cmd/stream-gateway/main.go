package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/config"
	orbitlog "github.com/mindreon/orbit-control/internal/platform/log"
	"github.com/mindreon/orbit-control/internal/platform/natsx"
	"github.com/mindreon/orbit-control/internal/streamgw"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	c, err := config.LoadStreamGateway()
	if err != nil {
		return err
	}
	verifier, err := auth.NewStreamVerifier(c.PublicKeys)
	if err != nil {
		return err
	}
	bus, err := natsx.Connect(c.NATSURL, "stream-gateway")
	if err != nil {
		return fmt.Errorf("gateway NATS connection failed")
	}
	defer bus.Close()
	logger := orbitlog.New(os.Stdout, c.LogFormat)
	gateway := streamgw.New(bus, verifier, streamgw.Config{
		MaxConnections: c.MaxConnections, PerTenant: c.PerTenant, PerSubject: c.PerSubject,
		BufferFrames: c.BufferFrames, BufferBytes: c.BufferBytes, Heartbeat: c.Heartbeat, WriteTimeout: c.WriteTimeout,
		ShutdownGrace: c.ShutdownGrace, MaxAge: c.MaxAge, InfoCache: c.InfoCache,
	}, logger)
	server := &http.Server{Addr: c.Addr, Handler: gateway.Router(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: time.Minute}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
	}
	gateway.BeginShutdown()
	shutdown, cancel := context.WithTimeout(context.Background(), c.ShutdownGrace+5*time.Second)
	defer cancel()
	httpDone := make(chan error, 1)
	go func() { httpDone <- server.Shutdown(shutdown) }()
	err = gateway.Shutdown(shutdown)
	return errors.Join(err, <-httpDone)
}
