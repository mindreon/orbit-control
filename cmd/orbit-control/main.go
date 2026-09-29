package main

import (
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/mindreon/orbit-control/internal/config"
	"github.com/mindreon/orbit-control/internal/httpapi"
)

// installLogger routes every log.Printf of the process through slog. ORBIT_LOG_FORMAT=json makes the lines JSON, which a
// log pipeline can index (tenant, task and the rest of a message stay in `msg`).
func installLogger() {
	options := &slog.HandlerOptions{Level: slog.LevelInfo}
	var handler slog.Handler = slog.NewTextHandler(os.Stderr, options)
	if config.Load().LogFormat == "json" {
		handler = slog.NewJSONHandler(os.Stderr, options)
	}
	slog.SetDefault(slog.New(handler))
}

func main() {
	installLogger()
	cfg := config.Load()
	addr := cfg.PublicListenAddr()
	if err := checkPublicBind(addr); err != nil {
		log.Fatal(err)
	}
	internalAddr := cfg.InternalListenAddr()
	public, internal, closeStore, err := httpapi.Handlers()
	if err != nil {
		log.Fatalf("orbit-control: %v", err)
	}
	defer closeStore()
	go func() {
		log.Printf("orbit-control internal listener on %s (/internal/*)", internalAddr)
		if err := server(internalAddr, internal).ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
	log.Printf("orbit-control listening on %s (tasks; TEMPORAL_ADDRESS enables TaskWorkflow)", addr)
	if err := server(addr, public).ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

// server has no overall write timeout because SSE streams are long-lived;
// the SSE handler sets a deadline on each write instead.
func server(addr string, h http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 10 * time.Second}
}

// checkPublicBind is the deploy gate for H1: until user auth (§17) lands,
// every /v1 path, including SSE replay, is unauthenticated, so the public
// listener may only bind loopback. ORBIT_ALLOW_UNAUTHENTICATED_BIND=1 lifts it
// for environments that are not externally reachable (e.g. a local Compose
// network); production must never set it.
func checkPublicBind(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("public listen address %q: %w", addr, err)
	}
	if isLoopbackHost(host) {
		return nil
	}
	if config.Load().AllowUnauthenticatedBind {
		log.Printf("WARNING: public listener %s is not loopback and control has no user auth (§17); "+
			"ORBIT_ALLOW_UNAUTHENTICATED_BIND=1 is only for environments that are not externally reachable", addr)
		return nil
	}
	return fmt.Errorf("refusing to bind the public listener to %s: control has no user auth yet (§17), "+
		"so it must not be externally reachable; bind loopback (the default), or set "+
		"ORBIT_ALLOW_UNAUTHENTICATED_BIND=1 only where the address is not reachable from outside", addr)
}

func isLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
