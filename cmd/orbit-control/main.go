package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/mindreon/orbit-control/internal/httpapi"
)

func main() {
	addr := publicListenAddr()
	if err := checkPublicBind(addr); err != nil {
		log.Fatal(err)
	}
	internalAddr := internalListenAddr()
	public, internal := httpapi.Handlers()
	go func() {
		log.Printf("orbit-control internal listener on %s (/internal/*)", internalAddr)
		if err := server(internalAddr, internal).ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
	log.Printf("orbit-control listening on %s (rooms + HITL; TEMPORAL_ADDRESS enables RoomWorkflow)", addr)
	if err := server(addr, public).ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

// server has no overall write timeout because SSE streams are long-lived;
// the SSE handler sets a deadline on each write instead.
func server(addr string, h http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: h, ReadHeaderTimeout: 10 * time.Second}
}

// publicListenAddr is ORBIT_PUBLIC_ADDR, else 127.0.0.1:$PORT (default 8080).
func publicListenAddr() string {
	if addr := os.Getenv("ORBIT_PUBLIC_ADDR"); addr != "" {
		return addr
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	return "127.0.0.1:" + port
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
	if os.Getenv("ORBIT_ALLOW_UNAUTHENTICATED_BIND") == "1" {
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

// internalListenAddr defaults to loopback so /internal/* is never reachable
// from outside the host unless a deployment opts in (e.g. ":8081" on a
// private network).
func internalListenAddr() string {
	if addr := os.Getenv("ORBIT_INTERNAL_ADDR"); addr != "" {
		return addr
	}
	return "127.0.0.1:8081"
}
