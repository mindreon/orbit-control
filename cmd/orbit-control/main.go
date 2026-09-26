package main

import (
	"log"
	"net/http"
	"os"

	"github.com/mindreon/orbit-control/internal/httpapi"
)

func main() {
	addr := listenAddr()
	internalAddr := internalListenAddr()
	public, internal := httpapi.Handlers()
	go func() {
		log.Printf("orbit-control internal listener on %s (/internal/*)", internalAddr)
		if err := http.ListenAndServe(internalAddr, internal); err != nil {
			log.Fatal(err)
		}
	}()
	log.Printf("orbit-control listening on %s (rooms + HITL; TEMPORAL_ADDRESS enables RoomWorkflow)", addr)
	if err := http.ListenAndServe(addr, public); err != nil {
		log.Fatal(err)
	}
}

func listenAddr() string {
	if port := os.Getenv("PORT"); port != "" {
		return ":" + port
	}
	return ":8080"
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
