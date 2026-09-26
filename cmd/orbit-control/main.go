package main

import (
	"log"
	"net/http"
	"os"

	"github.com/mindreon/orbit-control/internal/httpapi"
)

func main() {
	addr := listenAddr()
	h, closeStore, err := httpapi.Handler()
	if err != nil {
		log.Fatalf("orbit-control: %v", err)
	}
	defer closeStore()
	log.Printf("orbit-control listening on %s (rooms + HITL; TEMPORAL_ADDRESS enables RoomWorkflow)", addr)
	if err := http.ListenAndServe(addr, h); err != nil {
		log.Fatal(err)
	}
}

func listenAddr() string {
	if port := os.Getenv("PORT"); port != "" {
		return ":" + port
	}
	return ":8080"
}
