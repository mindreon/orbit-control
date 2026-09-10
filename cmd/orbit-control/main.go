package main

import (
	"log"
	"net/http"
	"os"

	"github.com/mindreon/orbit-control/internal/httpapi"
)

func main() {
	addr := listenAddr()
	log.Printf("orbit-control listening on %s (W1 rooms + HITL via orbit-worker)", addr)
	if err := http.ListenAndServe(addr, httpapi.Handler()); err != nil {
		log.Fatal(err)
	}
}

func listenAddr() string {
	if port := os.Getenv("PORT"); port != "" {
		return ":" + port
	}
	return ":8080"
}
