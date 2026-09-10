package main

import (
	"log"
	"net/http"
	"os"

	"github.com/mindreon/orbit-control/internal/httpapi"
)

func main() {
	addr := listenAddr()
	log.Printf("orbit-control W0 stub listening on %s (all routes return 501)", addr)
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
