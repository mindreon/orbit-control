package app

import (
	"log"
	"os"
	"strconv"
)

const defaultIngestMaxBytes = 1 << 20

// ingestMaxBytes is ORBIT_INGEST_MAX_BYTES, the cap of one POST /internal/events body. An invalid value keeps the
// default.
func ingestMaxBytes() int64 {
	raw := os.Getenv("ORBIT_INGEST_MAX_BYTES")
	if raw == "" {
		return defaultIngestMaxBytes
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		log.Printf("ignoring ORBIT_INGEST_MAX_BYTES=%q: want a positive integer", raw)
		return defaultIngestMaxBytes
	}
	return v
}
