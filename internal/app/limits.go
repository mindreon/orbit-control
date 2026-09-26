package app

import (
	"log"
	"os"
	"strconv"
	"time"
)

// Limits bounds what one control process spends on SSE streams, worker
// ingest, and the event logs of closed rooms.
type Limits struct {
	// MaxStreamsPerRoom and MaxStreamsPerClient cap concurrent SSE streams;
	// a client is the connection's remote IP (no proxy headers are trusted).
	MaxStreamsPerRoom   int
	MaxStreamsPerClient int
	// IngestMaxBytes caps one POST /internal/events body.
	IngestMaxBytes int64
	// SSEWriteTimeout bounds each SSE write, so a stalled reader cannot pin a
	// handler forever.
	SSEWriteTimeout time.Duration
	// MaxConsecutiveLags is how many buffer overflows in a row a stream
	// recovers from history before it sends reset "lagging" and closes.
	MaxConsecutiveLags int
	// ClosedRoomLogTTL is how long a closed room's event log stays readable
	// before it is freed.
	ClosedRoomLogTTL time.Duration
}

func DefaultLimits() Limits {
	return Limits{
		MaxStreamsPerRoom:   32,
		MaxStreamsPerClient: 16,
		IngestMaxBytes:      1 << 20,
		SSEWriteTimeout:     10 * time.Second,
		MaxConsecutiveLags:  3,
		ClosedRoomLogTTL:    15 * time.Minute,
	}
}

// LimitsFromEnv applies ORBIT_SSE_MAX_STREAMS_PER_ROOM,
// ORBIT_SSE_MAX_STREAMS_PER_CLIENT, ORBIT_INGEST_MAX_BYTES,
// ORBIT_SSE_WRITE_TIMEOUT, ORBIT_SSE_MAX_CONSECUTIVE_LAGS, and
// ORBIT_CLOSED_ROOM_LOG_TTL over the defaults. Invalid values keep the default.
func LimitsFromEnv() Limits {
	l := DefaultLimits()
	envInt("ORBIT_SSE_MAX_STREAMS_PER_ROOM", &l.MaxStreamsPerRoom)
	envInt("ORBIT_SSE_MAX_STREAMS_PER_CLIENT", &l.MaxStreamsPerClient)
	envInt("ORBIT_SSE_MAX_CONSECUTIVE_LAGS", &l.MaxConsecutiveLags)
	var ingest int
	if envInt("ORBIT_INGEST_MAX_BYTES", &ingest) {
		l.IngestMaxBytes = int64(ingest)
	}
	envDuration("ORBIT_SSE_WRITE_TIMEOUT", &l.SSEWriteTimeout)
	envDuration("ORBIT_CLOSED_ROOM_LOG_TTL", &l.ClosedRoomLogTTL)
	return l
}

func envInt(name string, dst *int) bool {
	raw := os.Getenv(name)
	if raw == "" {
		return false
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		log.Printf("ignoring %s=%q: want a positive integer", name, raw)
		return false
	}
	*dst = v
	return true
}

func envDuration(name string, dst *time.Duration) {
	raw := os.Getenv(name)
	if raw == "" {
		return
	}
	v, err := time.ParseDuration(raw)
	if err != nil || v < 0 {
		log.Printf("ignoring %s=%q: want a duration such as 10s", name, raw)
		return
	}
	*dst = v
}
