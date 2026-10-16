package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// StreamGateway contains only public verification keys and NATS/HTTP settings.
// It deliberately does not load control's database, KEK or signing configuration.
type StreamGateway struct {
	Addr, NATSURL, LogFormat                       string
	PublicKeys                                     map[string][]byte
	MaxConnections, PerTenant, PerSubject          int
	BufferFrames, BufferBytes                      int
	Heartbeat, WriteTimeout, ShutdownGrace, MaxAge time.Duration
	InfoCache                                      time.Duration
}

func LoadStreamGateway() (StreamGateway, error) {
	c := StreamGateway{Addr: env("ORBIT_STREAM_ADDR", "0.0.0.0:8082"), NATSURL: os.Getenv("ORBIT_NATS_URL"),
		LogFormat: env("ORBIT_LOG_FORMAT", "json")}
	if err := validateNATSURL(c.NATSURL); err != nil {
		return c, err
	}
	var err error
	c.PublicKeys, err = parseKeyList("ORBIT_STREAM_PUBLIC_KEYS")
	if err != nil {
		return c, err
	}
	if len(c.PublicKeys) == 0 {
		return c, fmt.Errorf("ORBIT_STREAM_PUBLIC_KEYS required")
	}
	for _, v := range []struct {
		name string
		out  *int
		def  int
	}{
		{name: "ORBIT_STREAM_MAX_CONNECTIONS", out: &c.MaxConnections, def: 10000},
		{name: "ORBIT_STREAM_MAX_CONNECTIONS_PER_TENANT", out: &c.PerTenant, def: 1000},
		{name: "ORBIT_STREAM_MAX_CONNECTIONS_PER_SUBJECT", out: &c.PerSubject, def: 50},
		{name: "ORBIT_STREAM_BUFFER_FRAMES", out: &c.BufferFrames, def: 256},
		{name: "ORBIT_STREAM_BUFFER_BYTES", out: &c.BufferBytes, def: 4 << 20},
	} {
		*v.out = v.def
		if raw := os.Getenv(v.name); raw != "" {
			*v.out, err = strconv.Atoi(raw)
			if err != nil || *v.out < 1 {
				return c, fmt.Errorf("%s must be positive", v.name)
			}
		}
	}
	if c.BufferBytes < 64<<10 {
		return c, fmt.Errorf("ORBIT_STREAM_BUFFER_BYTES must be at least 65536")
	}
	for _, v := range []struct {
		name string
		out  *time.Duration
		def  time.Duration
	}{
		{name: "ORBIT_STREAM_HEARTBEAT", out: &c.Heartbeat, def: 15 * time.Second},
		{name: "ORBIT_STREAM_WRITE_TIMEOUT", out: &c.WriteTimeout, def: 10 * time.Second},
		{name: "ORBIT_STREAM_SHUTDOWN_GRACE", out: &c.ShutdownGrace, def: 5 * time.Second},
		{name: "ORBIT_STREAM_MAX_CONNECTION_AGE", out: &c.MaxAge, def: time.Hour},
		{name: "ORBIT_STREAM_INFO_CACHE", out: &c.InfoCache, def: 2 * time.Second},
	} {
		*v.out = v.def
		if raw := os.Getenv(v.name); raw != "" {
			*v.out, err = time.ParseDuration(raw)
			if err != nil || *v.out <= 0 {
				return c, fmt.Errorf("%s must be a positive duration", v.name)
			}
		}
	}
	return c, nil
}
