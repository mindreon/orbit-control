package streamgw

import (
	"fmt"
	"net/http"
	"slices"
	"sync"
)

type metrics struct {
	mu     sync.Mutex
	values map[string]uint64
}

func (m *metrics) add(name string, n uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.values[name] += n
}

func (s *Server) serveMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	s.limits.mu.Lock()
	for _, scope := range []string{"run", "session"} {
		fmt.Fprintf(w, "streamgw_connections{scope=%q} %d\n", scope, s.limits.scopes[scope])
	}
	s.limits.mu.Unlock()
	s.metrics.mu.Lock()
	defer s.metrics.mu.Unlock()
	keys := make([]string, 0, len(s.metrics.values))
	for name := range s.metrics.values {
		keys = append(keys, name)
	}
	slices.Sort(keys)
	for _, name := range keys {
		fmt.Fprintf(w, "%s %d\n", name, s.metrics.values[name])
	}
}
