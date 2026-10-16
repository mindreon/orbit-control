package streamgw

import "sync"

type limits struct {
	mu                        sync.Mutex
	total                     int
	tenants, subjects, scopes map[string]int
}

func (s *Server) acquire(tenant, subject, scope string) (func(), bool) {
	s.limits.mu.Lock()
	defer s.limits.mu.Unlock()
	l := &s.limits
	if l.total >= s.cfg.MaxConnections || l.tenants[tenant] >= s.cfg.PerTenant || l.subjects[subject] >= s.cfg.PerSubject {
		return nil, false
	}
	l.total++
	l.tenants[tenant]++
	l.subjects[subject]++
	l.scopes[scope]++
	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.total--
		for _, v := range []struct {
			counts map[string]int
			key    string
		}{
			{counts: l.tenants, key: tenant}, {counts: l.subjects, key: subject}, {counts: l.scopes, key: scope},
		} {
			v.counts[v.key]--
			if v.counts[v.key] == 0 {
				delete(v.counts, v.key)
			}
		}
	}, true
}
