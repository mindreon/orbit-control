//go:build nats_integration

package sink

import (
	"context"
	"io"
	"net"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

// A test-owned TCP proxy interrupts only this sink's PG connections. The shared
// v2 PostgreSQL container and every other database/client remain untouched.
type pgProxy struct {
	listener    net.Listener
	target      string
	mu          sync.Mutex
	offline     bool
	connections map[net.Conn]bool
	wg          sync.WaitGroup
}

func startPGProxy(t *testing.T, target string) *pgProxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &pgProxy{listener: l, target: target, connections: map[net.Conn]bool{}}
	p.wg.Go(func() {
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			p.wg.Go(func() { p.forward(client) })
		}
	})
	t.Cleanup(func() {
		require.NoError(t, l.Close())
		p.setOffline(true)
		p.wg.Wait()
	})
	return p
}

func (p *pgProxy) forward(client net.Conn) {
	defer client.Close()
	p.mu.Lock()
	if p.offline {
		p.mu.Unlock()
		return
	}
	p.connections[client] = true
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.connections, client)
		p.mu.Unlock()
	}()
	upstream, err := net.DialTimeout("tcp", p.target, time.Second)
	if err != nil {
		return
	}
	defer upstream.Close()
	done := make(chan struct{})
	go func() {
		io.Copy(upstream, client)
		upstream.Close()
		close(done)
	}()
	io.Copy(client, upstream)
	client.Close()
	<-done
}

func (p *pgProxy) setOffline(value bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.offline = value
	if value {
		for conn := range p.connections {
			conn.Close()
		}
	}
}

func TestDatabaseOutageRecovery(t *testing.T) {
	f := newFixture(t)
	raw, err := url.Parse(os.Getenv("ORBIT_TEST_DATABASE_URL"))
	require.NoError(t, err)
	proxy := startPGProxy(t, raw.Host)
	raw.Host = proxy.listener.Addr().String()
	d, err := db.OpenApp(t.Context(), roleURL(t, raw.String(), "orbit_app"))
	require.NoError(t, err)
	defer d.Close()
	proxy.setOffline(true)
	const count = 12
	for n := range count {
		require.NoError(t, f.bus.Publish(t.Context(), f.subject, ids.New(), f.event(t, n+1, "run.status")))
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	s := New(d, f.consumer, f.logger, testMetrics(t))
	// Short per-attempt network failure; the real 20s outage is controlled by the
	// proxy, not by stopping PostgreSQL or sleeping for a database restart.
	go func() { done <- s.Run(ctx) }()
	defer func() {
		cancel()
		require.NoError(t, <-done)
	}()

	require.Eventually(t, func() bool {
		info, err := f.consumer.Info(t.Context())
		return err == nil && info.NumAckPending > 0
	}, 5*time.Second, 5*time.Millisecond)
	require.Zero(t, f.count(t, "run_events"))
	// Exercise a real 20s outage; unit timing checks use synctest.
	timer := time.NewTimer(20 * time.Second)
	defer timer.Stop()
	<-timer.C
	proxy.setOffline(false)
	require.Eventually(t, func() bool { return f.count(t, "run_events") == count }, 40*time.Second, 20*time.Millisecond)
	require.Zero(t, f.count(t, "run_event_rejects"))
	info, err := f.consumer.Info(t.Context())
	require.NoError(t, err)
	// Backoff keeps each message within three deliveries over this outage.
	require.EqualValues(t, count, info.Delivered.Consumer, "infrastructure outages retain each delivery without Nak")
	// Poll a fresh consumer-info snapshot to establish the durable ack boundary.
	require.Eventually(t, func() bool {
		info, err := f.consumer.Info(t.Context())
		return err == nil && info.NumAckPending == 0
	}, 5*time.Second, 10*time.Millisecond)
}
