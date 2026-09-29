package httpapi

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/mindreon/orbit-control/internal/config"
	"github.com/mindreon/orbit-control/internal/internalauth"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// forwardedHeader marks an ingest request that already went through one hop, so it is never forwarded twice.
const forwardedHeader = "X-Orbit-Ingest-Forwarded"

// ephemeralForwarder hands a worker's live event to the control replica that owns its task (09 §4.1). The ephemeral
// ring buffer lives in the owner's memory, and the owner is the replica the task's SSE clients are routed to; a worker
// may post to any replica. Delivery is best effort: a failed hop drops the event.
type ephemeralForwarder struct {
	self     string
	resolve  func() []string
	internal func(member string) string
	client   *http.Client

	mu      sync.Mutex
	ringKey string
	ring    *taskruntime.Ring
}

// newEphemeralForwarder returns nil when this is a single replica, which needs no forwarding.
func newEphemeralForwarder(opts Options) *ephemeralForwarder {
	resolve := memberResolver(opts)
	if resolve == nil {
		members := opts.TaskMembers
		resolve = func() []string { return members }
	}
	if len(resolve()) < 2 || opts.TaskMemberID == "" {
		return nil
	}
	return &ephemeralForwarder{
		self:     opts.TaskMemberID,
		resolve:  resolve,
		internal: internalAddressOf,
		client:   &http.Client{Timeout: 2 * time.Second},
	}
}

// internalAddressOf maps a member's public URL to its internal listener: the member's host with the internal port,
// or, when ORBIT_CONTROL_INTERNAL_MEMBERS lists the internal URLs in the same order as the members, that entry.
func internalAddressOf(member string) string {
	cfg := config.Load()
	if len(cfg.InternalMembers) > 0 {
		for index, public := range cfg.TaskMembers {
			if public == member && index < len(cfg.InternalMembers) {
				return cfg.InternalMembers[index]
			}
		}
	}
	public, err := url.Parse(member)
	if err != nil || public.Host == "" {
		return ""
	}
	port := "8081"
	if addr := cfg.InternalAddr; addr != "" {
		if index := strings.LastIndex(addr, ":"); index >= 0 && index+1 < len(addr) {
			port = addr[index+1:]
		}
	}
	return public.Scheme + "://" + public.Hostname() + ":" + port
}

func (f *ephemeralForwarder) ringFor(members []string) *taskruntime.Ring {
	key := strings.Join(members, ",")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ring == nil || f.ringKey != key {
		f.ring = taskruntime.NewRing(64)
		f.ring.SetMembers(members)
		f.ringKey = key
	}
	return f.ring
}

// remoteOwner is the internal URL of the replica that owns taskID, when that is not this one.
func (f *ephemeralForwarder) remoteOwner(taskID string) (string, bool) {
	members := f.resolve()
	if len(members) < 2 {
		return "", false
	}
	owner, ok := f.ringFor(members).Owner(taskID)
	if !ok {
		return "", false
	}
	ownerURL, err := url.Parse(owner)
	if err != nil {
		return "", false
	}
	host := ownerURL.Hostname()
	if host == f.self || strings.HasPrefix(host, f.self+".") || owner == f.self {
		return "", false
	}
	target := f.internal(owner)
	return target, target != ""
}

// forward posts the event to its owner. It reports whether the event was handed over (or dropped on purpose).
func (f *ephemeralForwarder) forward(ctx context.Context, r *http.Request, taskID string, raw []byte) bool {
	if r.Header.Get(forwardedHeader) == "1" {
		return false
	}
	target, remote := f.remoteOwner(taskID)
	if !remote {
		return false
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target+"/internal/events", bytes.NewReader(raw))
	if err != nil {
		return true
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(forwardedHeader, "1")
	request.Header.Set("Authorization", "Bearer "+internalauth.Token())
	if response, err := f.client.Do(request); err == nil {
		_ = response.Body.Close()
	}
	return true
}
