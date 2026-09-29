package httpapi

import (
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskRouterClosesStreamsWhenMembershipChanges(t *testing.T) {
	var changed atomic.Bool
	closed := make(chan struct{}, 1)
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	handler := taskRouterWithResolver(
		next,
		[]string{"http://control-a", "http://control-b"},
		"control-a",
		func() []string {
			if changed.Load() {
				return []string{"http://control-a", "http://control-c"}
			}
			return []string{"http://control-a", "http://control-b"}
		},
		func() { closed <- struct{}{} },
		10*time.Millisecond,
	)
	if handler == nil {
		t.Fatal("router was not created")
	}
	changed.Store(true)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("membership change did not notify subscribers")
	}
}
