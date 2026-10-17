package workflows_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

// touchAt sends a lease Touch at the given offset from the start of the workflow.
func (h *leaseHarness) touchAt(after time.Duration, touch w.TouchInput) {
	h.env.RegisterDelayedCallback(func() { h.env.SignalWorkflow(w.Touch, touch) }, after)
}
func (h *leaseHarness) paused() bool {
	for _, call := range h.calls {
		if call == contract.ActivitySandboxPause {
			return true
		}
	}
	return false
}

func TestLeaseStaysBusyUntilEveryHolderReleases(t *testing.T) {
	// T-L1 (F43): the lease is idle only when every holder has released it; a team's members hold it independently.
	run := func(t *testing.T, releaseB bool) *leaseHarness {
		h := newLeaseHarness(t, "opensandbox")
		h.env.RegisterDelayedCallback(func() { h.acquire(t, "initial", false) }, 0)
		// The team workflow acquires as "run" and then releases that holder; members hold the lease themselves.
		h.touchAt(500*time.Millisecond, w.TouchInput{RunID: "run", Holder: "run", Member: "team", InUse: false})
		h.touchAt(time.Second, w.TouchInput{RunID: "run", Holder: "run/a", Member: "a", InUse: true})
		h.touchAt(2*time.Second, w.TouchInput{RunID: "run", Holder: "run/b", Member: "b", InUse: true})
		h.touchAt(3*time.Second, w.TouchInput{RunID: "run", Holder: "run/a", Member: "a", InUse: false})
		if releaseB {
			h.touchAt(10*time.Minute, w.TouchInput{RunID: "run", Holder: "run/b", Member: "b", InUse: false})
		}
		h.archive(20 * time.Minute)
		h.execute(t)
		return h
	}
	t.Run("one holder left", func(t *testing.T) {
		require.False(t, run(t, false).paused(), "run/b still holds the lease")
	})
	t.Run("all released", func(t *testing.T) {
		require.True(t, run(t, true).paused(), "idle timing starts after the last holder releases")
	})
}

func TestLeaseTouchWithoutHolderKeysByRun(t *testing.T) {
	// T-L2: recorded histories send TouchInput{RunID} with no Holder; it must keep releasing by run id.
	h := newLeaseHarness(t, "opensandbox")
	h.env.RegisterDelayedCallback(func() { h.acquire(t, "initial", false) }, 0)
	h.touchAt(time.Second, w.TouchInput{RunID: "run", InUse: true})
	h.touchAt(2*time.Second, w.TouchInput{RunID: "run", Holder: "run", InUse: false})
	h.archive(20 * time.Minute)
	h.execute(t)
	require.True(t, h.paused(), "the legacy touch and the run-keyed holder are the same holder")
}
