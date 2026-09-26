package app

import (
	"errors"
	"testing"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/worker"
)

// Acceptance 2 (persistence): event ids come from one global sequence that is
// recovered from the audit logs, so a new process never reissues an id.
func TestGlobalEventSequencePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	boot := func() *App {
		a := New(worker.New(""))
		a.Store = store.New(dir)
		for _, id := range []string{"rm_a", "rm_b"} {
			a.Rooms[id] = &Room{ID: id, PermissionPreset: PermissionWorkspaceWrite, State: RoomRunning}
		}
		return a
	}
	publish := func(a *App, roomID string) uint64 {
		a.Publish(roomID, Event{"type": "room.steered", "roomId": roomID})
		items := a.ListActivity(roomID)
		return items[len(items)-1].Sequence
	}

	first := boot()
	var got []uint64
	for _, roomID := range []string{"rm_a", "rm_b", "rm_a", "rm_b", "rm_b"} {
		got = append(got, publish(first, roomID))
	}
	for i, id := range got {
		if id != uint64(i+1) {
			t.Fatalf("ids across rooms = %v, want one shared sequence 1..5", got)
		}
	}

	second := boot()
	if id := publish(second, "rm_a"); id != 6 {
		t.Fatalf("after restart rm_a got id %d, want 6", id)
	}

	var cursorErr *CursorError
	if _, _, err := second.EventsAfter("rm_a", 1); !errors.As(err, &cursorErr) || cursorErr.Reason != ResetExpired {
		t.Fatalf("rm_a cursor evicted by restart: err = %v, want expired", err)
	}
	if _, _, err := second.EventsAfter("rm_b", 1); !errors.As(err, &cursorErr) || cursorErr.Reason != ResetUnknown {
		t.Fatalf("rm_b with rm_a's id: err = %v, want unknown", err)
	}
}
