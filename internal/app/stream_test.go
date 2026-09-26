package app

import (
	"errors"
	"testing"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/worker"
)

// Acceptance 2 (persistence): the per-room sequence is recovered from the
// audit log, so a new process never reissues an id.
func TestSequencePersistsAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	roomID := "rm_seq"
	boot := func() *App {
		a := New(worker.New(""))
		a.Store = store.New(dir)
		a.Rooms[roomID] = &Room{ID: roomID, PermissionPreset: PermissionWorkspaceWrite, State: RoomRunning}
		return a
	}

	first := boot()
	for i := 0; i < 3; i++ {
		first.Publish(roomID, Event{"type": "room.steered", "roomId": roomID})
	}

	second := boot()
	second.Publish(roomID, Event{"type": "room.steered", "roomId": roomID})
	items := second.ListActivity(roomID)
	if len(items) != 1 || items[0].Sequence != 4 {
		t.Fatalf("after restart = %+v, want sequence 4", items)
	}

	var cursorErr *CursorError
	if _, _, err := second.EventsAfter(roomID, 1); !errors.As(err, &cursorErr) || cursorErr.Reason != ResetExpired {
		t.Fatalf("cursor before retained window: err = %v", err)
	}
}
