package app

import (
	"errors"
	"testing"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/worker"
)

func cursorReason(err error) ResetReason {
	var cursorErr *CursorError
	if errors.As(err, &cursorErr) {
		return cursorErr.Reason
	}
	return ""
}

// Acceptance 2 and 3: ids come from one process-global counter shared by all
// rooms; a restarted control has forgotten every id and answers with reset.
func TestEventIDsAreGlobalAndUnknownAfterRestart(t *testing.T) {
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
	if _, _, err := first.EventsAfter("rm_b", 1); cursorReason(err) != ResetUnknown {
		t.Fatalf("rm_b with rm_a's id: err = %v, want unknown", err)
	}

	restarted := boot()
	publish(restarted, "rm_b")
	for _, old := range []uint64{got[3], got[4]} {
		if _, _, err := restarted.EventsAfter("rm_b", old); cursorReason(err) != ResetUnknown {
			t.Fatalf("pre-restart id %d: err = %v, want unknown", old, err)
		}
	}
}

func TestMemoryEventLogEvictsPerTaskFromOneSequence(t *testing.T) {
	log := NewMemoryEventLog(2)
	for _, task := range []string{"a", "b", "a", "a", "b"} {
		if _, err := log.Append(task, ActivityEvent{RoomID: task}); err != nil {
			t.Fatal(err)
		}
	}
	events, _ := log.After("a", 0)
	if len(events) != 2 || events[0].Sequence != 3 || events[1].Sequence != 4 {
		t.Fatalf("task a retained %+v, want ids 3,4", events)
	}
	if through, _ := log.EvictedThrough("a"); through != 1 {
		t.Fatalf("task a evicted through %d, want 1", through)
	}
	if ok, _ := log.Contains("a", 2); ok {
		t.Fatal("task a claims task b's id 2")
	}
	if head, _ := log.Head("b"); head != 5 {
		t.Fatalf("task b head %d, want 5", head)
	}
	if last, _ := log.LastID(); last != 5 {
		t.Fatalf("LastID %d, want 5", last)
	}
}
