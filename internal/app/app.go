package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mindreon/orbit-control/internal/orch"
	"github.com/mindreon/orbit-control/internal/worker"
)

type RoomState string

const (
	RoomIdle             RoomState = "idle"
	RoomRunning          RoomState = "running"
	RoomAwaitingApproval RoomState = "awaiting_approval"
	RoomClosed           RoomState = "closed"

	PermissionWorkspaceWrite   = "workspace-write"
	PermissionDangerFullAccess = "danger-full-access"
	maxActivityPerRoom         = 500
)

type RuntimeSnapshot struct {
	Kernel    string `json:"kernel"`
	Protocol  string `json:"protocol"`
	Isolation string `json:"isolation"`
}

type Room struct {
	ID               string          `json:"id"`
	Kind             string          `json:"kind"`
	Title            string          `json:"title"`
	State            RoomState       `json:"state"`
	PermissionPreset string          `json:"permissionPreset"`
	Runtime          RuntimeSnapshot `json:"runtime"`
	SessionID        string          `json:"sessionId,omitempty"`
	CreatedAt        string          `json:"createdAt"`
}

type Message struct {
	ID        string `json:"id"`
	RoomID    string `json:"roomId"`
	Role      string `json:"role"`
	Type      string `json:"type,omitempty"`
	Text      string `json:"text"`
	CreatedAt string `json:"createdAt"`
}

type Approval struct {
	ID                string `json:"id"`
	RoomID            string `json:"roomId"`
	SessionID         string `json:"sessionId"`
	ApprovalRequestID string `json:"approvalRequestId"`
	ToolName          string `json:"toolName"`
	Reason            string `json:"reason,omitempty"`
	Status            string `json:"status"`
	Decision          string `json:"decision,omitempty"`
	CreatedAt         string `json:"createdAt"`
}

type Event map[string]any

type ActivityEvent struct {
	ID                string `json:"id"`
	Sequence          uint64 `json:"sequence"`
	Type              string `json:"type"`
	RoomID            string `json:"roomId"`
	SessionID         string `json:"sessionId,omitempty"`
	TurnID            string `json:"turnId,omitempty"`
	Source            string `json:"source"`
	Runtime           string `json:"runtime,omitempty"`
	Protocol          string `json:"protocol,omitempty"`
	Role              string `json:"role,omitempty"`
	Text              string `json:"text,omitempty"`
	ToolName          string `json:"toolName,omitempty"`
	CallID            string `json:"callId,omitempty"`
	ApprovalID        string `json:"approvalId,omitempty"`
	ApprovalRequestID string `json:"approvalRequestId,omitempty"`
	Reason            string `json:"reason,omitempty"`
	Status            string `json:"status,omitempty"`
	PermissionPreset  string `json:"permissionPreset,omitempty"`
	OccurredAt        string `json:"occurredAt"`
}

type CreateRoomInput struct {
	Kind             string
	Title            string
	PermissionPreset string
}

type App struct {
	mu          sync.Mutex
	Worker      *worker.Client
	Orch        *orch.Client // optional Temporal; nil → direct worker HTTP
	Rooms       map[string]*Room
	Messages    map[string][]Message
	Approvals   map[string]*Approval
	Activity    map[string][]ActivityEvent
	SessionRoom map[string]string
	sequences   map[string]uint64
	subs        map[string]map[chan []byte]struct{}
}

func New(w *worker.Client) *App {
	return NewWithOrch(w, nil)
}

func NewWithOrch(w *worker.Client, o *orch.Client) *App {
	return &App{
		Worker:      w,
		Orch:        o,
		Rooms:       map[string]*Room{},
		Messages:    map[string][]Message{},
		Approvals:   map[string]*Approval{},
		Activity:    map[string][]ActivityEvent{},
		SessionRoom: map[string]string{},
		sequences:   map[string]uint64{},
		subs:        map[string]map[chan []byte]struct{}{},
	}
}

func id(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func (a *App) CreateRoom(ctx context.Context, input CreateRoomInput) (*Room, error) {
	kind := strings.TrimSpace(input.Kind)
	if kind == "" {
		kind = "solo"
	}
	if kind != "solo" && kind != "collab" {
		return nil, fmt.Errorf("kind must be solo or collab")
	}
	permissionPreset := strings.TrimSpace(input.PermissionPreset)
	if permissionPreset == "" {
		permissionPreset = PermissionWorkspaceWrite
	}
	if permissionPreset != PermissionWorkspaceWrite && permissionPreset != PermissionDangerFullAccess {
		return nil, fmt.Errorf("permissionPreset must be workspace-write or danger-full-access")
	}
	room := &Room{
		ID:               id("rm_"),
		Kind:             kind,
		Title:            strings.TrimSpace(input.Title),
		State:            RoomIdle,
		PermissionPreset: permissionPreset,
		Runtime: RuntimeSnapshot{
			Kernel:    "dsh",
			Protocol:  "acp",
			Isolation: "process",
		},
		CreatedAt: now(),
	}
	a.mu.Lock()
	a.Rooms[room.ID] = room
	a.Messages[room.ID] = nil
	a.Activity[room.ID] = nil
	a.mu.Unlock()

	if a.Orch != nil {
		view, err := a.Orch.StartRoom(ctx, room.ID, kind, permissionPreset)
		if err != nil {
			a.mu.Lock()
			room.State = RoomClosed
			a.mu.Unlock()
			return room, fmt.Errorf("temporal StartRoom: %w", err)
		}
		a.mu.Lock()
		room.SessionID = view.SessionID
		room.State = RoomRunning
		a.SessionRoom[view.SessionID] = room.ID
		a.mu.Unlock()
		a.Publish(room.ID, Event{"type": "session.status", "roomId": room.ID, "sessionId": view.SessionID, "status": "running"})
		return room, nil
	}

	var out worker.OpenSessionOut
	err := a.Worker.Call(ctx, "openSession", map[string]any{
		"roomId":           room.ID,
		"turnId":           id("tn_"),
		"kind":             kind,
		"permissionPreset": permissionPreset,
	}, &out)
	if err != nil {
		a.mu.Lock()
		room.State = RoomClosed
		a.mu.Unlock()
		return room, fmt.Errorf("openSession: %w", err)
	}
	a.mu.Lock()
	room.SessionID = out.SessionID
	room.State = RoomRunning
	a.SessionRoom[out.SessionID] = room.ID
	a.mu.Unlock()
	a.Publish(room.ID, Event{"type": "session.status", "roomId": room.ID, "sessionId": out.SessionID, "status": "running"})
	return room, nil
}

func (a *App) ListRooms() []*Room {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]*Room, 0, len(a.Rooms))
	for _, r := range a.Rooms {
		cp := *r
		out = append(out, &cp)
	}
	return out
}

func (a *App) GetRoom(id string) (*Room, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.Rooms[id]
	if !ok {
		return nil, false
	}
	cp := *r
	return &cp, true
}

func (a *App) ListMessages(roomID string) []Message {
	a.mu.Lock()
	defer a.mu.Unlock()
	src := a.Messages[roomID]
	out := make([]Message, len(src))
	copy(out, src)
	return out
}

func (a *App) ListActivity(roomID string) []ActivityEvent {
	a.mu.Lock()
	defer a.mu.Unlock()
	src := a.Activity[roomID]
	out := make([]ActivityEvent, len(src))
	copy(out, src)
	return out
}

func (a *App) appendMessage(m Message) {
	a.Messages[m.RoomID] = append(a.Messages[m.RoomID], m)
}

func (a *App) PostMessage(ctx context.Context, roomID, text string) (*Room, *Approval, error) {
	a.mu.Lock()
	room, ok := a.Rooms[roomID]
	if !ok {
		a.mu.Unlock()
		return nil, nil, fmt.Errorf("room not found")
	}
	if room.State != RoomRunning {
		a.mu.Unlock()
		return nil, nil, fmt.Errorf("room is %s", room.State)
	}
	sessionID := room.SessionID
	user := Message{ID: id("msg_"), RoomID: roomID, Role: "user", Text: text, CreatedAt: now()}
	a.appendMessage(user)
	a.mu.Unlock()
	a.Publish(roomID, Event{"type": "assistant.message", "roomId": roomID, "role": "user", "text": text})

	if a.Orch != nil {
		turnID := id("tn_")
		res, err := a.Orch.RunTurn(ctx, roomID, turnID, text)
		if err != nil {
			return nil, nil, err
		}
		return a.applyTurnResult(ctx, roomID, sessionID, runTurnFromOrch(res))
	}

	var out worker.RunTurnOut
	err := a.Worker.Call(ctx, "runTurn", map[string]any{
		"roomId":    roomID,
		"sessionId": sessionID,
		"turnId":    id("tn_"),
		"message":   text,
	}, &out)
	if err != nil {
		return nil, nil, err
	}
	return a.applyTurnResult(ctx, roomID, sessionID, out)
}

func (a *App) persistAssistantTexts(roomID string, texts []string) {
	for _, text := range texts {
		if text == "" {
			continue
		}
		a.appendMessage(Message{
			ID: id("msg_"), RoomID: roomID, Role: "assistant", Text: text, CreatedAt: now(),
		})
	}
}

func (a *App) applyTurnResult(ctx context.Context, roomID, sessionID string, out worker.RunTurnOut) (*Room, *Approval, error) {
	_ = ctx
	a.mu.Lock()
	room := a.Rooms[roomID]
	if room == nil {
		a.mu.Unlock()
		return nil, nil, fmt.Errorf("room not found")
	}
	a.persistAssistantTexts(roomID, out.Texts)

	if out.Status == "needs_approval" {
		room.State = RoomAwaitingApproval
		ask := out.Approval
		appr := &Approval{
			ID:        id("ap_"),
			RoomID:    roomID,
			SessionID: sessionID,
			Status:    "pending",
			CreatedAt: now(),
		}
		if ask != nil {
			appr.ApprovalRequestID = ask.ApprovalRequestID
			appr.ToolName = ask.ToolName
			appr.Reason = ask.Reason
		}
		a.Approvals[appr.ID] = appr
		cpRoom := *room
		cpAppr := *appr
		a.mu.Unlock()
		a.Publish(roomID, Event{"type": "approval.asked", "roomId": roomID, "approvalId": appr.ID, "toolName": appr.ToolName, "reason": appr.Reason})
		return &cpRoom, &cpAppr, nil
	}

	// completed / continue: keep the ACP session open so the user can send again.
	room.State = RoomRunning
	cp := *room
	a.mu.Unlock()
	return &cp, nil, nil
}

func (a *App) Decide(ctx context.Context, approvalID, decision string) (*Approval, error) {
	a.mu.Lock()
	appr, ok := a.Approvals[approvalID]
	if !ok {
		a.mu.Unlock()
		return nil, fmt.Errorf("approval not found")
	}
	if appr.Status != "pending" {
		a.mu.Unlock()
		return nil, fmt.Errorf("approval already decided")
	}
	room := a.Rooms[appr.RoomID]
	sessionID := appr.SessionID
	reqID := appr.ApprovalRequestID
	roomID := appr.RoomID
	a.mu.Unlock()

	if a.Orch != nil {
		turnID := id("tn_")
		res, err := a.Orch.Decide(ctx, roomID, turnID, reqID, decision, id("tn_"))
		if err != nil {
			return nil, err
		}
		a.mu.Lock()
		appr.Status = "decided"
		appr.Decision = decision
		if decision == "reject" && room != nil {
			room.State = RoomClosed
		}
		cp := *appr
		a.mu.Unlock()
		if decision == "reject" {
			a.Publish(roomID, Event{"type": "session.status", "roomId": roomID, "status": "closed"})
			return &cp, nil
		}
		if res.Turn != nil {
			_, _, err = a.applyTurnResult(ctx, roomID, sessionID, runTurnFromOrch(*res.Turn))
		}
		return &cp, err
	}

	if decision == "reject" {
		_ = a.Worker.Call(ctx, "abort", map[string]any{
			"roomId": roomID, "sessionId": sessionID, "turnId": id("tn_"), "reason": "rejected",
		}, &worker.AbortedOut{})
		a.mu.Lock()
		appr.Status = "decided"
		appr.Decision = "reject"
		if room != nil {
			room.State = RoomClosed
		}
		cp := *appr
		a.mu.Unlock()
		a.Publish(roomID, Event{"type": "session.status", "roomId": roomID, "status": "closed"})
		return &cp, nil
	}

	var applied worker.AppliedOut
	if err := a.Worker.Call(ctx, "resolveApproval", map[string]any{
		"roomId":            roomID,
		"sessionId":         sessionID,
		"turnId":            id("tn_"),
		"approvalRequestId": reqID,
		"outcome":           "allowed-once",
	}, &applied); err != nil {
		return nil, err
	}
	var turn worker.RunTurnOut
	if err := a.Worker.Call(ctx, "runTurn", map[string]any{
		"roomId":              roomID,
		"sessionId":           sessionID,
		"turnId":              id("tn_"),
		"message":             "",
		"resumeAfterApproval": true,
	}, &turn); err != nil {
		return nil, err
	}
	a.mu.Lock()
	appr.Status = "decided"
	appr.Decision = "allow"
	a.mu.Unlock()
	_, _, err := a.applyTurnResult(ctx, roomID, sessionID, turn)
	a.mu.Lock()
	cp := *appr
	a.mu.Unlock()
	return &cp, err
}

func (a *App) ListApprovals() []*Approval {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]*Approval, 0, len(a.Approvals))
	for _, v := range a.Approvals {
		cp := *v
		out = append(out, &cp)
	}
	return out
}

func (a *App) AbortRoom(ctx context.Context, roomID string) error {
	a.mu.Lock()
	room, ok := a.Rooms[roomID]
	if !ok {
		a.mu.Unlock()
		return fmt.Errorf("room not found")
	}
	sessionID := room.SessionID
	a.mu.Unlock()
	if a.Orch != nil {
		_ = a.Orch.Abort(ctx, roomID, id("tn_"), "abort")
	} else {
		_ = a.Worker.Call(ctx, "abort", map[string]any{"roomId": roomID, "sessionId": sessionID, "turnId": id("tn_")}, &worker.AbortedOut{})
	}
	a.mu.Lock()
	room.State = RoomClosed
	a.mu.Unlock()
	a.Publish(roomID, Event{"type": "session.status", "roomId": roomID, "status": "closed"})
	return nil
}

func (a *App) SteerRoom(ctx context.Context, roomID, instruction string) (bool, error) {
	instruction = strings.TrimSpace(instruction)
	if instruction == "" {
		return false, fmt.Errorf("instruction is required")
	}
	a.mu.Lock()
	room, ok := a.Rooms[roomID]
	if !ok {
		a.mu.Unlock()
		return false, fmt.Errorf("room not found")
	}
	if room.State != RoomRunning {
		a.mu.Unlock()
		return false, fmt.Errorf("room is %s", room.State)
	}
	sessionID := room.SessionID
	a.mu.Unlock()

	accepted := true
	if a.Orch != nil {
		if err := a.Orch.Steer(ctx, roomID, id("tn_"), instruction); err != nil {
			return false, err
		}
	} else {
		var out worker.AcceptedOut
		if err := a.Worker.Call(ctx, "steer", map[string]any{
			"roomId": roomID, "sessionId": sessionID, "turnId": id("tn_"), "instruction": instruction,
		}, &out); err != nil {
			return false, err
		}
		accepted = out.Accepted
	}
	if accepted {
		a.Publish(roomID, Event{
			"type": "room.steered", "roomId": roomID, "sessionId": sessionID, "text": instruction, "status": "accepted",
		})
	}
	return accepted, nil
}

var workerEventTypes = map[string]struct{}{
	"session.status":    {},
	"assistant.message": {},
	"tool.call":         {},
	"tool.result":       {},
	"approval.asked":    {},
	"agent.started":     {},
	"agent.finished":    {},
	"usage":             {},
}

func eventString(ev Event, key string) string {
	value, _ := ev[key].(string)
	return value
}

func (a *App) Ingest(ev Event) error {
	eventType := eventString(ev, "type")
	if _, ok := workerEventTypes[eventType]; !ok {
		return fmt.Errorf("unsupported Orbit event type %q", eventType)
	}
	if occurredAt := eventString(ev, "occurredAt"); occurredAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, occurredAt); err != nil {
			return fmt.Errorf("occurredAt must be RFC3339")
		}
	}
	sessionID, _ := ev["sessionId"].(string)
	roomID, _ := ev["roomId"].(string)
	a.mu.Lock()
	mappedRoomID := ""
	if sessionID != "" {
		mappedRoomID = a.SessionRoom[sessionID]
	}
	if roomID == "" && sessionID != "" {
		roomID = mappedRoomID
	}
	_, roomExists := a.Rooms[roomID]
	a.mu.Unlock()
	if mappedRoomID != "" && roomID != mappedRoomID {
		return fmt.Errorf("event roomId does not match its session")
	}
	if roomID == "" || !roomExists {
		return fmt.Errorf("event is not associated with a known room")
	}
	// Assistant text is persisted from runTurn.texts to avoid duplicates when
	// ingest is also enabled. Activity stores the normalized live projection.
	a.publish(roomID, ev, "worker")
	return nil
}

func (a *App) Subscribe(roomID string) (<-chan []byte, func()) {
	ch := make(chan []byte, 32)
	a.mu.Lock()
	if a.subs[roomID] == nil {
		a.subs[roomID] = map[chan []byte]struct{}{}
	}
	a.subs[roomID][ch] = struct{}{}
	a.mu.Unlock()
	return ch, func() {
		a.mu.Lock()
		delete(a.subs[roomID], ch)
		a.mu.Unlock()
		close(ch)
	}
}

func (a *App) Publish(roomID string, ev Event) {
	a.publish(roomID, ev, "control")
}

func (a *App) publish(roomID string, ev Event, source string) {
	occurredAt := eventString(ev, "occurredAt")
	if occurredAt == "" {
		occurredAt = now()
	}
	eventID := eventString(ev, "eventId")
	if eventID == "" {
		eventID = id("ev_")
	}

	a.mu.Lock()
	room := a.Rooms[roomID]
	if room == nil {
		a.mu.Unlock()
		return
	}
	a.sequences[roomID]++
	item := ActivityEvent{
		ID:                eventID,
		Sequence:          a.sequences[roomID],
		Type:              eventString(ev, "type"),
		RoomID:            roomID,
		SessionID:         eventString(ev, "sessionId"),
		TurnID:            eventString(ev, "turnId"),
		Source:            source,
		Runtime:           "dsh",
		Protocol:          "acp",
		Role:              eventString(ev, "role"),
		Text:              eventString(ev, "text"),
		ToolName:          eventString(ev, "toolName"),
		CallID:            eventString(ev, "callId"),
		ApprovalID:        eventString(ev, "approvalId"),
		ApprovalRequestID: eventString(ev, "approvalRequestId"),
		Reason:            eventString(ev, "reason"),
		Status:            eventString(ev, "status"),
		PermissionPreset:  room.PermissionPreset,
		OccurredAt:        occurredAt,
	}
	history := append(a.Activity[roomID], item)
	if len(history) > maxActivityPerRoom {
		history = append([]ActivityEvent(nil), history[len(history)-maxActivityPerRoom:]...)
	}
	a.Activity[roomID] = history
	raw, err := json.Marshal(item)
	if err != nil {
		a.mu.Unlock()
		return
	}
	for ch := range a.subs[roomID] {
		select {
		case ch <- raw:
		default:
		}
	}
	a.mu.Unlock()
}

func runTurnFromOrch(res orch.RunTurnResult) worker.RunTurnOut {
	out := worker.RunTurnOut{Status: res.Status, Texts: res.Texts}
	if res.Approval != nil {
		out.Approval = &worker.Ask{
			ApprovalRequestID: res.Approval.ApprovalRequestID,
			ToolName:          res.Approval.ToolName,
			CallID:            res.Approval.CallID,
			Reason:            res.Approval.Reason,
		}
	}
	return out
}
