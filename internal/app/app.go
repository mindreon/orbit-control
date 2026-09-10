package app

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/mindreon/orbit-control/internal/worker"
)

type RoomState string

const (
	RoomIdle             RoomState = "idle"
	RoomRunning          RoomState = "running"
	RoomAwaitingApproval RoomState = "awaiting_approval"
	RoomClosed           RoomState = "closed"
)

type Room struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	Title     string    `json:"title"`
	State     RoomState `json:"state"`
	SessionID string    `json:"sessionId,omitempty"`
	CreatedAt string    `json:"createdAt"`
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

type App struct {
	mu          sync.Mutex
	Worker      *worker.Client
	Rooms       map[string]*Room
	Messages    map[string][]Message
	Approvals   map[string]*Approval
	SessionRoom map[string]string
	subs        map[string]map[chan []byte]struct{}
}

func New(w *worker.Client) *App {
	return &App{
		Worker:      w,
		Rooms:       map[string]*Room{},
		Messages:    map[string][]Message{},
		Approvals:   map[string]*Approval{},
		SessionRoom: map[string]string{},
		subs:        map[string]map[chan []byte]struct{}{},
	}
}

func id(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func (a *App) CreateRoom(ctx context.Context, kind, title string) (*Room, error) {
	if kind == "" {
		kind = "solo"
	}
	room := &Room{ID: id("rm_"), Kind: kind, Title: title, State: RoomIdle, CreatedAt: now()}
	a.mu.Lock()
	a.Rooms[room.ID] = room
	a.Messages[room.ID] = nil
	a.mu.Unlock()

	var out worker.OpenSessionOut
	err := a.Worker.Call(ctx, "openSession", map[string]any{
		"roomId": room.ID,
		"turnId": id("tn_"),
		"kind":   kind,
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

func (a *App) publishAssistantTexts(roomID string, texts []string) {
	for _, text := range texts {
		if text == "" {
			continue
		}
		a.Publish(roomID, Event{"type": "assistant.message", "roomId": roomID, "role": "assistant", "text": text})
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
	texts := append([]string(nil), out.Texts...)

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
		a.publishAssistantTexts(roomID, texts)
		a.Publish(roomID, Event{"type": "approval.asked", "roomId": roomID, "approvalId": appr.ID, "toolName": appr.ToolName, "reason": appr.Reason})
		return &cpRoom, &cpAppr, nil
	}

	// completed / continue: keep the ACP session open so the user can send again.
	room.State = RoomRunning
	cp := *room
	a.mu.Unlock()
	a.publishAssistantTexts(roomID, texts)
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
	_ = a.Worker.Call(ctx, "abort", map[string]any{"roomId": roomID, "sessionId": sessionID, "turnId": id("tn_")}, &worker.AbortedOut{})
	a.mu.Lock()
	room.State = RoomClosed
	a.mu.Unlock()
	a.Publish(roomID, Event{"type": "session.status", "roomId": roomID, "status": "closed"})
	return nil
}

func (a *App) Ingest(ev Event) {
	sessionID, _ := ev["sessionId"].(string)
	roomID, _ := ev["roomId"].(string)
	a.mu.Lock()
	if roomID == "" && sessionID != "" {
		roomID = a.SessionRoom[sessionID]
		ev["roomId"] = roomID
	}
	a.mu.Unlock()
	if roomID != "" {
		// Assistant text is persisted from runTurn.texts to avoid duplicates
		// when ingest is also enabled. SSE still fans the live event out.
		a.Publish(roomID, ev)
	}
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
	raw, err := json.Marshal(ev)
	if err != nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for ch := range a.subs[roomID] {
		select {
		case ch <- raw:
		default:
		}
	}
}
