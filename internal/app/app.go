package app

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mindreon/orbit-control/internal/orch"
	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/worker"
)

type RoomState string

const (
	RoomIdle             RoomState = "idle"
	RoomRunning          RoomState = "running"
	RoomAwaitingApproval RoomState = "awaiting_approval"
	RoomClosed           RoomState = "closed"

	PermissionWorkspaceWrite   = "workspace-write"
	PermissionReadOnly         = "read-only"
	PermissionDangerFullAccess = "danger-full-access"
	// RuntimeKernel is the live worker. Rooms do not require dsh or ACP.
	RuntimeKernel      = "agentscope"
	maxActivityPerRoom = 500
)

func normalizePermissionPreset(raw string) (string, error) {
	preset := strings.TrimSpace(raw)
	if preset == "" {
		preset = PermissionWorkspaceWrite
	}
	switch preset {
	case PermissionWorkspaceWrite, PermissionReadOnly, PermissionDangerFullAccess:
		return preset, nil
	default:
		return "", fmt.Errorf("permissionPreset must be workspace-write, read-only, or danger-full-access")
	}
}

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

// Event is a control-originated event payload (camelCase keys).
type Event map[string]any

type CreateRoomInput struct {
	Kind             string
	Title            string
	PermissionPreset string
	PersonaID        string
	GrantID          string
}

type App struct {
	mu            sync.Mutex
	Worker        *worker.Client
	Orch          *orch.Client // optional Temporal; nil → direct worker HTTP
	Store         *store.FileStore
	Rooms         map[string]*Room
	Messages      map[string][]Message
	Approvals     map[string]*Approval
	Events        EventLog
	SessionRoom   map[string]string
	Personas      map[string]*Persona
	McpConnectors map[string]*McpConnector
	CloudAgents   map[string]*CloudAgentJob
	grants        map[string]*grantRecord
	subs          map[string]map[*subscriber]struct{}
}

func New(w *worker.Client) *App {
	return NewWithOrch(w, nil)
}

func NewWithOrch(w *worker.Client, o *orch.Client) *App {
	a := &App{
		Worker:        w,
		Orch:          o,
		Store:         store.New(""),
		Rooms:         map[string]*Room{},
		Messages:      map[string][]Message{},
		Approvals:     map[string]*Approval{},
		Events:        NewMemoryEventLog(maxActivityPerRoom),
		SessionRoom:   map[string]string{},
		Personas:      map[string]*Persona{},
		McpConnectors: map[string]*McpConnector{},
		CloudAgents:   map[string]*CloudAgentJob{},
		grants:        map[string]*grantRecord{},
		subs:          map[string]map[*subscriber]struct{}{},
	}
	return a
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
	permissionPreset, err := normalizePermissionPreset(input.PermissionPreset)
	if err != nil {
		return nil, err
	}
	room := &Room{
		ID:               id("rm_"),
		Kind:             kind,
		Title:            strings.TrimSpace(input.Title),
		State:            RoomIdle,
		PermissionPreset: permissionPreset,
		Runtime: RuntimeSnapshot{
			Kernel:    RuntimeKernel,
			Isolation: "process",
		},
		CreatedAt: now(),
	}
	a.mu.Lock()
	a.Rooms[room.ID] = room
	a.Messages[room.ID] = nil
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

	personaID := strings.TrimSpace(input.PersonaID)
	grantID := strings.TrimSpace(input.GrantID)
	persona, connectors, grantEnv, err := a.CompositionForRoom(personaID, grantID)
	if err != nil {
		a.mu.Lock()
		room.State = RoomClosed
		a.mu.Unlock()
		return room, err
	}
	payload := map[string]any{
		"roomId":           room.ID,
		"turnId":           id("tn_"),
		"kind":             kind,
		"permissionPreset": permissionPreset,
	}
	if persona != nil {
		payload["persona"] = map[string]any{
			"id":              persona.ID,
			"name":            persona.Name,
			"instructions":    persona.Instructions,
			"mcpConnectorIds": persona.McpConnectorIDs,
		}
	}
	if len(connectors) > 0 {
		mcp := make([]map[string]any, 0, len(connectors))
		for _, c := range connectors {
			mcp = append(mcp, map[string]any{
				"id": c.ID, "name": c.Name, "command": c.Command,
				"args": c.Args, "envRefs": c.EnvRefs,
			})
		}
		payload["mcpConnectors"] = mcp
	}
	if grantID != "" {
		payload["grantId"] = grantID
	}
	if len(grantEnv) > 0 {
		payload["grantEnv"] = grantEnv
	}

	var out worker.OpenSessionOut
	err = a.Worker.Call(ctx, "openSession", payload, &out)
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

func (a *App) ListActivity(roomID string) []Envelope {
	out, _ := a.Events.After(roomID, 0)
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

// workerEventTypes is OrbitEvent.type from orbit-runtime schema/OrbitEvent.json (A1).
var workerEventTypes = map[string]struct{}{
	"session.status":       {},
	"assistant.message":    {},
	"assistant.delta":      {},
	"tool.call":            {},
	"tool.result":          {},
	"approval.asked":       {},
	"approval.resolved":    {},
	"question.asked":       {},
	"question.answered":    {},
	"todo.updated":         {},
	"usage":                {},
	"agent.started":        {},
	"agent.finished":       {},
	"agent.spawn_rejected": {},
	"turn.failed":          {},
}

// liveOnlyEventTypes are fanned out over SSE but never stored: they take no
// id, never count against the retained window, and are never replayed.
var liveOnlyEventTypes = map[string]struct{}{
	"assistant.delta": {},
}

func eventString(ev Event, key string) string {
	value, _ := ev[key].(string)
	return value
}

// Ingest validates a worker event's routing fields and stores or streams the
// body unchanged as the envelope payload.
func (a *App) Ingest(raw []byte) error {
	var payload bytes.Buffer
	if err := json.Compact(&payload, raw); err != nil || payload.Len() == 0 || payload.Bytes()[0] != '{' {
		return fmt.Errorf("event must be a JSON object")
	}
	var head struct {
		Type       string `json:"type"`
		RoomID     string `json:"roomId"`
		SessionID  string `json:"sessionId"`
		OccurredAt string `json:"occurredAt"`
	}
	if err := json.Unmarshal(payload.Bytes(), &head); err != nil {
		return fmt.Errorf("event type, roomId, sessionId, and occurredAt must be strings")
	}
	eventType := head.Type
	if _, ok := workerEventTypes[eventType]; !ok {
		return fmt.Errorf("unsupported Orbit event type %q", eventType)
	}
	ts := head.OccurredAt
	if ts != "" {
		if _, err := time.Parse(time.RFC3339Nano, ts); err != nil {
			return fmt.Errorf("occurredAt must be RFC3339")
		}
	} else {
		ts = now()
	}
	sessionID, roomID := head.SessionID, head.RoomID
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
	env := Envelope{Type: eventType, TaskID: roomID, TS: ts, Source: "worker", Payload: payload.Bytes()}
	if _, ok := liveOnlyEventTypes[eventType]; ok {
		a.publishLive(env)
		return nil
	}
	// Assistant text is persisted from runTurn.texts to avoid duplicates when
	// ingest is also enabled; the activity copy is the timeline record.
	a.publishDurable(env)
	return nil
}

// Publish records a control-originated event. Control owns this payload, so it
// fills in room context the worker would otherwise supply.
func (a *App) Publish(roomID string, ev Event) {
	a.mu.Lock()
	room := a.Rooms[roomID]
	var preset, kernel string
	if room != nil {
		preset, kernel = room.PermissionPreset, room.Runtime.Kernel
	}
	a.mu.Unlock()
	if room == nil {
		return
	}
	payload := make(Event, len(ev)+4)
	for k, v := range ev {
		payload[k] = v
	}
	defaults := map[string]string{
		"roomId":           roomID,
		"occurredAt":       now(),
		"runtime":          kernel,
		"permissionPreset": preset,
	}
	for k, v := range defaults {
		if eventString(payload, k) == "" && v != "" {
			payload[k] = v
		}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return
	}
	a.publishDurable(Envelope{
		Type: eventString(payload, "type"), TaskID: roomID, TS: eventString(payload, "occurredAt"),
		Source: "control", Payload: raw,
	})
}

func (a *App) publishDurable(env Envelope) {
	a.mu.Lock()
	defer a.mu.Unlock()
	room := a.Rooms[env.TaskID]
	if room == nil {
		return
	}
	// Append and broadcast under a.mu so SubscribeRoom's head snapshot sees
	// every event either in the log or in its buffer.
	env, err := a.Events.Append(env.TaskID, env)
	if err != nil {
		return
	}
	a.persistActivity(env.TaskID, env)
	a.persistRoom(room)
	raw, err := EncodeEnvelope(env)
	if err != nil {
		return
	}
	a.broadcastLocked(env.TaskID, StreamFrame{Seq: env.ID, Durable: true, Data: raw})
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
