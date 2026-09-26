package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/mindreon/orbit-control/internal/orch"
	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/store/memstore"
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

	DefaultTenantID        = "default"
	LocalIssuer            = "orbit-local"
	MaxIdempotencyKeyLen   = 128
	idempotencyTTL         = 24 * time.Hour
	defaultAbortTimeout    = 5 * time.Second
	defaultDeliveryTimeout = 30 * time.Second
)

// ErrInvalid marks caller errors (400).
var ErrInvalid = errors.New("invalid request")

// ErrDecisionDeliveryFailed means a decision was recorded but delivering it
// to the workflow failed or timed out (502). It never carries the upstream
// error text.
var ErrDecisionDeliveryFailed = errors.New("decision delivery failed")

// ErrNotFound and ErrIdempotencyKeyReused are the repository sentinels.
var (
	ErrNotFound             = store.ErrNotFound
	ErrIdempotencyKeyReused = store.ErrIdempotencyKeyReused
)

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

func normalizePermissionPreset(raw string) (string, error) {
	preset := strings.TrimSpace(raw)
	if preset == "" {
		preset = PermissionWorkspaceWrite
	}
	switch preset {
	case PermissionWorkspaceWrite, PermissionReadOnly, PermissionDangerFullAccess:
		return preset, nil
	default:
		return "", invalidf("permissionPreset must be workspace-write, read-only, or danger-full-access")
	}
}

// Principal is the authenticated caller. TenantID and UserID come only from
// the authenticator (session), never from request bodies (§17.5).
type Principal struct {
	TenantID string
	UserID   string
}

// Orchestrator is the Temporal RoomWorkflow surface control drives.
// *orch.Client implements it.
type Orchestrator interface {
	StartRoom(ctx context.Context, roomID, kind, permissionPreset string) (orch.RoomView, error)
	RunTurn(ctx context.Context, roomID, turnID, message string) (orch.RunTurnResult, error)
	Decide(ctx context.Context, roomID, turnID, approvalRequestID, decision, resumeTurnID string) (orch.DecideResult, error)
	Steer(ctx context.Context, roomID, turnID, instruction string) error
	Abort(ctx context.Context, roomID, turnID, reason string) error
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
	PersonaID        string
	GrantID          string
	// IdempotencyKey is the raw Idempotency-Key header; only its sha256 is
	// stored.
	IdempotencyKey string
}

// liveRoom is the per-process projection publish() needs. The repository is
// the source of truth; this is refilled from it on every successful read.
type liveRoom struct {
	TenantID         string
	Runtime          RuntimeSnapshot
	PermissionPreset string
}

type Options struct {
	Worker        *worker.Client
	Orch          Orchestrator
	Repo          store.Repository
	Log           *log.Logger
	DefaultTenant string
	// AbortTimeout bounds the workflow abort sent before a soft delete.
	AbortTimeout time.Duration
	// DeliveryTimeout bounds delivering an approval decision to the workflow.
	DeliveryTimeout time.Duration
}

type App struct {
	mu            sync.Mutex
	Worker        *worker.Client
	Orch          Orchestrator // optional Temporal; nil → direct worker HTTP
	Repo          store.Repository
	Store         *store.FileStore
	Log           *log.Logger
	DefaultTenant string
	AbortTimeout  time.Duration
	// DeliveryTimeout bounds resolveApproval / the decide Update.
	DeliveryTimeout time.Duration
	Activity        map[string][]ActivityEvent
	SessionRoom     map[string]string
	Personas        map[string]*Persona
	McpConnectors   map[string]*McpConnector
	CloudAgents     map[string]*CloudAgentJob
	live            map[string]liveRoom
	knownUsers      map[string]struct{}
	grants          map[string]*grantRecord
	sequences       map[string]uint64
	subs            map[string]map[chan []byte]struct{}
}

func New(w *worker.Client) *App {
	return NewWithOrch(w, nil)
}

func NewWithOrch(w *worker.Client, o *orch.Client) *App {
	opts := Options{Worker: w}
	if o != nil {
		opts.Orch = o
	}
	return NewWithOptions(opts)
}

func NewWithOptions(opts Options) *App {
	if opts.Repo == nil {
		opts.Repo = memstore.New()
	}
	if opts.Log == nil {
		opts.Log = log.Default()
	}
	if opts.DefaultTenant == "" {
		opts.DefaultTenant = DefaultTenantID
	}
	if opts.AbortTimeout <= 0 {
		opts.AbortTimeout = defaultAbortTimeout
	}
	if opts.DeliveryTimeout <= 0 {
		opts.DeliveryTimeout = defaultDeliveryTimeout
	}
	return &App{
		Worker:          opts.Worker,
		Orch:            opts.Orch,
		Repo:            opts.Repo,
		Store:           store.New(""),
		Log:             opts.Log,
		DefaultTenant:   opts.DefaultTenant,
		AbortTimeout:    opts.AbortTimeout,
		DeliveryTimeout: opts.DeliveryTimeout,
		Activity:        map[string][]ActivityEvent{},
		SessionRoom:     map[string]string{},
		Personas:        map[string]*Persona{},
		McpConnectors:   map[string]*McpConnector{},
		CloudAgents:     map[string]*CloudAgentJob{},
		live:            map[string]liveRoom{},
		knownUsers:      map[string]struct{}{},
		grants:          map[string]*grantRecord{},
		sequences:       map[string]uint64{},
		subs:            map[string]map[chan []byte]struct{}{},
	}
}

func id(prefix string) string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func roomFromRecord(rec store.RoomRecord) *Room {
	room := &Room{
		ID:               rec.ID,
		Kind:             rec.Kind,
		Title:            rec.Title,
		State:            RoomState(rec.State),
		PermissionPreset: rec.PermissionPreset,
		SessionID:        rec.SessionID,
		CreatedAt:        stamp(rec.CreatedAt),
	}
	_ = json.Unmarshal(rec.Runtime, &room.Runtime)
	return room
}

func messageFromRecord(rec store.MessageRecord) Message {
	return Message{ID: rec.ID, RoomID: rec.TaskID, Role: rec.Role, Type: rec.Type, Text: rec.Text, CreatedAt: stamp(rec.CreatedAt)}
}

func approvalFromRecord(rec store.ApprovalRecord) *Approval {
	return &Approval{
		ID:                rec.ID,
		RoomID:            rec.TaskID,
		SessionID:         rec.SessionID,
		ApprovalRequestID: rec.ApprovalRequestID,
		ToolName:          rec.ToolName,
		Reason:            rec.Reason,
		Status:            rec.Status,
		Decision:          rec.Decision,
		CreatedAt:         stamp(rec.CreatedAt),
	}
}

// remember caches what publish() and internal ingest need about a live room.
func (a *App) remember(rec store.RoomRecord) {
	room := roomFromRecord(rec)
	a.mu.Lock()
	a.live[rec.ID] = liveRoom{TenantID: rec.TenantID, Runtime: room.Runtime, PermissionPreset: rec.PermissionPreset}
	if rec.SessionID != "" {
		a.SessionRoom[rec.SessionID] = rec.ID
	}
	a.mu.Unlock()
}

// forget drops the in-process projection of a deleted room. SessionRoom is
// kept so late session-only worker events still resolve to the room and get
// the same 404 as events that carry roomId.
func (a *App) forget(roomID string) {
	a.mu.Lock()
	delete(a.live, roomID)
	delete(a.Activity, roomID)
	delete(a.sequences, roomID)
	a.mu.Unlock()
}

func (a *App) ensureUser(ctx context.Context, p Principal) error {
	key := p.TenantID + "\x00" + p.UserID
	a.mu.Lock()
	_, ok := a.knownUsers[key]
	a.mu.Unlock()
	if ok {
		return nil
	}
	if err := a.Repo.UpsertUser(ctx, p.TenantID, store.UserRecord{ID: p.UserID, Issuer: LocalIssuer, Subject: p.UserID}); err != nil {
		return err
	}
	a.mu.Lock()
	a.knownUsers[key] = struct{}{}
	a.mu.Unlock()
	return nil
}

func (a *App) getRoom(ctx context.Context, p Principal, roomID string) (store.RoomRecord, error) {
	rec, err := a.Repo.GetRoom(ctx, p.TenantID, p.UserID, roomID)
	if err != nil {
		return store.RoomRecord{}, err
	}
	a.remember(rec)
	return rec, nil
}

func (a *App) setRoomState(ctx context.Context, tenantID string, room *Room, state RoomState, sessionID string) error {
	if err := a.Repo.UpdateRoomState(ctx, tenantID, room.ID, string(state), sessionID); err != nil {
		return err
	}
	room.State = state
	if sessionID != "" {
		room.SessionID = sessionID
		a.mu.Lock()
		a.SessionRoom[sessionID] = room.ID
		a.mu.Unlock()
	}
	return nil
}

func createRequestHash(kind, title, preset, personaID, grantID string) []byte {
	raw, _ := json.Marshal([]string{kind, title, preset, personaID, grantID})
	sum := sha256.Sum256(raw)
	return sum[:]
}

// CreateRoom returns replayed=true when the Idempotency-Key matched an
// existing live room; no workflow is started in that case.
func (a *App) CreateRoom(ctx context.Context, p Principal, input CreateRoomInput) (*Room, bool, error) {
	kind := strings.TrimSpace(input.Kind)
	if kind == "" {
		kind = "solo"
	}
	if kind != "solo" && kind != "collab" {
		return nil, false, invalidf("kind must be solo or collab")
	}
	permissionPreset, err := normalizePermissionPreset(input.PermissionPreset)
	if err != nil {
		return nil, false, err
	}
	if len(input.IdempotencyKey) > MaxIdempotencyKeyLen {
		return nil, false, invalidf("Idempotency-Key must be at most %d characters", MaxIdempotencyKeyLen)
	}
	if err := a.ensureUser(ctx, p); err != nil {
		return nil, false, err
	}
	title := strings.TrimSpace(input.Title)
	personaID := strings.TrimSpace(input.PersonaID)
	grantID := strings.TrimSpace(input.GrantID)
	runtime, _ := json.Marshal(RuntimeSnapshot{Kernel: RuntimeKernel, Isolation: "process"})
	rec := store.RoomRecord{
		ID:               id("rm_"),
		CreatedBy:        p.UserID,
		Kind:             kind,
		Title:            title,
		State:            string(RoomIdle),
		PermissionPreset: permissionPreset,
		Runtime:          runtime,
		PersonaID:        personaID,
		CreatedAt:        time.Now().UTC(),
	}
	var idem *store.IdempotencyRecord
	if input.IdempotencyKey != "" {
		keyHash := sha256.Sum256([]byte(input.IdempotencyKey))
		idem = &store.IdempotencyRecord{
			CreatedBy:   p.UserID,
			KeyHash:     keyHash[:],
			RequestHash: createRequestHash(kind, title, permissionPreset, personaID, grantID),
			ExpiresAt:   time.Now().UTC().Add(idempotencyTTL),
		}
	}
	saved, replayed, err := a.Repo.CreateRoom(ctx, p.TenantID, rec, idem)
	if err != nil {
		return nil, false, err
	}
	a.remember(saved)
	room := roomFromRecord(saved)
	if replayed {
		return room, true, nil
	}

	if a.Orch != nil {
		view, err := a.Orch.StartRoom(ctx, room.ID, kind, permissionPreset)
		if err != nil {
			_ = a.setRoomState(ctx, p.TenantID, room, RoomClosed, "")
			return room, false, fmt.Errorf("temporal StartRoom: %w", err)
		}
		if err := a.setRoomState(ctx, p.TenantID, room, RoomRunning, view.SessionID); err != nil {
			return room, false, err
		}
		a.Publish(room.ID, Event{"type": "session.status", "roomId": room.ID, "sessionId": view.SessionID, "status": "running"})
		return room, false, nil
	}

	persona, connectors, grantEnv, err := a.CompositionForRoom(personaID, grantID)
	if err != nil {
		_ = a.setRoomState(ctx, p.TenantID, room, RoomClosed, "")
		return room, false, err
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
		_ = a.setRoomState(ctx, p.TenantID, room, RoomClosed, "")
		return room, false, fmt.Errorf("openSession: %w", err)
	}
	if err := a.setRoomState(ctx, p.TenantID, room, RoomRunning, out.SessionID); err != nil {
		return room, false, err
	}
	a.Publish(room.ID, Event{"type": "session.status", "roomId": room.ID, "sessionId": out.SessionID, "status": "running"})
	return room, false, nil
}

func (a *App) ListRooms(ctx context.Context, p Principal) ([]*Room, error) {
	recs, err := a.Repo.ListRooms(ctx, p.TenantID, p.UserID)
	if err != nil {
		return nil, err
	}
	out := make([]*Room, 0, len(recs))
	for _, rec := range recs {
		out = append(out, roomFromRecord(rec))
	}
	return out, nil
}

func (a *App) GetRoom(ctx context.Context, p Principal, roomID string) (*Room, error) {
	rec, err := a.getRoom(ctx, p, roomID)
	if err != nil {
		return nil, err
	}
	return roomFromRecord(rec), nil
}

func (a *App) ListMessages(ctx context.Context, p Principal, roomID string) ([]Message, error) {
	recs, err := a.Repo.ListMessages(ctx, p.TenantID, p.UserID, roomID)
	if err != nil {
		return nil, err
	}
	out := make([]Message, 0, len(recs))
	for _, rec := range recs {
		out = append(out, messageFromRecord(rec))
	}
	return out, nil
}

func (a *App) ListActivity(ctx context.Context, p Principal, roomID string) ([]ActivityEvent, error) {
	if _, err := a.getRoom(ctx, p, roomID); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	src := a.Activity[roomID]
	out := make([]ActivityEvent, len(src))
	copy(out, src)
	return out, nil
}

func (a *App) appendMessage(ctx context.Context, tenantID, roomID, role, text string) error {
	return a.Repo.AppendMessage(ctx, tenantID, store.MessageRecord{
		ID: id("msg_"), TaskID: roomID, Role: role, Text: text, CreatedAt: time.Now().UTC(),
	})
}

func (a *App) PostMessage(ctx context.Context, p Principal, roomID, text string) (*Room, *Approval, error) {
	rec, err := a.getRoom(ctx, p, roomID)
	if err != nil {
		return nil, nil, err
	}
	if RoomState(rec.State) != RoomRunning {
		return nil, nil, invalidf("room is %s", rec.State)
	}
	sessionID := rec.SessionID
	if err := a.appendMessage(ctx, p.TenantID, roomID, "user", text); err != nil {
		return nil, nil, err
	}
	a.Publish(roomID, Event{"type": "assistant.message", "roomId": roomID, "role": "user", "text": text})

	if a.Orch != nil {
		turnID := id("tn_")
		res, err := a.Orch.RunTurn(ctx, roomID, turnID, text)
		if err != nil {
			return nil, nil, err
		}
		return a.applyTurnResult(ctx, p.TenantID, roomID, runTurnFromOrch(res))
	}

	var out worker.RunTurnOut
	err = a.Worker.Call(ctx, "runTurn", map[string]any{
		"roomId":    roomID,
		"sessionId": sessionID,
		"turnId":    id("tn_"),
		"message":   text,
	}, &out)
	if err != nil {
		return nil, nil, err
	}
	return a.applyTurnResult(ctx, p.TenantID, roomID, out)
}

func (a *App) persistAssistantTexts(ctx context.Context, tenantID, roomID string, texts []string) error {
	for _, text := range texts {
		if text == "" {
			continue
		}
		if err := a.appendMessage(ctx, tenantID, roomID, "assistant", text); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) applyTurnResult(ctx context.Context, tenantID, roomID string, out worker.RunTurnOut) (*Room, *Approval, error) {
	if err := a.persistAssistantTexts(ctx, tenantID, roomID, out.Texts); err != nil {
		return nil, nil, err
	}
	state := RoomRunning
	var appr *store.ApprovalRecord
	if out.Status == "needs_approval" {
		state = RoomAwaitingApproval
		appr = &store.ApprovalRecord{ID: id("ap_"), TaskID: roomID, Status: "pending", CreatedAt: time.Now().UTC()}
		if ask := out.Approval; ask != nil {
			appr.ApprovalRequestID = ask.ApprovalRequestID
			appr.CallID = ask.CallID
			appr.ToolName = ask.ToolName
			appr.Reason = ask.Reason
		}
		if err := a.Repo.CreateApproval(ctx, tenantID, *appr); err != nil {
			return nil, nil, err
		}
	}
	if err := a.Repo.UpdateRoomState(ctx, tenantID, roomID, string(state), ""); err != nil {
		return nil, nil, err
	}
	rec, err := a.Repo.GetRoomForWorker(ctx, tenantID, roomID)
	if err != nil {
		return nil, nil, err
	}
	room := roomFromRecord(rec)
	if appr == nil {
		// completed / continue: keep the session open so the user can send again.
		return room, nil, nil
	}
	appr.SessionID = rec.SessionID
	a.Publish(roomID, Event{"type": "approval.asked", "roomId": roomID, "approvalId": appr.ID, "toolName": appr.ToolName, "reason": appr.Reason})
	return room, approvalFromRecord(*appr), nil
}

// Decide claims the approval with a conditional UPDATE (status = 'pending')
// before anything is sent to the workflow, so concurrent decisions produce
// exactly one delivery; the loser gets ErrApprovalNotPending (409). A decided
// approval is final: if delivery fails the error is returned and the claim
// stays, because a timed-out delivery may already have been applied.
func (a *App) Decide(ctx context.Context, p Principal, approvalID, decision string) (*Approval, error) {
	appr, err := a.Repo.GetApproval(ctx, p.TenantID, p.UserID, approvalID)
	if err != nil {
		return nil, err
	}
	if appr.Status != "pending" {
		return nil, store.ErrApprovalNotPending
	}
	sessionID := appr.SessionID
	reqID := appr.ApprovalRequestID
	roomID := appr.TaskID
	value := decision
	if a.Orch == nil && decision != "reject" {
		value = "allow"
	}
	if err := a.Repo.DecideApproval(ctx, p.TenantID, approvalID, "decided", value); err != nil {
		return nil, err
	}
	appr.Status = "decided"
	appr.Decision = value
	out := approvalFromRecord(appr)
	closeRoom := func() error {
		if err := a.Repo.UpdateRoomState(ctx, p.TenantID, roomID, string(RoomClosed), ""); err != nil {
			return err
		}
		a.Publish(roomID, Event{"type": "session.status", "roomId": roomID, "status": "closed"})
		return nil
	}

	if a.Orch != nil {
		dctx, cancel := context.WithTimeout(ctx, a.DeliveryTimeout)
		res, err := a.Orch.Decide(dctx, roomID, id("tn_"), reqID, decision, id("tn_"))
		timedOut := errors.Is(dctx.Err(), context.DeadlineExceeded)
		cancel()
		if err != nil {
			return nil, a.deliveryFailed(approvalID, timedOut)
		}
		if decision == "reject" {
			return out, closeRoom()
		}
		if res.Turn != nil {
			_, _, err = a.applyTurnResult(ctx, p.TenantID, roomID, runTurnFromOrch(*res.Turn))
		}
		return out, err
	}

	if decision == "reject" {
		_ = a.Worker.Call(ctx, "abort", map[string]any{
			"roomId": roomID, "sessionId": sessionID, "turnId": id("tn_"), "reason": "rejected",
		}, &worker.AbortedOut{})
		return out, closeRoom()
	}

	var applied worker.AppliedOut
	dctx, cancel := context.WithTimeout(ctx, a.DeliveryTimeout)
	err = a.Worker.Call(dctx, "resolveApproval", map[string]any{
		"roomId":            roomID,
		"sessionId":         sessionID,
		"turnId":            id("tn_"),
		"approvalRequestId": reqID,
		"outcome":           "allowed-once",
	}, &applied)
	timedOut := errors.Is(dctx.Err(), context.DeadlineExceeded)
	cancel()
	if err != nil {
		return nil, a.deliveryFailed(approvalID, timedOut)
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
	_, _, err = a.applyTurnResult(ctx, p.TenantID, roomID, turn)
	return out, err
}

// deliveryFailed logs a failed decision delivery and returns
// ErrDecisionDeliveryFailed. The approval stays decided: a timed-out delivery
// may already have been applied, so it is never re-delivered (FM-59).
func (a *App) deliveryFailed(approvalID string, timedOut bool) error {
	reason := "error"
	if timedOut {
		reason = "timeout"
	}
	a.Log.Printf("WARN alert=decision_delivery_failed approval=%s reason=%s", approvalID, reason)
	return ErrDecisionDeliveryFailed
}

func (a *App) ListApprovals(ctx context.Context, p Principal) ([]*Approval, error) {
	recs, err := a.Repo.ListApprovals(ctx, p.TenantID, p.UserID)
	if err != nil {
		return nil, err
	}
	out := make([]*Approval, 0, len(recs))
	for _, rec := range recs {
		out = append(out, approvalFromRecord(rec))
	}
	return out, nil
}

func (a *App) abortWorkflow(ctx context.Context, roomID, sessionID, reason string) error {
	if a.Orch != nil {
		return a.Orch.Abort(ctx, roomID, id("tn_"), reason)
	}
	return a.Worker.Call(ctx, "abort", map[string]any{
		"roomId": roomID, "sessionId": sessionID, "turnId": id("tn_"), "reason": reason,
	}, &worker.AbortedOut{})
}

func (a *App) AbortRoom(ctx context.Context, p Principal, roomID string) error {
	rec, err := a.getRoom(ctx, p, roomID)
	if err != nil {
		return err
	}
	_ = a.abortWorkflow(ctx, roomID, rec.SessionID, "abort")
	if err := a.Repo.UpdateRoomState(ctx, p.TenantID, roomID, string(RoomClosed), ""); err != nil {
		return err
	}
	a.Publish(roomID, Event{"type": "session.status", "roomId": roomID, "status": "closed"})
	return nil
}

// DeleteRoom soft-deletes a room created by p (§18.7a). The workflow abort is
// sent first; if it fails or times out the delete still happens and a
// warning is logged, because users must always be able to delete a task.
// Live SSE connections for the room are closed afterwards.
func (a *App) DeleteRoom(ctx context.Context, p Principal, roomID string) error {
	rec, err := a.getRoom(ctx, p, roomID)
	if err != nil {
		return err
	}
	abortCtx, cancel := context.WithTimeout(ctx, a.AbortTimeout)
	abortErr := a.abortWorkflow(abortCtx, roomID, rec.SessionID, "deleted")
	timedOut := errors.Is(abortCtx.Err(), context.DeadlineExceeded)
	cancel()
	if abortErr != nil {
		reason := "error"
		if timedOut || errors.Is(abortErr, context.DeadlineExceeded) {
			reason = "timeout"
		}
		// Only the task id and a fixed reason: worker/Temporal error text is
		// not logged here because it may echo request payloads.
		a.Log.Printf("WARN alert=room_delete_abort_failed task=%s reason=%s", roomID, reason)
	}
	n, err := a.Repo.SoftDeleteRoom(context.WithoutCancel(ctx), p.TenantID, p.UserID, roomID)
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrNotFound
	}
	a.forget(roomID)
	a.closeSubscribers(roomID)
	return nil
}

func (a *App) SteerRoom(ctx context.Context, p Principal, roomID, instruction string) (bool, error) {
	instruction = strings.TrimSpace(instruction)
	if instruction == "" {
		return false, invalidf("instruction is required")
	}
	rec, err := a.getRoom(ctx, p, roomID)
	if err != nil {
		return false, err
	}
	if RoomState(rec.State) != RoomRunning {
		return false, invalidf("room is %s", rec.State)
	}
	sessionID := rec.SessionID

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

// Ingest accepts a worker event. The room (and its tenant) is resolved by
// control, never taken from the event; a missing or soft-deleted room yields
// ErrNotFound (404, non-retryable for the worker).
func (a *App) Ingest(ctx context.Context, ev Event) error {
	eventType := eventString(ev, "type")
	if _, ok := workerEventTypes[eventType]; !ok {
		return invalidf("unsupported Orbit event type %q", eventType)
	}
	if occurredAt := eventString(ev, "occurredAt"); occurredAt != "" {
		if _, err := time.Parse(time.RFC3339Nano, occurredAt); err != nil {
			return invalidf("occurredAt must be RFC3339")
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
	tenantID := a.live[roomID].TenantID
	a.mu.Unlock()
	if mappedRoomID != "" && roomID != mappedRoomID {
		return invalidf("event roomId does not match its session")
	}
	if roomID == "" {
		return invalidf("event is not associated with a known room")
	}
	if tenantID == "" {
		tenantID = a.DefaultTenant
	}
	rec, err := a.Repo.GetRoomForWorker(ctx, tenantID, roomID)
	if err != nil {
		return err
	}
	a.remember(rec)
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
		if _, ok := a.subs[roomID][ch]; ok {
			delete(a.subs[roomID], ch)
			close(ch)
		}
		a.mu.Unlock()
	}
}

// closeSubscribers ends every live SSE stream of a room.
func (a *App) closeSubscribers(roomID string) {
	a.mu.Lock()
	for ch := range a.subs[roomID] {
		close(ch)
	}
	delete(a.subs, roomID)
	a.mu.Unlock()
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
	room, ok := a.live[roomID]
	if !ok {
		a.mu.Unlock()
		return
	}
	a.sequences[roomID]++
	runtimeName := eventString(ev, "runtime")
	if runtimeName == "" {
		runtimeName = room.Runtime.Kernel
	}
	if runtimeName == "" {
		runtimeName = RuntimeKernel
	}
	protocol := eventString(ev, "protocol")
	if protocol == "" {
		protocol = room.Runtime.Protocol
	}
	item := ActivityEvent{
		ID:                eventID,
		Sequence:          a.sequences[roomID],
		Type:              eventString(ev, "type"),
		RoomID:            roomID,
		SessionID:         eventString(ev, "sessionId"),
		TurnID:            eventString(ev, "turnId"),
		Source:            source,
		Runtime:           runtimeName,
		Protocol:          protocol,
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
	a.persistActivity(roomID, item)
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
