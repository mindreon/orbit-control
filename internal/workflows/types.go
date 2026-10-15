package workflows

import (
	"time"

	"github.com/mindreon/orbit-control/internal/contract"
)

type SessionInput struct {
	V           int           `json:"v"`
	TenantID    string        `json:"tenant_id"`
	WorkspaceID string        `json:"workspace_id"`
	SessionID   string        `json:"session_id"`
	OwnerID     string        `json:"owner_id"`
	Policy      string        `json:"policy"`
	Carry       *SessionCarry `json:"carry,omitempty"`
}
type SessionCarry struct {
	Queue  []SubmitMessageInput `json:"queue,omitempty"`
	Policy string               `json:"policy"`
}
type SubmitMessageInput struct {
	V                  int                     `json:"v"`
	RunID              string                  `json:"run_id"`
	MessageID          string                  `json:"message_id"`
	PrincipalID        string                  `json:"principal_id"`
	Trigger            string                  `json:"trigger"`
	Lock               contract.SegmentLockRef `json:"lock"`
	InputRef           string                  `json:"input_ref"`
	BudgetMillicredits int64                   `json:"budget_millicredits"`
	Limits             RunLimits               `json:"limits"`
	Sandbox            SandboxSpec             `json:"sandbox"`
}
type SubmitMessageOutput struct {
	RunID       string `json:"run_id"`
	Position    int    `json:"position"`
	Disposition string `json:"disposition"`
}
type CancelRunInput struct {
	RunID   string `json:"run_id"`
	ActorID string `json:"actor_id"`
	Reason  string `json:"reason"`
}
type CancelRunOutput struct {
	Disposition string `json:"disposition"`
}
type RunLimits struct {
	MaxIterations             int           `json:"max_iterations"`
	MaxWallSeconds            int           `json:"max_wall_seconds"`
	SegmentBudgetMillicredits int64         `json:"segment_budget_millicredits"`
	MaxActiveSeconds          int           `json:"max_active_seconds"`
	MaxSegments               int           `json:"max_segments"`
	RemindAfter               time.Duration `json:"remind_after"`
	EscalateAfter             time.Duration `json:"escalate_after"`
	ExpireAfter               time.Duration `json:"expire_after"`
	NoProgressLimit           int           `json:"no_progress_limit"`
}

func DefaultLimits(budget int64) RunLimits {
	return RunLimits{
		MaxIterations: 25, MaxWallSeconds: 600, SegmentBudgetMillicredits: max(1000, budget/4),
		MaxActiveSeconds: 14400, MaxSegments: 1000, RemindAfter: time.Hour,
		EscalateAfter: 24 * time.Hour, ExpireAfter: 72 * time.Hour, NoProgressLimit: 3,
	}
}

type SandboxSpec struct {
	IsolationKey string   `json:"isolation_key"`
	Profile      string   `json:"profile"`
	Egress       []string `json:"egress,omitempty"`
	TTLSeconds   int      `json:"ttl_seconds"`
}
type RunInput struct {
	V                  int                     `json:"v"`
	TenantID           string                  `json:"tenant_id"`
	WorkspaceID        string                  `json:"workspace_id"`
	SessionID          string                  `json:"session_id"`
	RunID              string                  `json:"run_id"`
	PrincipalID        string                  `json:"principal_id"`
	MessageID          string                  `json:"message_id"`
	Member             string                  `json:"member"`
	Lock               contract.SegmentLockRef `json:"lock"`
	AgentSessionKey    string                  `json:"agent_session_key"`
	EventSubject       string                  `json:"event_subject"`
	InputRef           string                  `json:"input_ref"`
	BudgetMillicredits int64                   `json:"budget_millicredits"`
	Limits             RunLimits               `json:"limits"`
	Sandbox            SandboxSpec             `json:"sandbox"`
	Tier               string                  `json:"tier"`
	Carry              *RunCarry               `json:"carry,omitempty"`
}
type Pending struct {
	ID        string                   `json:"id"`
	Kind      contract.InteractionKind `json:"kind"`
	Seq       int                      `json:"seq"`
	ExpiresAt time.Time                `json:"expires_at"`
	Decision  *contract.Decision       `json:"decision,omitempty"`
	Actor     string                   `json:"actor"`
	Expired   bool                     `json:"expired"`
}
type RunCarry struct {
	ContinuedAt       int                   `json:"continued_at"`
	Status            string                `json:"status"`
	CommittedSeq      int                   `json:"committed_seq"`
	CheckpointVersion int                   `json:"checkpoint_version"`
	PendingTurn       contract.SegmentTurn  `json:"pending_turn"`
	Steer             []contract.SteerItem  `json:"steer,omitempty"`
	UsedMillicredits  int64                 `json:"used_millicredits"`
	ActiveSeconds     int64                 `json:"active_seconds"`
	NoProgress        int                   `json:"no_progress"`
	Paused            bool                  `json:"paused"`
	Cancel            *CancelInput          `json:"cancel,omitempty"`
	Interaction       *Pending              `json:"interaction,omitempty"`
	Lease             contract.SandboxLease `json:"lease"`
	EventN            int64                 `json:"event_n"`
	CompactedFor      int                   `json:"compacted_for"`
	Acquisitions      int                   `json:"acquisitions"`
	SandboxRetries    int                   `json:"sandbox_retries"`
	ResumeTurn        *contract.SegmentTurn `json:"resume_turn,omitempty"`
}
type ResolveInteractionInput struct {
	InteractionID string            `json:"interaction_id"`
	ActorID       string            `json:"actor_id"`
	Decision      contract.Decision `json:"decision"`
}
type ResolveInteractionOutput struct {
	Accepted bool `json:"accepted"`
}
type SteerInput struct {
	MessageID  string `json:"message_id"`
	ContentRef string `json:"content_ref"`
	ActorID    string `json:"actor_id"`
}
type SteerOutput struct {
	Queued int `json:"queued"`
}
type CancelInput struct {
	ActorID string `json:"actor_id"`
	Reason  string `json:"reason"`
}
type ActorInput struct {
	ActorID string `json:"actor_id"`
}
type AssetSuspendedInput struct {
	AssetRef string `json:"asset_ref"`
	Severity string `json:"severity"`
}
type SetPolicyInput struct {
	Policy string `json:"policy"`
}
type ArchiveInput struct {
	ActorID string `json:"actor_id"`
}
type RunProgress struct {
	Status               string `json:"status"`
	Seq                  int    `json:"seq"`
	CheckpointVersion    int    `json:"checkpoint_version"`
	UsedMillicredits     int64  `json:"used_millicredits"`
	BudgetMillicredits   int64  `json:"budget_millicredits"`
	PendingInteractionID string `json:"pending_interaction_id,omitempty"`
	Paused               bool   `json:"paused"`
	ActiveSeconds        int64  `json:"active_seconds"`
}
type SessionState struct {
	ActiveRunID string `json:"active_run_id"`
	QueueLen    int    `json:"queue_len"`
	Status      string `json:"status"`
}
type TouchInput struct {
	RunID string `json:"run_id"`
	InUse bool   `json:"in_use"`
}
type LeaseInput struct {
	TenantID     string      `json:"tenant_id"`
	IsolationKey string      `json:"isolation_key"`
	Carry        *LeaseCarry `json:"carry,omitempty"`
}
type LeaseCarry struct {
	NextRenew  time.Time              `json:"next_renew"`
	EventN     int64                  `json:"event_n"`
	State      string                 `json:"state"`
	Lease      *contract.SandboxLease `json:"lease,omitempty"`
	SnapshotID string                 `json:"snapshot_id"`
	Gen        int                    `json:"gen"`
	SpecDigest string                 `json:"spec_digest"`
	InUse      map[string]time.Time   `json:"in_use,omitempty"`
	LastTouch  time.Time              `json:"last_touch"`
	Touches    int                    `json:"touches"`
	Archived   bool                   `json:"archived"`
	Spec       AcquireInput           `json:"spec"`
}
type AcquireInput struct {
	EventN      int64                   `json:"event_n"`
	EventAt     time.Time               `json:"event_at"`
	SpecDigest  string                  `json:"spec_digest"`
	RunID       string                  `json:"run_id"`
	SessionID   string                  `json:"session_id"`
	WorkspaceID string                  `json:"workspace_id"`
	Profile     string                  `json:"profile"`
	Egress      []string                `json:"egress,omitempty"`
	Mounts      []contract.SandboxMount `json:"mounts,omitempty"`
	TTLSeconds  int                     `json:"ttl_seconds"`
	Reacquire   bool                    `json:"reacquire"`
}

// RunOp carries the common tenant/run boundary for ctl Activities.
type RunOp struct {
	TenantID  string    `json:"tenant_id"`
	SessionID string    `json:"session_id"`
	RunID     string    `json:"run_id"`
	Member    string    `json:"member"`
	Seq       int       `json:"seq"`
	EventN    int64     `json:"event_n"`
	EventAt   time.Time `json:"event_at"`
}
type PrepareInput struct {
	RunOp
	SessionKey string `json:"session_key"`
	Budget     int64  `json:"budget"`
}
type PrepareOutput struct {
	CheckpointVersion int       `json:"checkpoint_version"`
	StartedAt         time.Time `json:"started_at"`
}
type AcquireSandboxInput struct {
	RunOp
	Spec    SandboxSpec `json:"spec"`
	LockID  string      `json:"lock_id"`
	Attempt int         `json:"attempt"`
}
type StartSegmentInput struct {
	RunOp
	ExpectedVersion int `json:"expected_version"`
}
type RecordSegmentInput struct {
	RunOp
	Result contract.SegmentResult `json:"result"`
}
type RecordSegmentOutput struct {
	RunUsedMillicredits int64 `json:"run_used_millicredits"`
}
type StatusInput struct {
	RunOp
	Status string `json:"status"`
	Reason string `json:"reason"`
}
type InteractionInput struct {
	RunOp
	Request    contract.InteractionRequest `json:"request"`
	RemindAt   time.Time                   `json:"remind_at"`
	EscalateAt time.Time                   `json:"escalate_at"`
	ExpiresAt  time.Time                   `json:"expires_at"`
}
type InteractionOutput struct {
	CreatedAt time.Time `json:"created_at"`
	Approvers []string  `json:"approvers,omitempty"`
}
type NotifyInput struct {
	RunOp
	InteractionID string `json:"interaction_id"`
	Kind          string `json:"kind"`
}
type DecisionInput struct {
	RunOp
	InteractionID string                   `json:"interaction_id"`
	Kind          contract.InteractionKind `json:"kind"`
	Decision      contract.Decision        `json:"decision"`
	Actor         string                   `json:"actor"`
	Expired       bool                     `json:"expired"`
}
type ReconcileInput struct {
	RunOp
	SessionKey string `json:"session_key"`
}
type ReconcileOutput struct {
	Result        *contract.SegmentResult `json:"result,omitempty"`
	LatestVersion int                     `json:"latest_version"`
}
type FinalizeInput struct {
	RunOp
	Status         string `json:"status"`
	ErrorCode      string `json:"error_code"`
	Reason         string `json:"reason"`
	FinalMessageID string `json:"final_message_id"`
	NeverStarted   bool   `json:"never_started"`
}
type SandboxStateInput struct {
	RunID        string                 `json:"run_id"`
	Restored     *bool                  `json:"restored,omitempty"`
	EventN       int64                  `json:"event_n"`
	EventAt      time.Time              `json:"event_at"`
	TenantID     string                 `json:"tenant_id"`
	IsolationKey string                 `json:"isolation_key"`
	SessionID    string                 `json:"session_id"`
	Gen          int                    `json:"gen"`
	State        string                 `json:"state"`
	Lease        *contract.SandboxLease `json:"lease,omitempty"`
	SnapshotID   string                 `json:"snapshot_id"`
	Profile      string                 `json:"profile"`
}
