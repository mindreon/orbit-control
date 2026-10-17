package workflows

import (
	"encoding/json/jsontext"
	"time"

	"github.com/mindreon/orbit-control/internal/contract"
)

type TeamSpec struct {
	Roster          []contract.RosterEntry `json:"roster"`
	Leader          string                 `json:"leader"`
	MaxParallel     int                    `json:"max_parallel"`
	ReviewMaxRounds int                    `json:"review_max_rounds"`
}
type TeamRunInput struct {
	V                                                               int
	TenantID, WorkspaceID, SessionID, RunID, PrincipalID, MessageID string
	Lock                                                            contract.SegmentLockRef
	EventSubject, InputRef                                          string
	BudgetMillicredits                                              int64
	Limits                                                          RunLimits
	Sandbox                                                         SandboxSpec
	Tier                                                            string
	Team                                                            TeamSpec
	Unattended                                                      bool
	Carry                                                           *TeamCarry `json:"carry,omitempty"`
}
type MemberCursor struct {
	Seq, CheckpointVersion int
	EventN                 int64
	Turn                   int
	ActiveSeconds          int64
}
type ActiveTurn struct {
	Member              string
	Turn                int
	Kind, TaskID        string
	SentMail, SentSteer []string
}
type AppliedUpdate struct {
	Member string
	Turn   int
	Result jsontext.Value
}
type TeamEvent struct {
	Type string
	Data jsontext.Value
}
type TeamCarry struct {
	Status, CancelCode       string
	Cancel                   *CancelInput
	Paused                   bool
	Board                    contract.TeamBoard
	NextTask, NextMail       int
	Mail                     map[string][]contract.MailboxMessage
	UserSteer                []contract.SteerItem
	Cursors                  map[string]MemberCursor
	Active                   map[string]ActiveTurn
	Applied                  map[string]AppliedUpdate
	Escalations              []string
	UsedMillicredits         int64
	Reserve, ReserveNotified bool
	Turns, IdleWakes, Steps  int
	EverCreated, Finished    bool
	StartedAt                time.Time
	Summary                  string
	LastLeaderFinal          *contract.FinalMessage
	EventN, BoardVersion     int64
	Outbox                   []TeamEvent
	Dirty                    bool
	Lease                    contract.SandboxLease
	Acquisitions             int
}
type MemberTaskInput struct {
	V                                                    int
	TenantID, WorkspaceID, SessionID, RunID, PrincipalID string
	Member                                               string
	Role                                                 contract.TeamRole
	Turn                                                 int
	Kind                                                 string
	Task                                                 *contract.TeamTaskBrief `json:"task,omitempty"`
	Lock                                                 contract.SegmentLockRef
	EventSubject                                         string
	Input                                                contract.SegmentTurn
	Mailbox                                              []contract.MailboxMessage
	Steer                                                []contract.SteerItem
	Cursor                                               MemberCursor
	RunUsedMillicredits, BudgetMillicredits              int64
	Limits                                               RunLimits
	Sandbox                                              SandboxSpec
	Lease                                                contract.SandboxLease
	Tier                                                 string
	Unattended                                           bool
	Carry                                                *RunCarry `json:"carry,omitempty"`
}
type TurnResult struct {
	Member                        string
	Turn                          int
	Status, Code                  string
	Final                         *contract.FinalMessage
	Cursor                        MemberCursor
	RunUsedMillicredits           int64
	DeliveredMail, DeliveredSteer []string
}
type DeliverInput struct {
	Mailbox []contract.MailboxMessage
	Steer   []contract.SteerItem
}
type TurnOp struct {
	TenantID, RunID, Member string
	Turn                    int
}
type PrepareTeamInput struct {
	RunOp
	Budget int64
	Team   TeamSpec
	Board  contract.TeamBoard
}
type TurnRow struct {
	Member                     string
	Turn                       int
	Kind, TaskID, Status, Code string
	StartedAt                  time.Time
	EndedAt                    *time.Time
}
type RecordBoardInput struct {
	RunOp
	Version int64
	Board   contract.TeamBoard
	Turns   []TurnRow
	Events  []TeamEvent
}
type FinalizeTeamInput struct {
	RunOp
	Status, Code, Summary string
	Board                 contract.TeamBoard
}

func NewRunInput(in SessionInput, msg SubmitMessageInput, subject string) RunInput {
	return RunInput{V: 1, TenantID: in.TenantID, WorkspaceID: in.WorkspaceID, SessionID: in.SessionID,
		RunID: msg.RunID, PrincipalID: msg.PrincipalID, MessageID: msg.MessageID, Member: MemberMain, Lock: msg.Lock,
		AgentSessionKey: AgentSessionKey(in.SessionID, msg.Lock.Digest), EventSubject: subject,
		InputRef: msg.InputRef, BudgetMillicredits: msg.BudgetMillicredits, Limits: msg.Limits, Sandbox: msg.Sandbox, Tier: "standard"}
}
func NewTeamRunInput(in SessionInput, msg SubmitMessageInput, subject string) TeamRunInput {
	return TeamRunInput{V: 1, TenantID: in.TenantID, WorkspaceID: in.WorkspaceID, SessionID: in.SessionID,
		RunID: msg.RunID, PrincipalID: msg.PrincipalID, MessageID: msg.MessageID, Lock: msg.Lock,
		EventSubject: subject, InputRef: msg.InputRef, BudgetMillicredits: msg.BudgetMillicredits,
		Limits: msg.Limits, Sandbox: msg.Sandbox, Tier: "standard", Team: *msg.Team}
}
