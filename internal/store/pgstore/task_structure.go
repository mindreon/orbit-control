package pgstore

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// The plan, node and attempt projections (09 §3). Each function applies one durable event. An event that names nothing
// the table could hold is skipped: it is stored in task_events either way, and it must not stop the events behind it.

var sha256Ref = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type planVersionRow struct {
	TenantID        string
	TaskID          string `gorm:"primaryKey"`
	PlanVersion     int    `gorm:"primaryKey"`
	ParentVersion   int
	Hash            string
	ChangeCommandID *string
	Actor           []byte    `gorm:"type:jsonb"`
	CreatedAt       time.Time `gorm:"autoCreateTime:false"`
}

func (planVersionRow) TableName() string { return "plan_versions" }

type taskNodeRow struct {
	TenantID         string
	TaskID           string `gorm:"primaryKey"`
	NodeID           string `gorm:"primaryKey"`
	NodeType         *string
	Title            *string
	Status           string
	Reason           string
	WorkspaceMode    *string
	OwnerProfile     *string
	DependsOn        []byte `gorm:"type:jsonb"`
	ParentNodeID     *string
	SopStep          []byte `gorm:"type:jsonb"`
	Frozen           bool
	CurrentAttemptID *string
	AttemptCount     int
	EntityVersion    int64
	UpdatedAt        time.Time `gorm:"autoUpdateTime:false"`
}

func (taskNodeRow) TableName() string { return "task_nodes" }

type stageAttemptRow struct {
	AttemptID     string `gorm:"primaryKey"`
	TenantID      string
	TaskID        string
	NodeID        string
	AttemptNo     int
	Status        string
	ProfileRef    string
	Runtime       []byte `gorm:"type:jsonb"`
	Failure       []byte `gorm:"type:jsonb"`
	Usage         []byte `gorm:"type:jsonb"`
	EntityVersion int64
	StartedAt     time.Time `gorm:"autoCreateTime:false"`
	FinishedAt    *time.Time
	// StatusChangedAt is when a workflow event last set the attempt's status; the resumed key is the
	// (activity_attempt, state_version) of the last attempt.resumed applied since then. Together they order a worker's
	// attempt.resumed (entity version 0) against the workflow's events.
	StatusChangedAt *time.Time
}

func (stageAttemptRow) TableName() string { return "stage_attempts" }

// terminalAttemptStatuses never change again: the row is history, and the worker's maintenance writes LOST or ABORTED
// into it when the attempt's workflow is gone.
var terminalAttemptStatuses = []string{"ACCEPTED", "REJECTED", "ABORTED", "LOST"}

// attemptOutcomes maps an attempt.finished outcome to the attempt's terminal status.
var attemptOutcomes = map[string]string{"completed": "ACCEPTED", "failed": "REJECTED", "cancelled": "ABORTED"}

// insertPlanVersion projects plan.version_committed. A version is written once: the row is history, and compaction
// commits a new version without touching the old ones. The graph is not in the event, so the column stays NULL.
func insertPlanVersion(tx *gorm.DB, event taskruntime.Event) error {
	var commit struct {
		PlanVersion     int             `json:"plan_version"`
		ParentVersion   *int            `json:"parent_version"`
		Hash            string          `json:"hash"`
		ChangeCommandID string          `json:"change_command_id"`
		CommandID       string          `json:"command_id"`
		Actor           json.RawMessage `json:"actor"`
	}
	if json.Unmarshal(event.Payload, &commit) != nil || commit.PlanVersion < 1 || !sha256Ref.MatchString(commit.Hash) {
		return nil
	}
	parent := commit.PlanVersion - 1
	if commit.ParentVersion != nil && *commit.ParentVersion >= 0 {
		parent = *commit.ParentVersion
	}
	command := commit.ChangeCommandID
	if command == "" {
		command = commit.CommandID
	}
	if len(commit.Actor) == 0 || string(commit.Actor) == "null" {
		commit.Actor = []byte("{}")
	}
	row := planVersionRow{
		TenantID: event.TenantID, TaskID: event.TaskID, PlanVersion: commit.PlanVersion, ParentVersion: parent,
		Hash: commit.Hash, ChangeCommandID: nonEmpty(command), Actor: commit.Actor, CreatedAt: event.Occurred,
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).
		Select("TenantID", "TaskID", "PlanVersion", "ParentVersion", "Hash", "ChangeCommandID", "Actor", "CreatedAt").Create(&row).Error
}

// projectNodeStatus projects node.status_changed. The row is created when it is missing, from what the event says the
// node is (an event from before the runtime enriched them says only the status, and the row's other columns stay NULL),
// and an event that is not newer than the row's changes nothing. A fact the event lacks never blanks one the row has: an
// update sets only the columns the event names. A node that compaction dropped from the live plan keeps its row.
func projectNodeStatus(tx *gorm.DB, event taskruntime.Event) error {
	var change struct {
		NodeID           string    `json:"node_id"`
		ToStatus         string    `json:"to_status"`
		Reason           string    `json:"reason"`
		Frozen           *bool     `json:"frozen"`
		NodeType         string    `json:"node_type"`
		Title            string    `json:"title"`
		WorkspaceAccess  string    `json:"workspace_access"`
		OwnerProfile     string    `json:"owner_profile"`
		DependsOn        *[]string `json:"depends_on"`
		AttemptCount     *int      `json:"attempt_count"`
		CurrentAttemptID string    `json:"current_attempt_id"`
		// Where the node sits in a compiled SOP: the node it nests under, and which step of which SOP it is.
		ParentNodeID string          `json:"parent_node_id"`
		SopStep      json.RawMessage `json:"sop_step"`
	}
	if json.Unmarshal(event.Payload, &change) != nil || !strings.HasPrefix(change.NodeID, "n_") || change.ToStatus == "" {
		return nil
	}
	values := map[string]any{"status": change.ToStatus, "reason": change.Reason, "entity_version": event.EntityVersion, "updated_at": event.Occurred}
	row := taskNodeRow{
		TenantID: event.TenantID, TaskID: event.TaskID, NodeID: change.NodeID, Status: change.ToStatus, Reason: change.Reason,
		EntityVersion: event.EntityVersion, UpdatedAt: event.Occurred,
	}
	selected := []string{"TenantID", "TaskID", "NodeID", "Status", "Reason", "EntityVersion", "UpdatedAt"}
	known := func(column, field string, value any) {
		values[column] = value
		selected = append(selected, field)
	}
	if change.Frozen != nil {
		known("frozen", "Frozen", *change.Frozen)
		row.Frozen = *change.Frozen
	}
	if change.NodeType != "" {
		known("node_type", "NodeType", change.NodeType)
		row.NodeType = &change.NodeType
	}
	if change.Title != "" {
		known("title", "Title", change.Title)
		row.Title = &change.Title
	}
	// The column is checked ('write', 'read', 'none'): a value outside it would fail the whole event, status included.
	if access := change.WorkspaceAccess; access == "write" || access == "read" || access == "none" {
		known("workspace_mode", "WorkspaceMode", access)
		row.WorkspaceMode = &access
	}
	if change.OwnerProfile != "" {
		known("owner_profile", "OwnerProfile", change.OwnerProfile)
		row.OwnerProfile = &change.OwnerProfile
	}
	if change.DependsOn != nil {
		dependsOn, _ := json.Marshal(*change.DependsOn)
		known("depends_on", "DependsOn", dependsOn)
		row.DependsOn = dependsOn
	}
	if change.AttemptCount != nil && *change.AttemptCount >= 0 {
		known("attempt_count", "AttemptCount", *change.AttemptCount)
		row.AttemptCount = *change.AttemptCount
	}
	if strings.HasPrefix(change.CurrentAttemptID, "att_") {
		known("current_attempt_id", "CurrentAttemptID", change.CurrentAttemptID)
		row.CurrentAttemptID = &change.CurrentAttemptID
	}
	if strings.HasPrefix(change.ParentNodeID, "n_") {
		known("parent_node_id", "ParentNodeID", change.ParentNodeID)
		row.ParentNodeID = &change.ParentNodeID
	}
	// The column holds a JSON object: any other value would fail the whole event, status included.
	if step := bytes.TrimSpace(change.SopStep); len(step) > 0 && step[0] == '{' {
		known("sop_step", "SopStep", []byte(step))
		row.SopStep = step
	}
	columns := make([]string, 0, len(values))
	for column := range values {
		columns = append(columns, column)
	}
	query := tx.Model(&taskNodeRow{}).Where("tenant_id = ? AND task_id = ? AND node_id = ?", event.TenantID, event.TaskID, change.NodeID)
	if event.EntityVersion > 0 {
		query = query.Where("entity_version < ?", event.EntityVersion)
	} else {
		query = query.Where("updated_at <= ?", event.Occurred)
	}
	result := query.Select(columns).Updates(values)
	if result.Error != nil || result.RowsAffected > 0 {
		return result.Error
	}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Select(selected).Create(&row).Error
}

// insertAttempt projects attempt.started. An attempt that is already there (a replay, or one the maintenance closed)
// is left alone.
func insertAttempt(tx *gorm.DB, event taskruntime.Event) error {
	var started struct {
		NodeID        string `json:"node_id"`
		AttemptID     string `json:"attempt_id"`
		AttemptNo     int    `json:"attempt_no"`
		Profile       string `json:"profile"`
		ConfigVersion *int   `json:"config_version"`
		// SwitchedFrom is the profile the node was on before a switch; BudgetReserved is what the attempt holds of the
		// task's budget while it runs. Both are optional, and fields this struct does not name are ignored.
		SwitchedFrom   string         `json:"switched_from"`
		BudgetReserved map[string]any `json:"budget_reserved"`
	}
	if json.Unmarshal(event.Payload, &started) != nil || !strings.HasPrefix(started.AttemptID, "att_") ||
		!strings.HasPrefix(started.NodeID, "n_") || started.AttemptNo < 1 {
		return nil
	}
	// What the event says about how the attempt started lives in the row's runtime column.
	attemptRuntime := map[string]any{}
	if started.ConfigVersion != nil {
		attemptRuntime["config_version"] = *started.ConfigVersion
	}
	if started.SwitchedFrom != "" {
		attemptRuntime["switched_from"] = started.SwitchedFrom
	}
	if reserved := normalizeReserved(started.BudgetReserved); len(reserved) > 0 {
		attemptRuntime["budget_reserved"] = reserved
	}
	runtime, _ := json.Marshal(attemptRuntime)
	row := stageAttemptRow{
		AttemptID: started.AttemptID, TenantID: event.TenantID, TaskID: event.TaskID, NodeID: started.NodeID, AttemptNo: started.AttemptNo,
		Status: "RUNNING", ProfileRef: started.Profile, Runtime: runtime, Usage: []byte("{}"), EntityVersion: event.EntityVersion,
		StartedAt: event.Occurred, StatusChangedAt: &event.Occurred,
	}
	return createAttempt(tx, row)
}

// createAttempt inserts an attempt's row unless the attempt, or its (node, attempt_no), already has one.
func createAttempt(tx *gorm.DB, row stageAttemptRow) error {
	return tx.Clauses(clause.OnConflict{DoNothing: true}).
		Select("AttemptID", "TenantID", "TaskID", "NodeID", "AttemptNo", "Status", "ProfileRef", "Runtime", "Failure", "Usage",
			"EntityVersion", "StartedAt", "FinishedAt", "StatusChangedAt").Create(&row).Error
}

// normalizeReserved keeps the limits an attempt actually reserved: a null means no limit was reserved for it.
func normalizeReserved(reserved map[string]any) map[string]any {
	out := make(map[string]any, len(reserved))
	for key, value := range reserved {
		if value != nil {
			out[key] = value
		}
	}
	return out
}

// moveAttempt updates one attempt's row unless it is in a terminal status and reports whether a row changed. A workflow
// event of the attempt's entity applies only when its version is newer than the row's.
func moveAttempt(tx *gorm.DB, event taskruntime.Event, attemptID string, values map[string]any) (bool, error) {
	query := tx.Model(&stageAttemptRow{}).
		Where("tenant_id = ? AND task_id = ? AND attempt_id = ? AND status NOT IN ?", event.TenantID, event.TaskID, attemptID, terminalAttemptStatuses)
	if event.EntityVersion > 0 {
		query = query.Where("entity_version < ?", event.EntityVersion)
	}
	values["entity_version"] = event.EntityVersion
	columns := make([]string, 0, len(values))
	for column := range values {
		columns = append(columns, column)
	}
	result := query.Select(columns).Updates(values)
	return result.RowsAffected > 0, result.Error
}

// projectAttemptEvent projects attempt.parked, attempt.resumed and attempt.finished onto the attempt's row. The
// workflow's events (parked, finished) stamp the row's status_changed_at and clear its resumed key; a worker's
// attempt.resumed is applied only when it is newer than the last one applied since, and not older than that stamp.
func projectAttemptEvent(tx *gorm.DB, event taskruntime.Event) error {
	var body struct {
		NodeID          string          `json:"node_id"`
		AttemptID       string          `json:"attempt_id"`
		AttemptNo       int             `json:"attempt_no"`
		Profile         string          `json:"profile"`
		ConfigVersion   *int            `json:"config_version"`
		Reason          string          `json:"reason"`
		Outcome         string          `json:"outcome"`
		Failure         json.RawMessage `json:"failure"`
		Usage           json.RawMessage `json:"usage"`
		ActivityAttempt int             `json:"activity_attempt"`
		StateVersion    int             `json:"state_version"`
	}
	if json.Unmarshal(event.Payload, &body) != nil || !strings.HasPrefix(body.AttemptID, "att_") {
		return nil
	}
	switch event.Type {
	case "attempt.parked":
		status := map[string]string{"approval": "PARKED_HITL", "input": "PARKED_INPUT"}[body.Reason]
		if status == "" {
			return nil
		}
		_, err := moveAttempt(tx, event, body.AttemptID, map[string]any{
			"status": status, "status_changed_at": event.Occurred, "resumed_activity_attempt": 0, "resumed_state_version": 0,
		})
		return err
	case "attempt.resumed":
		return resumeAttempt(tx, event, body.AttemptID, body.ActivityAttempt, body.StateVersion)
	}
	status := attemptOutcomes[body.Outcome]
	if status == "" {
		return nil
	}
	values := map[string]any{"status": status, "finished_at": event.Occurred, "status_changed_at": event.Occurred}
	if hasJSON(body.Failure) {
		values["failure"] = []byte(body.Failure)
	}
	if hasJSON(body.Usage) {
		values["usage"] = []byte(body.Usage)
	}
	moved, err := moveAttempt(tx, event, body.AttemptID, values)
	if err != nil || moved {
		return err
	}
	// attempt.started may have been missed. The row is created from the finish when the event says which attempt of its
	// node this was and what it ran as; an existing row (terminal, or newer than this event) is left as it is.
	if !strings.HasPrefix(body.NodeID, "n_") || body.AttemptNo < 1 || body.Profile == "" {
		return nil
	}
	attemptRuntime := map[string]any{}
	if body.ConfigVersion != nil {
		attemptRuntime["config_version"] = *body.ConfigVersion
	}
	runtime, _ := json.Marshal(attemptRuntime)
	row := stageAttemptRow{
		AttemptID: body.AttemptID, TenantID: event.TenantID, TaskID: event.TaskID, NodeID: body.NodeID, AttemptNo: body.AttemptNo,
		Status: status, ProfileRef: body.Profile, Runtime: runtime, Usage: []byte("{}"), EntityVersion: event.EntityVersion,
		StartedAt: event.Occurred, FinishedAt: &event.Occurred, StatusChangedAt: &event.Occurred,
	}
	if hasJSON(body.Failure) {
		row.Failure = body.Failure
	}
	if hasJSON(body.Usage) {
		row.Usage = body.Usage
	}
	return createAttempt(tx, row)
}

// resumeAttempt applies attempt.resumed: a parked (or starting) attempt is RUNNING again. The event is the worker's
// (entity version 0), so its order comes from the contract's rule: against the workflow's events by occurred_at (not
// older than the last status change), and among the resumed events since by (activity_attempt, state_version). An event
// without an activity attempt (from before the runtime sent one) is ordered by time alone.
func resumeAttempt(tx *gorm.DB, event taskruntime.Event, attemptID string, activityAttempt, stateVersion int) error {
	values := map[string]any{"status": "RUNNING"}
	query := tx.Model(&stageAttemptRow{}).
		Where("tenant_id = ? AND task_id = ? AND attempt_id = ? AND status IN ?", event.TenantID, event.TaskID, attemptID, []string{"STARTING", "PARKED_HITL", "PARKED_INPUT"}).
		Where("(status_changed_at IS NULL OR status_changed_at <= ?)", event.Occurred)
	if activityAttempt > 0 {
		query = query.Where("(resumed_activity_attempt < ? OR (resumed_activity_attempt = ? AND resumed_state_version < ?))", activityAttempt, activityAttempt, stateVersion)
		values["resumed_activity_attempt"] = activityAttempt
		values["resumed_state_version"] = stateVersion
	}
	columns := make([]string, 0, len(values))
	for column := range values {
		columns = append(columns, column)
	}
	return query.Select(columns).Updates(values).Error
}

func hasJSON(raw json.RawMessage) bool { return len(raw) > 0 && string(raw) != "null" }

// NodeStructure reads where the task's nodes sit in compiled SOPs (parent_node_id, sop_step), keyed by node id. A node
// that has neither is left out.
func (s *Store) NodeStructure(ctx context.Context, p taskruntime.Principal, taskID string) (map[string]taskruntime.NodeStructure, error) {
	var rows []taskNodeRow
	err := s.inTenant(ctx, p.TenantID, func(tx *gorm.DB) error {
		return tx.Select("NodeID", "ParentNodeID", "SopStep").
			Where("tenant_id = ? AND task_id = ? AND (parent_node_id IS NOT NULL OR sop_step IS NOT NULL)", p.TenantID, taskID).
			Find(&rows).Error
	})
	if err != nil {
		return nil, projectionErr("read task node structure", err)
	}
	out := make(map[string]taskruntime.NodeStructure, len(rows))
	for _, row := range rows {
		item := taskruntime.NodeStructure{SopStep: json.RawMessage(row.SopStep)}
		if row.ParentNodeID != nil {
			item.ParentNodeID = *row.ParentNodeID
		}
		out[row.NodeID] = item
	}
	return out, nil
}
