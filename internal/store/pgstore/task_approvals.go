package pgstore

import (
	"encoding/json"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

type taskApprovalRow struct {
	ApprovalID    string `gorm:"primaryKey"`
	TenantID      string
	TaskID        string
	NodeID        *string
	AttemptID     *string
	ToolCallID    *string
	Subject       []byte `gorm:"type:jsonb"`
	Status        string
	Comment       string
	Always        bool
	EntityVersion int64
	RequestedAt   time.Time `gorm:"autoCreateTime:false"`
	DecidedAt     *time.Time
}

func (taskApprovalRow) TableName() string { return "task_approvals" }

// approvalUpdatable are the only columns of task_approvals that orbit_app may UPDATE (00023): what a decision changes.
var approvalUpdatable = []string{"status", "comment", "always", "decided_at", "entity_version"}

// approvalDecisions are the statuses approval.decided may move a request to; PENDING is only ever the request.
var approvalDecisions = map[string]bool{"APPROVED": true, "REJECTED": true, "CANCELLED": true, "TAKEN_OVER": true}

// insertApproval projects approval.requested as a PENDING row. A request that is already there (a replay, or a
// decision that was projected first) is left alone, and one the table could not hold is skipped: the event is stored
// in task_events either way, and a malformed payload must not stop the events behind it.
func insertApproval(tx *gorm.DB, event taskruntime.Event) error {
	var request struct {
		ApprovalID string          `json:"approval_id"`
		NodeID     string          `json:"node_id"`
		AttemptID  string          `json:"attempt_id"`
		ToolCallID string          `json:"tool_call_id"`
		Subject    json.RawMessage `json:"subject"`
	}
	if json.Unmarshal(event.Payload, &request) != nil || !strings.HasPrefix(request.ApprovalID, "apr_") {
		return nil
	}
	if len(request.Subject) == 0 || string(request.Subject) == "null" {
		request.Subject = []byte("{}")
	}
	row := taskApprovalRow{
		ApprovalID: request.ApprovalID, TenantID: event.TenantID, TaskID: event.TaskID,
		NodeID: nonEmpty(request.NodeID), AttemptID: nonEmpty(request.AttemptID), ToolCallID: nonEmpty(request.ToolCallID),
		Subject: request.Subject, Status: "PENDING", EntityVersion: event.EntityVersion, RequestedAt: event.Occurred,
	}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "approval_id"}}, DoNothing: true}).
		Select("ApprovalID", "TenantID", "TaskID", "NodeID", "AttemptID", "ToolCallID", "Subject", "Status", "EntityVersion", "RequestedAt").
		Create(&row).Error
}

// decideApproval projects approval.decided onto the request's row. An event of the approval's entity applies only when
// its version is newer than the row's (an old one arriving late changes nothing); an event without a version applies
// only to a row that is still PENDING.
func decideApproval(tx *gorm.DB, event taskruntime.Event) error {
	var decision struct {
		ApprovalID string `json:"approval_id"`
		Status     string `json:"status"`
		Comment    string `json:"comment"`
		Always     bool   `json:"always"`
	}
	if json.Unmarshal(event.Payload, &decision) != nil || !approvalDecisions[decision.Status] {
		return nil
	}
	query := tx.Model(&taskApprovalRow{}).Where("approval_id = ? AND tenant_id = ?", decision.ApprovalID, event.TenantID)
	if event.EntityVersion > 0 {
		query = query.Where("entity_version < ?", event.EntityVersion)
	} else {
		query = query.Where("status = 'PENDING'")
	}
	return query.Select(approvalUpdatable).Updates(map[string]any{
		"status": decision.Status, "comment": decision.Comment, "always": decision.Always,
		"decided_at": event.Occurred, "entity_version": event.EntityVersion,
	}).Error
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
