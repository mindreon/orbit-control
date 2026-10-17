// Package workflows defines control-only payloads and durable names.
package workflows

import (
	"strconv"
	"strings"
)

const (
	TeamRunWorkflow      = "TeamRunWorkflow"
	MemberTaskWorkflow   = "MemberTaskWorkflow"
	ScheduledRunWorkflow = "ScheduledRunWorkflow"
	Deliver              = "Deliver"
	EndTurn              = "EndTurn"
	Board                = "Board"
	MemberMain           = "main"
	MemberTeam           = "team"
	TaskQueueCtlWait     = "ctl-wait"
)

func TeamRunWorkflowID(tenant, run string) string { return "team:" + tenant + ":" + run }
func MemberTaskWorkflowID(tenant, run, member string, turn int) string {
	return "member:" + tenant + ":" + run + ":" + member + ":" + strconv.Itoa(turn)
}
func RunWorkflowID(kind, tenant, run string) string {
	if kind == "team" {
		return TeamRunWorkflowID(tenant, run)
	}
	return AgentRunWorkflowID(tenant, run)
}
func TeamSessionKey(run, member string) string  { return "team:" + run + ":" + member }
func ScheduleID(tenant, schedule string) string { return "sched:" + tenant + ":" + schedule }
func ScheduledRunWorkflowIDBase(tenant, schedule string) string {
	return "schedrun:" + tenant + ":" + schedule
}
func TeamMember(member string) bool {
	return member != "" && member != MemberMain && member != MemberTeam
}
func LeaseHolder(run, member string) string {
	if TeamMember(member) {
		return run + "/" + member
	}
	return run
}

const (
	SessionWorkflow      = "SessionWorkflow"
	AgentRunWorkflow     = "AgentRunWorkflow"
	SandboxLeaseWorkflow = "SandboxLeaseWorkflow"
	SubmitMessage        = "SubmitMessage"
	CancelRun            = "CancelRun"
	SetPolicy            = "SetPolicy"
	Archive              = "Archive"
	State                = "State"
	ResolveInteraction   = "ResolveInteraction"
	Steer                = "Steer"
	Cancel               = "Cancel"
	Pause                = "Pause"
	Resume               = "Resume"
	AssetSuspended       = "AssetSuspended"
	Progress             = "Progress"
	Acquire              = "Acquire"
	Touch                = "Touch"
	Lease                = "Lease"
)

func SessionWorkflowID(tenant, session string) string {
	return "sess:" + tenant + ":" + session
}
func AgentRunWorkflowID(tenant, run string) string {
	return "run:" + tenant + ":" + run
}
func LeaseWorkflowID(tenant, key string) string {
	return "sbx:" + tenant + ":" + key
}
func IsolationKey(tenant, session string) string {
	return "t-" + tenant + "-s-" + session
}
func AgentSessionKey(session, digest string) string {
	return "sess:" + session + ":" + strings.TrimPrefix(digest, "sha256:")[:12]
}
func EventSubject(tenant, session, run string) string {
	return "run." + tenant + "." + session + "." + run
}

func Terminal(status string) bool {
	switch status {
	case "succeeded", "failed", "cancelled", "budget_exhausted", "timed_out":
		return true
	}
	return false
}
