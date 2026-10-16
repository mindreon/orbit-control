// Package workflows defines control-only payloads and durable names.
package workflows

import "strings"

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
