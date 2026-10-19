package workflows

import "time"

const UsageRollupWorkflow = "UsageRollupWorkflow"

type UsageRollupInput struct {
	Before      time.Time
	AfterStart  time.Time
	AfterTenant string
}

// DirtyHour and DirtyPage contain identifiers only, never ledger payloads.
type DirtyHour struct {
	TenantID string
	Hour     time.Time
}

type DirtyPage struct {
	AfterStart  time.Time
	AfterTenant string
	Before      time.Time
	Limit       int
}
