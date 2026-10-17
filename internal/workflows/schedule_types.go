package workflows

import "time"

type ScheduledRunInput struct{ TenantID, ScheduleID string }
type ScheduledSessionInput struct {
	TenantID, ScheduleID string
	FireAt, Now          time.Time
}
type ScheduledStart struct {
	Kind, Skip string
	Run        *RunInput
	Team       *TeamRunInput
}
type RecordScheduleInput struct {
	TenantID, ScheduleID string
	FireAt               time.Time
}
