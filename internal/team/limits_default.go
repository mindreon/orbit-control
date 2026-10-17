//go:build !workflow_test_limits

package team

const (
	MaxTurnsTotal   = 500
	MailboxCap      = 10
	TurnWallSeconds = 3600
)
