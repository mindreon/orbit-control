// Package team contains the pure scheduling rules for Orbit team runs.
package team

const (
	MaxTasks              = 32
	TitleBytes            = 200
	DescriptionBytes      = 4096
	ResultBytes           = 4096
	CommentBytes          = 1024
	MessageBytes          = 2048
	SteerCap              = 20
	FilesMax              = 20
	FileBytes             = 256
	SummaryBytes          = 8192
	BoardBytesMax         = 384 << 10
	AppliedPerTurnMax     = 32
	MailBytesMax          = 128 << 10
	NarrowedToolsMax      = 32
	ToolNameBytes         = 128
	MaxRoster             = 16
	MaxTaskFailures       = 3
	ReviewTurnWallSeconds = 1800
	TurnMaxSegments       = 50
	ReserveFraction       = 10
)
