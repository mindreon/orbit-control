package ids

import "github.com/google/uuid"

func New() string         { return uuid.Must(uuid.NewV7()).String() }
func Valid(s string) bool { _, err := uuid.Parse(s); return err == nil }
