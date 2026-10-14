package ids

import "uuid"

func New() string         { return uuid.NewV7().String() }
func Valid(s string) bool { _, err := uuid.Parse(s); return err == nil }
