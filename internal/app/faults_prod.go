//go:build !e2e

package app

func (a *App) skipResultWriteEnabled() bool { return false }
