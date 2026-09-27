//go:build !e2e

package app

func wrapOrch(o Orchestrator) Orchestrator { return o }

func (a *App) skipResultWriteEnabled() bool { return false }
