//go:build e2e

package app

// SetSkipResultWrite tells the Postgres decide path to mark the approval
// delivered and return without writing the resumed turn. The reconciler
// (or WriteResultAgain) performs that write. This symbol is not in the
// production build.
func (a *App) SetSkipResultWrite(skip bool) { a.skipResultWrite.Store(skip) }

func (a *App) skipResultWriteEnabled() bool { return a.skipResultWrite.Load() }
