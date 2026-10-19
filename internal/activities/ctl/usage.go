package ctl

import (
	"context"

	"github.com/mindreon/orbit-control/internal/usage"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func (a *Activities) ListDirtyHours(ctx context.Context, in w.DirtyPage) ([]w.DirtyHour, error) {
	return usage.ListDirtyHours(ctx, a.DB, in)
}

func (a *Activities) RollupHour(ctx context.Context, in w.DirtyHour) error {
	return usage.RollupHour(ctx, a.DB, in, nil)
}
