package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/usage"
)

func usageCommand(ctx context.Context, d *db.DB, args []string) error {
	if len(args) == 0 || args[0] != "rollup-mark" {
		return fmt.Errorf("usage requires rollup-mark")
	}
	flags := flag.NewFlagSet("usage rollup-mark", flag.ContinueOnError)
	from := flags.String("from", "", "first UTC hour (RFC3339)")
	to := flags.String("to", "", "exclusive final UTC hour (RFC3339)")
	tenant := flags.String("tenant", "", "optional tenant UUID")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 || *tenant != "" && !ids.Valid(*tenant) {
		return fmt.Errorf("invalid rollup-mark arguments")
	}
	start, err := time.Parse(time.RFC3339, *from)
	if err != nil {
		return fmt.Errorf("invalid --from")
	}
	end, err := time.Parse(time.RFC3339, *to)
	if err != nil {
		return fmt.Errorf("invalid --to")
	}
	return usage.MarkRollups(ctx, d, start, end, *tenant)
}
