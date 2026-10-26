package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed *.sql
var files embed.FS

func Run(ctx context.Context, url, command string) error {
	d, e := sql.Open("pgx", url)
	if e != nil {
		return fmt.Errorf("invalid admin database configuration")
	}
	defer d.Close()
	p, e := goose.NewProvider(goose.DialectPostgres, d, files)
	if e != nil {
		return e
	}
	switch command {
	case "up":
		if _, e = p.Up(ctx); e != nil {
			return e
		}
		for _, t := range []string{"run_events", "usage_ledger", "audit_log"} {
			if _, e = d.ExecContext(ctx, "SELECT ensure_monthly_partitions($1,2)", t); e != nil {
				return e
			}
		}
		return nil
	case "down":
		// Provider.Down rolls back a single migration; with more than one file
		// "down" must wipe the schema, matching the integration suite's up/down cycle.
		_, e = p.DownTo(ctx, 0)
		return e
	case "status":
		s, e := p.Status(ctx)
		if e != nil {
			return e
		}
		for _, v := range s {
			fmt.Printf("%d %s\n", v.Source.Version, v.State)
		}
		return nil
	default:
		return fmt.Errorf("migrate requires up, down or status")
	}
}
