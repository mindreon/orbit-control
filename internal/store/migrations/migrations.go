// Package migrations embeds the control schema (contract §18.2) and runs it
// with goose. Migrations run as the owner role (ORBIT_CONTROL_MIGRATE_DB_URL),
// never as orbit_app. Prod is forward-only; Down exists for CI and dev.
package migrations

import (
	"context"
	"database/sql"
	"embed"
	"io/fs"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed sql/*.sql
var embedded embed.FS

func provider(db *sql.DB) (*goose.Provider, error) {
	fsys, err := fs.Sub(embedded, "sql")
	if err != nil {
		return nil, err
	}
	return goose.NewProvider(goose.DialectPostgres, db, fsys)
}

func open(url string) (*sql.DB, error) {
	return sql.Open("pgx", url)
}

// Up applies all pending migrations. Running it on an up-to-date database is
// a no-op.
func Up(ctx context.Context, migrateURL string) error {
	db, err := open(migrateURL)
	if err != nil {
		return err
	}
	defer db.Close()
	p, err := provider(db)
	if err != nil {
		return err
	}
	_, err = p.Up(ctx)
	return err
}

// Reset rolls every migration back. CI and dev only.
func Reset(ctx context.Context, migrateURL string) error {
	db, err := open(migrateURL)
	if err != nil {
		return err
	}
	defer db.Close()
	p, err := provider(db)
	if err != nil {
		return err
	}
	_, err = p.DownTo(ctx, 0)
	return err
}
