package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/store/memstore"
	"github.com/mindreon/orbit-control/internal/store/migrations"
	"github.com/mindreon/orbit-control/internal/store/pgstore"
)

func prodConfig() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("ORBIT_ENV")), "prod") ||
		strings.EqualFold(strings.TrimSpace(os.Getenv("ORBIT_AUTH_MODE")), "oidc")
}

// openRepository selects storage per §18.2. In prod configuration a missing
// ORBIT_CONTROL_DB_URL is fatal; control never silently falls back to memory.
// Neither URL is ever logged or returned in an error.
func openRepository(ctx context.Context) (store.Repository, error) {
	url := strings.TrimSpace(os.Getenv("ORBIT_CONTROL_DB_URL"))
	if url == "" {
		if prodConfig() {
			return nil, errors.New("ORBIT_CONTROL_DB_URL is required when ORBIT_ENV=prod or ORBIT_AUTH_MODE=oidc; refusing to start with in-memory storage")
		}
		log.Printf("storage: in-memory (ORBIT_CONTROL_DB_URL unset; dev/test only)")
		return memstore.New(), nil
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if os.Getenv("ORBIT_CONTROL_MIGRATE_ON_START") == "1" {
		migrateURL := strings.TrimSpace(os.Getenv("ORBIT_CONTROL_MIGRATE_DB_URL"))
		if migrateURL == "" {
			return nil, errors.New("ORBIT_CONTROL_MIGRATE_ON_START=1 needs ORBIT_CONTROL_MIGRATE_DB_URL (owner role)")
		}
		if err := migrations.Up(ctx, migrateURL); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				return nil, fmt.Errorf("migrations failed: code=%s: %s", pgErr.Code, pgErr.Message)
			}
			return nil, pgstore.SanitizeConnError(err)
		}
		log.Printf("storage: migrations applied")
	}
	repo, err := pgstore.Open(ctx, url)
	if err != nil {
		return nil, err
	}
	log.Printf("storage: postgres")
	return repo, nil
}
