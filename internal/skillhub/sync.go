package skillhub

import (
	"context"
	"log"
	"time"

	"github.com/mindreon/orbit-control/internal/store"
)

const (
	pageSize    = 100
	maxPages    = 2000
	pageGap     = 200 * time.Millisecond
	retryWait   = 5 * time.Minute
	refreshWait = 6 * time.Hour
)

// Run keeps refreshing the local catalog until ctx is cancelled. A failed
// pass waits retryWait and tries again. A finished pass waits refreshWait.
// Listing the catalog does not enter this function.
func Run(ctx context.Context, repo store.Repository, tenantID string, client *Client, logger *log.Logger) {
	if logger == nil {
		logger = log.Default()
	}
	logger.Printf("skillhub: background sync started")
	for {
		if err := SyncOnce(ctx, repo, tenantID, client, logger); err != nil {
			logger.Printf("skillhub: sync failed: %v", err)
			if !sleep(ctx, retryWait) {
				return
			}
			continue
		}
		if !sleep(ctx, refreshWait) {
			return
		}
	}
}

// SyncOnce copies categories, the score-sorted skill list, and the trending
// showcase into repo. It stops when a page is short or the reported total is
// covered. Skill packages are not requested.
func SyncOnce(ctx context.Context, repo store.Repository, tenantID string, client *Client, logger *log.Logger) error {
	if logger == nil {
		logger = log.Default()
	}
	cats, err := client.ListCategories(ctx)
	if err != nil {
		return err
	}
	if err := repo.UpsertSkillCategories(ctx, tenantID, cats); err != nil {
		return err
	}
	var stored int
	for page := 1; page <= maxPages; page++ {
		rows, total, err := client.ListSkills(ctx, page, pageSize, "score")
		if err != nil {
			return err
		}
		if err := repo.UpsertSkillCatalog(ctx, tenantID, rows); err != nil {
			return err
		}
		stored += len(rows)
		logger.Printf("skillhub: stored page %d (%d rows, upstream total %d)", page, stored, total)
		if len(rows) == 0 || page*pageSize >= total {
			break
		}
		if !sleep(ctx, pageGap) {
			return ctx.Err()
		}
	}
	trending, err := client.ListTrending(ctx)
	if err != nil {
		logger.Printf("skillhub: trending refresh skipped: %v", err)
		return nil
	}
	if err := repo.ReplaceSkillTrending(ctx, tenantID, trending); err != nil {
		return err
	}
	logger.Printf("skillhub: trending ranks stored (%d)", len(trending))
	return nil
}

func sleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
