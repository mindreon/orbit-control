package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/sessions"
)

func requeueControlEvents(ctx context.Context, d *db.DB, args []string) error {
	f := flag.NewFlagSet("requeue-control-events", flag.ContinueOnError)
	q := sessions.RequeueControlEvents{}
	f.StringVar(&q.Tenant, "tenant", "", "tenant UUID (required)")
	f.StringVar(&q.Run, "run", "", "optional run UUID")
	f.StringVar(&q.EventID, "event-id", "", "optional event ID")
	f.StringVar(&q.Reason, "reason", "", "optional quarantine reason")
	f.BoolVar(&q.DryRun, "dry-run", false, "count without clearing quarantine")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	count, err := sessions.RequeueEvents(ctx, d, q)
	if err != nil {
		return err
	}
	return emit(map[string]any{"matched": count, "dry_run": q.DryRun})
}

func issueStreamToken(c config.Config, args []string) error {
	f := flag.NewFlagSet("issue-stream-token", flag.ContinueOnError)
	tenant := f.String("tenant", "", "tenant UUID")
	session := f.String("session", "", "session UUID")
	run := f.String("run", "", "run UUID (omit for session scope)")
	ttl := f.Duration("ttl", 15*time.Minute, "development token TTL; may be negative")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	claims := auth.StreamClaims{TenantID: *tenant, SessionID: *session, RunID: *run, Scope: "session"}
	if *run != "" {
		claims.Scope = "run"
	}
	token, err := auth.IssueDevelopmentStreamToken(c, c.StreamKeyFile, c.StreamKeyID, c.StreamPublicKeys, claims, *ttl)
	if err != nil {
		return err
	}
	return emit(map[string]any{"stream_token": token, "expires_in": int64(ttl.Seconds())})
}
