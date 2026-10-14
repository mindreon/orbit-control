package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/mindreon/orbit-control/internal/audit"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/tenancy"
	"github.com/mindreon/orbit-control/internal/usage"
	"github.com/mindreon/orbit-control/migrations"
)

func main() {
	if e := run(os.Args[1:]); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: orbit-admin migrate up|down|status | seed | create-tenant | create-user | grant-role | create-group | add-group-member | maintenance prune [--usage-before RFC3339] | audit verify --tenant UUID")
	}
	c, e := config.Load()
	if e != nil {
		return e
	}
	if e = c.ValidateAdmin(); e != nil {
		return e
	}
	duration := time.Minute
	if args[0] == "market" {
		duration = 2 * time.Hour
	}
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	if args[0] == "migrate" {
		if len(args) != 2 {
			return fmt.Errorf("migrate requires up|down|status")
		}
		return migrations.Run(ctx, c.AdminDatabaseURL, args[1])
	}
	d, e := db.Open(ctx, c.AdminDatabaseURL)
	if e != nil {
		return e
	}
	defer d.Close()
	if args[0] == "market" {
		return importMarket(ctx, d, c, args[1:])
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	tenant := f.String("tenant", "", "tenant UUID")
	pid := f.String("principal", "", "principal UUID")
	actor := f.String("actor", "", "acting principal UUID")
	email := f.String("email", "", "email")
	name := f.String("name", "", "display name")
	slug := f.String("slug", "", "tenant slug")
	role := f.String("role", "tenant.member", "role name")
	scopeType := f.String("scope-type", "tenant", "scope type")
	scopeID := f.String("scope", "", "scope UUID")
	subjectType := f.String("principal-type", "user", "user|service|group")
	issuer := f.String("issuer", auth.DevIssuer, "identity issuer")
	subject := f.String("subject", "", "identity provider subject")
	usageBefore := f.String("usage-before", "", "RFC3339 retention cutoff for both usage ledger and dedup keys")
	group := f.String("group", "", "group UUID")
	rest := args[1:]
	if args[0] == "maintenance" {
		if len(rest) == 0 || rest[0] != "prune" {
			return fmt.Errorf("maintenance requires prune")
		}
		rest = rest[1:]
	}
	if args[0] == "audit" {
		if len(rest) == 0 || rest[0] != "verify" {
			return fmt.Errorf("audit requires verify")
		}
		rest = rest[1:]
	}
	if e = f.Parse(rest); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	emit := func(v any) error { return json.NewEncoder(os.Stdout).Encode(v) }
	if args[0] == "maintenance" {
		var cutoff *time.Time
		if *usageBefore != "" {
			v, err := time.Parse(time.RFC3339, *usageBefore)
			if err != nil {
				return fmt.Errorf("--usage-before must be RFC3339")
			}
			if v.After(time.Now()) {
				return fmt.Errorf("--usage-before cannot be in the future")
			}
			cutoff = &v
		}
		return usage.Prune(ctx, d, cutoff)
	}
	if args[0] == "audit" {
		if !ids.Valid(*tenant) {
			return fmt.Errorf("--tenant UUID required")
		}
		return d.WithTenant(ctx, *tenant, func(tx pgx.Tx) error {
			n, e := audit.Verify(ctx, tx)
			if e != nil {
				return e
			}
			return emit(map[string]any{"head": n, "tenant_id": *tenant})
		})
	}
	if args[0] == "seed" {
		if *email == "" {
			*email = "dev@orbit.local"
		}
		if *name == "" {
			*name = "Orbit developer"
		}
		p := auth.Principal{}
		e = d.Global(ctx, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, "SELECT id::text,kind,email,display_name FROM principals WHERE idp_subject=$1", auth.DevIssuer+"|"+*email).Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName)
		})
		if e == pgx.ErrNoRows {
			p, e = tenancy.CreateUser(ctx, d, *email, *name, auth.DevIssuer+"|"+*email, "", "", auth.Principal{})
		}
		if e != nil {
			return e
		}
		ts, e := tenancy.Tenants(ctx, d, p.ID)
		if e != nil {
			return e
		}
		return emit(map[string]any{"principal": p, "tenants": ts})
	}
	if args[0] == "create-user" {
		if *subject == "" {
			*subject = *email
		}
		var a auth.Principal
		if *actor != "" {
			a, e = loadPrincipal(ctx, d, *actor)
			if e != nil {
				return e
			}
		}
		p, e := tenancy.CreateUser(ctx, d, *email, *name, *issuer+"|"+*subject, *tenant, *role, a)
		if e != nil {
			return e
		}
		return emit(p)
	}
	if args[0] == "create-tenant" {
		if !ids.Valid(*pid) {
			return fmt.Errorf("--principal UUID (initial owner) required")
		}
		p, e := loadPrincipal(ctx, d, *pid)
		if e != nil {
			return e
		}
		t, e := tenancy.CreateTenant(ctx, d, p, *slug, *name)
		if e != nil {
			return e
		}
		return emit(t)
	}
	if args[0] == "create-group" || args[0] == "add-group-member" {
		if !ids.Valid(*tenant) || !ids.Valid(*actor) {
			return fmt.Errorf("--tenant and --actor UUID required")
		}
		a, err := loadPrincipal(ctx, d, *actor)
		if err != nil {
			return err
		}
		return d.WithTenant(ctx, *tenant, func(tx pgx.Tx) error {
			if args[0] == "create-group" {
				id, err := tenancy.CreateGroup(ctx, tx, a, *tenant, *name)
				if err != nil {
					return err
				}
				return emit(map[string]string{"id": id})
			}
			if !ids.Valid(*group) || !ids.Valid(*pid) {
				return fmt.Errorf("--group and --principal UUID required")
			}
			return tenancy.AddGroupMember(ctx, tx, a, *tenant, *group, *pid)
		})
	}
	if args[0] == "grant-role" {
		if !ids.Valid(*tenant) || !ids.Valid(*pid) || !ids.Valid(*actor) {
			return fmt.Errorf("--tenant, --principal, --actor UUID required")
		}
		a, e := loadPrincipal(ctx, d, *actor)
		if e != nil {
			return e
		}
		if *scopeID == "" {
			*scopeID = *tenant
		}
		return d.WithTenant(ctx, *tenant, func(tx pgx.Tx) error {
			if e := tenancy.Bind(ctx, tx, a, *tenant, *subjectType, *pid, *role, *scopeType, *scopeID); e != nil {
				return e
			}
			return emit(map[string]any{"granted": *role})
		})
	}
	return fmt.Errorf("unknown command %s", args[0])
}
func loadPrincipal(ctx context.Context, d *db.DB, id string) (auth.Principal, error) {
	var p auth.Principal
	if !ids.Valid(id) {
		return p, httpx.E(422, "INVALID_INPUT", "Invalid principal")
	}
	e := d.Global(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, "SELECT id::text,kind,email,display_name FROM principals WHERE id=$1 AND status='active'", id).Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName)
	})
	return p, e
}
