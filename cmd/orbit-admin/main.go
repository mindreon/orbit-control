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
		return fmt.Errorf(
			"usage: orbit-admin migrate up|down|status | seed | create-tenant | create-user | " +
				"grant-role | create-group | add-group-member | maintenance prune [--usage-before RFC3339] " +
				"| audit verify --tenant UUID",
		)
	}
	c, e := config.Load()
	if e != nil {
		return e
	}
	if e = c.ValidateAdmin(); e != nil {
		return e
	}
	duration := time.Minute
	if args[0] == "seed-dev" {
		duration = 5 * time.Minute
	}
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
	if args[0] == "asset" {
		return retryAssetRelease(ctx, d, args[1:])
	}
	if args[0] == "put-secret" || args[0] == "put-model-profile" || args[0] == "seed-dev" {
		return runAdmin(ctx, d, c, args)
	}
	o, err := parseAdminOptions(args)
	if err != nil {
		return err
	}
	switch o.command {
	case "maintenance":
		return o.pruneUsage(ctx, d)
	case "audit":
		return o.verifyAudit(ctx, d)
	case "seed":
		return o.seedIdentity(ctx, d)
	case "create-user":
		return o.createUser(ctx, d)
	case "create-tenant":
		return o.createTenant(ctx, d)
	case "create-group", "add-group-member":
		return o.manageGroup(ctx, d)
	case "grant-role":
		return o.grantRole(ctx, d)
	default:
		return fmt.Errorf("unknown command %s", o.command)
	}
}

func loadPrincipal(ctx context.Context, d *db.DB, id string) (auth.Principal, error) {
	var p auth.Principal
	if !ids.Valid(id) {
		return p, httpx.E(422, "INVALID_INPUT", "Invalid principal")
	}
	e := d.Global(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, "SELECT id::text,kind,email,display_name FROM principals WHERE id=$1 AND status='active'", id).
			Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName)
	})
	return p, e
}

type adminOptions struct {
	command,
	tenant,
	pid,
	actor,
	email,
	name,
	slug,
	role,
	scopeType,
	scopeID,
	subjectType,
	issuer,
	subject,
	usageBefore,
	group string
}

func parseAdminOptions(args []string) (adminOptions, error) {
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
			return adminOptions{}, fmt.Errorf("maintenance requires prune")
		}
		rest = rest[1:]
	}
	if args[0] == "audit" {
		if len(rest) == 0 || rest[0] != "verify" {
			return adminOptions{}, fmt.Errorf("audit requires verify")
		}
		rest = rest[1:]
	}
	if e := f.Parse(rest); e != nil {
		return adminOptions{}, e
	}
	if f.NArg() != 0 {
		return adminOptions{}, fmt.Errorf("unexpected arguments")
	}
	return adminOptions{
		command:     args[0],
		tenant:      *tenant,
		pid:         *pid,
		actor:       *actor,
		email:       *email,
		name:        *name,
		slug:        *slug,
		role:        *role,
		scopeType:   *scopeType,
		scopeID:     *scopeID,
		subjectType: *subjectType,
		issuer:      *issuer,
		subject:     *subject,
		usageBefore: *usageBefore,
		group:       *group,
	}, nil
}

func (o adminOptions) pruneUsage(ctx context.Context, d *db.DB) error {

	var cutoff *time.Time
	if o.usageBefore != "" {
		v, err := time.Parse(time.RFC3339, o.usageBefore)
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

func (o adminOptions) verifyAudit(ctx context.Context, d *db.DB) error {

	if !ids.Valid(o.tenant) {
		return fmt.Errorf("--tenant UUID required")
	}
	return d.WithTenant(ctx, o.tenant, func(tx pgx.Tx) error {
		n, e := audit.Verify(ctx, tx)
		if e != nil {
			return e
		}
		return emit(map[string]any{"head": n, "tenant_id": o.tenant})
	})

}

func (o adminOptions) seedIdentity(ctx context.Context, d *db.DB) error {
	var e error

	if o.email == "" {
		o.email = "dev@orbit.local"
	}
	if o.name == "" {
		o.name = "Orbit developer"
	}
	p := auth.Principal{}
	e = d.Global(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, "SELECT id::text,kind,email,display_name FROM principals WHERE idp_subject=$1", auth.DevIssuer+"|"+o.email).
			Scan(&p.ID, &p.Kind, &p.Email, &p.DisplayName)
	})
	if e == pgx.ErrNoRows {
		p, e = tenancy.CreateUser(ctx, d, o.email, o.name, auth.DevIssuer+"|"+o.email, "", "", auth.Principal{})
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

func (o adminOptions) createUser(ctx context.Context, d *db.DB) error {
	var e error

	if o.subject == "" {
		o.subject = o.email
	}
	var a auth.Principal
	if o.actor != "" {
		a, e = loadPrincipal(ctx, d, o.actor)
		if e != nil {
			return e
		}
	}
	p, e := tenancy.CreateUser(ctx, d, o.email, o.name, o.issuer+"|"+o.subject, o.tenant, o.role, a)
	if e != nil {
		return e
	}
	return emit(p)

}

func (o adminOptions) createTenant(ctx context.Context, d *db.DB) error {

	if !ids.Valid(o.pid) {
		return fmt.Errorf("--principal UUID (initial owner) required")
	}
	p, e := loadPrincipal(ctx, d, o.pid)
	if e != nil {
		return e
	}
	t, e := tenancy.CreateTenant(ctx, d, p, o.slug, o.name)
	if e != nil {
		return e
	}
	return emit(t)

}

func (o adminOptions) manageGroup(ctx context.Context, d *db.DB) error {

	if !ids.Valid(o.tenant) || !ids.Valid(o.actor) {
		return fmt.Errorf("--tenant and --actor UUID required")
	}
	a, err := loadPrincipal(ctx, d, o.actor)
	if err != nil {
		return err
	}
	return d.WithTenant(ctx, o.tenant, func(tx pgx.Tx) error {
		if o.command == "create-group" {
			id, err := tenancy.CreateGroup(ctx, tx, a, o.tenant, o.name)
			if err != nil {
				return err
			}
			return emit(map[string]string{"id": id})
		}
		if !ids.Valid(o.group) || !ids.Valid(o.pid) {
			return fmt.Errorf("--group and --principal UUID required")
		}
		return tenancy.AddGroupMember(ctx, tx, a, o.tenant, o.group, o.pid)
	})

}

func (o adminOptions) grantRole(ctx context.Context, d *db.DB) error {

	if !ids.Valid(o.tenant) || !ids.Valid(o.pid) || !ids.Valid(o.actor) {
		return fmt.Errorf("--tenant, --principal, --actor UUID required")
	}
	a, e := loadPrincipal(ctx, d, o.actor)
	if e != nil {
		return e
	}
	if o.scopeID == "" {
		o.scopeID = o.tenant
	}
	return d.WithTenant(ctx, o.tenant, func(tx pgx.Tx) error {
		if e := tenancy.Bind(ctx, tx, a, o.tenant, o.subjectType, o.pid, o.role, o.scopeType, o.scopeID); e != nil {
			return e
		}
		return emit(map[string]any{"granted": o.role})
	})

}

func emit(value any) error { return json.NewEncoder(os.Stdout).Encode(value) }
