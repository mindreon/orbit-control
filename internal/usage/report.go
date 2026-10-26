package usage

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/policy"
)

type ReportOptions struct {
	Scope, ID, Granularity string
	From, To               time.Time
	GroupBy                []string
}

type UsageReport struct {
	Scope           string                      `json:"scope"`
	From            time.Time                   `json:"from"`
	To              time.Time                   `json:"to"`
	Granularity     string                      `json:"granularity"`
	ComputedThrough time.Time                   `json:"computed_through"`
	DirtyHours      int64                       `json:"dirty_hours"`
	Items           []map[string]jsontext.Value `json:"items"`
}

var dimensions = []struct{ key, column, output string }{
	{key: "workspace", column: "workspace_id", output: "workspace_id"}, {key: "principal", column: "principal_id", output: "principal_id"},
	{key: "asset", column: "asset_ref", output: "asset_ref"}, {key: "model_profile", column: "model_profile_id", output: "model_profile_id"},
	{key: "member", column: "member", output: "member"},
	{key: "source", column: "source", output: "source"},
	{key: "meter", column: "meter", output: "meter"},
}

func (o *ReportOptions) Validate(now time.Time) error {
	if o.Scope == "" {
		o.Scope = "tenant"
	}
	if o.Granularity == "" {
		o.Granularity = "day"
	}
	if o.To.IsZero() {
		o.To = now.UTC().Truncate(time.Hour)
	}
	if o.From.IsZero() {
		o.From = o.To.Add(-7 * 24 * time.Hour)
	}
	bad := func() error { return httpx.E(422, "INVALID_INPUT", "Invalid usage report parameters") }
	if !slices.Contains([]string{"tenant", "workspace", "user"}, o.Scope) ||
		!slices.Contains([]string{"hour", "day", "month"}, o.Granularity) {
		return bad()
	}
	if o.Scope == "workspace" && o.ID == "" || o.Scope == "tenant" && o.ID != "" || o.ID != "" && !ids.Valid(o.ID) {
		return bad()
	}
	if !o.From.Equal(o.From.UTC().Truncate(time.Hour)) || !o.To.Equal(o.To.UTC().Truncate(time.Hour)) ||
		!o.From.Before(o.To) || o.To.Sub(o.From) > 93*24*time.Hour {
		return bad()
	}
	seen := map[string]bool{}
	for _, key := range o.GroupBy {
		if seen[key] || !slices.ContainsFunc(dimensions, func(d struct{ key, column, output string }) bool { return d.key == key }) {
			return bad()
		}
		seen[key] = true
	}
	return nil
}

// ReportSQL never even computes a cross-meter quantity sum.
func ReportSQL(o ReportOptions) string {
	selects := []string{"date_trunc('" + o.Granularity + "',r.period_start,'UTC') AS period_start"}
	groups := []string{"1"}
	for _, d := range dimensions {
		if !slices.Contains(o.GroupBy, d.key) {
			continue
		}
		selects = append(selects, "r."+d.column)
		groups = append(groups, fmt.Sprint(len(selects)))
	}
	quantity := "NULL::text AS quantity"
	if slices.Contains(o.GroupBy, "meter") {
		quantity = "sum(r.quantity)::text AS quantity"
	}
	selects = append(selects, quantity, "sum(r.millicredits)::bigint AS millicredits")
	query := `SELECT ` + strings.Join(selects, ",") + ` FROM usage_rollups r
 WHERE r.tenant_id=$1 AND r.period_start >= $2 AND r.period_start < $3
 AND ($4='tenant' OR ($4='workspace' AND r.workspace_id=NULLIF($5,'')::uuid)
 OR ($4='user' AND r.principal_id=NULLIF($5,'')::uuid))
 AND ($6 OR r.workspace_id=$7::uuid) GROUP BY ` + strings.Join(groups, ",")
	object := []string{"'period_start',g.period_start", "'quantity',g.quantity", "'millicredits',g.millicredits"}
	for _, d := range dimensions {
		if !slices.Contains(o.GroupBy, d.key) {
			continue
		}
		null := "''"
		if strings.HasSuffix(d.column, "_id") {
			null = "'00000000-0000-0000-0000-000000000000'"
		}
		object = append(object, "'"+d.output+"',NULLIF(g."+d.column+"::text,"+null+")")
	}
	join := ""
	if slices.Contains(o.GroupBy, "model_profile") {
		join = " LEFT JOIN model_profiles p ON p.tenant_id=$1 AND p.id=g.model_profile_id"
		object = append(object, "'model_profile_name',p.name")
	}
	order := []string{"g.period_start"}
	for _, d := range dimensions {
		if slices.Contains(o.GroupBy, d.key) {
			order = append(order, "g."+d.column)
		}
	}
	return "SELECT jsonb_build_object(" + strings.Join(object, ",") + ") FROM (" + query + ") g" + join +
		" ORDER BY " + strings.Join(order, ",")
}

// usageArgs supplies the iam project clamp for the report query: an iam user
// token only sees its current baize project's usage, and with no current
// project none at all (a NULL uuid matches no workspace row).
func usageArgs(ctx context.Context) (bool, any) {
	ws, restricted := auth.IAM(ctx)
	if !restricted {
		return true, nil
	}
	if ws != "" {
		return false, ws
	}
	return false, nil
}

func Report(ctx context.Context, o ReportOptions) (UsageReport, error) {
	out := UsageReport{Scope: o.Scope, From: o.From, To: o.To, Granularity: o.Granularity, Items: []map[string]jsontext.Value{}}
	p, tenant := auth.From(ctx), auth.Tenant(ctx)
	a := policy.Authorizer{}
	auditResource := policy.Resource{Type: "tenant", ID: tenant}
	if o.Scope == "user" && o.ID == "" {
		o.ID = p.ID
	}
	var err error
	switch o.Scope {
	case "tenant":
		err = a.Require(ctx, "audit.read", auditResource)
	case "workspace":
		err = a.Require(ctx, "audit.read", auditResource)
		if err != nil {
			err = a.Require(ctx, "workspace.update", policy.Resource{Type: "workspace", ID: o.ID, WorkspaceID: o.ID})
		}
	case "user":
		if o.ID != p.ID {
			err = a.Require(ctx, "audit.read", auditResource)
		}
	}
	if err != nil {
		return out, err
	}
	tx := db.Tx(ctx)
	if err = tx.QueryRow(ctx, `SELECT least($2,min(period_start),date_trunc('hour',now(),'UTC')),
 count(*) FILTER(WHERE period_start >= $3 AND period_start < $2) FROM usage_rollup_dirty WHERE tenant_id=$1`,
		tenant, o.To, o.From).Scan(&out.ComputedThrough, &out.DirtyHours); err != nil {
		return out, err
	}
	anyWorkspace, clampWorkspace := usageArgs(ctx)
	rows, err := tx.Query(ctx, ReportSQL(o), tenant, o.From, o.To, o.Scope, o.ID, anyWorkspace, clampWorkspace)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return out, err
		}
		var item map[string]jsontext.Value
		if err := json.Unmarshal(raw, &item); err != nil {
			return out, err
		}
		out.Items = append(out.Items, item)
	}
	return out, rows.Err()
}

type CostItem struct {
	Member           *string `json:"member"`
	Segment          *int    `json:"segment"`
	Source           string  `json:"source"`
	Meter            Meter   `json:"meter"`
	ModelProfileID   *string `json:"model_profile_id"`
	ModelProfileName *string `json:"model_profile_name"`
	Model            *string `json:"model"`
	Quantity         string  `json:"quantity"`
	Millicredits     int64   `json:"millicredits"`
}

type Cost struct {
	RunID             string     `json:"run_id"`
	TotalMillicredits int64      `json:"total_millicredits"`
	Items             []CostItem `json:"items"`
}

func RunCost(ctx context.Context, run string) (Cost, error) {
	out := Cost{RunID: run, Items: []CostItem{}}
	rows, err := db.Tx(ctx).Query(ctx, `SELECT l.member,l.segment_seq,l.source,l.meter,l.model_profile_id::text,
 p.name,p.model,sum(l.quantity)::text,sum(l.millicredits)::bigint
 FROM usage_ledger l LEFT JOIN model_profiles p ON p.tenant_id=l.tenant_id AND p.id=l.model_profile_id
 WHERE l.tenant_id=$1 AND l.run_id=$2
 GROUP BY l.member,l.segment_seq,l.source,l.meter,l.model_profile_id,p.name,p.model
 ORDER BY l.member,l.segment_seq,l.source,l.meter,l.model_profile_id`, auth.Tenant(ctx), run)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var item CostItem
		if err = rows.Scan(&item.Member, &item.Segment, &item.Source, &item.Meter, &item.ModelProfileID,
			&item.ModelProfileName, &item.Model, &item.Quantity, &item.Millicredits); err != nil {
			return out, err
		}
		out.TotalMillicredits += item.Millicredits
		out.Items = append(out.Items, item)
	}
	return out, rows.Err()
}
