package usage

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/models"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/telemetry"
)

func ReportModelUsage(
	ctx context.Context, c auth.CapabilityClaims, req contract.UsageReport,
) (contract.UsageReportResponse, []TokenMeasurement, error) {
	var out contract.UsageReportResponse
	var points []TokenMeasurement

	tx := db.Tx(ctx)
	prefix := fmt.Sprintf("model:%s:%s:%d:%d:", c.Run, c.Member, c.Seq, c.Attempt)
	var workspace, principal string
	if err := tx.QueryRow(ctx, `SELECT s.workspace_id::text,r.created_by::text
 FROM runs r
 JOIN sessions s ON s.id=r.session_id
 WHERE r.tenant_id=$1
 AND r.id=$2`, c.Tenant, c.Run).Scan(&workspace, &principal); err != nil {
		return out, nil, err
	}
	for _, item := range req.Items {
		suffix, ok := strings.CutPrefix(item.IdempotencyKey, prefix)
		n, e := strconv.Atoi(suffix)
		if !ok || e != nil || n < 1 {
			return out, nil, httpx.E(422, "INVALID_USAGE_KEY", "Usage key must identify this attempt")
		}
		var raw []byte
		if err := tx.QueryRow(ctx, `SELECT model_profiles->$5->'pricing'
 FROM run_segments
 WHERE tenant_id=$1
 AND run_id=$2
 AND member=$3
 AND seq=$4
 AND model_profiles ? $5`, c.Tenant, c.Run, c.Member, c.Seq, models.BindingKey(c.Attempt, item.Model)).Scan(&raw); err !=
			nil {
			return out, nil, err
		}
		var pricing contract.ModelPricing
		if err := json.Unmarshal(raw, &pricing); err != nil {
			return out, nil, err
		}
		added, err := chargeModelTokens(ctx, c, workspace, principal, item, pricing)
		if err != nil {
			return out, nil, err
		}
		points = append(points, added...)

	}
	if err := tx.QueryRow(ctx, `SELECT COALESCE(sum(millicredits),0)::bigint
 FROM usage_ledger
 WHERE tenant_id=$1
 AND run_id=$2`, c.Tenant, c.Run).Scan(&out.RunUsedMillicredits); err != nil {
		return out, nil, err
	}
	return out, points, nil
}

func chargeModelTokens(
	ctx context.Context,
	c auth.CapabilityClaims,
	workspace, principal string,
	item contract.UsageItem,
	pricing contract.ModelPricing,
) ([]TokenMeasurement, error) {
	var points []TokenMeasurement
	for _, token := range []struct {
		suffix, kind string
		count        int
		price        int64
	}{
		{suffix: "in", kind: "input", count: item.InputTokens, price: pricing.InputPerMtokMillicredits},
		{
			suffix: "out", kind: "output",
			count: item.OutputTokens,
			price: pricing.OutputPerMtokMillicredits,
		},
		{
			suffix: "cr", kind: "cache_read",
			count: item.CacheReadTokens,
			price: pricing.CacheReadPerMtokMillicredits,
		},
		{
			suffix: "cw", kind: "cache_write",
			count: item.CacheWriteTokens,
			price: pricing.CacheWritePerMtokMillicredits,
		},
	} {
		cost, e := models.Price(token.count, token.price)
		if e != nil {
			return nil, httpx.E(422, "INVALID_USAGE", "Usage cost overflows")
		}
		added, e := RecordUsage(
			ctx,
			db.Tx(ctx),
			Entry{
				TenantID:       c.Tenant,
				WorkspaceID:    &workspace,
				PrincipalID:    &principal,
				RunID:          &c.Run,
				SegmentSeq:     &c.Seq,
				Source:         "model",
				Meter:          "tokens",
				Quantity:       strconv.Itoa(token.count),
				UnitPrice:      strconv.FormatInt(token.price, 10),
				Millicredits:   cost,
				IdempotencyKey: item.IdempotencyKey + ":" + token.suffix,
				OccurredAt:     item.OccurredAt,
			},
		)
		if e != nil {
			return nil, e
		}
		if added {
			points = append(points, TokenMeasurement{Tenant: c.Tenant, Model: item.Model, Type: token.kind, Count: int64(token.count)})
		}
	}
	return points, nil
}

// TokenMeasurement contains only metric dimensions, never usage payload content.
type TokenMeasurement struct {
	Tenant, Model, Type string
	Count               int64
}

func RecordTokens(ctx context.Context, points []TokenMeasurement) {
	for _, point := range points {
		telemetry.Counter("orbit.usage.tokens").Add(ctx, point.Count, attribute.String("orbit.tenant_id", point.Tenant),
			attribute.String("gen_ai.request.model", point.Model), attribute.String("orbit.token.type", point.Type))
	}
}
