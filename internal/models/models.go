package models

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"math/big"
	"time"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/secrets"
)

// Price rounds each token category upward, without overflowing the multiplication.
func Price(tokens int, perMillion int64) (int64, error) {
	if tokens < 0 || perMillion < 0 {
		return 0, fmt.Errorf("negative pricing input")
	}
	n := new(big.Int).Mul(big.NewInt(int64(tokens)), big.NewInt(perMillion))
	n.Add(n, big.NewInt(999999))
	n.Div(n, big.NewInt(1000000))
	if !n.IsInt64() {
		return 0, fmt.Errorf("price overflow")
	}
	return n.Int64(), nil
}
func Credentials(ctx context.Context, tenant, profile string, expires time.Time, keys secrets.Keys) (
	contract.ModelCredentials,
	string,
	error,
) {
	var c contract.ModelCredentials
	var id string
	var secret *string
	var params, pricing []byte
	err := db.Tx(ctx).QueryRow(
		ctx,
		"SELECT id::text,base_url,model,secret_id::text,params,pricing,con"+
			"text_window FROM model_profiles WHERE tenant_id=$1 AND (($2='' AN"+
			"D is_default) OR name=$2) ORDER BY id LIMIT 1",
		tenant,
		profile,
	).Scan(
		&id,
		&c.BaseUrl,
		&c.Model,
		&secret,
		&params,
		&pricing,
		&c.ContextWindow,
	)
	if err != nil {
		return c, id, err
	}
	if err = json.Unmarshal(params, &c.Params); err != nil {
		return c, id, err
	}
	if err = json.Unmarshal(pricing, &c.Pricing); err != nil {
		return c, id, err
	}
	if secret != nil {
		plain, e := keys.Get(ctx, tenant, *secret)
		if e != nil {
			return c, id, e
		}
		c.ApiKey = contract.Secret(plain)
	}
	c.ExpiresAt = expires
	return c, id, nil
}

// BindingKey scopes credentials to the attempt that actually received them.
func BindingKey(attempt int, model string) string {
	return fmt.Sprintf("%d:%s", attempt, model)
}

func Bind(
	ctx context.Context,
	c auth.CapabilityClaims,
	id string,
	credentials contract.ModelCredentials,
) error {
	binding, err := json.Marshal(struct {
		ID      string                `json:"id"`
		Pricing contract.ModelPricing `json:"pricing"`
	}{ID: id, Pricing: credentials.Pricing})
	if err != nil {
		return err
	}
	key := BindingKey(c.Attempt, credentials.Model)
	result, err := db.Tx(ctx).Exec(ctx, `UPDATE run_segments SET model_profiles=jsonb_set(model_profiles,ARRAY[$5],$6)
 WHERE tenant_id=$1 AND run_id=$2 AND member=$3 AND seq=$4
 AND (NOT model_profiles ? $5 OR model_profiles->$5=$6::jsonb)`, c.Tenant, c.Run, c.Member, c.Seq, key, binding)
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return httpx.E(
			409,
			"MODEL_PROFILE_CONFLICT",
			"A model is already bound to different credentials in this attempt",
		)
	}
	return nil
}
