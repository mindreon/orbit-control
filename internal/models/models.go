package models

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"math/big"
	"time"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
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
func Credentials(ctx context.Context, tenant, profile string, expires time.Time, keys secrets.Keys) (contract.ModelCredentials, error) {
	var c contract.ModelCredentials
	var secret *string
	var params, pricing []byte
	err := db.Tx(ctx).QueryRow(ctx, `SELECT base_url,model,secret_id::text,params,pricing,context_window FROM model_profiles WHERE tenant_id=$1 AND (($2='' AND is_default) OR name=$2) ORDER BY id LIMIT 1`, tenant, profile).Scan(&c.BaseUrl, &c.Model, &secret, &params, &pricing, &c.ContextWindow)
	if err != nil {
		return c, err
	}
	if err = json.Unmarshal(params, &c.Params); err != nil {
		return c, err
	}
	if err = json.Unmarshal(pricing, &c.Pricing); err != nil {
		return c, err
	}
	if secret != nil {
		plain, e := keys.Get(ctx, tenant, *secret)
		if e != nil {
			return c, e
		}
		c.ApiKey = contract.Secret(plain)
	}
	c.ExpiresAt = expires
	return c, nil
}
