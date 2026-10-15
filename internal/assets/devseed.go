package assets

import (
	"context"
	"fmt"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
)

// PublishDevelopmentSeed provisions only the trusted, embedded development agents.
// It is an administrative seed operation, never a user release endpoint.
func (s *Service) PublishDevelopmentSeed(ctx context.Context, a Asset, id, environment string) error {
	if environment != "dev" || a.Publisher != "dev" || a.Kind != "agent" ||
		(a.Name != "echo-agent" && a.Name != "notify-agent") {
		return fmt.Errorf("development seed publication requires dev seed agents and ORBIT_ENV=dev")
	}
	if err := s.Submit(ctx, a, id, "stable"); err != nil {
		return err
	}
	for _, state := range []string{"checking", "approved"} {
		if err := SetState(ctx, id, state); err != nil {
			return err
		}
	}
	v, err := s.Version(ctx, a.ID, id)
	if err != nil {
		return err
	}
	signature, err := s.Signer.Sign(a, v)
	if err != nil {
		return err
	}
	if _, err = db.Tx(ctx).Exec(ctx, `UPDATE asset_versions SET signature=$3,published_at=now(),release_started=true
 WHERE tenant_id=$1 AND id=$2`, auth.Tenant(ctx), id, signature); err != nil {
		return err
	}
	if err = SetState(ctx, id, "published"); err != nil {
		return err
	}
	if err = s.SetChannel(ctx, a, "stable", id); err != nil {
		return err
	}
	if _, err = db.Tx(ctx).Exec(ctx, `UPDATE assets SET latest_version=$3 WHERE tenant_id=$1 AND id=$2`,
		auth.Tenant(ctx), a.ID, v.Version); err != nil {
		return err
	}
	return Log(ctx, "asset.dev_seed_published", "asset_version", id, map[string]string{"key_id": s.Signer.ID})
}
