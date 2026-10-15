package integration

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	releaseactivities "github.com/mindreon/orbit-control/internal/activities/assetrelease"
	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/workflows/assetrelease"
)

func testAssetReviewBoundaries(
	t *testing.T,
	d *db.DB,
	s *assets.Service,
	a *releaseactivities.Activities,
	request assetRequest,
	token, reviewer, tenant, owner, reviewerID, workspace string,
) {
	t.Helper()
	ctx := t.Context()
	check := func(status, want int, out any) {
		t.Helper()
		if status != want {
			t.Fatalf("boundary status %d want %d: %v", status, want, out)
		}
	}
	create := func(name, visibility string) string {
		t.Helper()
		status, out, _ := request(
			"POST",
			"/v1/assets",
			token,
			tenant,
			"",
			map[string]any{"kind": "agent", "name": name, "visibility": visibility, "workspace_id": workspace},
		)
		check(status, 201, out)
		return out["id"].(string)
	}
	grant := func(id, kind, subject string) {
		t.Helper()
		status, out, _ := request(
			"POST",
			"/v1/assets/"+id+"/grants",
			token,
			tenant,
			"",
			map[string]string{"grantee_type": kind, "grantee_id": subject, "permission": "use"},
		)
		check(status, 202, out)
	}
	narrow := create("narrow-private", "tenant")
	grant(narrow, "tenant", tenant)
	grant(narrow, "workspace", workspace)
	status, out, _ := request(
		"PATCH",
		"/v1/assets/"+narrow,
		token,
		tenant,
		"",
		map[string]string{"visibility": "private"},
	)
	check(status, 200, out)
	status, out, _ = request("GET", "/v1/assets/"+narrow+"/grants", token, tenant, "", nil)
	check(status, 200, out)
	if len(out["items"].([]any)) != 0 {
		t.Fatal("broad grants survived narrowing")
	}
	status, out, _ = request("GET", "/v1/assets/"+narrow, reviewer, tenant, "", nil)
	check(status, 404, out)
	// Defense in depth: even a stale broad grant cannot override current visibility.
	if err := d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO asset_grants(tenant_id,asset_id,grantee_type,grantee_id,permission) "+
			"VALUES($1,$2,'tenant',$1,'use')", tenant, narrow)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	status, out, _ = request("GET", "/v1/assets/"+narrow, reviewer, tenant, "", nil)
	check(status, 404, out)
	narrowWS := create("narrow-workspace", "tenant")
	grant(narrowWS, "user", reviewerID)
	status, out, _ = request(
		"PATCH",
		"/v1/assets/"+narrowWS,
		token,
		tenant,
		"",
		map[string]string{"visibility": "workspace"},
	)
	check(status, 200, out)
	status, out, _ = request("GET", "/v1/assets/"+narrowWS+"/grants", token, tenant, "", nil)
	check(status, 200, out)
	if len(out["items"].([]any)) != 0 {
		t.Fatal("outside named grant survived workspace ceiling")
	}
	for _, target := range []string{"/v1/workspaces/" + workspace + "/installs", "/v1/installs"} {
		status, out, _ = request("POST", target, token, tenant, "", map[string]string{"asset_id": narrow})
		check(status, 403, out)
	}
	status, out, _ = request("POST", "/v1/installs", token, tenant, "", map[string]string{"asset_id": narrowWS})
	check(status, 403, out)
	status, out, _ = request("POST", "/v1/assets/"+narrow+":suspend", token, tenant, "", nil)
	check(status, 202, out)
	raw, err := bundle.Pack(map[string][]byte{"AGENTS.md": []byte("boundaries")})
	if err != nil {
		t.Fatal(err)
	}
	status, out, _ = request(
		"POST",
		"/v1/assets/"+narrow+"/versions?version=1.0.0",
		token,
		tenant,
		"inactive-upload",
		raw,
	)
	check(status, 422, out)
	status, out, _ = request("POST", "/v1/assets/"+narrow+":restore", token, tenant, "", nil)
	check(status, 202, out)
	// Tenant approvals cannot be weakened by workspace settings. Private assets
	// do not inherit workspace policy; workspace-visible assets use it directly.
	policyID := ids.New()
	if err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO policies(id,tenant_id,scope_type,scope_id,kind,body,version,status) "+
			"VALUES($1,$2,'tenant',$2,'tool_rules','{\"asset_review\":{\"approvals\":3}}',1,'active')", policyID, tenant); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE workspaces SET settings='{"assets":{"approvals":1,"review_required":false}}' WHERE id=$1`, workspace)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		visibility string
		approvals  int
		review     bool
	}{{visibility: "tenant",
		approvals: 3,
		review:    true},
		{visibility: "workspace",
			approvals: 1,
			review:    false},
		{visibility: "private",
			approvals: 3,
			review:    true}} {
		id := create("approvals-"+tc.visibility, tc.visibility)
		status, out, _ = request("POST", "/v1/assets/"+id+"/versions?version=1.0.0", token, tenant, "upload-"+id, raw)
		check(status, 201, out)
		version := out["id"].(string)
		if out["uploaded_by"] != owner {
			t.Fatal("uploader missing", out)
		}
		status, out, _ = request(
			"POST",
			"/v1/assets/"+id+"/versions/"+version+":submit",
			token,
			tenant,
			"",
			map[string]any{},
		)
		check(status, 202, out)
		in := assetrelease.Input{Tenant: tenant, VersionID: version, Generation: 1}
		snap, err := a.SnapshotAssetRelease(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Required != tc.approvals || snap.WorkspaceReview != tc.review {
			t.Fatalf("%s approvals %d review %v", tc.visibility, snap.Required, snap.WorkspaceReview)
		}
		status, out, _ = request(
			"POST",
			"/v1/assets/"+id+"/versions/"+version+":transition",
			token,
			tenant,
			"",
			map[string]string{"state": "draft"},
		)
		check(status, 409, out)
		// Infrastructure failure must leave checking retryable, not check_failed.
		result, err := a.AbortAssetRelease(
			ctx,
			assetrelease.Failure{Input: in, Code: "TestInfrastructure", Retryable: true},
		)
		if err != nil || result.State != "checking" {
			t.Fatal(result, err)
		}
		var releaseError string
		if err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			return tx.QueryRow(ctx, `SELECT release_error FROM asset_versions WHERE id=$1`, version).Scan(&releaseError)
		}); err != nil || releaseError != "TestInfrastructure" {
			t.Fatal(releaseError, err)
		}
		status, out, _ = request("POST", "/v1/assets/"+id+"/versions/"+version+":withdraw", token, tenant, "", nil)
		check(status, 202, out)
		result, err = a.AbortAssetRelease(ctx, assetrelease.Failure{Input: in, Withdrawn: true})
		if err != nil || result.State != "draft" {
			t.Fatal("withdraw", result, err)
		}
		// A withdrawn version may be submitted again; stale generation cannot mutate it.
		status, out, _ = request(
			"POST",
			"/v1/assets/"+id+"/versions/"+version+":submit",
			token,
			tenant,
			"",
			map[string]any{},
		)
		check(status, 202, out)
		if _, err = a.SnapshotAssetRelease(ctx, in); err == nil {
			t.Fatal("stale generation accepted")
		}
		in.Generation = 2
		if _, err = a.AbortAssetRelease(ctx, assetrelease.Failure{Input: in, Withdrawn: true}); err != nil {
			t.Fatal(err)
		}
	}
	if err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM policies WHERE id=$1`, policyID); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE workspaces SET settings='{}' WHERE id=$1`, workspace)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	uploaderAsset := create("uploader-review", "tenant")
	status, out, _ = request(
		"POST",
		"/v1/assets/"+uploaderAsset+"/grants",
		token,
		tenant,
		"",
		map[string]string{"grantee_type": "user", "grantee_id": reviewerID, "permission": "maintain"},
	)
	check(status, 202, out)
	status, out, _ = request(
		"POST",
		"/v1/assets/"+uploaderAsset+"/versions?version=1.0.0",
		reviewer,
		tenant,
		"uploader-test",
		raw,
	)
	check(status, 201, out)
	uploaderVersion := out["id"].(string)
	status, out, _ = request(
		"POST",
		"/v1/assets/"+uploaderAsset+"/versions/"+uploaderVersion+":submit",
		token,
		tenant,
		"",
		map[string]any{},
	)
	check(status, 202, out)
	in := assetrelease.Input{Tenant: tenant, VersionID: uploaderVersion, Generation: 1}
	if _, err = a.SnapshotAssetRelease(ctx, in); err != nil {
		t.Fatal(err)
	}
	reports := []contract.AssetCheckReport{}
	for _, kind := range []contract.AssetCheckKind{"structure", "compat", "secrets", "static", "injection", "permission_diff"} {
		reports = append(reports, contract.AssetCheckReport{Check: kind, CheckerVersion: "uploader-test"})
	}
	if err = a.AggregateAssetChecks(ctx, assetrelease.Aggregate{Input: in, Reports: reports, State: "in_review"}); err != nil {
		t.Fatal(err)
	}
	status, out, _ = request(
		"POST",
		"/v1/reviews/"+uploaderVersion+":decide",
		reviewer,
		tenant,
		"",
		map[string]string{"decision": "approve"},
	)
	check(status, 403, out)
	if _, err = a.AbortAssetRelease(ctx, assetrelease.Failure{Input: in, Withdrawn: true}); err != nil {
		t.Fatal(err)
	}
	testTeamPermissionDiff(t, d, s, a, tenant, owner, workspace)
}

func testTeamPermissionDiff(
	t *testing.T,
	d *db.DB,
	s *assets.Service,
	a *releaseactivities.Activities,
	tenant, owner, workspace string,
) {
	t.Helper()
	ctx := t.Context()
	var asset assets.Asset
	previous := ""
	for i := range 2 {
		files := map[string][]byte{
			"AGENTS.md": []byte("team"),
			"team.json": []byte(
				"{\"schema\":\"orbit.team/v1\",\"name\":\"permissions-team\",\"dispatch\":\"scheduled\",\"roles\":[{\"name" +
					"\":\"lead\",\"role\":\"leader\",\"agent\":\".\"},{\"name\":\"worker\",\"role\":\"teammate\",\"agent\":\"./member" +
					"s/worker\"}]}",
			),
			"agent.json": []byte(
				`{"schema":"orbit.agent/v2","name":"lead","permissions":{"secrets":[{"name":"KEY"}],"effectClasses":["network_read"]}}`,
			),
			"members/worker/AGENTS.md": []byte("member"),
		}
		if i == 1 {
			files["members/worker/agent.json"] = []byte(
				"{\"schema\":\"orbit.agent/v2\",\"name\":\"worker\",\"tools\":{\"builtin\":[\"shell\"]},\"permissions\":{\"s" +
					"ecrets\":[{\"name\":\"KEY\"}],\"network\":{\"egress\":[\"api.example.test\"]},\"effectClasses\":[\"sandb" +
					"ox_write\"]}}",
			)
		}
		raw, err := bundle.Pack(files)
		if err != nil {
			t.Fatal(err)
		}
		staged, err := s.PrepareUpload(
			auth.Context(ctx, auth.Principal{ID: owner, Kind: "user"}, tenant),
			fmt.Sprintf("1.%d.0", i),
			raw,
		)
		if err != nil {
			t.Fatal(err)
		}
		var version assets.Version
		if err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			ctx := auth.Context(db.Context(ctx, tx), auth.Principal{ID: owner, Kind: "user"}, tenant)
			if i == 0 {
				asset, err = s.Create(ctx, "team", "permissions-team", "workspace", workspace)
				if err != nil {
					return err
				}
			}
			version, err = s.Upload(ctx, asset, staged)
			if err != nil {
				return err
			}
			return s.Submit(ctx, asset, version.ID, "stable")
		}); err != nil {
			t.Fatal(err)
		}
		input := contract.AssetCheckInput{TenantId: tenant, AssetId: asset.ID, AssetVersionId: version.ID}
		if previous != "" {
			input.PreviousVersionId = &previous
		}
		report, err := a.AssetCheckPermissionDiff(ctx, input)
		if err != nil || len(report.Findings) != 1 || report.Findings[0].Severity != "review" {
			t.Fatal("member expansion missed", report, err)
		}
		if i == 1 {
			if len(version.Manifest.MemberPermissions) != 2 || len(version.Permissions.ToolsBuiltin) != 1 ||
				len(version.Permissions.Network.Egress) != 1 {
				t.Fatal("member permissions not persisted", version)
			}
			sig, err := s.Signer.Sign(asset, version)
			if err != nil {
				t.Fatal(err)
			}
			version.Signature = sig
			changed := version.Manifest.MemberPermissions["members/worker"]
			changed.Secrets = nil
			version.Manifest.MemberPermissions["members/worker"] = changed
			if s.Signer.Verify(asset, version) == nil {
				t.Fatal("member permissions not signed")
			}
		}
		if _,
			err = a.AbortAssetRelease(ctx,
			assetrelease.Failure{Input: assetrelease.Input{Tenant: tenant,
				VersionID:  version.ID,
				Generation: 1},
				Withdrawn: true}); err != nil {
			t.Fatal(err)
		}
		previous = version.ID
	}
}

// Guard SQL privileges through the actual app role, including draft versions.
func testAssetImmutability(t *testing.T, d *db.DB, tenant, version string) {
	t.Helper()
	for _, column := range []string{"uploaded_by", "permissions_jcs", "identity_jcs"} {
		t.Run(column, func(t *testing.T) {
			err := d.WithTenant(t.Context(), tenant, func(tx pgx.Tx) error {
				_, err := tx.Exec(t.Context(), `INSERT INTO asset_versions
				 SELECT (jsonb_populate_record(NULL::asset_versions,
				 to_jsonb(v) || jsonb_build_object('id',$2::text,'version','9.9.9',$3::text,NULL))).*
				 FROM asset_versions v WHERE tenant_id=$4 AND id=$1`, version, ids.New(), column, tenant)
				return err
			})
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != "23502" ||
				pgErr.ColumnName != column {
				t.Fatalf("missing %s accepted: %v", column, err)
			}
			err = d.WithTenant(t.Context(), tenant, func(tx pgx.Tx) error {
				_, err := tx.Exec(
					t.Context(),
					"UPDATE asset_versions SET "+pgx.Identifier{column}.Sanitize()+"=NULL WHERE tenant_id=$2 AND id=$1",
					version,
					tenant,
				)
				return err
			})
			if pgErr, ok := errors.AsType[*pgconn.PgError](err); !ok || pgErr.Code != "23514" {
				t.Fatalf("immutable %s changed: %v", column, err)
			}
		})
	}
	for _, query := range []string{`DELETE FROM asset_versions WHERE id=$1`,
		`DELETE FROM asset_files WHERE version_id=$1`,
		`UPDATE asset_files SET size_bytes=0 WHERE version_id=$1`} {
		err := d.WithTenant(t.Context(), tenant, func(tx pgx.Tx) error {
			_, err := tx.Exec(t.Context(), query, version)
			return err
		})
		if err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("immutable privilege %s: %v", query, err)
		}
	}
}
