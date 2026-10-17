package devseed

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io/fs"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/assets/bundle"
	seeds "github.com/mindreon/orbit-control/testdata"
)

func seedTeam(ctx context.Context, tx pgx.Tx, svc *assets.Service, out *Result) error {
	var assetID string
	err := tx.QueryRow(ctx, `SELECT id::text FROM assets WHERE tenant_id=$1 AND publisher='dev' AND kind='team' AND name='review-team'`,
		out.TenantID).Scan(&assetID)
	var asset assets.Asset
	if errors.Is(err, pgx.ErrNoRows) {
		asset, err = svc.Create(ctx, "team", "review-team", "private", out.WorkspaceID)
	} else if err == nil {
		asset, err = svc.Get(ctx, assetID, "asset.update")
	}
	if err != nil {
		return err
	}
	var versionID string
	err = tx.QueryRow(ctx, `SELECT id::text FROM asset_versions WHERE tenant_id=$1 AND asset_id=$2 AND version='1.0.0'`, out.TenantID,
		asset.ID).Scan(&versionID)
	if errors.Is(err, pgx.ErrNoRows) {
		files, err := teamFiles()
		if err != nil {
			return err
		}
		archive, err := bundle.Pack(files)
		if err != nil {
			return err
		}
		upload, err := svc.PrepareUpload(ctx, "1.0.0", archive)
		if err != nil {
			return err
		}
		version, err := svc.Upload(ctx, asset, upload)
		if err != nil {
			return err
		}
		versionID = version.ID
		if err = svc.Submit(ctx, asset, versionID, "stable"); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	out.Versions = append(out.Versions, versionID)
	return nil
}
func teamFiles() (map[string][]byte, error) {
	files := map[string][]byte{}
	manifest := map[string]any{"schema": "orbit.team/v1", "name": "review-team", "dispatch": "scheduled", "maxParallelMembers": 2,
		"review": map[string]any{"mode": "veto", "maxRounds": 3}, "roles": []map[string]any{
			{"name": "leader", "role": "leader", "agent": "./members/leader"},
			{"name": "writer", "role": "teammate", "count": 2, "agent": "./members/writer"},
			{"name": "reviewer", "role": "reviewer", "agent": "./members/reviewer"},
		}}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	files["team.json"] = raw
	for role, prompt := range map[string]string{
		"leader": "You lead a review team. Break requested work into tasks for writer-1 and writer-2, wait for review, " +
			"then call team_finish with a useful summary. Use read_file and write_file when needed.",
		"writer": "Complete the assigned task. Write the requested file under /workspace, inspect it, " +
			"then call team_complete_task with its relative path.",
		"reviewer": "Inspect the submitted files under /workspace against the acceptance criteria. " +
			"Call team_submit_verdict with approve or reject and concrete feedback.",
	} {
		agent, err := fs.ReadFile(seeds.Bundles, "bundles/echo-agent/agent.json")
		if err != nil {
			return nil, err
		}
		var body map[string]any
		if err = json.Unmarshal(agent, &body); err != nil {
			return nil, err
		}
		body["name"] = role
		body["tools"] = map[string]any{"builtin": []string{"read_file", "write_file"}}
		raw, err = json.Marshal(body)
		if err != nil {
			return nil, err
		}
		files["members/"+role+"/agent.json"] = raw
		files["members/"+role+"/AGENTS.md"] = []byte(prompt + "\n")
	}
	return files, nil
}
