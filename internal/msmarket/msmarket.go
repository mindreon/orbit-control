// Package msmarket copies the shipped ModelScope snapshot (skills, MCP
// servers, agents) into the local store. Listing reads that store. This
// package does not call modelscope.cn.
//
// The three embedded *.json.gz files come from tools/modelscope-crawler:
//
//	uv run ms-crawl export-orbit --out internal/msmarket
//
// skills_text.json.gz (the file bodies, ~0.5 GB) is too big to embed; it is
// read once from ORBIT_CATALOG_DIR when the deployment provides it.
package msmarket

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"github.com/mindreon/orbit-control/internal/store"
)

//go:embed skills.json.gz mcp.json.gz agents.json.gz
var embedded embed.FS

// Snapshot names recorded in catalog_snapshots, so a restart with an
// unchanged snapshot skips the copy.
const (
	snapshotName     = "msmarket"
	textSnapshotName = "msmarket-text"
	iconSnapshotName = "msmarket-icons"
	textSidecarName  = "skills_text.json.gz"
	iconSidecarName  = "icons.json.gz"
	// One text row can carry hundreds of kilobytes, so the batch stays small.
	textInstallChunk = 200
	// One icon row carries tens of kilobytes.
	iconInstallChunk = 100
)

// InstallAll stores the embedded snapshot and, when dir provides one, the
// skill text and icon sidecars. Already-stored snapshots are skipped by hash.
// It is meant to run in the background at process start.
func InstallAll(ctx context.Context, repo store.Repository, tenantID, dir string, logger *log.Logger) {
	if logger == nil {
		logger = log.Default()
	}
	replaced, err := installEmbedded(ctx, repo, tenantID, logger)
	if err != nil {
		logger.Printf("msmarket: snapshot not stored: %v", err)
		return
	}
	if err := installSkillText(ctx, repo, tenantID, dir, replaced, logger); err != nil {
		logger.Printf("msmarket: skill text not stored: %v", err)
	}
	if err := installIcons(ctx, repo, tenantID, dir, logger); err != nil {
		logger.Printf("msmarket: icons not stored: %v", err)
	}
}

// installEmbedded stores the skills/mcp/agents snapshot. It reports whether it
// replaced stored rows, because a replacement also wipes the skill text.
func installEmbedded(ctx context.Context, repo store.Repository, tenantID string, logger *log.Logger) (bool, error) {
	sha, err := hashEmbedded()
	if err != nil {
		return false, err
	}
	stored, known, err := repo.CatalogSnapshot(ctx, tenantID, snapshotName)
	if err != nil {
		return false, err
	}
	if known && stored == sha {
		logger.Printf("msmarket: snapshot %s unchanged, skipping", shortSha(sha))
		return false, nil
	}
	skills, categories, err := loadSkills()
	if err != nil {
		return false, fmt.Errorf("skills: %w", err)
	}
	servers, details, err := loadMcp()
	if err != nil {
		return false, fmt.Errorf("mcp: %w", err)
	}
	agents, err := loadAgents()
	if err != nil {
		return false, fmt.Errorf("agents: %w", err)
	}
	if err := repo.ReplaceSkills(ctx, tenantID, skills, categories); err != nil {
		return false, err
	}
	if err := repo.ReplaceMcpMarket(ctx, tenantID, servers, Categories(), details); err != nil {
		return false, err
	}
	if err := repo.ReplaceAgents(ctx, tenantID, agents); err != nil {
		return false, err
	}
	if err := repo.SetCatalogSnapshot(ctx, tenantID, snapshotName, sha); err != nil {
		return false, err
	}
	logger.Printf("msmarket: snapshot %s stored (%d skills, %d mcp servers, %d agents)",
		shortSha(sha), len(skills), len(servers), len(agents))
	// The replacement wiped the stored skill text, so the text snapshot record
	// no longer describes the rows. Empty it to force a text re-apply.
	if err := repo.SetCatalogSnapshot(ctx, tenantID, textSnapshotName, ""); err != nil {
		return false, err
	}
	return true, nil
}

func installSkillText(ctx context.Context, repo store.Repository, tenantID, dir string, force bool, logger *log.Logger) error {
	if dir == "" {
		return nil
	}
	path := filepath.Join(dir, textSidecarName)
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		logger.Printf("msmarket: no %s under %s, skill pages start without file text", textSidecarName, dir)
		return nil
	}
	if err != nil {
		return err
	}
	sha := hashBytes(raw)
	if !force {
		stored, known, err := repo.CatalogSnapshot(ctx, tenantID, textSnapshotName)
		if err != nil {
			return err
		}
		if known && stored == sha {
			logger.Printf("msmarket: skill text %s unchanged, skipping", shortSha(sha))
			return nil
		}
	}
	rows, err := readJSONL[skillTextRow](raw)
	if err != nil {
		return fmt.Errorf("%s: %w", textSidecarName, err)
	}
	batch := make([]store.SkillTextFilesRow, 0, textInstallChunk)
	filled := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := repo.SaveSkillTextFilesBatch(ctx, tenantID, batch); err != nil {
			return err
		}
		filled += len(batch)
		batch = batch[:0]
		return nil
	}
	for _, row := range rows {
		if row.ID == "" {
			continue
		}
		batch = append(batch, store.SkillTextFilesRow{ID: row.ID, Files: row.storeFiles()})
		if len(batch) >= textInstallChunk {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if err := repo.SetCatalogSnapshot(ctx, tenantID, textSnapshotName, sha); err != nil {
		return err
	}
	logger.Printf("msmarket: skill text %s stored (%d skills)", shortSha(sha), filled)
	return nil
}

// iconRow is one line of the icons.json.gz sidecar: base64 image bytes keyed
// by the source URL the snapshot rows point at.
type iconRow struct {
	URL  string `json:"url"`
	Type string `json:"type"`
	B64  string `json:"b64"`
}

// installIcons upserts the icon sidecar. Icons are keyed by URL, so the pass
// is additive and never needs a metadata-change invalidation.
func installIcons(ctx context.Context, repo store.Repository, tenantID, dir string, logger *log.Logger) error {
	if dir == "" {
		return nil
	}
	raw, err := os.ReadFile(filepath.Join(dir, iconSidecarName))
	if errors.Is(err, fs.ErrNotExist) {
		logger.Printf("msmarket: no %s under %s, cards keep letter avatars", iconSidecarName, dir)
		return nil
	}
	if err != nil {
		return err
	}
	sha := hashBytes(raw)
	stored, known, err := repo.CatalogSnapshot(ctx, tenantID, iconSnapshotName)
	if err != nil {
		return err
	}
	if known && stored == sha {
		logger.Printf("msmarket: icons %s unchanged, skipping", shortSha(sha))
		return nil
	}
	rows, err := readJSONL[iconRow](raw)
	if err != nil {
		return fmt.Errorf("%s: %w", iconSidecarName, err)
	}
	batch := make([]store.CatalogIcon, 0, iconInstallChunk)
	filled := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := repo.ReplaceCatalogIcons(ctx, tenantID, batch); err != nil {
			return err
		}
		filled += len(batch)
		batch = batch[:0]
		return nil
	}
	for _, row := range rows {
		if row.URL == "" || row.B64 == "" {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(row.B64)
		if err != nil {
			return fmt.Errorf("%s: bad base64 for %s: %w", iconSidecarName, row.URL, err)
		}
		ctype := row.Type
		if ctype == "" {
			ctype = "image/png"
		}
		batch = append(batch, store.CatalogIcon{URL: row.URL, ContentType: ctype, Data: data})
		if len(batch) >= iconInstallChunk {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if err := repo.SetCatalogSnapshot(ctx, tenantID, iconSnapshotName, sha); err != nil {
		return err
	}
	logger.Printf("msmarket: icons %s stored (%d icons)", shortSha(sha), filled)
	return nil
}

func hashEmbedded() (string, error) {
	digest := sha256.New()
	for _, name := range []string{"skills.json.gz", "mcp.json.gz", "agents.json.gz"} {
		raw, err := embedded.ReadFile(name)
		if err != nil {
			return "", err
		}
		fmt.Fprintf(digest, "%s:%d:", name, len(raw))
		digest.Write(raw)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func hashBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// mustEmbed reads one embedded snapshot file. A missing file is a build error
// (go:embed), so this only fails on a corrupted binary.
func mustEmbed(name string) []byte {
	raw, err := embedded.ReadFile(name)
	if err != nil {
		panic("msmarket: embedded " + name + ": " + err.Error())
	}
	return raw
}

func shortSha(sha string) string {
	if len(sha) > 12 {
		return sha[:12]
	}
	return sha
}

// readJSONL decodes a gzipped JSONL file into rows of T.
func readJSONL[T any](raw []byte) ([]T, error) {
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	defer zr.Close()
	var rows []T
	decoder := json.NewDecoder(zr)
	for {
		var row T
		if err := decoder.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				return rows, nil
			}
			return nil, err
		}
		rows = append(rows, row)
	}
}
