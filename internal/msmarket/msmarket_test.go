package msmarket

import (
	"bytes"
	"compress/gzip"
	"context"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/store/memstore"
)

func install(t *testing.T) (*memstore.Store, *log.Logger) {
	t.Helper()
	repo := memstore.New()
	var lines []string
	logger := log.New(funcWriter(func(p []byte) (int, error) {
		lines = append(lines, string(p))
		return len(p), nil
	}), "", 0)
	InstallAll(context.Background(), repo, "default", "", logger)
	if len(lines) == 0 {
		t.Fatal("the install logged nothing")
	}
	return repo, logger
}

func TestInstallStoresTheEmbeddedSnapshot(t *testing.T) {
	repo, _ := install(t)
	ctx := context.Background()
	page, err := repo.ListSkillCatalog(ctx, "default", store.SkillCatalogQuery{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total < 80000 {
		t.Fatalf("skill catalog is too small: %d", page.Total)
	}
	first := page.Items[0]
	if first.ID == "" || first.Name == "" {
		t.Fatalf("a skill row is empty: %+v", first)
	}
	mcp, err := repo.ListMcpMarket(ctx, "default", store.McpMarketQuery{PageSize: 1})
	if err != nil || mcp.Total < 12000 {
		t.Fatalf("mcp market: %v total=%d", err, mcp.Total)
	}
	agents, err := repo.ListAgentCatalog(ctx, "default", store.AgentCatalogQuery{PageSize: 1})
	if err != nil || agents.Total < 500 {
		t.Fatalf("agents: %v total=%d", err, agents.Total)
	}
	cats, err := repo.ListSkillCategories(ctx, "default")
	if err != nil || len(cats) < 5 {
		t.Fatalf("skill categories: %v %d", err, len(cats))
	}
}

func TestReinstallWithTheSameSnapshotIsSkipped(t *testing.T) {
	repo, _ := install(t)
	ctx := context.Background()
	sha, err := hashEmbedded()
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.SetCatalogSnapshot(ctx, "default", snapshotName, sha); err != nil {
		t.Fatal(err)
	}
	// A row the shipped snapshot does not have: it survives only if the
	// unchanged snapshot really skips the replacement.
	seed := store.SkillRecord{ID: "@local/private", Handle: "@local", Slug: "private", Name: "private"}
	if err := repo.ReplaceSkills(ctx, "default", []store.SkillRecord{seed}, nil); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetCatalogSnapshot(ctx, "default", snapshotName, sha); err != nil {
		t.Fatal(err)
	}
	InstallAll(ctx, repo, "default", "", nil)
	if got, err := repo.GetSkill(ctx, "default", seed.ID); err != nil {
		t.Fatalf("the unchanged snapshot was replaced anyway: %v", err)
	} else if got.Name != seed.Name {
		t.Fatalf("got %+v", got)
	}
}

func TestSkillTextSidecar(t *testing.T) {
	repo, _ := install(t)
	ctx := context.Background()
	page, _ := repo.ListSkillCatalog(ctx, "default", store.SkillCatalogQuery{})
	if page.Total == 0 {
		t.Fatal("empty catalog")
	}
	id := page.Items[0].ID

	dir := t.TempDir()
	// No sidecar file: a quiet no-op, rows stay without text.
	InstallAll(ctx, repo, "default", dir, nil)
	if _, known, err := repo.GetSkillTextFiles(ctx, "default", id); err != nil || known {
		t.Fatalf("no sidecar should fill nothing: known=%v err=%v", known, err)
	}

	sha, known, err := repo.CatalogSnapshot(ctx, "default", snapshotName)
	if err != nil || !known || sha == "" {
		t.Fatalf("snapshot hash missing: %v %v %q", err, known, sha)
	}
	// A sidecar with one row fills that row.
	payload := `{"id":"` + id + `","files":[{"path":"SKILL.md","body":"# 你好"}]}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, textSidecarName), gzipBytes(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	InstallAll(ctx, repo, "default", dir, nil)
	files, known, err := repo.GetSkillTextFiles(ctx, "default", id)
	if err != nil || !known || len(files) != 1 || files[0].Body != "# 你好" {
		t.Fatalf("sidecar text: %v %v %+v", err, known, files)
	}
	// A metadata replacement wipes the text rows; the next pass must re-apply
	// the sidecar even though its own hash did not change.
	if err := repo.SetCatalogSnapshot(ctx, "default", snapshotName, "stale"); err != nil {
		t.Fatal(err)
	}
	InstallAll(ctx, repo, "default", dir, nil)
	if files, known, err := repo.GetSkillTextFiles(ctx, "default", id); err != nil || !known || len(files) != 1 || files[0].Body != "# 你好" {
		t.Fatalf("text after metadata replace: %v %v %+v", err, known, files)
	}
	// An unknown id in the sidecar is ignored, not an error.
	other := `{"id":"@no/such-row","files":[{"path":"SKILL.md","body":"ghost"}]}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, textSidecarName), gzipBytes(other), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := repo.SetCatalogSnapshot(ctx, "default", textSnapshotName, ""); err != nil {
		t.Fatal(err)
	}
	InstallAll(ctx, repo, "default", dir, nil)
}

func TestMcpSnapshotRejectsURLsInCards(t *testing.T) {
	if _, _, err := loadMcp(); err != nil {
		t.Fatalf("the shipped mcp snapshot must load: %v", err)
	}
}

func gzipBytes(payload string) []byte {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write([]byte(payload))
	zw.Close()
	return buf.Bytes()
}

type funcWriter func(p []byte) (int, error)

func (f funcWriter) Write(p []byte) (int, error) { return f(p) }
