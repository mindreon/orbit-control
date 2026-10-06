package app

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/store"
	"github.com/mindreon/orbit-control/internal/store/memstore"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// The agent bundle (ADR-0013): the form is rendered into files, the spec is derived from them.
//
// How it can go wrong, written down before the code:
//   - AGENTS.md is missing or blank on any write path (form, import);
//   - a file the form has no field for (README.md, a skill directory, an unbound mcp.json entry) is lost on the next version;
//   - a field the form cleared (the soul) survives as a file;
//   - the derived spec differs from the form it was rendered from (order of connectors, catalog skills, instructions);
//   - a path that leaves the bundle, a file that is not text, or a bundle over its limits is stored;
//   - a literal credential in mcp.json is stored, or echoed in the error;
//   - skills.json lists a bundle skill whose directory is not there, or silently drops a directory it does not list;
//   - the bundle of one tenant is served to another through the worker's endpoint.

func bundleApp(t *testing.T) (*App, *memstore.Store) {
	t.Helper()
	repo := memstore.New()
	return &App{Repo: repo, Tasks: taskruntime.New(nil)}, repo
}

func connector(t *testing.T, repo *memstore.Store, tenantID string, rec store.McpConnectorRecord) {
	t.Helper()
	if err := repo.CreateMcpConnector(context.Background(), tenantID, rec); err != nil {
		t.Fatal(err)
	}
}

func filesOf(t *testing.T, a *App, p taskruntime.Principal, expert Expert) map[string]string {
	t.Helper()
	files, err := a.Tasks.ProfileFiles(context.Background(), p, expert.Ref)
	if err != nil {
		t.Fatal(err)
	}
	return bundleFromProfileFiles(files)
}

func TestFormRendersABundleAndTheSpecIsDerivedFromIt(t *testing.T) {
	a, repo := bundleApp(t)
	ctx, p := context.Background(), teamPrincipal("t")
	connector(t, repo, "t", store.McpConnectorRecord{ID: "mcp_b", Name: "second", Transport: "streamable_http", URL: "https://mcp.example.com/b", HeaderRefs: []string{"Authorization:GITHUB_TOKEN"}})
	connector(t, repo, "t", store.McpConnectorRecord{ID: "mcp_a", Name: "first", Command: "orbit-mcp", EnvRefs: []string{"A_TOKEN"}})
	catalogSkill(t, repo, "h/pdf", "pdf", "---\nname: pdf\n---\nuse")

	expert, err := a.CreateExpert(ctx, p, ExpertInput{
		Name: "Release", Instructions: " do it ", Soul: " warm ", Model: "m-1",
		ConnectorIDs: []string{"mcp_b", "mcp_a"}, SkillIDs: []string{"h/pdf"},
	})
	if err != nil {
		t.Fatal(err)
	}
	files := filesOf(t, a, p, expert)
	for _, name := range []string{"AGENTS.md", "SOUL.md", "agent.json", "skills.json", "mcp.json"} {
		if files[name] == "" {
			t.Errorf("%s was not rendered: %v", name, files)
		}
	}
	if files["AGENTS.md"] != "do it" || files["SOUL.md"] != "warm" {
		t.Errorf("AGENTS.md %q SOUL.md %q", files["AGENTS.md"], files["SOUL.md"])
	}
	if !strings.Contains(files["mcp.json"], "${secret:GITHUB_TOKEN}") || strings.Contains(files["mcp.json"], "xyz") {
		t.Errorf("mcp.json should carry the placeholder only: %s", files["mcp.json"])
	}
	d, err := deriveBundle(files)
	if err != nil {
		t.Fatal(err)
	}
	if d.Instructions != "do it" || d.Soul != "warm" || !reflect.DeepEqual(d.ConnectorIDs, []string{"mcp_b", "mcp_a"}) || !reflect.DeepEqual(d.CatalogSkills, []string{"h/pdf"}) {
		t.Errorf("derived %+v is not the form", d)
	}
	got, err := a.GetExpert(ctx, p, expert.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Soul != "warm" || got.Instructions != "do it" || !reflect.DeepEqual(got.ConnectorIDs, []string{"mcp_b", "mcp_a"}) || got.McpUnbound == nil {
		t.Errorf("expert %+v", got)
	}
	profile, _ := a.Tasks.GetProfile(ctx, p, expert.Ref)
	if profile.Spec["bundle_sha"] != bundleSHA(files) || profile.Spec["bundle_skills"] == nil {
		t.Errorf("spec lacks bundle_sha or bundle_skills: %v", profile.Spec)
	}
}

func TestAnUpdateCarriesOverWhatTheFormHasNoFieldFor(t *testing.T) {
	a, _ := bundleApp(t)
	ctx, p := context.Background(), teamPrincipal("t")
	base := bundleFiles{
		"README.md":             "readme",
		"skills/notes/SKILL.md": "---\nname: notes\n---\nbody",
		"skills/notes/extra.md": "more",
		"mcp.json":              `{"mcpServers":{"local":{"command":"npx","args":["x"]},"remote":{"url":"https://m.example/x"}}}`,
	}
	first, err := a.writeExpert(ctx, p, "expert_x", 1, ExpertInput{Name: "A", Instructions: "i", Soul: "s", baseFiles: base})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.McpUnbound) != 2 {
		t.Fatalf("both servers are unbound: %+v", first.McpUnbound)
	}
	second, err := a.UpdateExpert(ctx, p, "expert_x", ExpertInput{Name: "A2", Instructions: "i2"})
	if err != nil {
		t.Fatal(err)
	}
	files := filesOf(t, a, p, second)
	if files["README.md"] != "readme" || files["skills/notes/extra.md"] != "more" || !strings.Contains(files["mcp.json"], `"local"`) {
		t.Errorf("extra files were lost: %v", files)
	}
	if _, has := files["SOUL.md"]; has {
		t.Error("the soul the form cleared is still a file")
	}
	profile, _ := a.Tasks.GetProfile(ctx, p, second.Ref)
	if !reflect.DeepEqual(profile.Spec["bundle_skills"], []any{"notes"}) || second.Soul != "" || len(second.McpUnbound) != 2 {
		t.Errorf("derived: %v %+v", profile.Spec["bundle_skills"], second)
	}
	// The first version keeps its files.
	if filesOf(t, a, p, first)["SOUL.md"] != "s" {
		t.Error("version 1 changed")
	}
}

func TestAnExpertWithoutAgentsMdIsRefusedOnEveryPath(t *testing.T) {
	a, _ := bundleApp(t)
	ctx, p := context.Background(), teamPrincipal("t")
	for _, instructions := range []string{"", "  \n\t"} {
		_, err := a.CreateExpert(ctx, p, ExpertInput{Name: "x", Instructions: instructions})
		if code, field := codeOf(err); code != "AGENTS_MD_REQUIRED" || field != "instructions" {
			t.Errorf("%q: %q %q", instructions, code, field)
		}
	}
	e := singleExpert(t, a, p, "ok")
	if _, err := a.UpdateExpert(ctx, p, e.ID, ExpertInput{Name: "ok", Instructions: " "}); !errors.Is(err, ErrInvalid) {
		t.Errorf("update: %v", err)
	}
	if _, err := deriveBundle(bundleFiles{"SOUL.md": "only a soul"}); codeOnly(err) != "AGENTS_MD_REQUIRED" {
		t.Errorf("a bundle without AGENTS.md: %v", err)
	}
	if _, err := deriveBundle(bundleFiles{"AGENTS.md": " \n"}); codeOnly(err) != "AGENTS_MD_REQUIRED" {
		t.Errorf("a blank AGENTS.md: %v", err)
	}
}

func codeOnly(err error) string {
	code, _ := codeOf(err)
	return code
}

func TestSoulAndInstructionsShareOneLimit(t *testing.T) {
	a, _ := bundleApp(t)
	_, err := a.CreateExpert(context.Background(), teamPrincipal("t"), ExpertInput{
		Name: "x", Soul: strings.Repeat("s", 10000), Instructions: strings.Repeat("i", 10001),
	})
	if code, field := codeOf(err); code != "INSTRUCTIONS_TOO_LONG" || field != "soul" {
		t.Errorf("%q %q", code, field)
	}
}

func TestBundleValidation(t *testing.T) {
	ok := func(extra bundleFiles) bundleFiles {
		b := bundleFiles{"AGENTS.md": "do"}
		for k, v := range extra {
			b[k] = v
		}
		return b
	}
	many := bundleFiles{}
	for i := 0; i <= maxBundleFiles; i++ {
		many[strings.Repeat("a", 3)+string(rune('a'+i%26))+strings.Repeat("b", i/26)+".md"] = "x"
	}
	many["AGENTS.md"] = "do"
	cases := []struct {
		name  string
		files bundleFiles
		code  string
	}{
		{"parent path", ok(bundleFiles{"../x.md": "x"}), "BUNDLE_PATH_INVALID"},
		{"absolute path", ok(bundleFiles{"/x.md": "x"}), "BUNDLE_PATH_INVALID"},
		{"backslash", ok(bundleFiles{"a\\b.md": "x"}), "BUNDLE_PATH_INVALID"},
		{"not normalized", ok(bundleFiles{"a//b.md": "x"}), "BUNDLE_PATH_INVALID"},
		{"binary", ok(bundleFiles{"a.bin": "a\x00b"}), "BUNDLE_FILE_NOT_TEXT"},
		{"not utf-8", ok(bundleFiles{"a.bin": "\xff\xfe"}), "BUNDLE_FILE_NOT_TEXT"},
		{"big file", ok(bundleFiles{"big.md": strings.Repeat("x", maxBundleFileBytes+1)}), "BUNDLE_FILE_TOO_LARGE"},
		{"big bundle", func() bundleFiles {
			b := ok(nil)
			for i := 0; i < 17; i++ {
				b[string(rune('a'+i))+".md"] = strings.Repeat("x", maxBundleFileBytes)
			}
			return b
		}(), "BUNDLE_TOO_LARGE"},
		{"too many files", many, "BUNDLE_TOO_MANY_FILES"},
		{"skills.json not json", ok(bundleFiles{"skills.json": "nope"}), "SKILLS_JSON_INVALID"},
		{"skills.json bad source", ok(bundleFiles{"skills.json": `{"skills":[{"name":"a","source":"web"}]}`}), "SKILLS_JSON_INVALID"},
		{"skills.json catalog without id", ok(bundleFiles{"skills.json": `{"skills":[{"name":"a","source":"catalog"}]}`}), "SKILLS_JSON_INVALID"},
		{"skills.json duplicate", ok(bundleFiles{"skills.json": `{"skills":[{"name":"a","source":"catalog","id":"h/a"},{"name":"a","source":"catalog","id":"h/b"}]}`}), "SKILLS_JSON_INVALID"},
		{"skills.json lists a missing directory", ok(bundleFiles{"skills.json": `{"skills":[{"name":"a","source":"bundle"}]}`}), "BUNDLE_SKILL_MISSING"},
		{"mcp.json not json", ok(bundleFiles{"mcp.json": "["}), "MCP_JSON_INVALID"},
		{"mcp.json entry without target", ok(bundleFiles{"mcp.json": `{"mcpServers":{"a":{}}}`}), "MCP_JSON_INVALID"},
		{"literal bearer", ok(bundleFiles{"mcp.json": `{"mcpServers":{"a":{"url":"https://x.example","headers":{"Authorization":"Bearer abcdef0123456789abcdef0123456789"}}}}`}), "MCP_LITERAL_SECRET"},
		{"literal api key header", ok(bundleFiles{"mcp.json": `{"mcpServers":{"a":{"url":"https://x.example","headers":{"X-Api-Key":"hunter2"}}}}`}), "MCP_LITERAL_SECRET"},
		{"literal in env", ok(bundleFiles{"mcp.json": `{"mcpServers":{"a":{"command":"x","env":{"GH":"ghp_abcdefghijklmnop"}}}}`}), "MCP_LITERAL_SECRET"},
		{"credentials in the url", ok(bundleFiles{"mcp.json": `{"mcpServers":{"a":{"url":"https://user:pw@x.example"}}}`}), "MCP_LITERAL_SECRET"},
		{"bad agent.json schema", ok(bundleFiles{"agent.json": `{"schema":"other/v9"}`}), "AGENT_JSON_INVALID"},
	}
	for _, c := range cases {
		_, err := deriveBundle(c.files)
		if code := codeOnly(err); code != c.code {
			t.Errorf("%s: %q (%v), want %q", c.name, code, err, c.code)
		}
	}
	if _, err := deriveBundle(ok(bundleFiles{"mcp.json": `{"mcpServers":{"a":{"url":"https://x.example","headers":{"Authorization":"Bearer ${secret:T}","Accept":"application/json"}}}}`})); err != nil {
		t.Errorf("placeholders are allowed: %v", err)
	}
	// The error never echoes the value.
	_, err := deriveBundle(ok(bundleFiles{"mcp.json": `{"mcpServers":{"a":{"url":"https://x.example","headers":{"X-Api-Key":"hunter2"}}}}`}))
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("error %v", err)
	}
}

func TestSkillsLoadListedFirstThenTheUnlistedByName(t *testing.T) {
	d, err := deriveBundle(bundleFiles{
		"AGENTS.md":            "do",
		"skills/zeta/SKILL.md": "z", "skills/alpha/SKILL.md": "a", "skills/mid/SKILL.md": "m", "skills/nodoc/readme.md": "x",
		"skills.json": `{"skills":[{"name":"zeta","source":"bundle"},{"name":"pdf","source":"catalog","id":"h/pdf"}]}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(d.BundleSkills, []string{"zeta", "alpha", "mid"}) || !reflect.DeepEqual(d.CatalogSkills, []string{"h/pdf"}) {
		t.Errorf("%+v", d)
	}
}

func seedAgent(t *testing.T, repo *memstore.Store, rec store.AgentRecord) {
	t.Helper()
	if err := repo.ReplaceAgents(context.Background(), "t", []store.AgentRecord{rec}); err != nil {
		t.Fatal(err)
	}
}

func TestMarketImportMakesTheSnapshotFilesTheBundle(t *testing.T) {
	a, repo := bundleApp(t)
	ctx, p := context.Background(), teamPrincipal("t")
	connector(t, repo, "t", store.McpConnectorRecord{ID: "mcp_docs", Name: "Docs", Transport: "streamable_http", URL: "https://docs.example/mcp"})
	connector(t, repo, "t", store.McpConnectorRecord{ID: "mcp_other", Name: "other", Transport: "streamable_http", URL: "https://by-url.example/mcp"})
	seedAgent(t, repo, store.AgentRecord{
		ID: "h/agent", Handle: "h", Slug: "agent", Name: "Agent", Description: "d", FilesKnown: true,
		SystemPrompts: []store.AgentPrompt{{Filename: "p", Content: "prompt text"}},
		Files: []store.SkillFile{
			{Path: "README.md", Body: "readme"},
			{Path: "SOUL.md", Body: "gentle"},
			{Path: "skills/pdf/SKILL.md", Body: "---\nname: pdf\n---\nx"},
			{Path: "skills/pdf/template.md", Body: "t"},
			{Path: "logo.png", Body: "PNG\x00\x01"},
			{Path: "../evil.md", Body: "x"},
			{Path: "big.md", Body: strings.Repeat("x", maxBundleFileBytes+1)},
			{Path: "mcp.json", Body: `{"mcpServers":{
				"docs":{"url":"https://elsewhere.example/mcp"},
				"byurl":{"url":"https://by-url.example/mcp"},
				"local":{"command":"npx","args":["srv"]},
				"leaky":{"url":"https://leak.example","headers":{"Authorization":"Bearer abcdef0123456789abcdef0123456789"}}}}`},
		},
	})
	imported, err := a.ExpertFromAgent(ctx, p, "h", "agent")
	if err != nil {
		t.Fatal(err)
	}
	files := filesOf(t, a, p, imported.Expert)
	if files["AGENTS.md"] != "prompt text" || files["SOUL.md"] != "gentle" || files["README.md"] != "readme" || files["skills/pdf/template.md"] != "t" {
		t.Errorf("files %v", files)
	}
	if strings.Contains(files["mcp.json"], "abcdef0123456789") {
		t.Errorf("the credential was stored: %s", files["mcp.json"])
	}
	profile, _ := a.Tasks.GetProfile(ctx, p, imported.Expert.Ref)
	if !reflect.DeepEqual(profile.Spec["bundle_skills"], []any{"pdf"}) {
		t.Errorf("skills: %v", profile.Spec)
	}
	if !reflect.DeepEqual(imported.Expert.ConnectorIDs, []string{"mcp_docs", "mcp_other"}) {
		t.Errorf("bound %v", imported.Expert.ConnectorIDs)
	}
	names := map[string]string{}
	for _, u := range imported.McpUnbound {
		names[u.Name] = u.Reason
	}
	if len(names) != 2 || names["local"] == "" || names["leaky"] == "" {
		t.Errorf("unbound %+v", imported.McpUnbound)
	}
	skipped := map[string]bool{}
	for _, s := range imported.SkippedFiles {
		skipped[s.Path] = true
	}
	for _, path := range []string{"logo.png", "../evil.md", "big.md", "mcp.json"} {
		if !skipped[path] {
			t.Errorf("%s should be listed as skipped: %+v", path, imported.SkippedFiles)
		}
	}
	if !reflect.DeepEqual(imported.Expert.McpUnbound, imported.McpUnbound) {
		t.Error("expert and result disagree on unbound servers")
	}
}

func TestMarketImportAlwaysHasAgentsMd(t *testing.T) {
	a, repo := bundleApp(t)
	ctx, p := context.Background(), teamPrincipal("t")
	cases := []struct {
		name  string
		agent store.AgentRecord
		want  string
	}{
		{"file wins", store.AgentRecord{Files: []store.SkillFile{{Path: "AGENTS.md", Body: "from file"}}, FilesKnown: true, SystemPrompts: []store.AgentPrompt{{Content: "prompt"}}}, "from file"},
		{"blank file falls back to the prompts", store.AgentRecord{Files: []store.SkillFile{{Path: "AGENTS.md", Body: "  \n"}}, FilesKnown: true, SystemPrompts: []store.AgentPrompt{{Content: "prompt"}}}, "prompt"},
		{"prompts", store.AgentRecord{SystemPrompts: []store.AgentPrompt{{Content: "prompt a"}, {Content: "prompt b"}}}, "prompt a\n\nprompt b"},
		{"name and description", store.AgentRecord{Description: "does things"}, "Helper\n\ndoes things"},
		{"name only", store.AgentRecord{}, "Helper"},
	}
	for _, c := range cases {
		c.agent.ID, c.agent.Handle, c.agent.Slug, c.agent.Name = "h/a", "h", "a", "Helper"
		seedAgent(t, repo, c.agent)
		imported, err := a.ExpertFromAgent(ctx, p, "h", "a")
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got := filesOf(t, a, p, imported.Expert)["AGENTS.md"]; got != c.want {
			t.Errorf("%s: AGENTS.md %q, want %q", c.name, got, c.want)
		}
	}
}

func TestBundleSkillsAreServedToTheWorkerPerTenant(t *testing.T) {
	a, _ := bundleApp(t)
	ctx, mine := context.Background(), teamPrincipal("t")
	base := bundleFiles{
		"skills/pdf/SKILL.md": "---\nname: pdf\n---\nx", "skills/pdf/t.md": "t",
		"skills/noskillmd/readme.md": "r", "README.md": "readme",
	}
	e, err := a.writeExpert(ctx, mine, "expert_s", 1, ExpertInput{Name: "S", Instructions: "i", baseFiles: base})
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := a.ExpertSkillBundleForWorker(ctx, "t", "expert_s", e.Version, "pdf")
	if err != nil || bundle.Name != "pdf" || len(bundle.Files) != 2 {
		t.Fatalf("%+v %v", bundle, err)
	}
	for _, c := range []struct {
		tenant, id string
		version    int
		name       string
	}{
		{"other", "expert_s", 1, "pdf"},   // another tenant
		{"", "expert_s", 1, "pdf"},        // no tenant
		{"t", "expert_none", 1, "pdf"},    // unknown expert
		{"t", "expert_s", 2, "pdf"},       // unknown version
		{"t", "expert_s", 1, "nope"},      // unknown skill
		{"t", "expert_s", 1, "noskillmd"}, // a directory that is no skill
		{"t", "expert_s", 1, "../README.md"},
	} {
		if _, err := a.ExpertSkillBundleForWorker(ctx, c.tenant, c.id, c.version, c.name); !errors.Is(err, store.ErrNotFound) {
			t.Errorf("%+v: %v", c, err)
		}
	}
}

func TestFilesOfAVersionWithoutBundleAreEmptyAndTeamsHaveNone(t *testing.T) {
	a, _ := bundleApp(t)
	ctx, p := context.Background(), teamPrincipal("t")
	// A version as it was written before bundles: a profile with a spec only.
	old, err := a.Tasks.RegisterProfile(ctx, p, taskruntime.Profile{ProfileID: "expert_old", Version: 1, Spec: map[string]any{"kind": "expert", "name": "Old", "instructions": "x"}})
	if err != nil {
		t.Fatal(err)
	}
	version, files, err := a.ExpertFiles(ctx, p, "expert_old", 0)
	if err != nil || version != old.Version || len(files) != 0 {
		t.Errorf("%d %v %v", version, files, err)
	}
	if _, _, err := a.ExpertFiles(ctx, teamPrincipal("other"), "expert_old", 0); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("another tenant: %v", err)
	}
	e := singleExpert(t, a, p, "New")
	_, files, _ = a.ExpertFiles(ctx, p, e.ID, 1)
	raw, _ := json.Marshal(files)
	if !strings.Contains(string(raw), `"path":"AGENTS.md"`) || !strings.Contains(string(raw), `"sha256"`) {
		t.Errorf("%s", raw)
	}
	content, err := a.ExpertFile(ctx, p, e.ID, 1, "AGENTS.md")
	if err != nil || content.Content != "do New" || content.SHA256 != fileSHA("do New") {
		t.Errorf("%+v %v", content, err)
	}
	if _, err := a.ExpertFile(ctx, p, e.ID, 1, "missing.md"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("missing: %v", err)
	}
}
