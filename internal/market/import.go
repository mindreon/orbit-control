// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// MCP portable-config conversion adapted from jiuwenswarm; see NOTICE.
package market

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/assets/bundle"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

type Result struct {
	Imported    int `json:"imported"`
	Skipped     int `json:"skipped"`
	Unsupported int `json:"unsupported"`
}
type snapshot struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Slug        string   `json:"slug"`
	Description string   `json:"description"`
	Readme      string   `json:"readme"`
	Catalogues  []string `json:"catalogues"`
	Files       []struct {
		Path string `json:"path"`
		Body string `json:"body"`
	} `json:"files"`
	Prompts []struct {
		Filename string `json:"filename"`
		Content  string `json:"content"`
	} `json:"systemPrompts"`
}

var nonSlug = regexp.MustCompile(`[^a-z0-9-]+`)
var fences = regexp.MustCompile("(?s)```(?:json)?\\s*\\n(.*?)```")

func nameFor(row snapshot) string {
	return AssetName(row.ID, row.Slug, row.Name)
}

// AssetName is the agent name a catalog row was imported as.
func AssetName(id, slug, display string) string {
	s := strings.ToLower(slug)
	if s == "" {
		s = display
	}
	s = strings.Trim(nonSlug.ReplaceAllString(s, "-"), "-")
	if len(s) > 40 {
		s = strings.Trim(s[:40], "-")
	}
	if len(s) < 3 {
		s = "market"
	}
	sum := fmt.Sprintf("%x", sha256.Sum256([]byte(id)))[:8]
	return "cat-" + s + "-" + sum
}
func Convert(raw []byte) (contract.AssetKind, string, map[string][]byte, error) {
	var row snapshot
	if err := json.Unmarshal(raw, &row); err != nil {
		return "", "", nil, fmt.Errorf("invalid snapshot row")
	}
	name := nameFor(row)
	files := map[string][]byte{}
	if row.Files != nil || row.Prompts != nil {
		for _, f := range row.Files {
			if _, err := bundle.SafePath(f.Path); err != nil {
				return "", "", nil, fmt.Errorf("unsafe snapshot file")
			}
			if _, exists := files[f.Path]; exists {
				return "", "", nil, fmt.Errorf("duplicate snapshot file")
			}
			files[f.Path] = []byte(f.Body)
		}
		for _, p := range row.Prompts {
			if p.Filename == "AGENTS.md" || p.Filename == "SOUL.md" {
				if len(files[p.Filename]) == 0 {
					files[p.Filename] = []byte(p.Content)
				}
			}
		}
		if len(files["AGENTS.md"]) == 0 {
			text := strings.TrimSpace(row.Description)
			if text == "" {
				text = strings.TrimSpace(row.Readme)
			}
			if text == "" {
				return "", "", nil, fmt.Errorf("snapshot has no agent instructions")
			}
			files["AGENTS.md"] = []byte(text)
		}
		files["AGENTS.md"] = clipBytes(files["AGENTS.md"], 19999)
		if soul := files["SOUL.md"]; len(soul) > 0 {
			files["SOUL.md"] = clipBytes(soul, 19999)
		}
		if len(files["README.md"]) == 0 && strings.TrimSpace(row.Readme) != "" {
			files["README.md"] = clipBytes([]byte(row.Readme), (256<<10)-1)
		}
		for path, body := range files {
			base := path
			if i := strings.LastIndex(path, "/"); i >= 0 {
				base = path[i+1:]
			}
			// Snapshot skill and MCP manifests are not Orbit manifests. Keeping
			// them makes the bundle check reject the whole agent.
			if base == "skills.json" || base == "mcp.json" || base != "agent.json" && strings.HasSuffix(base, ".json") {
				delete(files, path)
				continue
			}
			if path == "AGENTS.md" || path == "SOUL.md" || path == "README.md" || path == "agent.json" {
				continue
			}
			if len(body) > (256<<10)-1 || strings.ContainsRune(string(body), 0) {
				delete(files, path)
			}
		}
		if err := ensureManifest(files, name, row.Name, strings.TrimSpace(row.Description), row.Catalogues); err != nil {
			return "", "", nil, err
		}
		return "agent", name, files, nil
	}
	for _, match := range fences.FindAllStringSubmatch(row.Readme, -1) {
		config, err := parseMCPExample([]byte(match[1]))
		if err != nil {
			continue
		}
		converted, err := ConvertMCP(config)
		if err != nil {
			continue
		}
		content, err := json.Marshal(converted, json.Deterministic(true))
		if err != nil {
			return "", "", nil, err
		}
		files["mcp.json"] = content
		return "mcp_server", name, files, nil
	}
	return "", "", nil, fmt.Errorf("snapshot has no embedded asset content")
}

func clipBytes(b []byte, n int) []byte {
	if len(b) <= n {
		return b
	}
	b = b[:n]
	for len(b) > 0 && b[len(b)-1]&0xC0 == 0x80 {
		b = b[:len(b)-1]
	}
	return b
}

func ensureManifest(files map[string][]byte, name, display, description string, tags []string) error {
	manifest := map[string]any{}
	if raw := files["agent.json"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &manifest); err != nil {
			manifest = map[string]any{}
		}
	}
	if manifest["schema"] == nil {
		manifest["schema"] = "orbit.agent/v2"
	}
	if manifest["name"] == nil || manifest["name"] == "" {
		manifest["name"] = name
	}
	if description != "" && manifest["description"] == nil {
		manifest["description"] = description
	}
	if display != "" {
		names, _ := manifest["displayName"].(map[string]any)
		if names == nil {
			names = map[string]any{}
		}
		if names["zh"] == nil {
			names["zh"] = display
		}
		manifest["displayName"] = names
	}
	if len(tags) > 0 && manifest["tags"] == nil {
		manifest["tags"] = tags
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	files["agent.json"] = raw
	return nil
}

var existingPlaceholder = regexp.MustCompile(`^\$\{(?:secret:)?([A-Za-z_][A-Za-z0-9_]*)\}$`)

var secretName = regexp.MustCompile(`[^A-Za-z0-9_]`)
var packageName = regexp.MustCompile(`^(?:@[a-z0-9_.-]+/)?[a-z0-9][a-z0-9_.-]*(?:@[a-zA-Z0-9_.^~-]+)?$`)

func ConvertMCP(in contract.McpManifest) (contract.McpManifest, error) {
	out := contract.McpManifest{McpServers: contract.McpManifestMcpServers{}}
	if len(in.McpServers) != 1 {
		return out, fmt.Errorf("portable MCP config must contain exactly one server")
	}
	for name, server := range in.McpServers {
		portable := contract.McpServer{}
		placeholder := func(field, value string) string {
			if match := existingPlaceholder.FindStringSubmatch(value); match != nil {
				return "${secret:" + match[1] + "}"
			}
			return "${secret:MCP_" + strings.ToUpper(secretName.ReplaceAllString(name+"_"+field, "_")) + "}"
		}
		if server.Url != nil {
			if len(server.Env) > 0 {
				return out, fmt.Errorf("remote MCP cannot include process environment")
			}
			u, e := url.Parse(*server.Url)
			if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" ||
				server.Command != nil {
				return out, fmt.Errorf("nonportable MCP endpoint")
			}
			host := strings.ToLower(u.Hostname())
			if host == "localhost" || strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") {
				return out, fmt.Errorf("nonportable MCP endpoint")
			}
			if ip, e := netip.ParseAddr(host); e == nil && !globalEndpointIP(ip) {
				return out, fmt.Errorf("nonportable MCP endpoint")
			}
			portable.Url = new("https://" + placeholder("ENDPOINT", *server.Url))
			portable.Transport = server.Transport
			if portable.Transport == nil {
				portable.Transport = new(contract.McpTransport("streamable_http"))
			}
		} else if err := convertStdioMCP(server, &portable, placeholder); err != nil {
			return out, err
		}

		portable.Env = contract.McpServerEnv{}
		for key, value := range server.Env {
			if !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(key) {
				return out, fmt.Errorf("invalid MCP environment key")
			}
			portable.Env[key] = placeholder("ENV_"+key, value)
		}
		portable.Headers = contract.McpServerHeaders{}
		for key, value := range server.Headers {
			if !regexp.MustCompile("^[!#$%&\x27*+.^_`|~0-9A-Za-z-]+$").MatchString(key) {
				return out, fmt.Errorf("invalid MCP header key")
			}
			p := placeholder("HEADER_"+key, strings.TrimPrefix(value, "Bearer "))
			if strings.HasPrefix(value, "Bearer ") {
				p = "Bearer " + p
			}
			portable.Headers[key] = p
		}
		portable.Tools = server.Tools
		out.McpServers[name] = portable
	}
	return out, nil
}
func Import(ctx context.Context, d *db.DB, s *assets.Service, tenant, actor, file string) (Result, error) {
	result := Result{}
	f, err := os.Open(file)
	if err != nil {
		return result, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return result, err
	}
	defer gz.Close()
	scanner := bufio.NewScanner(io.LimitReader(gz, 1<<30))
	scanner.Buffer(make([]byte, 64<<10), 16<<20)
	for scanner.Scan() {
		kind, name, files, err := Convert(scanner.Bytes())
		if err != nil {
			result.Unsupported++
			continue
		}
		archive, err := bundle.Pack(files)
		if err != nil {
			fmt.Fprintf(os.Stderr, "skip %s: %v\n", name, err)
			result.Unsupported++
			continue
		}
		staged, err := s.PrepareUpload(auth.Context(ctx, auth.Principal{ID: actor, Kind: "user"}, tenant), "0.1.0", archive)
		if err != nil {
			fmt.Fprintf(os.Stderr, "skip %s: %v\n", name, err)
			result.Unsupported++
			continue
		}
		skipped := false
		err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			ctx := auth.Context(db.Context(ctx, tx), auth.Principal{ID: actor, Kind: "user"}, tenant)
			if e := tenancy.Active(ctx, tx, actor); e != nil {
				return e
			}
			var exists bool
			if e := tx.QueryRow(ctx,
				(`SELECT EXISTS(SELECT 1
 FROM assets
 WHERE tenant_id=$1
 AND kind=$2
 AND name=$3)`),
				tenant,
				kind,
				name).Scan(&exists); e != nil {
				return e
			}
			if exists {
				skipped = true
				return nil
			}
			// Private releases publish after the checks, so the catalog can show
			// the display name and description without waiting for a reviewer.
			a, e := s.Create(ctx, kind, name, "private", "")
			if e != nil {
				return e
			}
			v, e := s.Upload(ctx, a, staged)
			if e != nil {
				return e
			}
			return s.Submit(ctx, a, v.ID, "stable")
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "skip %s: %v\n", name, err)
			result.Unsupported++
			continue
		}
		if skipped {
			result.Skipped++
		} else {
			result.Imported++
		}
	}
	if err = scanner.Err(); err != nil {
		return result, err
	}
	return result, nil
}

// Market READMEs use both Claude's type and Orbit's transport spelling. Copy
// only portable runtime fields; account and connection state are never imported.
func parseMCPExample(raw []byte) (contract.McpManifest, error) {
	var root map[string]any
	var out contract.McpManifest
	if err := json.Unmarshal(raw, &root); err != nil {
		return out, err
	}
	servers, ok := root["mcpServers"].(map[string]any)
	if !ok || len(servers) != 1 {
		return out, fmt.Errorf("MCP servers missing")
	}
	converted := map[string]any{}
	for name, value := range servers {
		server, ok := value.(map[string]any)
		if !ok {
			return out, fmt.Errorf("invalid MCP example")
		}
		if cwd, ok := server["cwd"]; ok && cwd != "" {
			return out, fmt.Errorf("local MCP working directory")
		}
		portable := map[string]any{}
		for _, key := range []string{"ref", "url", "transport", "headers", "command", "args", "env", "tools"} {
			if value, ok := server[key]; ok {
				portable[key] = value
			}
		}
		if _, remote := portable["url"]; remote {
			transport, _ := server["transport"].(string)
			if transport == "" {
				transport, _ = server["type"].(string)
			}
			switch transport {
			case "", "http", "streamable-http":
				transport = "streamable_http"
			}
			portable["transport"] = transport
		}
		converted[name] = portable
	}
	normalized, err := json.Marshal(map[string]any{"mcpServers": converted}, json.Deterministic(true))
	if err != nil {
		return out, err
	}
	if err = contract.ValidateStrict("McpManifest", normalized); err != nil {
		return out, err
	}
	err = json.Unmarshal(normalized, &out)
	return out, err
}

// netip.IsGlobalUnicast includes several IANA special-purpose ranges which
// Python is_global (the portable-config source rule) excludes.
func globalEndpointIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() {
		return false
	}
	for _, cidr := range []string{"0.0.0.0/8",
		"100.64.0.0/10",
		"192.0.0.0/24",
		"192.0.2.0/24",
		"198.18.0.0/15",
		"198.51.100.0/24",
		"203.0.113.0/24",
		"240.0.0.0/4",
		"100::/64",
		"2001:db8::/32",
		"2001::/23",
		"2002::/16",
		"3fff::/20",
		"5f00::/16"} {
		if netip.MustParsePrefix(cidr).Contains(ip) {
			return false
		}
	}
	return true
}

func convertStdioMCP(server contract.McpServer, portable *contract.McpServer, placeholder func(string, string) string) error {
	if len(server.Headers) > 0 {
		return fmt.Errorf("stdio MCP cannot include HTTP headers")
	}
	if server.Command == nil ||
		(*server.Command != "npx" &&
			*server.Command != "uvx" &&
			*server.Command != "python" &&
			*server.Command != "python3") {
		return fmt.Errorf("MCP command requires manual conversion")
	}
	portable.Command = server.Command
	args := server.Args
	seen := false
	if *server.Command == "python" || *server.Command == "python3" {
		if len(args) < 2 || args[0] != "-m" || !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*$`).MatchString(args[1]) {
			return fmt.Errorf("MCP module required")
		}
		portable.Args = append(portable.Args, args[:2]...)
		args = args[2:]
		seen = true
	}
	expect := false
	for i, arg := range args {
		if !seen {
			if *server.Command == "npx" && (arg == "-y" || arg == "--yes") {
				portable.Args = append(portable.Args, arg)
				continue
			}
			if !packageName.MatchString(arg) {
				return fmt.Errorf("MCP package requires manual conversion")
			}
			portable.Args = append(portable.Args, arg)
			seen = true
		} else if strings.HasPrefix(arg, "--") {
			flag, value, has := strings.Cut(arg, "=")
			if !regexp.MustCompile(`^--[A-Za-z][A-Za-z0-9-]*$`).MatchString(flag) {
				return fmt.Errorf("unsupported MCP flag")
			}
			if has {
				portable.Args = append(portable.Args, flag+"="+placeholder(fmt.Sprintf("ARG_%d", i), value))
			} else {
				portable.Args = append(portable.Args, flag)
			}
			expect = !has
		} else if expect {
			portable.Args = append(portable.Args, placeholder(fmt.Sprintf("ARG_%d", i), arg))
			expect = false
		} else {
			return fmt.Errorf("unsupported MCP positional argument")
		}
	}
	if !seen {
		return fmt.Errorf("MCP package required")
	}
	return nil
}
