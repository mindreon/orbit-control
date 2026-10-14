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
	ID          string `json:"id"`
	Name        string `json:"name"`
	Slug        string `json:"slug"`
	Description string `json:"description"`
	Readme      string `json:"readme"`
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
	s := strings.ToLower(row.Slug)
	if s == "" {
		s = row.Name
	}
	s = strings.Trim(nonSlug.ReplaceAllString(s, "-"), "-")
	if len(s) > 45 {
		s = s[:45]
	}
	if len(s) < 3 {
		s = "market"
	}
	return fmt.Sprintf("%s-%x", s, sha256.Sum256([]byte(row.ID)))[:min(len(s)+13, 64)]
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
			return "", "", nil, fmt.Errorf("snapshot has no agent instructions")
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
			if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Fragment != "" || server.Command != nil {
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
		} else {
			if len(server.Headers) > 0 {
				return out, fmt.Errorf("stdio MCP cannot include HTTP headers")
			}
			if server.Command == nil || (*server.Command != "npx" && *server.Command != "uvx" && *server.Command != "python" && *server.Command != "python3") {
				return out, fmt.Errorf("MCP command requires manual conversion")
			}
			portable.Command = server.Command
			args := server.Args
			seen := false
			if *server.Command == "python" || *server.Command == "python3" {
				if len(args) < 2 || args[0] != "-m" || !regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_.]*$`).MatchString(args[1]) {
					return out, fmt.Errorf("MCP module required")
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
						return out, fmt.Errorf("MCP package requires manual conversion")
					}
					portable.Args = append(portable.Args, arg)
					seen = true
				} else if strings.HasPrefix(arg, "--") {
					flag, value, has := strings.Cut(arg, "=")
					if !regexp.MustCompile(`^--[A-Za-z][A-Za-z0-9-]*$`).MatchString(flag) {
						return out, fmt.Errorf("unsupported MCP flag")
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
					return out, fmt.Errorf("unsupported MCP positional argument")
				}
			}
			if !seen {
				return out, fmt.Errorf("MCP package required")
			}
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
			return result, err
		}
		skipped := false
		err = d.WithTenant(ctx, tenant, func(tx pgx.Tx) error {
			ctx := auth.Context(db.Context(ctx, tx), auth.Principal{ID: actor, Kind: "user"}, tenant)
			if e := tenancy.Active(ctx, tx, actor); e != nil {
				return e
			}
			var exists bool
			if e := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM assets WHERE tenant_id=$1 AND kind=$2 AND name=$3)`, tenant, kind, name).Scan(&exists); e != nil {
				return e
			}
			if exists {
				skipped = true
				return nil
			}
			a, e := s.Create(ctx, kind, name, "tenant", "")
			if e != nil {
				return e
			}
			v, e := s.Upload(ctx,a,"0.1.0",archive)
			if e != nil {
				return e
			}
			return s.Submit(ctx, a, v.ID, "stable")
		})
		if err != nil {
			return result, err
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
	for _, cidr := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "100::/64", "2001:db8::/32", "2001::/23", "2002::/16", "3fff::/20", "5f00::/16"} {
		if netip.MustParsePrefix(cidr).Contains(ip) {
			return false
		}
	}
	return true
}
