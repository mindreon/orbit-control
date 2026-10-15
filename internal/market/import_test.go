package market

import (
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/mindreon/orbit-control/internal/contract"
)

func TestPortableMCPCredentials(t *testing.T) {
	input := contract.McpManifest{
		McpServers: contract.McpManifestMcpServers{
			"mail": {
				Url:     new("https://mcp.example.test/personal-value?key=private-value"),
				Headers: contract.McpServerHeaders{"Authorization": "Bearer private-value"},
			},
		},
	}
	out, e := ConvertMCP(input)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := json.Marshal(out)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(raw), "private-value") || strings.Contains(string(raw), "personal-value") {
		t.Fatal("credential retained")
	}
	if !strings.Contains(string(raw), "${secret:MCP_MAIL_") {
		t.Fatal("missing contract placeholder")
	}
	if e = contract.ValidateStrict("McpManifest", raw); e != nil {
		t.Fatal(e)
	}
	input.McpServers["mail"] = contract.McpServer{
		Command: new("npx"),
		Args:    []string{"-y", "@acme/server", "--token", "private-value"},
	}
	out, e = ConvertMCP(input)
	if e != nil {
		t.Fatal(e)
	}
	if out.McpServers["mail"].Args[3] == "private-value" {
		t.Fatal("argument credential retained")
	}
	input.McpServers["mail"] = contract.McpServer{Command: new("/usr/local/personal")}
	if _, e = ConvertMCP(input); e == nil {
		t.Fatal("local command imported")
	}
}
func TestSnapshotsRequireContent(t *testing.T) {
	if _, _, _, e := Convert([]byte(`{"id":"skill","slug":"skill","sourceUrl":"https://example.test"}`)); e == nil {
		t.Fatal("metadata became fake skill")
	}
	kind, name, files, e := Convert(
		[]byte(
			`{"id":"one","slug":"sample","systemPrompts":[{"filename":"AGENTS.md","content":"instructions"}],"files":[]}`,
		),
	)
	if e != nil || kind != "agent" || name == "" || string(files["AGENTS.md"]) != "instructions" {
		t.Fatal(kind, name, e)
	}
	_, _, _, e = Convert([]byte(`{"id":"one","files":[{"path":"../escape","body":"bad"}]}`))
	if e == nil {
		t.Fatal("unsafe snapshot path accepted")
	}
}

func TestMarketMCPTransportNormalization(t *testing.T) {
	raw := []byte(
		"{\"id\":\"mcp-one\",\"name\":\"server\",\"readme\":\"```json\\n{\\\"mcpServers\\\":{\\\"demo\\\":{\\\"url\\\":\\\"ht" +
			"tps://example.test/private-value\\\",\\\"type\\\":\\\"streamable-http\\\",\\\"headers\\\":{\\\"Authorizati" +
			"on\\\":\\\"Bearer private-value\\\"}}}}\\n```\"}",
	)
	kind, _, files, err := Convert(raw)
	if err != nil || kind != "mcp_server" {
		t.Fatal(kind, err)
	}
	if strings.Contains(string(files["mcp.json"]), "private-value") {
		t.Fatal("credential retained")
	}
	first := string(files["mcp.json"])
	for range 10 {
		_, _, files, err = Convert(raw)
		if err != nil || string(files["mcp.json"]) != first {
			t.Fatal("nondeterministic import", err)
		}
	}
}

func TestMCPPlaceholderNames(t *testing.T) {
	in := contract.McpManifest{McpServers: contract.McpManifestMcpServers{"demo": {
		Command: new("npx"), Args: []string{"demo", "--token=${TOKEN}", "--key", "${secret:KEY}"},
		Env: contract.McpServerEnv{"API_KEY": "${EXISTING}"},
	}}}
	out, err := ConvertMCP(in)
	if err != nil {
		t.Fatal(err)
	}
	server := out.McpServers["demo"]
	if server.Args[1] != "--token=${secret:TOKEN}" || server.Args[3] != "${secret:KEY}" ||
		server.Env["API_KEY"] != "${secret:EXISTING}" {
		t.Fatalf("placeholder names changed: %+v", server)
	}
}

func TestMCPRejectsPrivateEndpoints(t *testing.T) {
	for _, endpoint := range []string{"https://localhost/mcp",
		"https://host.local/mcp",
		"https://127.0.0.1/mcp",
		"https://10.0.0.1/mcp",
		"https://[::1]/mcp",
		"https://[fc00::1]/mcp"} {
		t.Run(endpoint, func(t *testing.T) {
			in := contract.McpManifest{McpServers: contract.McpManifestMcpServers{"demo": {Url: new(endpoint)}}}
			if _, err := ConvertMCP(in); err == nil {
				t.Fatal("nonportable endpoint accepted")
			}
		})
	}
}

func TestMCPPortableBoundaries(t *testing.T) {
	for _, server := range []contract.McpServer{
		{Command: new("npx"), Args: []string{"demo"}, Env: contract.McpServerEnv{"BAD-KEY": "x"}},
		{Url: new("https://example.test"), Headers: contract.McpServerHeaders{"Bad Header": "x"}},
		{Url: new("https://100.64.0.1")},
		{Url: new("https://192.0.2.1")},
		{Url: new("https://[2001:db8::1]")},
		{Url: new("https://[::ffff:100.64.0.1]")},
	} {
		if _, err := ConvertMCP(contract.McpManifest{McpServers: contract.McpManifestMcpServers{"demo": server}}); err == nil {
			t.Fatal("nonportable config accepted")
		}
	}
	if _,
		err := ConvertMCP(contract.McpManifest{McpServers: contract.McpManifestMcpServers{"a": {Url: new("https://example.test")},
		"b": {Url: new("https://other.test")}}}); err == nil {
		t.Fatal("multiple servers accepted")
	}
}
