package app

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/mindreon/orbit-control/internal/store"
	taskruntime "github.com/mindreon/orbit-control/internal/task"
)

// The agent bundle (ADR-0013, orbit.agent/v1): the files an expert version is made of. The form is rendered into the
// bundle, and the spec the worker reads is derived from it, so there is one writer and one source of truth.

const (
	bundleSchema = "orbit.agent/v1"

	fileAgents = "AGENTS.md"
	fileSoul   = "SOUL.md"
	fileSkills = "skills.json"
	fileMCP    = "mcp.json"
	fileAgent  = "agent.json"

	skillsDir = "skills/"
	skillFile = "SKILL.md"

	maxBundleFiles     = 512
	maxBundleFileBytes = 256 << 10
	maxBundleBytes     = 4 << 20
	maxBundlePathBytes = 300
)

// bundleFiles maps a relative path to its UTF-8 text.
type bundleFiles map[string]string

func (b bundleFiles) clone() bundleFiles {
	out := make(bundleFiles, len(b))
	for k, v := range b {
		out[k] = v
	}
	return out
}

func (b bundleFiles) sortedPaths() []string {
	paths := make([]string, 0, len(b))
	for p := range b {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths
}

func fileSHA(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func (b bundleFiles) profileFiles() []taskruntime.ProfileFile {
	files := make([]taskruntime.ProfileFile, 0, len(b))
	for _, p := range b.sortedPaths() {
		files = append(files, taskruntime.ProfileFile{Path: p, Content: b[p], SHA256: fileSHA(b[p])})
	}
	return files
}

func bundleFromProfileFiles(files []taskruntime.ProfileFile) bundleFiles {
	out := make(bundleFiles, len(files))
	for _, f := range files {
		out[f.Path] = f.Content
	}
	return out
}

// bundleSHA is the digest of a whole bundle: every path with the digest of its content, in path order.
func bundleSHA(b bundleFiles) string {
	h := sha256.New()
	for _, p := range b.sortedPaths() {
		fmt.Fprintf(h, "%s\x00%s\n", p, fileSHA(b[p]))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// pathProblem says why a path is not a relative, normalized bundle path; empty when it is one.
func pathProblem(p string) string {
	switch {
	case p == "":
		return "the path is empty"
	case len(p) > maxBundlePathBytes:
		return "the path is too long"
	case strings.HasPrefix(p, "/"):
		return "the path is absolute"
	case strings.Contains(p, "\\"):
		return "the path has a backslash"
	case !utf8.ValidString(p) || strings.ContainsFunc(p, func(r rune) bool { return r < 0x20 || r == 0x7f }):
		return "the path has a control character"
	case path.Clean(p) != p:
		return "the path is not normalized"
	}
	for _, segment := range strings.Split(p, "/") {
		if segment == ".." || segment == "." || segment == "" {
			return "the path leaves its directory"
		}
	}
	return ""
}

// textProblem says why content cannot be stored: only UTF-8 text without NUL is.
func textProblem(content string) string {
	if !utf8.ValidString(content) || strings.IndexByte(content, 0) >= 0 {
		return "the file is not UTF-8 text"
	}
	return ""
}

func (b bundleFiles) validate() error {
	if len(b) > maxBundleFiles {
		return invalidField("BUNDLE_TOO_MANY_FILES", "files", fmt.Sprintf("a bundle has at most %d files", maxBundleFiles))
	}
	total := 0
	for _, p := range b.sortedPaths() {
		field := fmt.Sprintf("files[%s]", p)
		switch {
		case pathProblem(p) != "":
			return invalidField("BUNDLE_PATH_INVALID", field, pathProblem(p))
		case textProblem(b[p]) != "":
			return invalidField("BUNDLE_FILE_NOT_TEXT", field, textProblem(b[p]))
		case len(b[p]) > maxBundleFileBytes:
			return invalidField("BUNDLE_FILE_TOO_LARGE", field, fmt.Sprintf("a file is at most %d bytes", maxBundleFileBytes))
		}
		total += len(b[p])
	}
	if total > maxBundleBytes {
		return invalidField("BUNDLE_TOO_LARGE", "files", fmt.Sprintf("a bundle is at most %d bytes", maxBundleBytes))
	}
	return nil
}

// ---- skills.json ----

type skillEntry struct {
	Name   string `json:"name"`
	Source string `json:"source"`
	ID     string `json:"id,omitempty"`
}

func parseSkillsJSON(content string) ([]skillEntry, error) {
	bad := func(reason string) error { return invalidField("SKILLS_JSON_INVALID", fileSkills, reason) }
	var doc struct {
		Skills []skillEntry `json:"skills"`
	}
	if err := json.Unmarshal([]byte(content), &doc); err != nil {
		return nil, bad("skills.json is not {\"skills\":[{name, source, id}]}")
	}
	names, ids := map[string]bool{}, map[string]bool{}
	for i, entry := range doc.Skills {
		at := fmt.Sprintf("skills[%d]", i)
		switch {
		case strings.TrimSpace(entry.Name) == "":
			return nil, bad(at + " has no name")
		case entry.Source != "bundle" && entry.Source != "catalog":
			return nil, bad(at + ": source is bundle or catalog")
		case entry.Source == "catalog" && entry.ID == "":
			return nil, bad(at + ": a catalog skill needs an id")
		case names[entry.Name]:
			return nil, bad(at + ": a skill name is listed twice")
		case entry.Source == "catalog" && ids[entry.ID]:
			return nil, bad(at + ": a catalog skill is listed twice")
		}
		names[entry.Name] = true
		if entry.Source == "catalog" {
			ids[entry.ID] = true
		}
	}
	return doc.Skills, nil
}

func renderSkillsJSON(entries []skillEntry) string {
	raw, _ := json.MarshalIndent(map[string]any{"skills": entries}, "", "  ")
	return string(raw) + "\n"
}

// bundleSkillNames are the names of the skills/<name>/ directories that hold a SKILL.md, sorted.
func (b bundleFiles) bundleSkillNames() []string {
	var names []string
	for _, p := range b.sortedPaths() {
		if rest, ok := strings.CutPrefix(p, skillsDir); ok {
			if name, file, ok := strings.Cut(rest, "/"); ok && file == skillFile {
				names = append(names, name)
			}
		}
	}
	return names
}

// ---- mcp.json ----

// mcpServer is one entry of mcp.json. Obj keeps every field the author wrote, so an entry that is not loaded survives
// an edit of the form unchanged.
type mcpServer struct {
	Name string
	Obj  map[string]any
}

// MCPUnbound is an entry of mcp.json that is not bound to a connector of the tenant, so it is not loaded.
type MCPUnbound struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

func (s mcpServer) str(key string) string {
	v, _ := s.Obj[key].(string)
	return v
}

func (s mcpServer) connectorID() string { return s.str("connector_id") }

func (s mcpServer) unboundReason() string {
	if s.str("command") != "" {
		return "stdio servers are not supported; add the server as a connector"
	}
	return "not bound to a connector of this tenant"
}

var secretPlaceholder = regexp.MustCompile(`\$\{secret:[A-Za-z_][A-Za-z0-9_]*\}`)

// parseMCPJSON reads mcpServers in the order they were written.
func parseMCPJSON(content string) ([]mcpServer, error) {
	bad := func(reason string) error { return invalidField("MCP_JSON_INVALID", fileMCP, reason) }
	var doc struct {
		Servers json.RawMessage `json:"mcpServers"`
	}
	if err := json.Unmarshal([]byte(content), &doc); err != nil {
		return nil, bad("mcp.json is not {\"mcpServers\":{...}}")
	}
	if len(bytes.TrimSpace(doc.Servers)) == 0 || string(bytes.TrimSpace(doc.Servers)) == "null" {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(doc.Servers))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, bad("mcpServers must be an object")
	}
	var servers []mcpServer
	seen := map[string]bool{}
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return nil, bad("mcpServers is not valid JSON")
		}
		name, _ := keyTok.(string)
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil || obj == nil {
			return nil, bad(fmt.Sprintf("mcpServers.%s must be an object", name))
		}
		if name == "" || seen[name] {
			return nil, bad("a server needs a unique name")
		}
		seen[name] = true
		server := mcpServer{Name: name, Obj: obj}
		if err := server.check(); err != nil {
			return nil, err
		}
		servers = append(servers, server)
	}
	return servers, nil
}

func (s mcpServer) check() error {
	bad := func(reason string) error {
		return invalidField("MCP_JSON_INVALID", fmt.Sprintf("mcp.json mcpServers.%s", s.Name), reason)
	}
	for _, key := range []string{"url", "transport", "command", "connector_id"} {
		if v, present := s.Obj[key]; present {
			if _, ok := v.(string); !ok {
				return bad(key + " must be a string")
			}
		}
	}
	if s.str("url") == "" && s.str("command") == "" {
		return bad("a server needs a url or a command")
	}
	if t := s.str("transport"); t != "" && t != "streamable_http" && t != "sse" && t != "stdio" {
		return bad("transport is streamable_http or sse")
	}
	for _, key := range []string{"headers", "env"} {
		if v, present := s.Obj[key]; present {
			m, ok := v.(map[string]any)
			if !ok {
				return bad(key + " must be an object of strings")
			}
			for _, value := range m {
				if _, ok := value.(string); !ok {
					return bad(key + " must be an object of strings")
				}
			}
		}
	}
	if v, present := s.Obj["args"]; present {
		list, ok := v.([]any)
		if !ok {
			return bad("args must be a list of strings")
		}
		for _, item := range list {
			if _, ok := item.(string); !ok {
				return bad("args must be a list of strings")
			}
		}
	}
	return nil
}

var (
	// A value that looks like a credential: a known vendor prefix, or a long run of token characters with a digit.
	tokenPrefix = regexp.MustCompile(`(?i)(^|[^A-Za-z0-9])(sk-|ghp_|gho_|ghs_|github_pat_|xox[abprs]-|akia|aiza|glpat-)[A-Za-z0-9_\-]{8,}`)
	tokenRun    = regexp.MustCompile(`[A-Za-z0-9_\-+=]{32,}`)
	sensitive   = regexp.MustCompile(`(?i)(authorization|token|secret|api[-_]?key|password|passwd|cookie)`)
	schemeWord  = regexp.MustCompile(`(?i)^(bearer|basic|token)\s*$`)
)

// literalSecret reports whether value, once its ${secret:NAME} placeholders are taken out, holds a credential. A header
// or env value of a sensitive name may carry nothing but a scheme word and placeholders.
func literalSecret(name, value string) bool {
	rest := strings.TrimSpace(secretPlaceholder.ReplaceAllString(value, ""))
	if rest == "" || schemeWord.MatchString(rest) {
		return false
	}
	if sensitive.MatchString(name) {
		return true
	}
	return containsToken(rest)
}

func containsToken(text string) bool {
	if tokenPrefix.MatchString(text) {
		return true
	}
	for _, run := range tokenRun.FindAllString(text, -1) {
		if strings.ContainsAny(run, "0123456789") && strings.ContainsFunc(run, func(r rune) bool { return r >= 'A' && r <= 'z' }) {
			return true
		}
	}
	return false
}

// secretFields lists the fields of an entry that hold a literal credential: "headers.Authorization", "env.TOKEN",
// "args[1]", "url".
func (s mcpServer) secretFields() []string {
	var found []string
	for _, key := range []string{"headers", "env"} {
		if m, ok := s.Obj[key].(map[string]any); ok {
			for name, value := range m {
				if literalSecret(name, value.(string)) {
					found = append(found, key+"."+name)
				}
			}
		}
	}
	if args, ok := s.Obj["args"].([]any); ok {
		for i, item := range args {
			if containsToken(secretPlaceholder.ReplaceAllString(item.(string), "")) {
				found = append(found, fmt.Sprintf("args[%d]", i))
			}
		}
	}
	if raw := s.str("url"); raw != "" {
		rest := secretPlaceholder.ReplaceAllString(raw, "")
		if i := strings.Index(rest, "://"); i >= 0 {
			if authority, _, _ := strings.Cut(rest[i+3:], "/"); strings.Contains(authority, "@") {
				found = append(found, "url")
			} else if _, query, ok := strings.Cut(rest, "?"); ok && containsToken(query) {
				found = append(found, "url")
			}
		}
	}
	sort.Strings(found)
	return found
}

// checkNoLiteralSecrets refuses an mcp.json that holds a credential: a bundle never has one, only placeholders that
// the bound connector's secret fills at run time. The value is never put in the error.
func checkNoLiteralSecrets(servers []mcpServer) error {
	for _, server := range servers {
		if fields := server.secretFields(); len(fields) > 0 {
			return invalidField("MCP_LITERAL_SECRET", fmt.Sprintf("mcp.json mcpServers.%s", server.Name),
				fmt.Sprintf("%s holds a literal credential; use a ${secret:NAME} placeholder", fields[0]))
		}
	}
	return nil
}

// scrub removes the fields that hold a literal credential and returns them.
func (s mcpServer) scrub() []string {
	fields := s.secretFields()
	for _, field := range fields {
		switch {
		case field == "url":
			delete(s.Obj, "url")
		case strings.HasPrefix(field, "args["):
			delete(s.Obj, "args")
		default:
			kind, name, _ := strings.Cut(field, ".")
			delete(s.Obj[kind].(map[string]any), name)
		}
	}
	return fields
}

func renderMCPJSON(servers []mcpServer) string {
	var buf bytes.Buffer
	buf.WriteString(`{"mcpServers":{`)
	for i, server := range servers {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, _ := json.Marshal(server.Name)
		body, _ := json.Marshal(server.Obj) // keys sorted, which keeps the file stable
		buf.Write(key)
		buf.WriteByte(':')
		buf.Write(body)
	}
	buf.WriteString(`}}`)
	var out bytes.Buffer
	_ = json.Indent(&out, buf.Bytes(), "", "  ")
	return out.String() + "\n"
}

// connectorServer renders a connector of the tenant as an mcp.json entry bound to it. Only names of secrets appear.
func connectorServer(name string, rec store.McpConnectorRecord) mcpServer {
	obj := map[string]any{"connector_id": rec.ID}
	if rec.URL != "" {
		obj["url"] = rec.URL
		transport := rec.Transport
		if transport == "" || transport == "stdio" {
			transport = "streamable_http"
		}
		obj["transport"] = transport
		if refs := decodeHeaderRefs(rec.HeaderRefs); len(refs) > 0 {
			headers := map[string]any{}
			for _, ref := range refs {
				headers[ref.Name] = "${secret:" + ref.Env + "}"
			}
			obj["headers"] = headers
		}
	} else {
		obj["command"] = rec.Command
		if len(rec.Args) > 0 {
			obj["args"] = stringsToAny(rec.Args)
		}
		if len(rec.EnvRefs) > 0 {
			env := map[string]any{}
			for _, ref := range rec.EnvRefs {
				env[ref] = "${secret:" + ref + "}"
			}
			obj["env"] = env
		}
	}
	return mcpServer{Name: name, Obj: obj}
}

func stringsToAny(in []string) []any {
	out := make([]any, len(in))
	for i, s := range in {
		out[i] = s
	}
	return out
}

// ---- derive ----

// derived is what the worker reads, taken out of a bundle.
type derived struct {
	Instructions  string
	Soul          string
	CatalogSkills []string // catalog skill ids, in skills.json order
	BundleSkills  []string // bundle skill names, in load order
	ConnectorIDs  []string // bound entries of mcp.json, in file order
	Unbound       []MCPUnbound
}

var errAgentsRequired = func() error {
	return invalidField("AGENTS_MD_REQUIRED", "instructions", "an expert needs instructions: its AGENTS.md cannot be missing or empty")
}

// deriveBundle checks the bundle as a whole and reads the spec fields out of it. A bundle skill that skills.json lists
// but that has no skills/<name>/SKILL.md is an error; a directory that skills.json does not list still loads, after the
// listed ones and in name order.
func deriveBundle(b bundleFiles) (derived, error) {
	var d derived
	if err := b.validate(); err != nil {
		return d, err
	}
	d.Instructions = strings.TrimSpace(b[fileAgents])
	if d.Instructions == "" {
		return d, errAgentsRequired()
	}
	d.Soul = strings.TrimSpace(b[fileSoul])
	if err := checkAgentJSON(b[fileAgent]); err != nil {
		return d, err
	}
	dirs := map[string]bool{}
	for _, name := range b.bundleSkillNames() {
		dirs[name] = true
	}
	listed := map[string]bool{}
	if content, ok := b[fileSkills]; ok {
		entries, err := parseSkillsJSON(content)
		if err != nil {
			return d, err
		}
		for i, entry := range entries {
			if entry.Source == "catalog" {
				d.CatalogSkills = append(d.CatalogSkills, entry.ID)
				continue
			}
			if !dirs[entry.Name] {
				return d, invalidField("BUNDLE_SKILL_MISSING", fmt.Sprintf("skills.json skills[%d]", i),
					fmt.Sprintf("skills/%s/SKILL.md does not exist", entry.Name))
			}
			listed[entry.Name] = true
			d.BundleSkills = append(d.BundleSkills, entry.Name)
		}
	}
	for _, name := range b.bundleSkillNames() {
		if !listed[name] {
			d.BundleSkills = append(d.BundleSkills, name)
		}
	}
	if content, ok := b[fileMCP]; ok {
		servers, err := parseMCPJSON(content)
		if err != nil {
			return d, err
		}
		if err := checkNoLiteralSecrets(servers); err != nil {
			return d, err
		}
		for _, server := range servers {
			if id := server.connectorID(); id != "" {
				d.ConnectorIDs = append(d.ConnectorIDs, id)
			} else {
				d.Unbound = append(d.Unbound, MCPUnbound{Name: server.Name, Reason: server.unboundReason()})
			}
		}
	}
	return d, nil
}

func checkAgentJSON(content string) error {
	if content == "" {
		return nil
	}
	var doc struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal([]byte(content), &doc); err != nil || (doc.Schema != "" && doc.Schema != bundleSchema) {
		return invalidField("AGENT_JSON_INVALID", fileAgent, "agent.json is {\"schema\":\""+bundleSchema+"\", name, description, model}")
	}
	return nil
}

// ---- render ----

// renderBundle turns the form into files. The files of base that the form has no field for (README.md, skill
// directories, entries of mcp.json that are not bound) are kept as they are; the ones the form does own are replaced.
func renderBundle(in ExpertInput, base bundleFiles, connectors map[string]store.McpConnectorRecord, skillNames map[string]string) bundleFiles {
	out := base.clone()
	out[fileAgents] = in.Instructions
	if in.Soul != "" {
		out[fileSoul] = in.Soul
	} else {
		delete(out, fileSoul)
	}

	description := in.description
	if description == "" {
		var prev struct {
			Description string `json:"description"`
		}
		_ = json.Unmarshal([]byte(base[fileAgent]), &prev)
		description = prev.Description
	}
	agent := map[string]any{"schema": bundleSchema, "name": in.Name}
	if description != "" {
		agent["description"] = description
	}
	if in.Model != "" {
		agent["model"] = in.Model
	}
	raw, _ := json.MarshalIndent(agent, "", "  ")
	out[fileAgent] = string(raw) + "\n"

	// skills.json: bundle entries stay where they were, catalog entries are the form's.
	wanted := map[string]bool{}
	for _, skillID := range in.SkillIDs {
		wanted[skillID] = true
	}
	var entries []skillEntry
	have := map[string]bool{}
	dirs := map[string]bool{}
	for _, name := range out.bundleSkillNames() {
		dirs[name] = true
	}
	if prev, err := parseSkillsJSON(base[fileSkills]); err == nil || base[fileSkills] == "" {
		for _, entry := range prev {
			switch {
			case entry.Source == "bundle" && dirs[entry.Name]:
				entries = append(entries, entry)
			case entry.Source == "catalog" && wanted[entry.ID]:
				have[entry.ID] = true
				entries = append(entries, skillEntry{Name: orDefault(skillNames[entry.ID], entry.Name), Source: "catalog", ID: entry.ID})
			}
		}
	}
	for _, skillID := range in.SkillIDs {
		if !have[skillID] {
			entries = append(entries, skillEntry{Name: orDefault(skillNames[skillID], skillID), Source: "catalog", ID: skillID})
		}
	}
	if len(entries) > 0 {
		out[fileSkills] = renderSkillsJSON(entries)
	} else {
		delete(out, fileSkills)
	}

	// mcp.json: entries that are not bound stay, the form's connectors are the bound ones.
	var servers []mcpServer
	taken := map[string]bool{}
	prevNames := map[string]string{} // connector id -> the name its entry had
	if prev, err := parseMCPJSON(base[fileMCP]); err == nil || base[fileMCP] == "" {
		for _, server := range prev {
			if id := server.connectorID(); id != "" {
				prevNames[id] = server.Name
				continue
			}
			taken[server.Name] = true
			servers = append(servers, server)
		}
	}
	var bound []mcpServer
	for _, connectorID := range in.ConnectorIDs {
		rec, ok := connectors[connectorID]
		if !ok {
			continue
		}
		name := orDefault(prevNames[connectorID], orDefault(rec.Name, connectorID))
		stem := name
		for n := 2; taken[name]; n++ {
			name = fmt.Sprintf("%s-%d", stem, n)
		}
		taken[name] = true
		bound = append(bound, connectorServer(name, rec))
	}
	servers = append(bound, servers...)
	if len(servers) > 0 {
		out[fileMCP] = renderMCPJSON(servers)
	} else {
		delete(out, fileMCP)
	}
	return out
}

func orDefault(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
