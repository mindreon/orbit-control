package integration

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/assets"
	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/httpapi"
	"github.com/mindreon/orbit-control/internal/platform/config"
	"github.com/mindreon/orbit-control/internal/platform/ids"
	"github.com/mindreon/orbit-control/internal/platform/objstore"
	"github.com/mindreon/orbit-control/internal/tenancy"
)

func wp9Handler(t *testing.T, f *runFixture, objects objstore.ObjectStore) (http.Handler, string) {
	t.Helper()
	c := config.Config{AuthMode: "dev", KeyFile: t.TempDir() + "/auth.key", CapabilityKeyFile: t.TempDir() + "/cap.key",
		StreamKeyFile: t.TempDir() + "/stream.key", StreamKeyID: "test", CapabilityKeyID: "test",
		SecretsKeyVersion: 1, SecretsKEKs: map[int][]byte{1: make([]byte, 32)}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, err := auth.New(t.Context(), f.d, c, logger)
	require.NoError(t, err)
	t.Cleanup(a.Close)
	token, err := a.Signer.DevToken(auth.Principal{ID: f.actor, Kind: "user"})
	require.NoError(t, err)
	return (&httpapi.Handler{DB: f.d, Auth: a, Config: c, Logger: logger, Objects: objects, Assets: &assets.Service{}}).Router(), token
}

func wp9Role(t *testing.T, f *runFixture, role, scope, id string) {
	t.Helper()
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		return tenancy.Bind(ctx, tx, auth.From(ctx), f.tenant, "user", f.actor, role, scope, id)
	})
}

func TestWP9ChronologicalPagination(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	f := newRunFixture(t, d, admin)
	wp9Role(t, f, "session.viewer", "session", f.session)
	base := time.Date(2026, 10, 19, 12, 0, 0, 123456000, time.UTC)
	runs := []string{ids.Derive(f.tenant, "z"), ids.Derive(f.tenant, "a"), ids.Derive(f.tenant, "b")}
	slices.Sort(runs)
	slices.Reverse(runs) // Chronological order deliberately opposes the UUID order.
	_, err := admin.Exec(t.Context(), "UPDATE runs SET created_at=$2 WHERE id=$1", f.run, base.Add(-time.Hour))
	require.NoError(t, err)
	for i, id := range runs {
		at := base
		if i == 2 {
			at = base.Add(time.Microsecond)
		}
		_, err = admin.Exec(t.Context(), `INSERT INTO runs(id,tenant_id,session_id,trigger,status,created_by,budget_millicredits,created_at)
 VALUES($1,$2,$3,'user_message','queued',$4,1000,$5)`, id, f.tenant, f.session, f.actor, at)
		require.NoError(t, err)
	}
	// The equal-timestamp pair is ordered by UUID rather than insertion order.
	runs[0], runs[1] = runs[1], runs[0]
	want := append([]string{f.run}, runs...)
	h, token := wp9Handler(t, f, runObjects{})
	for _, order := range []string{"", "asc", "desc"} {
		expected := slices.Clone(want)
		if order == "desc" {
			slices.Reverse(expected)
		}
		cursor := ""
		for _, id := range expected {
			path := "/v1/sessions/" + f.session + "/runs?limit=1&order=" + order + "&cursor=" + url.QueryEscape(cursor)
			status, page, raw := publicRequest(t, h, token, f.tenant, "GET", path, "")
			require.Equal(t, 200, status, raw)
			items := page["items"].([]any)
			require.Len(t, items, 1)
			item := items[0].(map[string]any)
			require.Equal(t, id, item["id"])
			require.NotEmpty(t, item["created_at"])
			cursor = page["next_cursor"].(string)
		}
		require.Empty(t, cursor)
	}
	for _, query := range []string{"order=sideways", "cursor=bad"} {
		status, _, _ := publicRequest(t, h, token, f.tenant, "GET", "/v1/sessions/"+f.session+"/runs?"+query, "")
		require.Contains(t, []int{400, 422}, status)
	}
	wp9Role(t, f, "ws.viewer", "workspace", f.workspace)
	sessionIDs := []string{ids.New(), ids.New(), ids.New()}
	for i, id := range sessionIDs {
		_, err = admin.Exec(t.Context(), `INSERT INTO sessions(id,tenant_id,workspace_id,owner_id,title,entry_kind,entry_ref,
 message_policy,status,last_activity_at) VALUES($1,$2,$3,$4,'Recent','agent','test/ref','queue','idle',$5)`,
			id, f.tenant, f.workspace, f.actor, base.Add(time.Duration(i)*time.Minute))
		require.NoError(t, err)
	}
	_, err = admin.Exec(t.Context(), "UPDATE sessions SET last_activity_at=$2 WHERE id=$1", f.session, base.Add(-time.Hour))
	require.NoError(t, err)
	expected := []string{sessionIDs[2], sessionIDs[1], sessionIDs[0], f.session}
	cursor := ""
	for _, id := range expected {
		path := "/v1/sessions?workspace_id=" + f.workspace + "&order=recent&limit=1&cursor=" + url.QueryEscape(cursor)
		status, page, raw := publicRequest(t, h, token, f.tenant, "GET", path, "")
		require.Equal(t, 200, status, raw)
		require.Equal(t, id, page["items"].([]any)[0].(map[string]any)["id"])
		cursor = page["next_cursor"].(string)
	}
	require.Empty(t, cursor)
}

func wp9Asset(t *testing.T, admin *pgx.Conn, f *runFixture, kind, name, visibility, owner string) string {
	t.Helper()
	id := ids.New()
	_, err := admin.Exec(t.Context(), `INSERT INTO assets(id,tenant_id,publisher,kind,name,visibility,owner_id,status)
 VALUES($1,$2,'publisher',$3,$4,$5,$6,'active')`, id, f.tenant, kind, name, visibility, owner)
	require.NoError(t, err)
	return id
}

func wp9Version(t *testing.T, admin *pgx.Conn, f *runFixture, asset, version, state, manifest string) string {
	t.Helper()
	id := ids.New()
	_, err := admin.Exec(t.Context(), `INSERT INTO asset_versions(id,tenant_id,asset_id,version,manifest,content_digest,
 blob_index_ref,permissions,compat,state,uploaded_by,permissions_jcs,identity_jcs)
 VALUES($1,$2,$3,$4,$5,'sha256:fixture','fixture','{}','{}',$6,$7,'{}','{}')`,
		id, f.tenant, asset, version, []byte(manifest), state, f.actor)
	require.NoError(t, err)
	return id
}

func TestWP9AssetFiltersAndMetadata(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	f := newRunFixture(t, d, admin)
	other := newRunFixture(t, d, admin)
	agent := wp9Asset(t, admin, f, "agent", "same-name", "tenant", f.actor)
	team := wp9Asset(t, admin, f, "team", "same-name", "tenant", f.actor)
	hidden := wp9Asset(t, admin, f, "skill", "same-name", "private", other.actor)
	wp9Asset(t, admin, f, "mcp_server", "same-name-extra", "tenant", f.actor)
	wp9Asset(t, admin, other, "agent", "same-name", "tenant", other.actor)
	h, token := wp9Handler(t, f, runObjects{})
	status, page, raw := publicRequest(t, h, token, f.tenant, "GET", "/v1/assets?ref=publisher/same-name", "")
	require.Equal(t, 200, status, raw)
	require.Len(t, page["items"], 2, "same reference across visible kinds, but no substring or invisible match")
	for _, kind := range []string{"agent", "team"} {
		status, page, raw = publicRequest(t, h, token, f.tenant, "GET", "/v1/assets?ref=publisher/same-name&kind="+kind, "")
		require.Equal(t, 200, status, raw)
		require.Len(t, page["items"], 1)
		require.Equal(t, kind, page["items"].([]any)[0].(map[string]any)["kind"])
	}
	for _, ref := range []string{"Publisher/same-name", "publisher/Same-name", "publisher/same"} {
		status, page, raw = publicRequest(t, h, token, f.tenant, "GET", "/v1/assets?ref="+ref, "")
		require.Equal(t, 200, status, raw)
		require.Empty(t, page["items"])
	}
	status, page, raw = publicRequest(t, h, token, f.tenant, "GET", "/v1/assets?owner=me&kind=skill", "")
	require.Equal(t, 200, status, raw)
	require.Empty(t, page["items"])
	_, err := admin.Exec(t.Context(), "UPDATE assets SET visibility='tenant' WHERE id=$1", hidden)
	require.NoError(t, err)
	status, page, raw = publicRequest(t, h, token, f.tenant, "GET", "/v1/assets?owner=me&ref=publisher/same-name", "")
	require.Equal(t, 200, status, raw)
	require.Len(t, page["items"], 2, "visible non-owned asset is still excluded")
	for _, id := range []string{agent, team} {
		status, value, raw := publicRequest(t, h, token, f.tenant, "GET", "/v1/assets/"+id, "")
		require.Equal(t, 200, status, raw)
		require.Contains(t, value, "display_name")
		require.Nil(t, value["display_name"])
		require.Nil(t, value["description"])
	}
	stable := wp9Version(t, admin, f, agent, "1.0.0", "published",
		`{"agent":{"displayName":{"en":"English expert","zh-CN":"Regional expert","zh":"Stable expert"},"description":"Stable description"}}`)
	beta := wp9Version(t, admin, f, agent, "2.0.0", "published",
		`{"agent":{"displayName":{"en":"Beta expert"},"description":"Beta description"}}`)
	teamVersion := wp9Version(t, admin, f, team, "1.0.0", "published",
		`{"team":{"description":"Team description"},"agent":{"description":"Leader"}}`)
	for _, channel := range []struct{ asset, version, name string }{
		{asset: agent, version: stable, name: "stable"},
		{asset: agent, version: beta, name: "beta"},
		{asset: team, version: teamVersion, name: "stable"},
	} {
		_, err = admin.Exec(t.Context(), `INSERT INTO asset_channels(tenant_id,asset_id,channel,version_id,updated_by)
 VALUES($1,$2,$3,$4,$5)`, f.tenant, channel.asset, channel.name, channel.version, f.actor)
		require.NoError(t, err)
	}
	status, page, raw = publicRequest(t, h, token, f.tenant, "GET", "/v1/assets?ref=publisher/same-name", "")
	require.Equal(t, 200, status, raw)
	for _, item := range page["items"].([]any) {
		value := item.(map[string]any)
		if value["id"] == agent {
			assert.Equal(t, "Stable expert", value["display_name"])
			require.Equal(t, "Stable description", value["description"])
		}
		if value["id"] == team {
			require.Nil(t, value["display_name"])
			require.Equal(t, "Team description", value["description"])
		}
	}
	_, err = admin.Exec(t.Context(), "UPDATE asset_channels SET version_id=$3 WHERE tenant_id=$1 AND asset_id=$2 AND channel='stable'",
		f.tenant, agent, beta)
	require.NoError(t, err)
	status, value, raw := publicRequest(t, h, token, f.tenant, "GET", "/v1/assets/"+agent, "")
	require.Equal(t, 200, status, raw)
	assert.Equal(t, "Beta expert", value["display_name"], "metadata follows current stable channel, not latest version")
	require.Equal(t, "Beta description", value["description"])
	for i, locale := range []struct {
		names, want string
	}{
		{names: `{"en":"English","zh-CN":"Regional"}`, want: "Regional"},
		{names: `{"fr":"Français"}`, want: "Français"},
		{names: `{"ja":"Japanese","de":"Deutsch","fr":"Français"}`, want: "Deutsch"},
	} {
		t.Run(locale.want, func(t *testing.T) {
			version := wp9Version(t, admin, f, agent, fmt.Sprintf("3.0.%d", i), "published",
				`{"agent":{"displayName":`+locale.names+`}}`)
			_, err := admin.Exec(t.Context(),
				`UPDATE asset_channels SET version_id=$3 WHERE tenant_id=$1 AND asset_id=$2 AND channel='stable'`, f.tenant, agent, version)
			require.NoError(t, err)
			status, value, raw := publicRequest(t, h, token, f.tenant, "GET", "/v1/assets/"+agent, "")
			require.Equal(t, 200, status, raw)
			require.Equal(t, locale.want, value["display_name"])
		})
	}
}

func TestWP9Dependents(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	f := newRunFixture(t, d, admin)
	other := newRunFixture(t, d, admin)
	target := wp9Asset(t, admin, f, "skill", "dependency", "tenant", f.actor)
	reference := "publisher/dependency"
	first := wp9Asset(t, admin, f, "agent", "first", "tenant", f.actor)
	second := wp9Asset(t, admin, f, "agent", "second", "tenant", f.actor)
	team := wp9Asset(t, admin, f, "team", "team", "tenant", f.actor)
	hidden := wp9Asset(t, admin, f, "agent", "hidden", "private", other.actor)
	foreign := wp9Asset(t, admin, other, "agent", "foreign", "tenant", other.actor)
	for _, spec := range []struct {
		f                           *runFixture
		asset, version, state, kind string
	}{
		{f: f, asset: first, version: "1.0.0", state: "draft", kind: "skill"},
		{f: f, asset: first, version: "1.0.1", state: "published", kind: "skill"},
		{f: f, asset: first, version: "1.0.2", state: "rejected", kind: "skill"},
		{f: f, asset: first, version: "1.0.3", state: "yanked", kind: "skill"},
		{f: f, asset: second, version: "1.0.0", state: "draft", kind: "skill"},
		{f: f, asset: team, version: "1.0.0", state: "draft", kind: "skill"},
		{f: f, asset: hidden, version: "1.0.0", state: "draft", kind: "skill"},
		{f: other, asset: foreign, version: "1.0.0", state: "draft", kind: "skill"},
		{f: f, asset: second, version: "1.0.1", state: "published", kind: "mcp_server"},
	} {
		v := wp9Version(t, admin, spec.f, spec.asset, spec.version, spec.state, `{}`)
		_, err := admin.Exec(t.Context(), `INSERT INTO asset_dependencies(tenant_id,version_id,dep_kind,dep_ref,"constraint")
 VALUES($1,$2,$3,$4,'*')`, spec.f.tenant, v, spec.kind, reference)
		require.NoError(t, err)
	}
	h, token := wp9Handler(t, f, runObjects{})
	cursor := ""
	for i, id := range []string{first, second} {
		status, page, raw := publicRequest(t, h, token, f.tenant, "GET", "/v1/assets/"+target+"/dependents?kind=agent&limit=1&cursor="+cursor, "")
		require.Equal(t, 200, status, raw)
		items := page["items"].([]any)
		require.Len(t, items, 1)
		item := items[0].(map[string]any)
		require.Equal(t, id, item["asset"].(map[string]any)["id"])
		require.Len(t, item["versions"], 2-i, "multiple versions aggregate, rejected/yanked and other dep kinds excluded")
		cursor = page["next_cursor"].(string)
	}
	require.Empty(t, cursor)
	status, page, raw := publicRequest(t, h, token, f.tenant, "GET", "/v1/assets/"+target+"/dependents", "")
	require.Equal(t, 200, status, raw)
	require.Len(t, page["items"], 3, "hidden and cross-tenant dependents are omitted")
	status, page, raw = publicRequest(t, h, token, f.tenant, "GET", "/v1/assets/"+target+"/dependents?kind=team", "")
	require.Equal(t, 200, status, raw)
	require.Len(t, page["items"], 1)
	for _, query := range []string{"kind=skill", "limit=51", "cursor=bad"} {
		status, _, _ := publicRequest(t, h, token, f.tenant, "GET", "/v1/assets/"+target+"/dependents?"+query, "")
		require.Contains(t, []int{400, 422}, status)
	}
	foreignTarget := wp9Asset(t, admin, other, "skill", "dependency", "tenant", other.actor)
	for _, id := range []string{hidden, foreignTarget} {
		status, _, raw = publicRequest(t, h, token, f.tenant, "GET", "/v1/assets/"+id+"/dependents", "")
		require.Equal(t, 404, status, raw)
	}
	// A selective reverse lookup needs realistic cardinality; tiny fixtures can
	// legitimately prefer the covering primary key instead of the new index.
	_, err := admin.Exec(t.Context(), `INSERT INTO asset_versions(id,tenant_id,asset_id,version,manifest,content_digest,
 blob_index_ref,permissions,compat,state,uploaded_by,permissions_jcs,identity_jcs)
 SELECT md5($1::text||n)::uuid,$1::uuid,$2::uuid,'noise-'||n,'{}','fixture','fixture','{}','{}','draft',$3,'{}','{}'
 FROM generate_series(1,5000) n`, f.tenant, first, f.actor)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), `INSERT INTO asset_dependencies(tenant_id,version_id,dep_kind,dep_ref,"constraint")
 SELECT $1::uuid,md5($1::text||n)::uuid,'skill','noise/'||n,'*' FROM generate_series(1,5000) n`, f.tenant)
	require.NoError(t, err)
	_, err = admin.Exec(t.Context(), "ANALYZE asset_versions; ANALYZE asset_dependencies")
	require.NoError(t, err)
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "SET LOCAL enable_seqscan=off")
		require.NoError(t, err)
		var plan string
		require.NoError(t, tx.QueryRow(ctx, `EXPLAIN (FORMAT JSON) SELECT v.asset_id
 FROM asset_dependencies d JOIN asset_versions v ON (v.tenant_id,v.id)=(d.tenant_id,d.version_id)
 WHERE d.tenant_id=$1 AND d.dep_kind='skill' AND d.dep_ref=$2 AND v.state NOT IN ('rejected','yanked')`,
			f.tenant, reference).Scan(&plan))
		require.Contains(t, plan, "asset_dependencies_tenant_id_dep_kind_dep_ref_idx")
		return nil
	})
}

func TestWP9AdminMetadata(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	f := newRunFixture(t, d, admin)
	other := newRunFixture(t, d, admin)
	for _, fixture := range []*runFixture{f, other} {
		for i := range 2 {
			secret := ids.New()
			_, err := admin.Exec(t.Context(), `INSERT INTO secrets(id,tenant_id,name,scope,ciphertext,key_version,created_by)
 VALUES($1,$2,$3,'tenant','private-ciphertext-canary',1,$4)`, secret, fixture.tenant, fmt.Sprintf("secret-%d", i), fixture.actor)
			require.NoError(t, err)
			_, err = admin.Exec(t.Context(), `INSERT INTO model_profiles(id,tenant_id,name,provider,base_url,model,secret_id,params)
 VALUES($1,$2,$3,'provider','http://private-base-canary','model',$4,'{"private-param-canary":true}')`,
				ids.New(), fixture.tenant, fmt.Sprintf("model-%d", i), secret)
			require.NoError(t, err)
			_, err = admin.Exec(t.Context(), `INSERT INTO quota_policies(id,tenant_id,scope_type,scope_id,metric,"limit",period,action)
 VALUES($1,$2,'tenant',$2,'tokens',9007199254740993,'day','block')`, ids.New(), fixture.tenant)
			require.NoError(t, err)
		}
	}
	h, token := wp9Handler(t, f, runObjects{})
	for _, role := range []string{"tenant.member", "ws.admin", "tenant.admin", "tenant.owner"} {
		scope, id := "tenant", f.tenant
		if strings.HasPrefix(role, "ws.") {
			scope, id = "workspace", f.workspace
		}
		wp9Role(t, f, role, scope, id)
		for _, name := range []string{"models", "secrets", "quotas"} {
			status, page, raw := publicRequest(t, h, token, f.tenant, "GET", "/v1/admin/"+name+"?limit=1", "")
			if role == "tenant.member" || role == "ws.admin" {
				require.Equal(t, 403, status, raw)
				continue
			}
			require.Equal(t, 200, status, raw)
			require.Len(t, page["items"], 1)
			cursor := page["next_cursor"].(string)
			require.NotEmpty(t, cursor)
			for _, canary := range []string{"ciphertext", "private-ciphertext-canary", "base_url", "private-base-canary",
				"params", "private-param-canary", "secret_id", "key_version", "length", "updated_at"} {
				require.NotContains(t, raw, canary)
			}
			item := page["items"].([]any)[0].(map[string]any)
			expected := map[string][]string{
				"models":  {"id", "name", "provider", "model", "is_default", "created_at"},
				"secrets": {"id", "name", "scope", "created_at"},
				"quotas":  {"id", "scope_type", "scope_id", "metric", "limit", "period", "action", "created_at"},
			}[name]
			require.Len(t, item, len(expected))
			for _, key := range expected {
				require.Contains(t, item, key)
			}
			if name == "quotas" {
				require.Equal(t, "9007199254740993", item["limit"])
			}
			status, page, raw = publicRequest(t, h, token, f.tenant, "GET", "/v1/admin/"+name+"?limit=1&cursor="+cursor, "")
			require.Equal(t, 200, status, raw)
			require.Len(t, page["items"], 1, "no rows from another tenant")
			require.Empty(t, page["next_cursor"])
		}
	}
}

func TestWP9ArtifactContent(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	f := newRunFixture(t, d, admin)
	other := newRunFixture(t, d, admin)
	artifact := ids.New()
	_, err := admin.Exec(t.Context(), `INSERT INTO artifacts(id,tenant_id,session_id,run_id,path,mime,size,digest,object_ref,status)
 VALUES($1,$2,$3,$4,'outputs/file.txt','text/plain',999,'sha256:test',$5,'final')`,
		artifact, f.tenant, f.session, f.run, "t/"+f.tenant+"/artifacts/file.txt")
	require.NoError(t, err)
	h, token := wp9Handler(t, f, runObjects{})
	request := func(tenant string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/v1/artifacts/"+artifact+"/content?inline=1", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Orbit-Tenant", tenant)
		out := httptest.NewRecorder()
		h.ServeHTTP(out, r)
		return out
	}
	require.Equal(t, 404, request(f.tenant).Code, "membership without session access does not authorize artifacts")
	wp9Role(t, f, "session.viewer", "session", f.session)
	out := request(f.tenant)
	require.Equal(t, 200, out.Code, out.Body.String())
	require.Equal(t, "artifact bytes", out.Body.String())
	require.Equal(t, "inline; filename*=UTF-8''file.txt", out.Header().Get("Content-Disposition"))
	require.Equal(t, "14", out.Header().Get("Content-Length"))
	require.Equal(t, "nosniff", out.Header().Get("X-Content-Type-Options"))
	foreignHandler, foreignToken := wp9Handler(t, other, runObjects{})
	r := httptest.NewRequest("GET", "/v1/artifacts/"+artifact+"/content", nil)
	r.Header.Set("Authorization", "Bearer "+foreignToken)
	r.Header.Set("X-Orbit-Tenant", other.tenant)
	out = httptest.NewRecorder()
	foreignHandler.ServeHTTP(out, r)
	require.Equal(t, 404, out.Code, out.Body.String())
	// The original API-client redirect endpoint remains intact.
	r = httptest.NewRequest("GET", "/v1/artifacts/"+artifact, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-Orbit-Tenant", f.tenant)
	out = httptest.NewRecorder()
	h.ServeHTTP(out, r)
	require.Equal(t, 302, out.Code)
	require.Equal(t, "http://objects.invalid/file", out.Header().Get("Location"))
}

type wp9StreamObjects struct {
	runObjects
	open func(context.Context, string, string) (io.ReadCloser, int64, error)
}

func (s wp9StreamObjects) OpenObject(ctx context.Context, tenant, key string) (io.ReadCloser, int64, error) {
	return s.open(ctx, tenant, key)
}

type wp9StreamingBody struct {
	remaining int64
	closed    bool
	maxRead   int
}

func (b *wp9StreamingBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		return 0, io.EOF
	}
	b.maxRead = max(b.maxRead, len(p))
	n := int(min(int64(len(p)), b.remaining))
	clear(p[:n])
	b.remaining -= int64(n)
	return n, nil
}
func (b *wp9StreamingBody) Close() error {
	b.closed = true
	return nil
}

type wp9DiscardWriter struct {
	header http.Header
	status int
	size   int64
}

func (w *wp9DiscardWriter) Header() http.Header    { return w.header }
func (w *wp9DiscardWriter) WriteHeader(status int) { w.status = status }
func (w *wp9DiscardWriter) Write(p []byte) (int, error) {
	w.size += int64(len(p))
	return len(p), nil
}

func TestWP9ArtifactStreamsAfterTransaction(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	f := newRunFixture(t, d, admin)
	wp9Role(t, f, "session.viewer", "session", f.session)
	artifact := ids.New()
	const size int64 = 51 << 20
	_, err := admin.Exec(t.Context(), `INSERT INTO artifacts(id,tenant_id,session_id,run_id,path,mime,size,digest,object_ref,status)
 VALUES($1,$2,$3,$4,'large.bin','application/octet-stream',$5,'sha256:test',$6,'final')`,
		artifact, f.tenant, f.session, f.run, size, "t/"+f.tenant+"/artifacts/large.bin")
	require.NoError(t, err)
	body := &wp9StreamingBody{remaining: size}
	opened := false
	objects := wp9StreamObjects{open: func(ctx context.Context, tenant, key string) (io.ReadCloser, int64, error) {
		opened = true
		require.Equal(t, f.tenant, tenant)
		require.Equal(t, "t/"+f.tenant+"/artifacts/large.bin", key)
		// An authorization SELECT's AccessShareLock would still exist while its
		// transaction is open, so ACCESS EXCLUSIVE NOWAIT proves it has closed.
		tx, err := admin.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "LOCK TABLE artifacts IN ACCESS EXCLUSIVE MODE NOWAIT")
		require.NoError(t, err, "artifact stream must open outside the authorization transaction")
		require.NoError(t, tx.Rollback(ctx))
		return body, size, nil
	}}
	h, token := wp9Handler(t, f, objects)
	r := httptest.NewRequest("GET", "/v1/artifacts/"+artifact+"/content", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("X-Orbit-Tenant", f.tenant)
	out := &wp9DiscardWriter{header: make(http.Header)}
	h.ServeHTTP(out, r)
	require.True(t, opened)
	require.Equal(t, 200, out.status)
	require.Equal(t, size, out.size, "stream has no 50 MiB upload or 20 MiB asset-file cap")
	require.LessOrEqual(t, body.maxRead, 64<<10, "all HTTP wrappers must preserve bounded streaming")
	require.True(t, body.closed)
	t.Run("HEAD", func(t *testing.T) {
		body := &wp9StreamingBody{remaining: size}
		objects.open = func(context.Context, string, string) (io.ReadCloser, int64, error) { return body, size, nil }
		h, token := wp9Handler(t, f, objects)
		r := httptest.NewRequest("HEAD", "/v1/artifacts/"+artifact+"/content", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Orbit-Tenant", f.tenant)
		out := &wp9DiscardWriter{header: make(http.Header)}
		h.ServeHTTP(out, r)
		require.Equal(t, 200, out.status)
		require.Equal(t, fmt.Sprint(size), out.header.Get("Content-Length"))
		require.Zero(t, out.size, "HEAD must not write a body")
		require.Zero(t, body.maxRead, "HEAD must not read the object stream")
		require.True(t, body.closed)
	})
}

func TestWP9ArtifactMissingObject(t *testing.T) {
	d, admin, _ := rollupDatabase(t)
	f := newRunFixture(t, d, admin)
	wp9Role(t, f, "session.viewer", "session", f.session)
	artifact := ids.New()
	_, err := admin.Exec(t.Context(), `INSERT INTO artifacts(id,tenant_id,session_id,run_id,path,mime,size,digest,object_ref,status)
 VALUES($1,$2,$3,$4,'missing.txt','text/plain',5,'sha256:test',$5,'final')`,
		artifact, f.tenant, f.session, f.run, "t/"+f.tenant+"/artifacts/missing")
	require.NoError(t, err)
	objects := wp9StreamObjects{open: func(context.Context, string, string) (io.ReadCloser, int64, error) {
		return nil, 0, fs.ErrNotExist
	}}
	h, token := wp9Handler(t, f, objects)
	status, value, raw := publicRequest(t, h, token, f.tenant, "GET", "/v1/artifacts/"+artifact+"/content", "")
	require.Equal(t, 404, status, raw)
	require.Equal(t, "NOT_FOUND", value["error"].(map[string]any)["code"])
}
