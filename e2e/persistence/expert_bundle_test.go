//go:build e2e

package persistence

import (
	"net/http"
	"testing"
)

// The files of an expert version (ADR-0013, migration 00028) live in agent_profile_files under tenant RLS and are
// written with the profile.
//
// How it can go wrong, written down before the code:
//   - another tenant lists or reads the files of an expert it does not own;
//   - an update rewrites the files of the version before it;
//   - a rejected request leaves files of a profile that was never made;
//   - the files are lost, or differ, when read again.

const bundleTenant, bundleOther = "t-bundle-a", "t-bundle-b"

func TestExpertBundleFilesAreVersionedAndIsolated(t *testing.T) {
	a := startServer(t, serverOpts{tenant: bundleTenant, maxConns: 4})
	other := startServer(t, serverOpts{tenant: bundleOther, maxConns: 4})
	headers := userCSRF(expertUser)

	a.check(t, "BUNDLE/needs-agents-md", expertContract, "instructions cannot be blank", httpReq{
		Method: http.MethodPost, Path: "/v1/experts", Headers: headers, Body: `{"name":"x","soul":"kind"}`,
	}, httpExp{Status: http.StatusBadRequest, BodyIncludes: []string{"AGENTS_MD_REQUIRED"}})

	created := a.check(t, "BUNDLE/create", expertContract, "create an expert with a soul", httpReq{
		Method: http.MethodPost, Path: "/v1/experts", Headers: headers, Body: `{"name":"Soulful","instructions":"Work.","soul":"Kind."}`,
	}, httpExp{Status: http.StatusCreated, BodyIncludes: []string{`"soul":"Kind."`, `"mcp_unbound":[]`}})
	id := decodeField(t, created.Body, "expert_id")

	a.check(t, "BUNDLE/files", expertContract, "the version lists its files", httpReq{
		Method: http.MethodGet, Path: "/v1/experts/" + id + "/files", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"path":"AGENTS.md"`, `"path":"SOUL.md"`, `"path":"agent.json"`, `"sha256"`}})
	other.check(t, "BUNDLE/foreign-list", expertContract, "another tenant cannot list them", httpReq{
		Method: http.MethodGet, Path: "/v1/experts/" + id + "/files", Headers: headers,
	}, httpExp{Status: http.StatusNotFound})
	other.check(t, "BUNDLE/foreign-read", expertContract, "another tenant cannot read one", httpReq{
		Method: http.MethodGet, Path: "/v1/experts/" + id + "/files/SOUL.md", Headers: headers,
	}, httpExp{Status: http.StatusNotFound, BodyExcludes: []string{"Kind."}})

	a.check(t, "BUNDLE/update", expertContract, "an update that clears the soul is version 2", httpReq{
		Method: http.MethodPut, Path: "/v1/experts/" + id, Headers: headers, Body: `{"name":"Soulful","instructions":"Work more."}`,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"version":2`}})
	a.check(t, "BUNDLE/v1-files-stay", expertContract, "version 1 keeps its soul", httpReq{
		Method: http.MethodGet, Path: "/v1/experts/" + id + "/files/SOUL.md?version=1", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyIncludes: []string{`"content":"Kind."`}})
	a.check(t, "BUNDLE/v2-files", expertContract, "version 2 has none", httpReq{
		Method: http.MethodGet, Path: "/v1/experts/" + id + "/files?version=2", Headers: headers,
	}, httpExp{Status: http.StatusOK, BodyExcludes: []string{`"path":"SOUL.md"`}})
}
