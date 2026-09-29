//go:build e2e

package persistence

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// S-DB-12: artifact blobs land under the tenant directory, verified against their digest and size limit.
func TestSDB12ArtifactBlobs(t *testing.T) {
	const c = "S-DB-12"
	const tenant = "t-sdb12"
	dir := t.TempDir()
	// 4 MiB is large enough that a streamed reject must not retain the body,
	// and the 1 MiB RSS margin stays above allocator noise in CI.
	const limit = 4 << 20
	srv := startServer(t, serverOpts{tenant: tenant, maxConns: 4, artifactDir: dir, artifactMax: limit})

	body := []byte("orbit-blob")
	sum := sha256.Sum256(body)
	digest := hex.EncodeToString(sum[:])
	postBlob := func(base, tenantID, digest string, payload io.Reader) httpAct {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, base+"/internal/artifact-blobs?tenantId="+tenantID, payload)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Content-Digest", digest)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		raw, _ := io.ReadAll(res.Body)
		return httpAct{Status: res.StatusCode, Body: string(raw)}
	}
	ok := postBlob(srv.internal, tenant, digest, strings.NewReader(string(body)))
	again := postBlob(srv.internal, tenant, digest, strings.NewReader(string(body)))
	_, realErr := os.Stat(filepath.Join(dir, tenant, digest))
	record(t, caseInput{ID: "S-DB-12/a-tenant-directory", Contract: c, Kind: "e2e", FailureModes: []string{"FM-63"},
		Description: "the blob lands under the tenant directory and storing the same bytes again is a no-op",
		Steps:       []string{"POST /internal/artifact-blobs for the tenant", "stat the tenant path", "POST the same bytes again"},
		Request:     map[string]string{"tenant": tenant},
		Expected:    map[string]any{"status": 200, "fileUnderTenant": true, "secondStatus": 200},
		Actual:      map[string]any{"status": ok.Status, "body": ok.Body, "fileUnderTenant": realErr == nil, "secondStatus": again.Status},
		Pass:        ok.Status == 200 && strings.Contains(ok.Body, tenant+"/"+digest) && realErr == nil && again.Status == 200})

	noTenant := postBlob(srv.internal, "", digest, strings.NewReader(string(body)))
	traversal := postBlob(srv.internal, "..", digest, strings.NewReader(string(body)))
	record(t, caseInput{ID: "S-DB-12/b-tenant-required", Contract: c, Kind: "e2e", FailureModes: []string{"FM-63"},
		Description: "a missing tenant is 400 and a tenant that is a path is 404: nothing is written outside the tenant directory",
		Steps:       []string{"POST without tenantId", "POST with tenantId=.."},
		Request:     map[string]string{"tenantId": "<empty> | .."},
		Expected:    map[string]any{"missing": 400, "traversal": 404},
		Actual:      map[string]any{"missing": noTenant.Status, "traversal": traversal.Status},
		Pass:        noTenant.Status == 400 && traversal.Status == 404})

	mismatch := postBlob(srv.internal, tenant, strings.Repeat("ab", 32), strings.NewReader(string(body)))
	badName := postBlob(srv.internal, tenant, "../x", strings.NewReader(string(body)))
	left := blobTemps(dir)
	record(t, caseInput{ID: "S-DB-12/c-digest-mismatch", Contract: c, Kind: "e2e", FailureModes: []string{"FM-63"},
		Description: "X-Content-Digest mismatch → 422 and no file left; a header that is not 64 hex is 400 and does not affect the path",
		Steps:       []string{"POST with a different 64-hex digest", "POST with X-Content-Digest ../x", "list the artifact directory"},
		Request:     map[string]string{"badDigest": "../x"},
		Expected:    map[string]any{"mismatch": 422, "malformed": 400, "tempsLeft": 0},
		Actual:      map[string]any{"mismatch": mismatch.Status, "malformed": badName.Status, "tempsLeft": left},
		Pass:        mismatch.Status == 422 && badName.Status == 400 && left == 0})

	before := vmRSS()
	tooBig := postBlob(srv.internal, tenant, digest, &zeroReader{n: limit + 1})
	after := vmRSS()
	left = blobTemps(dir)
	rssOK := after-before < 1<<20
	record(t, caseInput{ID: "S-DB-12/d-size-limit", Contract: c, Kind: "e2e", FailureModes: []string{"FM-63"},
		Description: "body of limit+1 bytes → 413, RSS stays far below the body size, no temp file left",
		Steps:       []string{"POST a streaming body of ORBIT_ARTIFACT_MAX_BYTES+1", "read VmRSS", "list temp files"},
		Request:     map[string]any{"limit": limit, "body": limit + 1},
		Expected:    map[string]any{"status": 413, "rssFarBelowBody": true, "tempsLeft": 0},
		Actual:      map[string]any{"status": tooBig.Status, "rssFarBelowBody": rssOK, "tempsLeft": left},
		Pass:        tooBig.Status == 413 && rssOK && left == 0})

	pubBlob := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/internal/artifact-blobs?tenantId=" + tenant, Body: string(body)})
	pubEvents := sendTo(t, srv.base, httpReq{Method: "POST", Path: "/internal/events", Body: `{"schema":"orbit.event/3"}`})
	record(t, caseInput{ID: "S-DB-12/e-public-listener", Contract: c, Kind: "e2e", FailureModes: []string{"FM-63"},
		Description: "/internal/artifact-blobs and /internal/events on the public listener return 404",
		Steps:       []string{"POST both paths on the public listener"},
		Request:     map[string]string{"public": "/internal/artifact-blobs, /internal/events"},
		Expected:    map[string]any{"publicBlob": 404, "publicEvents": 404},
		Actual:      map[string]any{"publicBlob": pubBlob.Status, "publicEvents": pubEvents.Status},
		Pass:        pubBlob.Status == 404 && pubEvents.Status == 404})
}

func blobTemps(dir string) int {
	n := 0
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasPrefix(d.Name(), ".blob-") {
			n++
		}
		return nil
	})
	return n
}

func vmRSS() int64 {
	raw, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				n, _ := strconv.ParseInt(fields[1], 10, 64)
				return n * 1024
			}
		}
	}
	return 0
}

type zeroReader struct{ n int }

func (z *zeroReader) Read(p []byte) (int, error) {
	if z.n <= 0 {
		return 0, io.EOF
	}
	if len(p) > z.n {
		p = p[:z.n]
	}
	for i := range p {
		p[i] = 0
	}
	z.n -= len(p)
	return len(p), nil
}
