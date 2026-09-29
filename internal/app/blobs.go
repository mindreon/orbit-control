package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/mindreon/orbit-control/internal/store"
)

// ErrDigestMismatch is HTTP 422: X-Content-Digest is not the sha256 of the body.
var ErrDigestMismatch = errors.New("content digest mismatch")

// ErrTooLarge is HTTP 413: the body exceeded ORBIT_ARTIFACT_MAX_BYTES.
var ErrTooLarge = errors.New("artifact too large")

var digestHex = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

// SaveArtifactBlob streams a blob onto {ArtifactDir}/{tenant}/{sha256}.
// tenant comes from the room row. digestHeader is only compared, never used
// as a path. A body over the limit is rejected while streaming and leaves
// no temp file (FM-63).
func (a *App) SaveArtifactBlob(taskID, tenantHint, digestHeader string, body io.Reader) (string, error) {
	if !digestHex.MatchString(strings.TrimSpace(digestHeader)) {
		return "", invalidf("X-Content-Digest must be 64 hex characters")
	}
	es, ok := a.Repo.(store.EventStore)
	if a.ArtifactDir == "" {
		return "", invalidf("artifact storage is not configured")
	}
	tenantID := strings.TrimSpace(tenantHint)
	if tenantID == "" {
		if !ok {
			return "", invalidf("artifact tenant is required")
		}
		var err error
		tenantID, err = es.FindRoomTenant(context.Background(), taskID)
		if err != nil {
			return "", err
		}
	}
	if tenantID == "" || strings.Contains(tenantID, "/") || strings.Contains(tenantID, `\`) || strings.Contains(tenantID, "..") {
		return "", ErrNotFound
	}
	tenantDir := filepath.Join(a.ArtifactDir, tenantID)
	if err := os.MkdirAll(tenantDir, 0o755); err != nil {
		return "", err
	}
	tmp, err := os.CreateTemp(tenantDir, ".blob-*")
	if err != nil {
		return "", err
	}
	tmpName := tmp.Name()
	keep := false
	defer func() {
		_ = tmp.Close()
		if !keep {
			_ = os.Remove(tmpName)
		}
	}()
	hash := sha256.New()
	buf := make([]byte, 32*1024)
	var n int64
	for {
		nr, rerr := body.Read(buf)
		if nr > 0 {
			n += int64(nr)
			if n > a.ArtifactMaxBytes {
				return "", ErrTooLarge
			}
			if _, werr := hash.Write(buf[:nr]); werr != nil {
				return "", werr
			}
			if _, werr := tmp.Write(buf[:nr]); werr != nil {
				return "", werr
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return "", rerr
		}
	}
	if err := tmp.Sync(); err != nil {
		return "", err
	}
	sum := hex.EncodeToString(hash.Sum(nil))
	if !strings.EqualFold(sum, strings.TrimSpace(digestHeader)) {
		return "", ErrDigestMismatch
	}
	dest := filepath.Join(tenantDir, sum)
	if _, err := os.Stat(dest); err == nil {
		return tenantID + "/" + sum, nil
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmpName, dest); err != nil {
		if _, statErr := os.Stat(dest); statErr == nil {
			return tenantID + "/" + sum, nil
		}
		return "", err
	}
	keep = true
	if dir, err := os.Open(tenantDir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return tenantID + "/" + sum, nil
}
