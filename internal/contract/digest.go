package contract

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/mindreon/orbit-control/internal/platform/jcs"
)

// ObjectDigest hashes RFC 8785 JCS JSON.
func ObjectDigest(raw []byte) (string, error) {
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(canonical)), nil
}

// RunLockDigest hashes a wire lock after omitting only its top-level lock_id and digest.
// Use the wire encoder first so empty optional arrays are omitted consistently.
func RunLockDigest(raw []byte) (string, error) {
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return "", err
	}
	var value map[string]json.RawMessage
	if err := json.Unmarshal(canonical, &value); err != nil || value == nil {
		return "", fmt.Errorf("run lock must be an object")
	}
	delete(value, "lock_id")
	delete(value, "digest")
	content, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return ObjectDigest(content)
}

var fileDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// BundleDigest hashes sorted UTF-8 path, NUL, bare file hash, newline records.
func BundleDigest(files map[string]string) (string, error) {
	keys := make([]string, 0, len(files))
	for name, digest := range files {
		if name == "." || name == ".." || name == "" || strings.HasPrefix(name, "/") || path.Clean(name) != name || strings.HasPrefix(name, "../") || strings.ContainsAny(name, "\x00\n\\") || !fileDigestPattern.MatchString(digest) {
			return "", fmt.Errorf("invalid bundle path or digest")
		}
		keys = append(keys, name)
	}
	slices.Sort(keys)
	var preimage strings.Builder
	for _, name := range keys {
		preimage.WriteString(name + "\x00" + strings.TrimPrefix(files[name], "sha256:") + "\n")
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(preimage.String()))), nil
}

// MatchGlob uses Go path.Match, including its syntax error behavior.
func MatchGlob(pattern, value string) (bool, error) { return path.Match(pattern, value) }
