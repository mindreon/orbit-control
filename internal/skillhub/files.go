package skillhub

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"path"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/mindreon/orbit-control/internal/store"
)

const (
	maxZip         = 4 << 20
	maxUnpacked    = 4 << 20
	maxTextFiles   = 40
	maxFileBytes   = 48 << 10
	maxStoredRunes = 200_000
)

// TextFiles downloads one public skill package and keeps text files for
// display. It does not install the skill or run anything inside the package.
// handle may be empty; the download name is then the slug alone.
func (c *Client) TextFiles(ctx context.Context, handle, slug string) ([]store.SkillFile, error) {
	name := slug
	if handle != "" {
		name = "@" + handle + "/" + slug
	}
	body, err := c.get(ctx, "/api/v1/download?slug="+url.QueryEscape(name))
	if err != nil {
		return nil, err
	}
	if len(body) > maxZip {
		return nil, errors.New("skillhub package too large")
	}
	return textFilesFromZip(body)
}

func textFilesFromZip(body []byte) ([]store.SkillFile, error) {
	reader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return nil, errors.New("skillhub package")
	}
	var unpacked uint64
	for _, file := range reader.File {
		unpacked += file.UncompressedSize64
		if unpacked > maxUnpacked {
			return nil, errors.New("skillhub package too large")
		}
	}
	out := make([]store.SkillFile, 0, 8)
	var runes int
	// The overview document is small. Take it before the rune budget is spent
	// on later files in zip order.
	ordered := append([]*zip.File(nil), reader.File...)
	sort.SliceStable(ordered, func(i, j int) bool {
		return overviewRank(ordered[i].Name) < overviewRank(ordered[j].Name)
	})
	for _, file := range ordered {
		if file.FileInfo().IsDir() {
			continue
		}
		name, ok := safeZipPath(file.Name)
		if !ok || !textExt(name) || secretName(name) {
			continue
		}
		if len(out) >= maxTextFiles {
			break
		}
		rc, err := file.Open()
		if err != nil {
			continue
		}
		raw, err := io.ReadAll(io.LimitReader(rc, maxFileBytes+1))
		rc.Close()
		if err != nil || len(raw) == 0 || bytes.IndexByte(raw, 0) >= 0 {
			continue
		}
		if len(raw) > maxFileBytes {
			raw = raw[:maxFileBytes]
		}
		text := strings.ToValidUTF8(string(raw), "")
		text = strings.ReplaceAll(text, "\x00", "")
		if !utf8.ValidString(text) || strings.TrimSpace(text) == "" {
			continue
		}
		runes += utf8.RuneCountInString(text)
		if runes > maxStoredRunes {
			break
		}
		out = append(out, store.SkillFile{Path: name, Body: text})
	}
	if out == nil {
		out = []store.SkillFile{}
	}
	return out, nil
}

func overviewRank(name string) int {
	name = strings.TrimPrefix(strings.ReplaceAll(name, "\\", "/"), "/")
	if strings.Contains(name, "/") {
		return 2
	}
	switch strings.ToLower(name) {
	case "skill.md":
		return 0
	case "readme.md", "skills.md":
		return 1
	default:
		return 2
	}
}

func safeZipPath(name string) (string, bool) {
	name = strings.ReplaceAll(name, "\\", "/")
	name = strings.TrimPrefix(name, "/")
	if name == "" || strings.Contains(name, "\x00") {
		return "", false
	}
	clean := path.Clean(name)
	if clean != name || clean == "." || strings.HasPrefix(clean, "../") || clean == ".." {
		return "", false
	}
	if len(clean) > 200 {
		return "", false
	}
	return clean, true
}

func textExt(name string) bool {
	ext := strings.ToLower(path.Ext(name))
	switch ext {
	case ".md", ".txt", ".json", ".yaml", ".yml", ".toml", ".py", ".js", ".ts", ".tsx", ".jsx", ".css", ".html", ".xml", ".csv":
		return true
	default:
		return false
	}
}

func secretName(name string) bool {
	base := strings.ToLower(path.Base(name))
	switch {
	case base == ".env" || strings.HasPrefix(base, ".env."):
		return true
	case strings.Contains(base, "secret"), strings.Contains(base, "credential"), strings.Contains(base, "password"):
		return true
	case strings.HasSuffix(base, ".pem"), base == "id_rsa", base == "id_ed25519":
		return true
	default:
		return false
	}
}
