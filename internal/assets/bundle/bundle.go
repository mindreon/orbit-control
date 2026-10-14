// Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
// Exclusion and extraction rules adapted from jiuwenswarm; see NOTICE.
package bundle

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"maps"
	"path"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/mindreon/orbit-control/internal/contract"
)

const MaxArchive = 50 << 20

type Limits struct{ Files, FileBytes, TotalBytes, TextBytes, TextTotal, Depth, Ratio int }

var DefaultLimits = Limits{512, 20 << 20, 50 << 20, 256 << 10, 4 << 20, 32, 200}

type File struct {
	Path, Digest string
	Data         []byte
	Text         bool
}
type Bundle struct {
	Files    []File
	Digest   string
	Archive  []byte
	Excluded []string
}

func SafePath(raw string) (string, error) {
	if !utf8.ValidString(raw) || strings.ContainsAny(raw, "\x00\n\r\\:") || strings.HasPrefix(raw, "/") {
		return "", fmt.Errorf("unsafe archive path")
	}
	raw = strings.TrimSuffix(raw, "/")
	for part := range strings.SplitSeq(raw, "/") {
		if part == ".." || part == "." || part == "" {
			return "", fmt.Errorf("unsafe archive path")
		}
	}
	if raw == "" || path.Clean(raw) != raw {
		return "", fmt.Errorf("unsafe archive path")
	}
	return raw, nil
}
func excluded(name string) bool {
	for part := range strings.SplitSeq(name, "/") {
		if slices.Contains([]string{".git", ".archive", "__pycache__", ".pytest_cache", "node_modules", "out", ".venv"}, part) {
			return true
		}
	}
	base := path.Base(name)
	return slices.Contains([]string{"id_rsa", "id_ed25519", "credentials.json", "token.json", "tokens.json", ".DS_Store"}, base) || strings.HasSuffix(base, ".pyc") || strings.HasSuffix(base, ".key") || strings.HasSuffix(base, ".p12") || strings.HasSuffix(base, ".pfx") || strings.HasPrefix(base, ".env") && base != ".env.example" && base != ".env.template"
}

// Parse never writes archive paths to the filesystem. Limits include excluded files.
func Parse(raw []byte, limits Limits) (Bundle, error) {
	var b Bundle
	if len(raw) == 0 || len(raw) > MaxArchive {
		return b, fmt.Errorf("archive size limit exceeded")
	}
	seen := map[string]bool{}
	names := map[string]bool{}
	dirs := map[string]bool{}
	total, textTotal, count := 0, 0, 0
	add := func(name string, directory bool, size int64, r io.Reader) error {
		name, err := SafePath(name)
		if err != nil {
			return err
		}
		key := strings.ToLower(name)
		if seen[key] {
			return fmt.Errorf("duplicate or case-conflicting archive path")
		}
		seen[key] = true
		if strings.Count(name, "/")+1 > limits.Depth {
			return fmt.Errorf("path depth limit exceeded")
		}
		for parent := path.Dir(key); parent != "."; parent = path.Dir(parent) {
			if names[parent] {
				return fmt.Errorf("file/directory conflict")
			}
			dirs[parent] = true
		}
		if directory {
			if names[key] {
				return fmt.Errorf("file/directory conflict")
			}
			dirs[key] = true
			return nil
		}
		if dirs[key] {
			return fmt.Errorf("file/directory conflict")
		}
		names[key] = true
		count++
		if count > limits.Files || size < 0 || size > int64(limits.FileBytes) {
			return fmt.Errorf("archive file limit exceeded")
		}
		data, err := io.ReadAll(io.LimitReader(r, int64(limits.FileBytes)+1))
		if err != nil {
			return fmt.Errorf("invalid archive content")
		}
		total += len(data)
		if len(data) > limits.FileBytes || int64(len(data)) != size || total > limits.TotalBytes || total > len(raw)*limits.Ratio {
			return fmt.Errorf("archive expansion limit exceeded")
		}
		text := utf8.Valid(data) && !bytes.ContainsRune(data, 0)
		if text {
			textTotal += len(data)
			if len(data) > limits.TextBytes || textTotal > limits.TextTotal {
				return fmt.Errorf("text size limit exceeded")
			}
		}
		if excluded(name) {
			b.Excluded = append(b.Excluded, name)
			return nil
		}
		b.Files = append(b.Files, File{name, fmt.Sprintf("sha256:%x", sha256.Sum256(data)), data, text})
		return nil
	}
	if bytes.HasPrefix(raw, []byte("PK")) {
		z, err := zip.NewReader(bytes.NewReader(raw), int64(len(raw)))
		if err != nil {
			return b, fmt.Errorf("invalid zip")
		}
		if len(z.File) > limits.Files*limits.Depth {
			return b, fmt.Errorf("archive entry limit exceeded")
		}
		for _, f := range z.File {
			if !f.Mode().IsRegular() && !f.FileInfo().IsDir() {
				return b, fmt.Errorf("archive links and special files forbidden")
			}
			if f.UncompressedSize64 > uint64(limits.FileBytes) || f.UncompressedSize64 > max(1, f.CompressedSize64)*uint64(limits.Ratio) {
				return b, fmt.Errorf("archive expansion limit exceeded")
			}
			r, err := f.Open()
			if err != nil {
				return b, fmt.Errorf("invalid zip member")
			}
			err = add(f.Name, f.FileInfo().IsDir(), int64(f.UncompressedSize64), r)
			ce := r.Close()
			if err != nil {
				return b, err
			}
			if ce != nil {
				return b, ce
			}
		}
	} else {
		gz, err := gzip.NewReader(bytes.NewReader(raw))
		if err != nil {
			return b, fmt.Errorf("expected zip or tar.gz")
		}
		defer gz.Close()
		// Bound headers and padding as well as file data, including trailing members.
		budget := int64(limits.TotalBytes+limits.Files*limits.Depth*1024) + 1
		expanded := &io.LimitedReader{R: gz, N: budget}
		tr := tar.NewReader(expanded)
		entries := 0
		for {
			h, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return b, fmt.Errorf("invalid tar")
			}
			entries++
			if entries > limits.Files*limits.Depth {
				return b, fmt.Errorf("archive entry limit exceeded")
			}
			if h.Typeflag != tar.TypeReg && h.Typeflag != tar.TypeDir {
				return b, fmt.Errorf("archive links and special files forbidden")
			}
			if err = add(h.Name, h.Typeflag == tar.TypeDir, h.Size, tr); err != nil {
				return b, err
			}
		}
		if _, err = io.Copy(io.Discard, expanded); err != nil || expanded.N <= 0 || budget-expanded.N > int64(len(raw)*limits.Ratio) {
			return b, fmt.Errorf("archive expansion limit exceeded")
		}
	}
	if len(b.Files) == 0 {
		return b, fmt.Errorf("empty bundle")
	}
	slices.SortFunc(b.Files, func(a, c File) int { return strings.Compare(a.Path, c.Path) })
	hashes := map[string]string{}
	for _, f := range b.Files {
		hashes[f.Path] = f.Digest
	}
	var err error
	b.Digest, err = contract.BundleDigest(hashes)
	if err != nil {
		return b, err
	}
	files := map[string][]byte{}
	for _, f := range b.Files {
		files[f.Path] = f.Data
	}
	b.Archive, err = Pack(files)
	slices.Sort(b.Excluded)
	return b, err
}

// Pack produces a stable ZIP: sorted POSIX names, fixed timestamp/mode, no extra metadata.
func Pack(files map[string][]byte) ([]byte, error) {
	var out bytes.Buffer
	zw := zip.NewWriter(&out)
	for _, name := range slices.Sorted(maps.Keys(files)) {
		if _, err := SafePath(name); err != nil {
			return nil, err
		}
		h := &zip.FileHeader{Name: name, Method: zip.Deflate}
		h.SetMode(0644)
		w, err := zw.CreateHeader(h)
		if err != nil {
			return nil, err
		}
		if _, err = w.Write(files[name]); err != nil {
			return nil, err
		}
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
