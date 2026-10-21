package market

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"
)

// Skill text lives on disk, the same tree main reads: <root>/<handle>/<slug>/.
// The snapshot only has the card. A missing root means the detail page has no file text.

const (
	maxSkillFileBytes = 256 << 10
	maxSkillFiles     = 500
	maxSkillBytes     = 8 << 20
	maxSkillPart      = 200
)

type SkillFile struct {
	Path string `json:"path"`
	Body string `json:"body"`
}

var skillsRoot string

func SetSkillsRoot(dir string) {
	skillsRoot = strings.TrimSpace(dir)
}

func SkillFiles(handle, slug string) ([]SkillFile, error) {
	if skillsRoot == "" || !skillPart(handle) || !skillPart(slug) {
		return []SkillFile{}, nil
	}
	root, err := filepath.Abs(skillsRoot)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(root, handle, slug)
	if !plainDir(filepath.Dir(dir)) || !plainDir(dir) {
		return []SkillFile{}, nil
	}
	var files []SkillFile
	seen, total := 0, 0
	err = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&fs.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return nil
		}
		if seen++; seen > maxSkillFiles {
			return errSkillTooLarge
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > maxSkillFileBytes {
			return nil
		}
		if total += int(info.Size()); total > maxSkillBytes {
			return errSkillTooLarge
		}
		body, ok, err := readSkillText(path, info)
		if err != nil || !ok {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		files = append(files, SkillFile{Path: filepath.ToSlash(rel), Body: body})
		return nil
	})
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, errSkillTooLarge) {
		return []SkillFile{}, nil
	}
	if err != nil {
		return nil, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	if files == nil {
		files = []SkillFile{}
	}
	return files, nil
}

var errSkillTooLarge = errors.New("skill is too large")

func skillPart(part string) bool {
	return part != "" && len(part) <= maxSkillPart && !strings.HasPrefix(part, ".") && !strings.ContainsAny(part, "/\\\x00")
}

func plainDir(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.IsDir() && info.Mode()&fs.ModeSymlink == 0
}

func readSkillText(path string, listed fs.FileInfo) (string, bool, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", false, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return "", false, err
	}
	if !os.SameFile(listed, opened) {
		return "", false, nil
	}
	buffer := make([]byte, 0, listed.Size())
	chunk := make([]byte, 32<<10)
	for {
		n, err := file.Read(chunk)
		buffer = append(buffer, chunk[:n]...)
		if len(buffer) > maxSkillFileBytes {
			return "", false, nil
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", false, err
		}
	}
	if !utf8.Valid(buffer) || strings.ContainsRune(string(buffer), 0) {
		return "", false, nil
	}
	return string(buffer), true, nil
}
