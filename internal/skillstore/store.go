// Package skillstore reads skills from a directory tree an operator keeps up to date with rsync or git:
//
//	<root>/<handle>/<slug>/SKILL.md, and the files beside it
//
// The skill id ("handle/slug") is the mapping, so there is no index to keep in step and nothing to import. A skill is
// text a stranger wrote, and the library is the operator's disk: nothing outside <root> is ever read, a symlink is never
// followed, and a skill that is too big or not text is not served.
package skillstore

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

var (
	ErrNotFound = errors.New("skill not found")
	ErrTooLarge = errors.New("skill is too large")
)

const (
	MaxFileBytes = 256 << 10 // a bigger file is left out of the skill
	MaxFiles     = 500       // a skill with more files is refused
	MaxBytes     = 8 << 20   // a skill with more text is refused
	maxPart      = 200
)

type File struct {
	Path string // slash-separated, relative to the skill's directory
	Body string
}

type Store struct{ root string }

// Open returns the library at root. An empty root means none is configured (a nil store). A root that is set but is not
// a directory is an error: a silent empty library would hide a mistake in the deployment.
func Open(root string) (*Store, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, nil
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, errors.New("skills directory is not a directory: " + root)
	}
	return &Store{root: abs}, nil
}

// Dir is where a skill lives, or false when the id is not one that names a place inside the library.
func (s *Store) Dir(id string) (string, bool) {
	parts := strings.Split(id, "/")
	if len(parts) != 2 {
		return "", false
	}
	for _, part := range parts {
		if !validPart(part) {
			return "", false
		}
	}
	return filepath.Join(s.root, parts[0], parts[1]), true
}

func validPart(part string) bool {
	return part != "" && len(part) <= maxPart && !strings.HasPrefix(part, ".") && !strings.ContainsAny(part, "\\\x00")
}

// Files reads a skill: its regular text files, sorted, none over MaxFileBytes. A skill that is replaced while it is read is
// found whole or not found, provided it is replaced atomically (written beside, then renamed into place, as rsync and
// `ms-crawl extract-skills` do). Editing a file in place can be read half written, which no reader can tell apart.
func (s *Store) Files(id string) ([]File, error) {
	dir, ok := s.Dir(id)
	if !ok || !plainDir(filepath.Dir(dir)) || !plainDir(dir) {
		return nil, ErrNotFound
	}
	var files []File
	seen, total := 0, 0
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.Type()&fs.ModeSymlink != 0 || !entry.Type().IsRegular() {
			return nil
		}
		if seen++; seen > MaxFiles {
			return ErrTooLarge
		}
		listed, err := entry.Info()
		if err != nil {
			return err
		}
		if listed.Size() > MaxFileBytes {
			return nil
		}
		if total += int(listed.Size()); total > MaxBytes {
			return ErrTooLarge
		}
		body, ok, err := readText(path, listed)
		if err != nil || !ok {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		files = append(files, File{Path: filepath.ToSlash(rel), Body: body})
		return nil
	})
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil, ErrNotFound
	case err != nil:
		return nil, err
	case len(files) == 0:
		return nil, ErrNotFound
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, nil
}

// plainDir is true for a directory that is not a symlink to one.
func plainDir(path string) bool {
	info, err := os.Lstat(path)
	return err == nil && info.IsDir()
}

// readText reads a file only if it is still the one that was listed (not swapped for a symlink) and is UTF-8 text.
func readText(path string, listed fs.FileInfo) (string, bool, error) {
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
		if len(buffer) > MaxFileBytes {
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
