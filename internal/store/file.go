package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// FileStore is a tiny durable JSON/JSONL helper for W2.
// It intentionally avoids database drivers.
type FileStore struct {
	mu  sync.Mutex
	Dir string
}

func New(dir string) *FileStore {
	if dir == "" {
		dir = os.Getenv("ORBIT_DATA_DIR")
	}
	if dir == "" {
		dir = ".orbit-data"
	}
	_ = os.MkdirAll(dir, 0o750)
	return &FileStore{Dir: dir}
}

func (s *FileStore) path(parts ...string) string {
	return filepath.Join(append([]string{s.Dir}, parts...)...)
}

func (s *FileStore) WriteJSON(rel string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.path(rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s *FileStore) ReadJSON(rel string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(s.path(rel))
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, v)
}

// List returns the names of regular files directly under rel. A missing
// directory is empty.
func (s *FileStore) List(rel string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(s.path(rel))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Type().IsRegular() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

func (s *FileStore) AppendJSONL(rel string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.path(rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(append(raw, '\n'))
	return err
}

func (s *FileStore) ReadJSONL(rel string, decode func([]byte) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := os.ReadFile(s.path(rel))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	start := 0
	for i, b := range raw {
		if b != '\n' {
			continue
		}
		line := raw[start:i]
		start = i + 1
		if len(line) == 0 {
			continue
		}
		if err := decode(line); err != nil {
			return err
		}
	}
	if start < len(raw) {
		return decode(raw[start:])
	}
	return nil
}
