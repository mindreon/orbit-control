package skillstore

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// A skill library is a directory tree an operator updates with rsync or git: <root>/<handle>/<slug>/SKILL.md and the
// files beside it. The skill id is the mapping, so there is no index to keep in step.
//
// How it can go wrong, written down before the code:
//   - an id built to leave the root (`..`, a separator inside a part, an absolute path, NUL, an empty part, one far longer
//     than any real id) reads a file that is not a skill;
//   - a symlink inside a skill, or a skill directory that is itself a symlink, is followed out of the root;
//   - a binary or non-UTF-8 file, or one over the per-file cap, is served as text;
//   - a skill with too many files, or too many bytes, is served whole and fills the worker's disk;
//   - a missing skill is told apart from a failure of the disk only by luck, or an empty directory counts as a skill;
//   - files come back in the order the disk gave them, or with the operating system's separators;
//   - a skill replaced (atomically, as rsync does) while it is read crashes the read instead of being a plain not-found or a
//     whole, consistent read.

func write(t *testing.T, root, rel, body string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

func open(t *testing.T, root string) *Store {
	t.Helper()
	store, err := Open(root)
	if err != nil || store == nil {
		t.Fatalf("open %s: %v", root, err)
	}
	return store
}

func TestAnIdThatCouldLeaveTheRootIsNotFound(t *testing.T) {
	root := t.TempDir()
	write(t, filepath.Dir(root), "outside/secret/SKILL.md", "not a skill of this library")
	write(t, root, "ok/skill/SKILL.md", "fine")
	store := open(t, root)
	for _, id := range []string{
		"", "ok", "/ok/skill", "ok//skill", "../outside/secret", "ok/../../outside/secret", "ok/..",
		"./ok/skill", "ok/skill/", "ok/skill/extra", "ok\\skill", "ok/sk\x00ill", strings.Repeat("h", 300) + "/s",
		"ok/" + strings.Repeat("s", 300),
	} {
		if _, err := store.Files(id); !errors.Is(err, ErrNotFound) {
			t.Errorf("id %q: want not found, got %v", id, err)
		}
	}
	if _, err := store.Files("ok/skill"); err != nil {
		t.Errorf("an ordinary id is found: %v", err)
	}
}

func TestASymlinkIsNeverFollowed(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	write(t, outside, "secret.txt", "outside the library")
	write(t, outside, "elsewhere/SKILL.md", "another skill, outside")
	write(t, root, "h/linked-file/SKILL.md", "ok")
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(root, "h/linked-file/leak.txt")); err != nil {
		t.Skip("symlinks are not available")
	}
	if err := os.Symlink(filepath.Join(outside, "elsewhere"), filepath.Join(root, "h/linked-dir")); err != nil {
		t.Fatal(err)
	}
	store := open(t, root)
	files, err := store.Files("h/linked-file")
	if err != nil {
		t.Fatal(err)
	}
	if got := paths(files); !reflect.DeepEqual(got, []string{"SKILL.md"}) {
		t.Errorf("a symlinked file is skipped, got %v", got)
	}
	if _, err := store.Files("h/linked-dir"); !errors.Is(err, ErrNotFound) {
		t.Errorf("a skill that is a symlink is not found, got %v", err)
	}
}

func TestOnlyUsableTextIsServed(t *testing.T) {
	root := t.TempDir()
	write(t, root, "h/s/SKILL.md", "---\nname: S\n---\nbody")
	write(t, root, "h/s/binary.bin", "a\x00b")
	write(t, root, "h/s/latin1.txt", "caf\xe9")
	write(t, root, "h/s/big.md", strings.Repeat("x", MaxFileBytes+1))
	write(t, root, "h/s/refs/b.md", "b")
	write(t, root, "h/s/refs/a.md", "a")
	files, err := open(t, root).Files("h/s")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"SKILL.md", "refs/a.md", "refs/b.md"}
	if got := paths(files); !reflect.DeepEqual(got, want) {
		t.Errorf("binary, non-UTF-8 and oversized files are left out, the rest sorted with slashes: got %v want %v", got, want)
	}
}

func TestASkillThatIsTooBigIsRefused(t *testing.T) {
	root := t.TempDir()
	for i := 0; i <= MaxFiles; i++ {
		write(t, root, "h/many/f"+strings.Repeat("0", 3)+string(rune('a'+i%26))+strings.Repeat("x", i/26)+".md", "x")
	}
	write(t, root, "h/many/SKILL.md", "x")
	chunk := strings.Repeat("y", MaxFileBytes)
	for i := 0; i <= MaxBytes/MaxFileBytes; i++ {
		write(t, root, "h/heavy/p"+string(rune('a'+i))+".md", chunk)
	}
	store := open(t, root)
	for _, id := range []string{"h/many", "h/heavy"} {
		if _, err := store.Files(id); !errors.Is(err, ErrTooLarge) {
			t.Errorf("%s: want too large, got %v", id, err)
		}
	}
}

func TestAMissingOrEmptySkillIsNotFound(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "h/empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, root, "h/file-not-dir", "x")
	store := open(t, root)
	for _, id := range []string{"h/missing", "nobody/none", "h/empty", "h/file-not-dir"} {
		if _, err := store.Files(id); !errors.Is(err, ErrNotFound) {
			t.Errorf("%s: want not found, got %v", id, err)
		}
	}
}

func TestOpenNeedsARealDirectory(t *testing.T) {
	if store, err := Open(""); store != nil || err != nil {
		t.Errorf("no directory configured is no store and no error, got %v %v", store, err)
	}
	if _, err := Open(filepath.Join(t.TempDir(), "absent")); err == nil {
		t.Error("a configured directory that does not exist is an error, not a silent empty library")
	}
	file := filepath.Join(t.TempDir(), "f")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(file); err == nil {
		t.Error("a file is not a library")
	}
}

func TestASkillReplacedWhileItIsReadIsConsistentOrNotFound(t *testing.T) {
	root := t.TempDir()
	write(t, root, "h/s/SKILL.md", "v1")
	store := open(t, root)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			// Written beside, then renamed into place: the way a skill is updated.
			write(t, root, "h/.next/SKILL.md", "v2")
			_ = os.RemoveAll(filepath.Join(root, "h/s"))
			if err := os.Rename(filepath.Join(root, "h/.next"), filepath.Join(root, "h/s")); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 200; i++ {
		files, err := store.Files("h/s")
		if err != nil && !errors.Is(err, ErrNotFound) {
			t.Fatalf("a replaced skill is found or not found, never another error: %v", err)
		}
		for _, f := range files {
			if f.Body != "v1" && f.Body != "v2" {
				t.Fatalf("torn read: %q", f.Body)
			}
		}
	}
	<-done
}

func paths(files []File) []string {
	out := make([]string, 0, len(files))
	for _, f := range files {
		out = append(out, f.Path)
	}
	return out
}
