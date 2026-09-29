package app

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/mindreon/orbit-control/internal/store/memstore"
)

func TestSaveArtifactBlobAcceptsTenantHintWithMemoryRepository(t *testing.T) {
	dir := t.TempDir()
	payload := []byte("completed")
	sum := sha256.Sum256(payload)
	a := &App{Repo: memstore.New(), ArtifactDir: dir, ArtifactMaxBytes: 1024}

	ref, err := a.SaveArtifactBlob("task_01ARZ3NDEKTSV4RRFFQ69G5FAV", "tenant", hex.EncodeToString(sum[:]), bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if ref != "tenant/"+hex.EncodeToString(sum[:]) {
		t.Fatalf("unexpected storage ref %q", ref)
	}
	got, err := os.ReadFile(filepath.Join(dir, "tenant", hex.EncodeToString(sum[:])))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatalf("stored payload %q", got)
	}
}
