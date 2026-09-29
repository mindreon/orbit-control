package config

import "testing"

func TestPublicListenAddrUsesPort(t *testing.T) {
	t.Setenv("ORBIT_PUBLIC_ADDR", "")
	t.Setenv("PORT", "9090")
	if got := Load().PublicListenAddr(); got != "127.0.0.1:9090" {
		t.Fatalf("addr = %q", got)
	}
}

func TestPublicListenAddrPrefersExplicitAddress(t *testing.T) {
	t.Setenv("ORBIT_PUBLIC_ADDR", "127.0.0.1:18080")
	t.Setenv("PORT", "9090")
	if got := Load().PublicListenAddr(); got != "127.0.0.1:18080" {
		t.Fatalf("addr = %q", got)
	}
}

func TestArtifactMaxIgnoresGarbage(t *testing.T) {
	t.Setenv("ORBIT_ARTIFACT_MAX_BYTES", "nope")
	if got := Load().ArtifactMaxBytes; got != 0 {
		t.Fatalf("artifact max = %d, want 0", got)
	}
}

func TestIngestMaxFallsBack(t *testing.T) {
	t.Setenv("ORBIT_INGEST_MAX_BYTES", "")
	if got := Load().IngestMaxBytes; got != defaultIngestMax {
		t.Fatalf("ingest max = %d, want %d", got, defaultIngestMax)
	}
}

func TestSkillHubSyncDefaultsOn(t *testing.T) {
	t.Setenv("ORBIT_SKILLHUB_SYNC", "")
	if !Load().SkillHubSync {
		t.Fatal("sync should stay on when unset")
	}
	t.Setenv("ORBIT_SKILLHUB_SYNC", "0")
	if Load().SkillHubSync {
		t.Fatal("sync should turn off only for 0")
	}
}
