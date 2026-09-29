package task

import (
	"context"
	"errors"
)

func (s *Service) Manifests(ctx context.Context, p Principal, taskID string) ([]ArtifactManifest, error) {
	if _, err := s.Get(ctx, p, taskID); err != nil {
		return nil, err
	}
	if s.projection != nil {
		return s.projection.ListManifests(ctx, p, taskID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]ArtifactManifest, 0)
	for _, manifest := range s.manifests {
		if manifest.TaskID == taskID {
			manifest.Entries = append([]map[string]any(nil), manifest.Entries...)
			items = append(items, manifest)
		}
	}
	return items, nil
}

func (s *Service) GetManifest(ctx context.Context, p Principal, manifestID string) (ArtifactManifest, error) {
	if s.projection != nil {
		return s.projection.GetManifest(ctx, p, manifestID)
	}
	s.mu.Lock()
	manifest, ok := s.manifests[manifestID]
	s.mu.Unlock()
	if !ok {
		return ArtifactManifest{}, ErrNotFound
	}
	if _, err := s.Get(ctx, p, manifest.TaskID); err != nil {
		return ArtifactManifest{}, err
	}
	manifest.Entries = append([]map[string]any(nil), manifest.Entries...)
	return manifest, nil
}

func (s *Service) PresignArtifact(ctx context.Context, p Principal, manifestID, name string) (string, error) {
	if s.artifactSigner == nil {
		return "", errors.New("artifact storage is not configured")
	}
	manifest, err := s.GetManifest(ctx, p, manifestID)
	if err != nil {
		return "", err
	}
	for _, entry := range manifest.Entries {
		if entryName, _ := entry["name"].(string); entryName == name {
			return s.artifactSigner.PresignArtifact(ctx, p, manifest, entry)
		}
	}
	return "", ErrNotFound
}
