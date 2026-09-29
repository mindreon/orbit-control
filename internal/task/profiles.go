package task

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (s *Service) RegisterProfile(ctx context.Context, p Principal, profile Profile) (Profile, error) {
	if p.TenantID == "" || p.UserID == "" || profile.ProfileID == "" || profile.Version < 1 {
		return Profile{}, errors.New("tenant, user, profile_id and positive version are required")
	}
	profile.Ref = fmt.Sprintf("%s@%d", profile.ProfileID, profile.Version)
	profile.Spec = cloneMap(profile.Spec)
	if s.projection != nil {
		return s.projection.RegisterProfile(ctx, p, profile)
	}
	profile.CreatedAt = time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.profiles[p.TenantID] == nil {
		s.profiles[p.TenantID] = map[string]Profile{}
	}
	if existing, ok := s.profiles[p.TenantID][profile.Ref]; ok {
		if !jsonEqual(existing.Spec, profile.Spec) {
			return Profile{}, ErrIdempotencyConflict
		}
		return existing, nil
	}
	s.profiles[p.TenantID][profile.Ref] = profile
	return profile, nil
}

func (s *Service) ListProfiles(ctx context.Context, p Principal) ([]Profile, error) {
	if s.projection != nil {
		return s.projection.ListProfiles(ctx, p)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]Profile, 0, len(s.profiles[p.TenantID]))
	for _, profile := range s.profiles[p.TenantID] {
		profile.Spec = cloneMap(profile.Spec)
		items = append(items, profile)
	}
	return items, nil
}

func (s *Service) GetProfile(ctx context.Context, p Principal, ref string) (Profile, error) {
	if s.projection != nil {
		return s.projection.GetProfile(ctx, p, ref)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	profile, ok := s.profiles[p.TenantID][ref]
	if !ok {
		return Profile{}, ErrNotFound
	}
	profile.Spec = cloneMap(profile.Spec)
	return profile, nil
}

func PersonaProfileRef(id string) string { return "persona_" + id + "@1" }
