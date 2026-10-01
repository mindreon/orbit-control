package app

import (
	"context"
	"errors"

	"github.com/mindreon/orbit-control/internal/skillstore"
	"github.com/mindreon/orbit-control/internal/store"
)

// skillFiles reads a skill's text: from the library on disk when it has the skill, else from what the catalog holds.
// known is false when neither can say. A skill the library has but cannot serve (too big) is not read from the catalog
// instead: that would be another version of it.
func (a *App) skillFiles(ctx context.Context, tenantID, skillID string) (files []store.SkillFile, known bool, err error) {
	if a.Skills != nil {
		found, err := a.Skills.Files(skillID)
		switch {
		case err == nil:
			files = make([]store.SkillFile, 0, len(found))
			for _, file := range found {
				files = append(files, store.SkillFile{Path: file.Path, Body: file.Body})
			}
			return files, true, nil
		case errors.Is(err, skillstore.ErrTooLarge):
			return nil, false, nil
		case !errors.Is(err, skillstore.ErrNotFound):
			return nil, false, err
		}
	}
	return a.Repo.GetSkillTextFiles(ctx, tenantID, skillID)
}
