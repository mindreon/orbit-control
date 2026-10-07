package httpapi

import (
	"net/http"

	"github.com/mindreon/orbit-control/internal/app"
)

// modelCatalog filters the configured model list to usable names (valid, deduped, in order) and keeps the default only
// when it looks like a model name. An entry the deployment misspells is dropped rather than served to a picker.
func modelCatalog(items []string, fallback string) ([]string, string) {
	out := make([]string, 0, len(items))
	seen := make(map[string]bool, len(items))
	for _, model := range items {
		if !app.ValidModelName(model) || seen[model] {
			continue
		}
		seen[model] = true
		out = append(out, model)
	}
	if !app.ValidModelName(fallback) {
		fallback = ""
	}
	return out, fallback
}

func writeModels(w http.ResponseWriter, items []string, fallback string) {
	list, def := modelCatalog(items, fallback)
	writeJSON(w, http.StatusOK, map[string]any{"items": list, "default": def})
}
