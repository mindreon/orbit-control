package assets

import "github.com/mindreon/orbit-control/internal/platform/httpx"

var transitions = map[string][]string{
	"draft": {"submitted"}, "submitted": {"checking"}, "checking": {"check_failed", "in_review", "approved"},
	"check_failed": {"draft"}, "in_review": {"changes_requested", "rejected", "approved"}, "changes_requested": {"draft"},
	"approved": {"published"}, "published": {"deprecated", "suspended"}, "suspended": {"published", "yanked"}, "deprecated": {"yanked"},
}

func Transition(from, to string) error {
	for _, next := range transitions[from] {
		if next == to {
			return nil
		}
	}
	return httpx.E(409, "INVALID_ASSET_STATE", "Asset version state transition is not permitted")
}
func Usable(state string) bool { return state == "published" || state == "deprecated" }
