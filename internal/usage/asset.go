package usage

import (
	"fmt"

	"github.com/mindreon/orbit-control/internal/contract"
)

// AssetRef resolves the immutable agent asset that performed a model call.
func AssetRef(lock contract.RunLock, member string) *string {
	for _, m := range lock.Members {
		if string(m.Name) != member {
			continue
		}
		if m.AgentEntry < 0 || m.AgentEntry >= len(lock.Entries) {
			return nil
		}
		e := lock.Entries[m.AgentEntry]
		ref := fmt.Sprintf("%s:%s@%s", e.Kind, e.Ref, e.Version)
		return &ref
	}
	if lock.Entry.Ref == "" {
		return nil
	}
	ref := fmt.Sprintf("%s:%s@%s", lock.Entry.Kind, lock.Entry.Ref, lock.Entry.Version)
	return &ref
}
