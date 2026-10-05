package httpapi

import (
	"errors"
	"net/http"

	"github.com/mindreon/orbit-control/internal/orch"
)

// writeOrchRefusal answers an update the workflow refused (or failed) with the status its type maps to and the type as
// the error code. It reports whether err was such a refusal; the caller handles any other error.
func writeOrchRefusal(w http.ResponseWriter, err error) bool {
	var refusal *orch.Refusal
	if !errors.As(err, &refusal) {
		return false
	}
	writeErr(w, refusal.Status(), refusal.Type, refusal.Message)
	return true
}
