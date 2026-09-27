//go:build e2e

package httpapi

import (
	"context"
	"net/http"
	"os"

	"github.com/mindreon/orbit-control/internal/app"
)

func registerE2ERoutes(mux *http.ServeMux, runtime *app.App) {
	// The e2e binary must export WriteResultAgain. The production build
	// compiles e2e_routes_prod.go instead, so this reference is absent there.
	keepWriteResultAgain(runtime)
	mux.HandleFunc("POST /internal/e2e/reconcile", func(w http.ResponseWriter, r *http.Request) {
		if err := runtime.ReconcileDeliveredResults(r.Context()); err != nil {
			runtime.Log.Printf("reconcile: %v", err)
			writeErr(w, http.StatusInternalServerError, "INTERNAL", "reconcile failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
}

// keepWriteResultAgain is called from route registration so the linker
// retains App.WriteResultAgain in the e2e binary. The branch stays closed
// during tests; the production build does not compile this file.
func keepWriteResultAgain(runtime *app.App) {
	if os.Getenv("ORBIT_E2E_KEEP_WRITE_RESULT_AGAIN") == "1" {
		_, _ = runtime.WriteResultAgain(context.Background(), "", "", 0)
	}
}
