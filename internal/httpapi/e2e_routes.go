//go:build e2e

package httpapi

import (
	"net/http"

	"github.com/mindreon/orbit-control/internal/app"
)

func registerE2ERoutes(mux *http.ServeMux, runtime *app.App) {
	mux.HandleFunc("POST /internal/e2e/reconcile", func(w http.ResponseWriter, r *http.Request) {
		if err := runtime.ReconcileDeliveredResults(r.Context()); err != nil {
			runtime.Log.Printf("reconcile: %v", err)
			writeErr(w, http.StatusInternalServerError, "INTERNAL", "reconcile failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	})
}
