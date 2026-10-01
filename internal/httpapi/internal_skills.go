package httpapi

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/mindreon/orbit-control/internal/app"
	"github.com/mindreon/orbit-control/internal/internalauth"
)

// registerInternalSkills serves skill files to the worker (15 T8.4). The catalog is shared by all tenants, so the
// default tenant is only what the store needs to answer.
func registerInternalSkills(engine *gin.Engine, runtime *app.App) {
	serve := func(handle, slug string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !internalauth.Authorized(r) {
				writeErr(w, http.StatusUnauthorized, "UNAUTHORIZED", "internal token required")
				return
			}
			bundle, err := runtime.SkillBundleForWorker(r.Context(), runtime.DefaultTenant, handle, slug)
			if err != nil {
				// Whatever the reason, the worker is told only that there is nothing to stage.
				writeAppErr(runtime.Log, w, err, "skill not found", http.StatusInternalServerError, "INTERNAL")
				return
			}
			writeJSON(w, http.StatusOK, bundle)
		}
	}
	engine.GET("/internal/skills/:handle/:slug", ginAdapt(func(w http.ResponseWriter, r *http.Request) {
		serve(r.PathValue("handle"), r.PathValue("slug"))(w, r)
	}))
	engine.GET("/internal/skills/:handle", ginAdapt(func(w http.ResponseWriter, r *http.Request) {
		serve("", r.PathValue("handle"))(w, r)
	}))
}
