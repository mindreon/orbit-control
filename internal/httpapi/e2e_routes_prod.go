//go:build !e2e

package httpapi

import (
	"net/http"

	"github.com/mindreon/orbit-control/internal/app"
)

func registerE2ERoutes(*http.ServeMux, *app.App) {}
