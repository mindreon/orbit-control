package httpapi

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/platform/httpx"
)

func TestTemporalErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
		retry  bool
	}{
		{name: "validator", err: temporal.NewApplicationError("finished", "RUN_NOT_ACTIVE"), status: 409, code: "RUN_NOT_ACTIVE"},
		{name: "not found", err: serviceerror.NewNotFound("finished"), status: 409, code: "RUN_NOT_ACTIVE"},
		{name: "unavailable", err: serviceerror.NewUnavailable("offline"), status: 503, code: "TEMPORAL_UNAVAILABLE", retry: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got *httpx.Error
			require.True(t, errors.As(temporalError(tc.err, "RUN_NOT_ACTIVE"), &got))
			require.Equal(t, tc.status, got.Status)
			require.Equal(t, tc.code, got.Code)
			require.Equal(t, tc.retry, got.Retryable)
		})
	}
}
func TestRunRoutesDeclareTransactions(t *testing.T) {
	count := 0
	for _, route := range routes {
		if route.TwoPhase {
			count++
			require.False(t, route.Public)
			require.False(t, route.TenantOptional)
		}
	}
	require.Equal(t, 13, count)
}
