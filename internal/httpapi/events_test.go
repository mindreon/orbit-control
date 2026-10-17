package httpapi

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/auth"
	"github.com/mindreon/orbit-control/internal/platform/db"
	"github.com/mindreon/orbit-control/internal/platform/httpx"
	"github.com/mindreon/orbit-control/internal/platform/ids"
)

type historyTx struct {
	pgx.Tx
	t               *testing.T
	tenant, session string
	order           []string
}
type historyRow struct{ session string }

func (r historyRow) Scan(dest ...any) error {
	*dest[0].(*[]byte) = []byte(`{"id":"` + r.session + `"}`)
	return nil
}
func (tx *historyTx) QueryRow(context.Context, string, ...any) pgx.Row {
	return historyRow{session: tx.session}
}

type historyRows struct {
	pgx.Rows
	tenant        string
	binding, read bool
}

func (r *historyRows) Next() bool {
	if r.binding && !r.read {
		r.read = true
		return true
	}
	return false
}
func (r *historyRows) Scan(dest ...any) error {
	*dest[0].(*string) = "tenant.owner"
	*dest[1].(*string) = "tenant"
	*dest[2].(*string) = r.tenant
	return nil
}
func (*historyRows) Close()     {}
func (*historyRows) Err() error { return nil }
func (tx *historyTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.Contains(sql, "FROM role_bindings") {
		return &historyRows{binding: true, tenant: tx.tenant}, nil
	}
	tx.order = append(tx.order, "SELECT")
	require.Equal(tx.t, []string{"floor", "SELECT"}, tx.order, "the safety proof must precede the projection snapshot")
	require.IsType(tx.t, int64(0), args[3])
	require.EqualValues(tx.t, 7, args[7])
	return &historyRows{}, nil
}

type historyFloorFunc func(context.Context) (uint64, error)

func (fn historyFloorFunc) Floor(ctx context.Context) (uint64, error) { return fn(ctx) }

func TestHistoryFloorBeforeSelectAndUnavailable(t *testing.T) {
	for _, mode := range []string{"cached", "cold", "missing-provider"} {
		t.Run(mode, func(t *testing.T) {
			tx := &historyTx{t: t, tenant: ids.New(), session: ids.New()}
			h := &Handler{EventFloor: historyFloorFunc(func(context.Context) (uint64, error) {
				tx.order = append(tx.order, "floor")
				if mode == "cold" {
					return 0, errors.New("NATS offline and no cache")
				}
				return 7, nil
			})}
			if mode == "missing-provider" {
				h.EventFloor = nil
			}
			ctx := auth.Context(t.Context(), auth.Principal{ID: ids.New()}, tx.tenant)
			ctx = db.Context(ctx, tx)
			r := httptest.NewRequest("GET", "/history", nil).WithContext(ctx)
			r.SetPathValue("s", tx.session)
			out := httptest.NewRecorder()
			err := h.history(out, r)
			if mode == "cached" {
				require.NoError(t, err)
				require.Equal(t, 200, out.Code)
				require.Contains(t, out.Body.String(), `"resume_from":"7"`)
			} else {
				require.Error(t, err)
				httpx.WriteError(out, err)
				require.Equal(t, 503, out.Code)
				require.Contains(t, out.Body.String(), `"code":"STREAM_UNAVAILABLE"`)
				require.Contains(t, out.Body.String(), `"retryable":true`)
				require.NotContains(t, tx.order, "SELECT")
			}
		})
	}
}
