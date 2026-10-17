package sessions

import (
	"context"
	"math"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/mindreon/orbit-control/internal/platform/db"
)

type pageTx struct {
	pgx.Tx
	t    *testing.T
	seqs []uint64
}
type pageRows struct {
	pgx.Rows
	seqs  []uint64
	index int
}

func (r *pageRows) Next() bool { return r.index < len(r.seqs) }
func (r *pageRows) Scan(dest ...any) error {
	*dest[0].(*uint64) = r.seqs[r.index]
	*dest[1].(*[]byte) = []byte(`{}`)
	r.index++
	return nil
}
func (*pageRows) Close()     {}
func (*pageRows) Err() error { return nil }
func (tx pageTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	require.GreaterOrEqual(tx.t, args[3].(int64), int64(0), "uint64 overflow must return before querying")
	require.Contains(tx.t, sql, "stream_seq>$4::bigint AND stream_seq<=$8::bigint")
	require.IsType(tx.t, int64(0), args[3])
	require.IsType(tx.t, int64(0), args[7])
	return &pageRows{seqs: tx.seqs}, nil
}
func TestE35EventPageRules(t *testing.T) {
	for _, tc := range []struct {
		name         string
		a, b         uint64
		seqs         []uint64
		next, resume string
		count        int
	}{
		{name: "non-final", a: 0, b: 9, seqs: []uint64{1, 3, 5}, next: "3", resume: "3", count: 2},
		{name: "final", a: 0, b: 9, seqs: []uint64{1, 3}, next: "", resume: "9", count: 2},
		{name: "empty-before", a: 2, b: 9, seqs: nil, next: "", resume: "9", count: 0},
		{name: "empty-after", a: 12, b: 9, seqs: nil, next: "", resume: "12", count: 0},
		{name: "filtered-final", a: 0, b: 9, seqs: []uint64{3}, next: "", resume: "9", count: 1},
		{name: "above-bigint", a: uint64(math.MaxInt64) + 1, b: 9, seqs: nil, next: "", resume: "9223372036854775808", count: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := db.Context(t.Context(), pageTx{t: t, seqs: tc.seqs})
			page, err := Events(ctx, "session", "", EventQuery{After: tc.a, Floor: tc.b, Limit: 2})
			require.NoError(t, err)
			require.Len(t, page.Items, tc.count)
			require.Equal(t, tc.next, page.NextCursor)
			require.Equal(t, tc.resume, page.ResumeFrom)
		})
	}
}
