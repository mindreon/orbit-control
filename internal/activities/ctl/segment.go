package ctl

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/temporal"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

// SegmentResult has no identity fields in the wire contract. Bind it to the
// persisted input identity before updating projections, including on retries.
func validateSegmentIdentity(ctx context.Context, tx pgx.Tx, in w.RecordSegmentInput) error {
	var valid bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1
   FROM run_segments s
   JOIN runs r
   ON r.tenant_id=s.tenant_id
   AND r.id=s.run_id
   WHERE r.tenant_id=$1
   AND r.id=$2
   AND r.session_id=$3
   AND s.member=$4
   AND s.seq=$5
   AND (s.status='running' OR s.checkpoint_version=$6))`, in.TenantID, in.RunID, in.SessionID, in.Member, in.Seq,
		in.Result.CheckpointVersion).Scan(&valid)
	if err != nil {
		return err
	}
	if !valid {
		return temporal.NewNonRetryableApplicationError("segment identity mismatch", contract.ErrorInvalidSegmentInput, nil)
	}
	if f := in.Result.Final; f != nil && f.ContentRef != nil &&
		!strings.HasPrefix(*f.ContentRef, "t/"+in.TenantID+"/runs/"+in.RunID+"/") {
		return temporal.NewNonRetryableApplicationError("message reference outside run", contract.ErrorInvalidSegmentInput, nil)
	}
	return nil
}
