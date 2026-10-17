//go:build workflow_test_limits

package membertask

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"

	"github.com/mindreon/orbit-control/internal/contract"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func TestContinueAsNewCarriesMailboxAndEndTurn(t *testing.T) {
	// T-M7 (§1.5): ContinuedAt starts at the cursor so a turn does not continue as new immediately; when it
	// does, the mailbox, EndTurn and delivery records travel with it.
	r := newRig(t, step{after: sec, result: yielded(8)}, step{after: sec, result: yielded(9)}, step{after: 10 * sec, result: yielded(10)})
	r.in.Cursor.Seq = 3
	r.in.Mailbox = []contract.MailboxMessage{mail("first", "one")}
	r.at(8*sec, func() { deliverMail(r, "second"); endTurn(r) })
	_, err := r.run()
	var next *workflow.ContinueAsNewError
	require.ErrorAs(t, err, &next)
	require.Len(t, r.segments, 3, "seq 4, 5 and 6 ran before the first Continue-As-New")
	require.Equal(t, 4, r.segments[0].Seq)
	var in w.MemberTaskInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(next.Input, &in))
	require.Equal(t, 6, in.Carry.CommittedSeq)
	require.Equal(t, 6, in.Carry.ContinuedAt)
	require.True(t, in.Carry.EndTurn)
	require.Equal(t, []string{"first"}, in.Carry.DeliveredMail)
	require.Len(t, in.Carry.Mailbox, 1)
	require.Equal(t, "second", in.Carry.Mailbox[0].MessageId)
}
