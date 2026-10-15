package integration

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/mocks"

	"github.com/mindreon/orbit-control/internal/contract"
	"github.com/mindreon/orbit-control/internal/platform/db"
	w "github.com/mindreon/orbit-control/internal/workflows"
)

func testSandboxAcquireGenerationAndProvider(t *testing.T, d *db.DB, admin *pgx.Conn) {
	f := newRunFixture(t, d, admin)
	f.start(t)
	isolation := w.IsolationKey(f.tenant, f.session)
	require.NoError(t, f.activities.RecordSandboxState(t.Context(), w.SandboxStateInput{
		TenantID: f.tenant, SessionID: f.session, IsolationKey: isolation, Gen: 2, State: "destroyed",
		Provider: "opensandbox", Profile: "default",
	}))
	tc := mocks.NewClient(t)
	handle := mocks.NewWorkflowUpdateHandle(t)
	var received []w.AcquireInput
	tc.On("NewWithStartWorkflowOperation", mock.Anything, w.SandboxLeaseWorkflow, mock.Anything).Return(nil)
	tc.On("UpdateWithStartWorkflow", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		options := args[1].(client.UpdateWithStartWorkflowOptions)
		require.Equal(t, w.Acquire, options.UpdateOptions.UpdateName)
		received = append(received, options.UpdateOptions.Args[0].(w.AcquireInput))
	}).Return(handle, nil)
	handle.On("Get", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		out := args[1].(*contract.SandboxLease)
		*out = contract.SandboxLease{Provider: received[len(received)-1].Provider, SandboxId: "new-box"}
	}).Return(nil)
	f.activities.Temporal = tc
	var digests []string
	for _, provider := range []contract.SandboxProvider{"local", "opensandbox"} {
		f.activities.SandboxProvider = provider
		lease, err := f.activities.AcquireSandbox(t.Context(), w.AcquireSandboxInput{
			RunOp: f.op(), Spec: w.SandboxSpec{IsolationKey: isolation, Profile: "default", TTLSeconds: 1800}, LockID: f.lock,
		})
		require.NoError(t, err)
		ac := received[len(received)-1]
		require.Equal(t, 2, ac.Gen, "restart must receive the persisted generation")
		require.Equal(t, provider, ac.Provider)
		require.Equal(t, provider, lease.Provider)
		require.NotEmpty(t, ac.SpecDigest)
		digests = append(digests, ac.SpecDigest)
	}
	require.NotEqual(t, digests[0], digests[1], "provider is part of the sandbox spec identity")
	// Creating projection has no returned lease yet and must use the selected provider.
	require.NoError(t, f.activities.RecordSandboxState(t.Context(), w.SandboxStateInput{
		TenantID: f.tenant, SessionID: f.session, IsolationKey: isolation, Gen: 3, State: "creating", Provider: "opensandbox", Profile: "default",
	}))
	f.tx(t, func(ctx context.Context, tx pgx.Tx) error {
		var provider string
		require.NoError(t, tx.QueryRow(ctx, `SELECT provider FROM sandboxes WHERE tenant_id=$1 AND isolation_key=$2`,
			f.tenant, isolation).Scan(&provider))
		require.Equal(t, "opensandbox", provider)
		return nil
	})
}
