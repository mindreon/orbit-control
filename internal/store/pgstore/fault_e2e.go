//go:build e2e

package pgstore

// BeforeDeliveryUpdate runs before a delivery UPDATE, outside the
// transaction, so another connection can win the row first (S-ID-18).
// A non-nil error fails the call before the UPDATE.
var BeforeDeliveryUpdate func(transition string) error

// AbortBeforeCommit runs inside the T4 transaction after not_delivered is
// written and before commit. A non-nil error rolls the transaction back
// (S-ID-12 crash between T4 and commit).
var AbortBeforeCommit func(transition string) error

func noteDeliveryFault(transition string) error {
	if BeforeDeliveryUpdate == nil || transition == "" {
		return nil
	}
	return BeforeDeliveryUpdate(transition)
}

func abortBeforeCommit(transition string) error {
	if AbortBeforeCommit == nil || transition == "" {
		return nil
	}
	return AbortBeforeCommit(transition)
}
