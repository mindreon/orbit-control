//go:build !e2e

package pgstore

func noteDeliveryFault(string) error { return nil }

func abortBeforeCommit(string) error { return nil }
