// Package contractv3 holds the Go types for Orbit contract v3 (TaskWorkflow):
// commands, Update and Signal payloads, Query views and orbit.event/3.
//
// The source of truth is orbit-runtime's orbit_contracts.v3 (pydantic).
// contracts.json and testdata/examples.json are copies of orbit-runtime's
// schema/v3 files; scripts/sync-contracts.sh refreshes them and regenerates
// contracts_gen.go. Do not edit the copies or the generated file by hand.
package contractv3

//go:generate go run ../contractgen -in contracts.json -out contracts_gen.go -pkg contractv3
