// Package contract holds the Go types of contract v2, generated from the JSON
// Schema files in contract/v2/schema (the source of truth) by
// scripts/gen-contracts.sh. orbit-runtime keeps a byte-identical copy of the
// schemas and generates its pydantic models from it.
//
// Do not edit contract_gen.go. Change a schema, run scripts/gen-contracts.sh and
// commit both; scripts/check-contracts-gen.sh fails when they differ.
package contract
