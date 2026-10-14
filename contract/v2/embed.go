// Package contractv2 embeds the source schemas used at process boundaries.
package contractv2

import "embed"

// Schemas is the contract source of truth, embedded for offline validation.
//
//go:embed schema/*.json
var Schemas embed.FS
