// Package testdata contains the authored development agent bundles.
package testdata

import "embed"

//go:embed bundles
var Bundles embed.FS
