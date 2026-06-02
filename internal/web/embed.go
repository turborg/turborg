package web

import "embed"

// staticFS is the bundled reference UI. Single vanilla-JS file, no build
// step. It's a reference client with a stable wire protocol — embedders
// can build their own UI against the same protocol.
//
//go:embed static
var staticFS embed.FS
