package web

import "embed"

// staticFS is the bundled reference UI. Single vanilla-JS file, no build
// step. Polished Angular UI lives in xshellz's client repo; this is the
// frozen contract reference the team builds against.
//
//go:embed static
var staticFS embed.FS
