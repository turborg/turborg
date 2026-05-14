// Package version exposes the build version of turborg. release-please
// rewrites the Version constant below on every release PR. A single
// constant in a tiny package keeps the bump diff minimal and avoids
// pulling the import graph for anyone who only needs the version string.
package version

// Version is the current turborg release. Bumped by release-please.
const Version = "0.1.0"
