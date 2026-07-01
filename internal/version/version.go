// Package version exposes the build version of turborg. release-please
// rewrites the Version literal below on every release PR (driven by the
// `x-release-please-version` marker comment); local + CI builds may
// override it via `-ldflags="-X .../version.Version=<value>"`. A single
// var in a tiny package keeps the bump diff minimal and avoids pulling
// the import graph for anyone who only needs the version string.
package version

// Version is the current turborg release. Must be a `var` (not `const`)
// so the linker can override it with `-X` at build time — `-X` is a
// no-op on consts. Source-of-truth value here is bumped by release-please
// on the marker line.
var Version = "0.17.0" // x-release-please-version
