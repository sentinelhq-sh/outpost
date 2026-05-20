// Package version exposes build-time metadata for logs and the
// User-Agent header sent to the SentinelHQ server.
package version

// Version is overridden at build time via -ldflags "-X .../version.Version=..."
// from the release pipeline. The "dev" default keeps local builds identifiable.
var (
	Version = "dev"
	Commit  = "unknown"
)
