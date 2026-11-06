// Package buildinfo exposes version information injected at build time.
package buildinfo

import (
	"fmt"
	"runtime"
)

// Set via -ldflags "-X github.com/thealexjo/binwave/internal/buildinfo.Version=...".
var (
	Version = "dev"
	Commit  = "unknown"
	Date    = "unknown"
)

// Info is a snapshot of the build metadata.
type Info struct {
	Version   string
	Commit    string
	Date      string
	GoVersion string
}

// Get returns the build metadata of the running binary.
func Get() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		Date:      Date,
		GoVersion: runtime.Version(),
	}
}

// String renders the build metadata as a single line.
func (i Info) String() string {
	return fmt.Sprintf("binwave %s (commit %s, built %s, %s)", i.Version, i.Commit, i.Date, i.GoVersion)
}
