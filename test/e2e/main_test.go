//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/thealexjo/binwave/test/testenv"
)

// binaryPath is the binwave executable built once for the whole run. The scenarios start it as a
// real process, so they exercise what runs in production: signals, exit codes, and the log output.
var binaryPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "binwave-e2e")
	if err != nil {
		fmt.Fprintln(os.Stderr, "create temp dir:", err)
		os.Exit(1)
	}
	binaryPath = filepath.Join(dir, "binwave")

	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..")
	build := exec.Command("go", "build", "-o", binaryPath, "./cmd/binwave")
	build.Dir = root
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build binwave: %v\n%s", err, out)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}

	code := testenv.Run(m)
	_ = os.RemoveAll(dir)
	os.Exit(code)
}
