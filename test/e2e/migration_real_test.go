//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

// The real online schema change tools run against binwave only on request: they are a container
// image to pull and a binary to build, and they make the run slower, so they are not part of the
// pipelines. Run them when a new version of a tool comes out, or when the simulation
// (test/migrationsim) is suspected of drifting away from the tools:
//
//	BINWAVE_TEST_REAL_TOOLS=1 BINWAVE_TEST_GHOST_BIN=/path/to/linux/gh-ost \
//	    go test -tags e2e -race -count=1 -run RealTools ./test/e2e
//
// pt-online-schema-change comes from the percona/percona-toolkit image. gh-ost has no image: the
// linux binary is built from its sources (go build with CGO_ENABLED=0) and mounted into a
// container that shares the network namespace of the MySQL container, because gh-ost checks that the
// port the server reports is the one it connected to.
const (
	realToolsEnv        = "BINWAVE_TEST_REAL_TOOLS"
	ghostBinaryEnv      = "BINWAVE_TEST_GHOST_BIN"
	perconaToolkitImage = "percona/percona-toolkit"
)

func requireRealTools(t *testing.T) {
	t.Helper()
	if os.Getenv(realToolsEnv) == "" {
		t.Skipf("%s is not set: the real tools are not run by default, the simulation stands in for them", realToolsEnv)
	}
}

func runContainer(cmd *exec.Cmd) error {
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w\n%s", err, tail(string(out), 3000))
	}
	return nil
}

func realPtOSC() migrationTool {
	return migrationTool{"pt-online-schema-change", func(ctx context.Context, s *stack, alter string) error {
		return runContainer(exec.CommandContext(ctx, "docker", "run", "--rm", "--network=container:"+s.my.ContainerID, perconaToolkitImage,
			"pt-online-schema-change", "--alter", alter,
			"h=127.0.0.1,P=3306,u=root,p="+s.my.Password+",D=binwave,t=acct",
			"--execute", "--recursion-method=none", "--no-check-alter",
			// Small chunks with pauses make the copy last long enough for binwave to be killed in it.
			"--chunk-size=50", "--sleep=0.05"))
	}}
}

func realGhost(t *testing.T, extra ...string) migrationTool {
	bin := os.Getenv(ghostBinaryEnv)
	if bin == "" {
		t.Skipf("%s is not set: gh-ost is a binary to build, see migration_real_test.go", ghostBinaryEnv)
	}
	return migrationTool{"gh-ost", func(ctx context.Context, s *stack, alter string) error {
		args := append([]string{"run", "--rm", "--network=container:" + s.my.ContainerID, "-v", bin + ":/gh-ost:ro", "alpine:3.20", "/gh-ost"}, extra...)
		return runContainer(exec.CommandContext(ctx, "docker", append(args,
			"--host=127.0.0.1", "--port=3306", "--user=root", "--password="+s.my.Password, "--database=binwave", "--table=acct",
			"--alter="+alter, "--allow-on-master", "--assume-rbr", "--execute", "--ok-to-drop-table", "--approve-renamed-columns",
			"--initially-drop-ghost-table", "--initially-drop-old-table",
			// The workload rewrites a small set of rows all the time; small chunks and many retries
			// keep gh-ost from giving up on a row lock, which is its business, not binwave's.
			"--chunk-size=100", "--default-retries=100")...))
	}}
}

// TestRealToolsUnderLoad is TestOnlineSchemaChangeUnderLoad with the real tools.
func TestRealToolsUnderLoad(t *testing.T) {
	requireRealTools(t)
	t.Parallel()
	t.Run("pt-online-schema-change", func(t *testing.T) { t.Parallel(); migrateUnderLoad(t, realPtOSC(), false) })
	t.Run("gh-ost", func(t *testing.T) { t.Parallel(); migrateUnderLoad(t, realGhost(t), false) })
	t.Run("gh-ost two-step", func(t *testing.T) { t.Parallel(); migrateUnderLoad(t, realGhost(t, "--cut-over=two-step"), false) })
}

// TestRealToolsUnderLoadAndKillNine is TestOnlineSchemaChangeUnderLoadAndKillNine with the real tools.
func TestRealToolsUnderLoadAndKillNine(t *testing.T) {
	requireRealTools(t)
	t.Parallel()
	t.Run("pt-online-schema-change", func(t *testing.T) { t.Parallel(); migrateUnderLoad(t, realPtOSC(), true) })
	t.Run("gh-ost", func(t *testing.T) { t.Parallel(); migrateUnderLoad(t, realGhost(t), true) })
}
