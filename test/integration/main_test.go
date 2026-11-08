//go:build integration

package integration

import (
	"os"
	"testing"

	"github.com/thealexjo/binwave/test/testenv"
)

// TestMain runs the tests on pooled database servers and removes them at the end.
func TestMain(m *testing.M) { os.Exit(testenv.Run(m)) }
