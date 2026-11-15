//go:build e2e && race

package e2e

// raceEnabled: the tests run under the race detector, and so does the binwave they start.
const raceEnabled = true
