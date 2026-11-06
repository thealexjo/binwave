package errcode

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestEveryCodeIsDocumented keeps internal/errcode (the source of truth) and
// docs/reference/error-codes.md in sync: every code in All must appear in the doc, and every
// code-shaped token in the doc must be a real constant, so a typo or a forgotten row is a
// failing test, not a silent drift.
func TestEveryCodeIsDocumented(t *testing.T) {
	data, err := os.ReadFile("../../docs/reference/error-codes.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(data)

	known := make(map[Code]bool, len(All))
	for _, c := range All {
		known[c] = true
		if !regexpCode.MatchString(string(c)) {
			t.Errorf("%s does not look like a code (SECTION + 3 digits)", c)
		}
		if want := "`" + string(c) + "`"; !strings.Contains(doc, want) {
			t.Errorf("%s is in All but not documented in docs/reference/error-codes.md", c)
		}
	}

	for _, m := range regexpDocCode.FindAllStringSubmatch(doc, -1) {
		if code := Code(m[1]); !known[code] {
			t.Errorf("docs/reference/error-codes.md documents %s, which is not in errcode.All", code)
		}
	}
}

var (
	regexpCode    = regexp.MustCompile(`^[A-Z]{3,4}[0-9]{3}$`)
	regexpDocCode = regexp.MustCompile("`([A-Z]{3,4}[0-9]{3})`")
)
