package termstyle

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

var escape = regexp.MustCompile("\x1b\\[[0-9;]*m")

func TestDetect(t *testing.T) {
	tests := []struct {
		name      string
		terminal  bool
		env       map[string]string
		wantOn    bool
		wantTrue  bool
		wantCodes string
	}{
		{name: "not a terminal", terminal: false, wantOn: false},
		{name: "terminal", terminal: true, wantOn: true, wantCodes: "\x1b[38;5;33m"},
		{name: "terminal with true color", terminal: true, env: map[string]string{"COLORTERM": "truecolor"}, wantOn: true, wantTrue: true, wantCodes: "\x1b[38;2;47;123;255m"},
		{name: "24bit is true color too", terminal: true, env: map[string]string{"COLORTERM": "24bit"}, wantOn: true, wantTrue: true},
		{name: "NO_COLOR", terminal: true, env: map[string]string{"NO_COLOR": "1"}, wantOn: false},
		{name: "dumb terminal", terminal: true, env: map[string]string{"TERM": "dumb"}, wantOn: false},
		{name: "forced for a pipe", terminal: false, env: map[string]string{"CLICOLOR_FORCE": "1"}, wantOn: true},
		{name: "forced off by zero", terminal: false, env: map[string]string{"CLICOLOR_FORCE": "0"}, wantOn: false},
		{name: "NO_COLOR wins over force", terminal: true, env: map[string]string{"CLICOLOR_FORCE": "1", "NO_COLOR": "1"}, wantOn: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := detect(tt.terminal, func(k string) string { return tt.env[k] })
			if s.Enabled() != tt.wantOn || s.trueColor != tt.wantTrue {
				t.Fatalf("detect = %+v, want on=%v trueColor=%v", s, tt.wantOn, tt.wantTrue)
			}
			if tt.wantCodes != "" && !strings.HasPrefix(s.Done("x"), tt.wantCodes) {
				t.Errorf("Done(x) = %q, want it to start with %q", s.Done("x"), tt.wantCodes)
			}
		})
	}
}

// TestColorNeverChangesTheText: the colored output is the plain one with escape codes around parts
// of it, so what is copied from a terminal, or read by a script, is the same.
func TestColorNeverChangesTheText(t *testing.T) {
	for _, s := range []Style{{}, {on: true}, {on: true, trueColor: true}} {
		for _, f := range []func(string) string{s.Done, s.Pending, s.Failed} {
			if got := escape.ReplaceAllString(f("[####]"), ""); got != "[####]" {
				t.Errorf("%+v: %q without escape codes, want [####]", s, got)
			}
			if got := f(""); got != "" {
				t.Errorf("%+v: an empty string became %q", s, got)
			}
		}
	}
}

func TestPlainStyleWritesNoEscapeCodes(t *testing.T) {
	var s Style
	if out := s.Done("a") + s.Pending("b") + s.Failed("c"); out != "abc" {
		t.Errorf("plain style wrote %q", out)
	}
	if s.Mark() != nil {
		t.Error("plain style draws the mark")
	}
}

func TestOutputThatIsNotAFileIsNotColored(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	t.Setenv("CLICOLOR_FORCE", "")
	if For(&bytes.Buffer{}).Enabled() {
		t.Error("a buffer is colored")
	}
}

func TestMarkIsSevenLinesOfEqualWidth(t *testing.T) {
	lines := Style{on: true}.Mark()
	if len(lines) != (len(markPixels)+1)/2 {
		t.Fatalf("%d lines, want %d", len(lines), (len(markPixels)+1)/2)
	}
	for i, l := range lines {
		plain := escape.ReplaceAllString(l, "")
		if n := utf8.RuneCountInString(plain); n != MarkWidth {
			t.Errorf("line %d is %d cells wide, want %d: %q", i, n, MarkWidth, plain)
		}
		if strings.Trim(plain, " ▀▄") != "" {
			t.Errorf("line %d has more than half blocks: %q", i, plain)
		}
	}
	for _, row := range markPixels {
		if len(row) != MarkWidth {
			t.Fatalf("pixel row %q is not %d wide", row, MarkWidth)
		}
		if strings.Trim(row, ".BCYO") != "" {
			t.Fatalf("pixel row %q has an unknown color", row)
		}
	}
}
