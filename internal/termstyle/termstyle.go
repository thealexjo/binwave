// Package termstyle colors the output of CLI commands that a person reads at a terminal: the
// status, the check report, the quarantine list. Logs are never colored: they go to journald and
// to parsers, where escape codes are noise.
//
// Color is used sparingly. Two brand colors mark progress (blue for what is done, orange for what
// is complete but waits on something else) and attention (paused, warnings); red is kept for
// failures and comes from the terminal's own theme, because an operator looks for red, not for a
// brand. Everything else stays plain, and the text is the same with or without color.
package termstyle

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// Brand colors, as 24-bit RGB and as the closest entry of the 256-color palette for terminals that
// do not announce true color. Cyan and yellow of the logo are left out of text: they are hard to
// read on a light terminal background.
var (
	blue   = color{rgb: [3]uint8{0x2F, 0x7B, 0xFF}, ansi256: 33}
	orange = color{rgb: [3]uint8{0xFF, 0x8A, 0x1F}, ansi256: 208}
	cyan   = color{rgb: [3]uint8{0x33, 0xC8, 0xFF}, ansi256: 45}
	yellow = color{rgb: [3]uint8{0xFF, 0xC8, 0x3D}, ansi256: 221}
)

type color struct {
	rgb     [3]uint8
	ansi256 uint8
}

const reset = "\x1b[0m"

// Style colors strings for one output. The zero Style colors nothing.
type Style struct {
	on        bool
	trueColor bool
}

// For returns the style for output written to w: colored when w is a terminal, unless NO_COLOR is
// set or the terminal is dumb. CLICOLOR_FORCE=1 colors output that is not a terminal (watch --color
// is the case: watch gives the command a pipe); NO_COLOR still wins over it.
func For(w io.Writer) Style {
	return detect(isTerminal(w), os.Getenv)
}

func detect(terminal bool, getenv func(string) string) Style {
	if getenv("NO_COLOR") != "" {
		return Style{}
	}
	forced := getenv("CLICOLOR_FORCE") != "" && getenv("CLICOLOR_FORCE") != "0"
	if !forced && (!terminal || getenv("TERM") == "dumb") {
		return Style{}
	}
	ct := strings.ToLower(getenv("COLORTERM"))
	return Style{on: true, trueColor: ct == "truecolor" || ct == "24bit"}
}

// isTerminal reports whether w is a character device, without a dependency on x/term: a terminal
// is the only character device the CLI writes to.
func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// Enabled reports whether this style colors anything.
func (s Style) Enabled() bool { return s.on }

// Done is the color of finished work: the filled part of a progress bar.
func (s Style) Done(text string) string { return s.fg(blue, text) }

// Pending is the color of work that is complete but waits on something else, and of a state that
// needs attention without being a failure: paused, rebuilding, a warning.
func (s Style) Pending(text string) string { return s.fg(orange, text) }

// Failed is the color of a failure: the terminal's own red, in bold.
func (s Style) Failed(text string) string {
	if !s.on || text == "" {
		return text
	}
	return "\x1b[1;31m" + text + reset
}

func (s Style) fg(c color, text string) string {
	if !s.on || text == "" {
		return text
	}
	return s.code(38, c) + text + reset
}

// code is the escape sequence that sets the foreground (38) or the background (48) color.
func (s Style) code(layer int, c color) string {
	if s.trueColor {
		return fmt.Sprintf("\x1b[%d;2;%d;%d;%dm", layer, c.rgb[0], c.rgb[1], c.rgb[2])
	}
	return fmt.Sprintf("\x1b[%d;5;%dm", layer, c.ansi256)
}
