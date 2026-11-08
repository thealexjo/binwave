package termstyle

import "strings"

// markPixels is the binwave mark, pixel for pixel, as in docs/assets/binwave-mark-dark.svg:
// B blue, C cyan, Y yellow, O orange, a dot is empty. Blocks of color read on both a dark and a
// light background, so the mark keeps all four colors, unlike text.
var markPixels = [...]string{
	"........OO...",
	"........OO...",
	".....OO.OO...",
	".....YO.OO.OO",
	".....YY.OO.OO",
	".....YY.OO.OO",
	"..BCCCY.YO.OO",
	"..BBCC..YY.OO",
	"........YY.OO",
	"BBBBBBCCC..YO",
	"BBBBBBBCC..YY",
	"...........YY",
	".BBBBBBBBCCC.",
	".BBBBBBBBBC..",
}

var markColors = map[byte]color{'B': blue, 'C': cyan, 'Y': yellow, 'O': orange}

// MarkWidth is the width of a line of Mark, in terminal cells: the width of a row of markPixels.
const MarkWidth = 13

// Mark returns the binwave mark drawn with half blocks, two pixels to a terminal cell, one string
// per line; nil when the style colors nothing, because without color the mark is only noise.
func (s Style) Mark() []string {
	if !s.on {
		return nil
	}
	lines := make([]string, 0, (len(markPixels)+1)/2)
	for row := 0; row < len(markPixels); row += 2 {
		var b strings.Builder
		for col := range MarkWidth {
			top, bottom := pixel(row, col), pixel(row+1, col)
			switch {
			case top == nil && bottom == nil:
				b.WriteByte(' ')
			case bottom == nil:
				b.WriteString(s.code(38, *top) + "▀" + reset)
			case top == nil:
				b.WriteString(s.code(38, *bottom) + "▄" + reset)
			default:
				b.WriteString(s.code(38, *top) + s.code(48, *bottom) + "▀" + reset)
			}
		}
		lines = append(lines, b.String())
	}
	return lines
}

func pixel(row, col int) *color {
	if row >= len(markPixels) {
		return nil
	}
	c, ok := markColors[markPixels[row][col]]
	if !ok {
		return nil
	}
	return &c
}
