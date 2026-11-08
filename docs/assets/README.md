# The binwave mark

The final exports of the mark for the repository. The sources and all iterations live outside the repository.

| File | Where it is used |
|---|---|
| `binwave-mark-dark.svg` | README on the dark GitHub theme |
| `binwave-mark-light.svg` | README on the light GitHub theme |

The same mark, pixel for pixel, is drawn with half blocks in `binwave version` in the terminal (`internal/termstyle/mark.go`): when the mark changes, both places change.

Palette:

| Color | Dark background | Light background | Where else |
|---|---|---|---|
| Blue | `#2F7BFF` | `#1D5FE6` | the done part of the progress bar in the CLI, the main series of the dashboard |
| Light blue | `#33C8FF` | `#0AA2D8` | only the mark and the dashboard: on a light terminal, text in this color is hard to read |
| Orange | `#FF8A1F` | `#EE6A00` | the second part of the progress bar, `paused`, warnings, lag on the dashboard |
| Yellow | `#FFC83D` | `#F2A100` | only the mark |
| Dark blue | `#0C1A3A` | — | the background of the mark with a plate |

Red in the CLI is not a color of the mark but the red of the terminal theme: it marks only a failure.
