// SPDX-License-Identifier: AGPL-3.0-only
//
// Derived from juju (https://github.com/juju/juju), AGPL-3.0: core/output/output.go (the status colours).

package main

import (
	"io"
	"os"
	"regexp"
	"strings"
)

// Colours as juju's status uses them (core/output): good states green, busy ones yellow, bad ones red.
const (
	reset  = "\033[0m"
	red    = "\033[31m"
	green  = "\033[32m"
	yellow = "\033[33m"
	cyan   = "\033[36m"
	gray   = "\033[90m"
)

var stateColors = map[string]string{
	"active": green, "running": green, "idle": green, "executing": green, "joined": green,
	"allocating": yellow, "lost": yellow, "maintenance": yellow, "waiting": yellow, "unknown": yellow, "joining": yellow,
	"suspended": yellow, "removing": yellow, "stopped": yellow,
	"blocked": red, "error": red, "failed": red, "terminated": red,
}

// wantColor is whether to colour w: --color forces it, --no-color or NO_COLOR turn it off, else it needs a terminal.
func wantColor(w io.Writer, force, off bool) bool {
	switch {
	case off:
		return false
	case force:
		return true
	}
	if _, ok := os.LookupEnv("NO_COLOR"); ok {
		return false
	}
	return isTerminal(w)
}

// columns that get a colour, by header name: the cell's word picks it, or a fixed colour.
var (
	stateColumns = map[string]bool{"Status": true, "Workload": true, "Agent": true}
	fixedColumns = map[string]string{"Address": cyan, "Message": gray}
)

var headerCell = regexp.MustCompile(`\S+(?: \S+)*`)

// colorize adds colour codes to rendered tabular status lines. A table is a header line after a blank line (or the
// first line), and the colours go on by header name, so the alignment the tab writer made stays as it was.
func colorize(lines []string) {
	var cols []struct {
		name       string
		start, end int
	}
	header := true
	for i, line := range lines {
		if line == "" {
			header = true
			continue
		}
		if header {
			header = false
			cols = cols[:0]
			for _, m := range headerCell.FindAllStringIndex(line, -1) {
				cols = append(cols, struct {
					name       string
					start, end int
				}{line[m[0]:m[1]], m[0], len(line)})
			}
			for j := 0; j+1 < len(cols); j++ {
				cols[j].end = cols[j+1].start
			}
			continue
		}
		// Work from the right so earlier offsets stay valid.
		for j := len(cols) - 1; j >= 0; j-- {
			c := cols[j]
			if c.start >= len(line) {
				continue
			}
			end := min(c.end, len(line))
			cell := line[c.start:end]
			word := strings.TrimRight(cell, " ")
			if word == "" {
				continue
			}
			code := fixedColumns[c.name]
			if stateColumns[c.name] {
				code = stateColors[word]
			}
			if code == "" {
				continue
			}
			line = line[:c.start] + code + word + reset + line[c.start+len(word):]
		}
		lines[i] = line
	}
}
