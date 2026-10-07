package agent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// HookSpec describes one hook process.
type HookSpec struct {
	Name   string
	Dir    string   // charm directory, the working directory
	Path   string   // the dispatch script
	Env    []string // the complete environment (not inherited)
	Output io.Writer
	// Args are extra arguments to Path.
	Args []string
	// Stdout and Stderr, when set, receive the process's streams instead of Output.
	Stdout, Stderr io.Writer
}

// HookRunner runs a hook process. Missing is true when the dispatch script does not exist (the hook counts as not implemented).
type HookRunner interface {
	Run(ctx context.Context, spec HookSpec) (exit int, missing bool, err error)
}

// ExecRunner runs hooks as child processes, in their own process group so that a shutdown kills the whole tree.
type ExecRunner struct {
	// KillDelay is how long a hook gets after SIGTERM before SIGKILL (default 5s).
	KillDelay time.Duration
}

// Run implements HookRunner. A non-zero exit status is a normal result, not an error.
func (r ExecRunner) Run(ctx context.Context, spec HookSpec) (int, bool, error) {
	if _, err := os.Stat(spec.Path); err != nil {
		if os.IsNotExist(err) {
			return 0, true, nil
		}
		return -1, false, err
	}
	cmd := exec.CommandContext(ctx, spec.Path, spec.Args...)
	cmd.Dir = spec.Dir
	cmd.Env = spec.Env
	cmd.Stdout, cmd.Stderr = spec.Output, spec.Output
	if spec.Stdout != nil {
		cmd.Stdout = spec.Stdout
	}
	if spec.Stderr != nil {
		cmd.Stderr = spec.Stderr
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) }
	cmd.WaitDelay = r.KillDelay
	if cmd.WaitDelay == 0 {
		cmd.WaitDelay = 5 * time.Second
	}
	err := cmd.Run()
	if err == nil {
		return 0, false, nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ctx.Err() != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return ee.ExitCode(), false, nil
	}
	return -1, false, err
}

// lineWriter prefixes each line written to it and passes it to out, one write per line.
type lineWriter struct {
	mu     sync.Mutex
	out    io.Writer
	prefix string
	buf    bytes.Buffer
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf.Write(p)
	for {
		i := bytes.IndexByte(w.buf.Bytes(), '\n')
		if i < 0 {
			break
		}
		line := w.buf.Next(i + 1)
		_, _ = io.WriteString(w.out, w.prefix+string(bytes.TrimRight(line, "\r\n"))+"\n")
	}
	return len(p), nil
}

// Flush writes any final unterminated line.
func (w *lineWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.buf.Len() > 0 {
		_, _ = io.WriteString(w.out, w.prefix+w.buf.String()+"\n")
		w.buf.Reset()
	}
}
