//go:build unix

package mcphttp

import (
	"cmp"
	"os/exec"
	"syscall"
)

// isolate runs cmd in its own process group, so signalling the group also
// reaches its descendants.
func isolate(cmd *exec.Cmd) error {
	attrs := *cmp.Or(cmd.SysProcAttr, &syscall.SysProcAttr{})
	attrs.Setpgid = true
	cmd.SysProcAttr = &attrs
	return nil
}

func signal(cmd *exec.Cmd, signal syscall.Signal) { _ = syscall.Kill(-cmd.Process.Pid, signal) }
