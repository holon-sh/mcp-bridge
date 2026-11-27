//go:build !unix

package mcphttp

import (
	"errors"
	"os/exec"
	"syscall"
)

// isolate refuses to run cmd: stopping a server and its descendants needs Unix
// process groups. The rest of the module, such as mcp-bridge connect, works here.
func isolate(*exec.Cmd) error { return errors.New("mcp-bridge serve requires a Unix process host") }

func signal(cmd *exec.Cmd, _ syscall.Signal) { _ = cmd.Process.Kill() }
