package mcphttp

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/holon-sh/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type processConnection struct {
	mcp.Connection
	cmd     *exec.Cmd
	stdin   *os.File
	log     io.Writer
	exited  chan struct{}
	once    sync.Once
	writeMu sync.Mutex
}

// startProcess runs cmd in its own process group, so stopping it also stops
// its descendants.
func startProcess(ctx context.Context, cmd *exec.Cmd) (*processConnection, error) {
	in, input, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	output, out, err := os.Pipe()
	if err != nil {
		in.Close()
		input.Close()
		return nil, err
	}
	attrs := *cmp.Or(cmd.SysProcAttr, &syscall.SysProcAttr{})
	attrs.Setpgid = true
	cmd.SysProcAttr = &attrs
	cmd.Stdin, cmd.Stdout = in, out
	cmd.WaitDelay = min(cmp.Or(cmd.WaitDelay, shutdownGrace), shutdownGrace)
	err = cmd.Start()
	in.Close()
	out.Close()
	if err != nil {
		input.Close()
		output.Close()
		return nil, fmt.Errorf("start MCP process: %w", err)
	}
	// IOTransport.Connect only wraps the pipes and cannot fail.
	conn, _ := (&mcp.IOTransport{Reader: output, Writer: input, MaxLineLength: proxy.MaxMessageBytes}).Connect(ctx)
	p := &processConnection{Connection: conn, cmd: cmd, stdin: input, log: cmp.Or(cmd.Stderr, io.Discard), exited: make(chan struct{})}
	go func() { _ = cmd.Wait(); p.signal(syscall.SIGKILL); close(p.exited) }()
	return p, nil
}

func (p *processConnection) signal(signal syscall.Signal) {
	_ = syscall.Kill(-p.cmd.Process.Pid, signal)
}

func (p *processConnection) Write(ctx context.Context, msg jsonrpc.Message) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	if err := p.stdin.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return p.Connection.Write(ctx, msg)
}

// Close closes the process's input, then signals its process group with
// SIGTERM and SIGKILL until it exits.
func (p *processConnection) Close() error {
	p.once.Do(func() {
		_ = p.stdin.Close()
		for _, signal := range []syscall.Signal{syscall.SIGTERM, syscall.SIGKILL} {
			select {
			case <-p.exited:
			case <-time.After(shutdownGrace):
				p.signal(signal)
			}
		}
		<-p.exited
		_ = p.Connection.Close()
	})
	return nil
}
