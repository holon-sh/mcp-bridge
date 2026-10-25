package mcphttp

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
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

func startProcess(ctx context.Context, cmd *exec.Cmd) (*processConnection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cmd == nil || cmd.Process != nil || cmd.Stdin != nil || cmd.Stdout != nil {
		return nil, errors.New("bridge requires an unstarted command with unassigned stdin and stdout")
	}
	if err := prepareProcess(cmd); err != nil {
		return nil, err
	}
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
	cmd.Stdin, cmd.Stdout = in, out
	logger := &lockedWriter{Writer: cmp.Or(cmd.Stderr, io.Discard)}
	cmd.Stderr = logger
	cmd.WaitDelay = min(cmp.Or(cmd.WaitDelay, shutdownGrace), shutdownGrace)
	if err := cmd.Start(); err != nil {
		in.Close()
		input.Close()
		output.Close()
		out.Close()
		return nil, fmt.Errorf("start MCP process: %w", err)
	}
	in.Close()
	out.Close()
	// IOTransport.Connect only wraps the pipes and cannot fail.
	conn, _ := (&mcp.IOTransport{Reader: output, Writer: input, MaxLineLength: proxy.MaxMessageBytes}).Connect(ctx)
	p := &processConnection{Connection: conn, cmd: cmd, stdin: input, log: logger, exited: make(chan struct{})}
	go func() { _ = cmd.Wait(); signalProcess(cmd, true); close(p.exited) }()
	return p, nil
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

func (p *processConnection) Close() error {
	p.once.Do(func() {
		_ = p.stdin.Close()
		select {
		case <-p.exited:
		case <-time.After(shutdownGrace):
			signalProcess(p.cmd, false)
			select {
			case <-p.exited:
			case <-time.After(shutdownGrace):
				signalProcess(p.cmd, true)
				<-p.exited
			}
		}
		_ = p.Connection.Close()
	})
	return nil
}
