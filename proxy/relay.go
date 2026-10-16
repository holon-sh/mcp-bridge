package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Relay forwards MCP messages between an upstream server and a downstream client.
// It closes both connections when forwarding ends or ctx is cancelled.
// A downstream EOF or closed downstream output completes normally; upstream
// disconnects, cancellation, and other transport failures return errors.
func Relay(ctx context.Context, upstream, downstream mcp.Connection) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 2)
	var pumps sync.WaitGroup
	pump := func(from, to mcp.Connection, fromUpstream bool) error {
		for {
			msg, err := from.Read(ctx)
			switch {
			case errors.Is(err, io.EOF) && !fromUpstream:
				return nil
			case errors.Is(err, io.EOF):
				return errors.New("MCP upstream closed")
			case err != nil:
				return err
			}
			if data, _ := jsonrpc.EncodeMessage(msg); len(data) > MaxMessageBytes {
				return errors.New("MCP message limit exceeded")
			}
			err = to.Write(ctx, msg)
			if fromUpstream && (errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, mcp.ErrConnectionClosed)) {
				return nil
			}
			if err != nil {
				return err
			}
		}
	}
	pumps.Go(func() { results <- pump(downstream, upstream, false) })
	pumps.Go(func() { results <- pump(upstream, downstream, true) })
	var err error
	select {
	case err = <-results:
	case <-ctx.Done():
		err = ctx.Err()
	}
	cancel()
	pumps.Go(func() { _ = upstream.Close() })
	pumps.Go(func() { _ = downstream.Close() })
	pumps.Wait()
	if err != nil {
		return fmt.Errorf("MCP relay: %w", err)
	}
	return nil
}
