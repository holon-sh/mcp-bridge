package mcpstdio

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"github.com/hughhan1/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Run exposes a remote MCP server through a local transport and closes both
// connections when forwarding ends. A local EOF or closed local output completes
// normally. Remote disconnects, cancellation, and other transport failures return
// errors.
func Run(ctx context.Context, remote, local mcp.Transport) error {
	if remote == nil || local == nil {
		return errors.New("remote and local transports are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	downstream, err := local.Connect(ctx)
	if err != nil {
		return err
	}
	defer downstream.Close()
	if httpRemote, ok := remote.(*mcp.StreamableClientTransport); ok {
		localConnection := downstream
		stop := context.AfterFunc(ctx, func() { localConnection.Close() })
		message, err := downstream.Read(ctx)
		stop()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		downstream = &replayConnection{Connection: downstream, first: message}
		if req, ok := message.(*jsonrpc.Request); ok && req.Method == "initialize" && req.IsCall() {
			var initialize mcp.InitializeParams
			if err := json.Unmarshal(req.Params, &initialize); err == nil && initialize.ClientInfo != nil && initialize.Capabilities != nil && proxy.IsLegacyVersion(initialize.ProtocolVersion) {
				translate, err := requiresTranslation(ctx, httpRemote, &initialize)
				if err != nil {
					return err
				}
				if translate {
					return runLegacy(ctx, httpRemote, downstream, &initialize)
				}
			}
		}
	}
	upstream, err := connectRemote(ctx, remote)
	if err != nil {
		return err
	}
	return proxy.Relay(ctx, upstream, downstream)
}
