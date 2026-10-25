package mcpstdio

import (
	"encoding/json"
	"testing"

	"github.com/holon-sh/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestRemoteWriteRejections(t *testing.T) {
	for _, test := range []struct {
		name, method, params string
		requests             int
		closed               bool
	}{
		{name: "closed", method: "tools/list", params: `{}`, requests: 1, closed: true},
		{name: "malformed initialize", method: "initialize", params: `{"protocolVersion":"2025-11-25","capabilities":1}`, requests: 1},
		{name: "current initialize", method: "initialize", params: `{"protocolVersion":"2026-07-28"}`, requests: 1},
		{name: "request limit", method: "tools/list", params: `{}`, requests: proxy.MaxRequests + 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			remote, upstream := mcp.NewInMemoryTransports()
			peer, _ := upstream.Connect(t.Context())
			go func() {
				for _, err := peer.Read(t.Context()); err == nil; _, err = peer.Read(t.Context()) {
				}
			}()
			conn, err := connectRemote(t.Context(), remote)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if test.closed {
				conn.Close()
			}
			for i := range test.requests {
				id, _ := jsonrpc.MakeID(float64(i))
				err = conn.Write(t.Context(), &jsonrpc.Request{ID: id, Method: test.method, Params: json.RawMessage(test.params)})
			}
			if err == nil {
				t.Fatal("remote accepted the write")
			}
		})
	}
}
