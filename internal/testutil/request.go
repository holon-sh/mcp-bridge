package testutil

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/holon-sh/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

func Request(t *testing.T, method string, params map[string]any) *http.Request {
	t.Helper()
	if params == nil {
		params = map[string]any{}
	}
	meta, _ := params["_meta"].(map[string]any)
	if meta == nil {
		meta = map[string]any{}
		params["_meta"] = meta
	}
	meta["io.modelcontextprotocol/protocolVersion"] = proxy.CurrentVersion
	meta["io.modelcontextprotocol/clientInfo"] = map[string]any{"name": "test", "version": "1"}
	meta["io.modelcontextprotocol/clientCapabilities"] = map[string]any{"extensions": map[string]any{"io.modelcontextprotocol/tasks": map[string]any{}}}
	id, _ := jsonrpc.MakeID(float64(1))
	body, _ := json.Marshal(params)
	message := &jsonrpc.Request{ID: id, Method: method, Params: body}
	raw, _ := jsonrpc.EncodeMessage(message)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://example/mcp", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set(proxy.HeaderProtocolVersion, proxy.CurrentVersion)
	if err := proxy.SetToolHeaders(req.Header, message, nil); err != nil {
		t.Fatal(err)
	}
	return req
}
