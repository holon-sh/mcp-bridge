package testutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/holon-sh/mcp-bridge/proxy"
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
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://example/mcp", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	req.Header.Set(proxy.HeaderProtocolVersion, proxy.CurrentVersion)
	req.Header.Set(proxy.HeaderMethod, method)
	if method == proxy.MethodCallTool {
		req.Header.Set(proxy.HeaderName, fmt.Sprint(params["name"]))
	}
	return req
}
