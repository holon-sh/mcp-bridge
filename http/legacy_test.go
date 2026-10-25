package mcphttp_test

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	mcphttp "github.com/holon-sh/mcp-bridge/http"
	"github.com/holon-sh/mcp-bridge/internal/remote"
	"github.com/holon-sh/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestLegacyToolsClients(t *testing.T) {
	backend := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mcphttp.ReadMessage(w, r, proxy.MaxMessageBytes).(*jsonrpc.Request)
		if err := proxy.ValidateHeaders(r.Header, request, proxy.CurrentVersion); err != nil {
			t.Error(err)
		}
		identity := r.Header.Get("Authorization")
		if identity == "" {
			t.Error("lost request credential")
		}
		result := json.RawMessage(`{"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"tenant":{"type":"string","x-mcp-header":"Tenant"},"region":{"type":"string","x-mcp-header":"Region"}}}}]}`)
		headers := http.Header{"Mcp-Protocol-Version": {proxy.CurrentVersion}, "Mcp-Method": {request.Method}}
		if request.Method == "tools/call" {
			headers["Mcp-Name"] = []string{"echo"}
			headers["Mcp-Param-Tenant"] = []string{proxy.EncodeHeader("公司")}
			if !bytes.Contains(request.Params, []byte(`9007199254740993`)) || !bytes.Contains(request.Params, []byte(`"progressToken":"call"`)) {
				t.Errorf("changed request params: %s", request.Params)
			}
			result, _ = json.Marshal(map[string]any{"content": []map[string]string{{"type": "text", "text": identity}}})
		}
		received := maps.Clone(r.Header)
		maps.DeleteFunc(received, func(key string, _ []string) bool { return !strings.HasPrefix(key, "Mcp-") })
		if !reflect.DeepEqual(received, headers) {
			t.Errorf("MCP headers = %v, want %v", received, headers)
		}
		data, _ := jsonrpc.EncodeMessage(&jsonrpc.Response{ID: request.ID, Result: result})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	})
	server := httptest.NewServer(mcphttp.WithLegacyTools(backend, &mcp.Implementation{Name: "example", Version: "1"}))
	defer server.Close()
	for _, version := range []string{"2025-03-26", "2025-06-18", "2025-11-25"} {
		t.Run(version, func(t *testing.T) {
			transport := remote.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("Mcp-Session-Id") != "" {
					t.Error("bridge allocated a persistent session")
				}
				r = r.Clone(r.Context())
				r.Header.Set("Authorization", "Bearer "+version)
				r.Header.Set("Mcp-Method", "forged")
				r.Header.Set("Mcp-Name", "forged")
				r.Header.Set("Mcp-Param-Tenant", "forged")
				r.Header.Set("Mcp-Param-Region", "forged")
				return server.Client().Transport.RoundTrip(r)
			})
			client := mcp.NewClient(&mcp.Implementation{Name: "client", Version: "1"}, nil)
			session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: server.URL, HTTPClient: &http.Client{Transport: transport}}, &mcp.ClientSessionOptions{ProtocolVersion: version})
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			if got := session.InitializeResult(); got.ProtocolVersion != version || got.Capabilities.Tools == nil {
				t.Fatalf("initialize = %+v", got)
			}
			if tools, err := session.ListTools(t.Context(), nil); err != nil || tools.Tools[0].Name != "echo" {
				t.Fatalf("tools = %+v, %v", tools, err)
			}
			result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Meta: mcp.Meta{"progressToken": "call"}, Name: "echo", Arguments: json.RawMessage(`{"tenant":"公司","number":9007199254740993}`)})
			if err != nil || result.Content[0].(*mcp.TextContent).Text != "Bearer "+version {
				t.Fatalf("call = %+v, %v", result, err)
			}
			var wire *jsonrpc.Error
			if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"tenant": 1.5}}); !errors.As(err, &wire) || wire.Code != jsonrpc.CodeInvalidParams {
				t.Fatalf("invalid header argument = %v", err)
			}
		})
	}
}

func TestLegacyToolsBoundaries(t *testing.T) {
	call := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo"}}`
	for _, tc := range []struct {
		name, version, method, body, origin, challenge, reply string
		upstream, status                                      int
	}{
		{name: "current request", method: "POST", version: proxy.CurrentVersion, body: `{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}}`, status: http.StatusTeapot},
		{name: "GET", method: "GET", status: 405},
		{name: "ping", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"ping"}`, status: 200},
		{name: "client response", method: "POST", body: `{"jsonrpc":"2.0","id":1,"result":{}}`, status: 202},
		{name: "unknown cancellation", method: "POST", body: `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}`, status: 202},
		{name: "foreign origin", method: "POST", origin: "https://other.example", status: 403},
		{name: "null params", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":null}`, status: 400},
		{name: "missing tool name", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"arguments":{}}}`, status: 400},
		{name: "unadvertised subscription", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"subscriptions/listen","params":{}}`, status: 404},
		{name: "catalog authorization challenge", method: "POST", version: "2025-06-18", body: call, upstream: 401, challenge: `Bearer resource_metadata="https://example.test/metadata"`, reply: `{"error":"login required"}`, status: 401},
		{name: "tools/list", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`, upstream: 200, reply: `{"jsonrpc":"2.0","id":1,"result":{}}`, status: 200},
		{name: "invalid catalog", method: "POST", version: "2025-06-18", body: call, upstream: 200, reply: `{"jsonrpc":"2.0","id":1,"result":{}}`, status: 502},
		{name: "catalog JSON-RPC error", method: "POST", version: "2025-06-18", body: call, upstream: 200, reply: `{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"session inactive","data":{"retry":false}}}`, status: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := mcphttp.WithLegacyTools(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("WWW-Authenticate", tc.challenge)
				w.WriteHeader(cmp.Or(tc.upstream, http.StatusTeapot))
				io.WriteString(w, tc.reply)
			}), &mcp.Implementation{Name: "test", Version: "1"})
			r := httptest.NewRequest(tc.method, "https://example.test/mcp", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Accept", "application/json, text/event-stream")
			r.Header.Set("Mcp-Protocol-Version", tc.version)
			r.Header.Set("Origin", tc.origin)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.status || w.Header().Get("WWW-Authenticate") != tc.challenge || tc.status == tc.upstream && w.Body.String() != tc.reply {
				t.Fatalf("response = %d %v %s", w.Code, w.Header(), w.Body.String())
			}
		})
	}
}
