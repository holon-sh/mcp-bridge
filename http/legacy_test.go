package mcphttp_test

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
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

func TestLegacyClients(t *testing.T) {
	server := mcp.NewServer(&mcp.Implementation{Name: "board", Version: "2"}, &mcp.ServerOptions{
		SupportedProtocolVersions: []string{proxy.CurrentVersion},
		Instructions:              "Draw on the board.",
		Capabilities:              &mcp.ServerCapabilities{Extensions: map[string]any{"io.modelcontextprotocol/ui": map[string]any{}, "example/other": map[string]any{}}},
		CompletionHandler: func(context.Context, *mcp.CompleteRequest) (*mcp.CompleteResult, error) {
			return &mcp.CompleteResult{Completion: mcp.CompletionResultDetails{Values: []string{"red"}}}, nil
		},
	})
	schema := json.RawMessage(`{"type":"object","properties":{"tenant":{"type":"string","x-mcp-header":"Tenant"},"region":{"type":"string","x-mcp-header":"Region"}}}`)
	server.AddTool(&mcp.Tool{Name: "echo", InputSchema: schema},
		func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			if req.Params.Meta["progressToken"] != "call" || !bytes.Contains(req.Params.Arguments, []byte("9007199254740993")) {
				return nil, errors.New("changed request params")
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: req.Extra.Header.Get("Authorization")}}}, nil
		})
	server.AddResource(&mcp.Resource{URI: "ui://board/view.html", Name: "view"}, func(context.Context, *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{URI: "ui://board/view.html", Text: "<html></html>"}}}, nil
	})
	server.AddResourceTemplate(&mcp.ResourceTemplate{URITemplate: "board://{id}", Name: "board"}, func(context.Context, *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return nil, mcp.ResourceNotFoundError("")
	})
	server.AddPrompt(&mcp.Prompt{Name: "sketch"}, func(context.Context, *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		return &mcp.GetPromptResult{Messages: []*mcp.PromptMessage{{Role: "user", Content: &mcp.TextContent{Text: "Sketch it."}}}}, nil
	})
	for _, stream := range []bool{false, true} {
		current := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: !stream})
		bridge := httptest.NewServer(mcphttp.WithLegacyClients(current, func(string) http.Handler { return current }, proxy.MaxMessageBytes))
		defer bridge.Close()
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
					return bridge.Client().Transport.RoundTrip(r)
				})
				client := mcp.NewClient(&mcp.Implementation{Name: "client", Version: "1"}, nil)
				session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: bridge.URL, HTTPClient: &http.Client{Transport: transport}}, &mcp.ClientSessionOptions{ProtocolVersion: version})
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				initialized := session.InitializeResult()
				caps := initialized.Capabilities
				if initialized.ProtocolVersion != version || initialized.ServerInfo.Name != "board" || initialized.Instructions != "Draw on the board." ||
					caps.Tools == nil || caps.Resources == nil || caps.Prompts == nil || caps.Completions == nil || caps.Logging != nil ||
					!reflect.DeepEqual(caps.Extensions, map[string]any{"io.modelcontextprotocol/ui": map[string]any{}}) {
					t.Fatalf("initialize = %+v %+v", initialized, caps)
				}
				if tools, err := session.ListTools(t.Context(), nil); err != nil || tools.Tools[0].Name != "echo" {
					t.Fatalf("tools = %+v, %v", tools, err)
				}
				result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Meta: mcp.Meta{"progressToken": "call"}, Name: "echo", Arguments: json.RawMessage(`{"tenant":"公司","number":9007199254740993}`)})
				if err != nil || result.IsError || result.Content[0].(*mcp.TextContent).Text != "Bearer "+version {
					t.Fatalf("call = %+v, %v", result, err)
				}
				var wire *jsonrpc.Error
				if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"tenant": 1.5}}); !errors.As(err, &wire) || wire.Code != jsonrpc.CodeInvalidParams {
					t.Fatalf("invalid header argument = %v", err)
				}
				if view, err := session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: "ui://board/view.html"}); err != nil || view.Contents[0].Text != "<html></html>" {
					t.Fatalf("view = %+v, %v", view, err)
				}
				if resources, err := session.ListResources(t.Context(), nil); err != nil || len(resources.Resources) != 1 {
					t.Fatalf("resources = %+v, %v", resources, err)
				}
				if templates, err := session.ListResourceTemplates(t.Context(), nil); err != nil || len(templates.ResourceTemplates) != 1 {
					t.Fatalf("templates = %+v, %v", templates, err)
				}
				if prompts, err := session.ListPrompts(t.Context(), nil); err != nil || len(prompts.Prompts) != 1 {
					t.Fatalf("prompts = %+v, %v", prompts, err)
				}
				if prompt, err := session.GetPrompt(t.Context(), &mcp.GetPromptParams{Name: "sketch"}); err != nil || len(prompt.Messages) != 1 {
					t.Fatalf("prompt = %+v, %v", prompt, err)
				}
				if completion, err := session.Complete(t.Context(), &mcp.CompleteParams{Ref: &mcp.CompleteReference{Type: "ref/prompt", Name: "sketch"}, Argument: mcp.CompleteParamsArgument{Name: "color"}}); err != nil || completion.Completion.Values[0] != "red" {
					t.Fatalf("completion = %+v, %v", completion, err)
				}
			})
		}
	}
}

func TestLegacyClientsBoundaries(t *testing.T) {
	call := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo"}}`
	initialize := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"c","version":"1"}}}`
	for _, tc := range []struct {
		name, version, method, body, origin, contentType, challenge, reply, want string
		upstream, status                                                         int
	}{
		{name: "current request", method: "POST", version: proxy.CurrentVersion, body: `{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}}`, reply: "unread", want: "unread", status: http.StatusTeapot},
		{name: "GET", method: "GET", status: 405},
		{name: "ping", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"ping"}`, status: 200},
		{name: "client response", method: "POST", body: `{"jsonrpc":"2.0","id":1,"result":{}}`, status: 202},
		{name: "unknown cancellation", method: "POST", body: `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}`, status: 202},
		{name: "foreign origin", method: "POST", origin: "https://other.example", status: 403},
		{name: "too large", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"pad":"` + strings.Repeat("x", 1<<10) + `"}}`, status: 413},
		{name: "null params", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":null}`, status: 400},
		{name: "missing tool name", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"arguments":{}}}`, status: 400},
		{name: "unadvertised subscription", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"subscriptions/listen","params":{}}`, status: 404},
		{name: "catalog authorization challenge", method: "POST", version: "2025-06-18", body: call, upstream: 401, challenge: `Bearer resource_metadata="https://example.test/metadata"`, reply: `{"error":"login required"}`, want: `{"error":"login required"}`, status: 401},
		{name: "tools/list", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`, upstream: 200, reply: `{"jsonrpc":"2.0","id":1,"result":{}}`, want: `{"jsonrpc":"2.0","id":1,"result":{}}`, status: 200},
		{name: "invalid catalog", method: "POST", version: "2025-06-18", body: call, upstream: 200, reply: `{"jsonrpc":"2.0","id":1,"result":{}}`, status: 502},
		{name: "catalog JSON-RPC error", method: "POST", version: "2025-06-18", body: call, upstream: 200, reply: `{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"session inactive","data":{"retry":false}}}`, want: `{"jsonrpc":"2.0","id":1,"error":{"code":-32001,"message":"session inactive","data":{"retry":false}}}`, status: 200},
		{name: "unreadable discovery", method: "POST", body: initialize, upstream: 200, contentType: "text/event-stream", reply: "event: message\n\n", status: 502},
		{name: "discovery challenge", method: "POST", body: initialize, upstream: 401, challenge: `Bearer realm="example"`, reply: `{"error":"login required"}`, want: `{"error":"login required"}`, status: 401},
		{name: "discovery split across data fields", method: "POST", body: initialize, upstream: 200, contentType: "text/event-stream",
			reply: "event: message\ndata: {\"jsonrpc\":\"2.0\",\ndata: \"id\":1,\"result\":{\"supportedVersions\":[\"2026-07-28\"],\"capabilities\":{\"tools\":{}}}}\n\n", status: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", cmp.Or(tc.contentType, "application/json"))
				w.Header().Set("WWW-Authenticate", tc.challenge)
				w.WriteHeader(cmp.Or(tc.upstream, http.StatusTeapot))
				io.WriteString(w, tc.reply)
			})
			handler := mcphttp.WithLegacyClients(upstream, func(string) http.Handler { return upstream }, 1<<10)
			r := httptest.NewRequest(tc.method, "https://example.test/mcp", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Accept", "application/json, text/event-stream")
			r.Header.Set("Mcp-Protocol-Version", tc.version)
			r.Header.Set("Origin", tc.origin)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if w.Code != tc.status || w.Header().Get("WWW-Authenticate") != tc.challenge || tc.want != "" && w.Body.String() != tc.want {
				t.Fatalf("response = %d %v %s", w.Code, w.Header(), w.Body.String())
			}
		})
	}
}
