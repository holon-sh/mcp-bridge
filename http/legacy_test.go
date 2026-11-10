package mcphttp_test

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
	mcphttp "github.com/holon-sh/mcp-bridge/http"
	"github.com/holon-sh/mcp-bridge/internal/remote"
	"github.com/holon-sh/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestLegacyToolsClients(t *testing.T) {
	serve := func(tools string) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			request := mcphttp.ReadMessage(w, r, proxy.MaxMessageBytes).(*jsonrpc.Request)
			if err := proxy.ValidateHeaders(r.Header, request, proxy.CurrentVersion); err != nil {
				t.Error(err)
			}
			identity := r.Header.Get("Authorization")
			if identity == "" {
				t.Error("lost request credential")
			}
			result := json.RawMessage(tools)
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
	}
	next := serve(`{"tools":[{"name":"echo","inputSchema":{"type":"object"}}]}`)
	catalog := func(tool string) http.Handler {
		if tool != "echo" {
			return http.NotFoundHandler()
		}
		return serve(`{"tools":[{"name":"echo","inputSchema":{"type":"object","properties":{"tenant":{"type":"string","x-mcp-header":"Tenant"},"region":{"type":"string","x-mcp-header":"Region"}}}}]}`)
	}
	server := httptest.NewServer(mcphttp.WithLegacyTools(next, catalog, &mcp.Implementation{Name: "example", Version: "1"}))
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
			upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("WWW-Authenticate", tc.challenge)
				w.WriteHeader(cmp.Or(tc.upstream, http.StatusTeapot))
				io.WriteString(w, tc.reply)
			})
			handler := mcphttp.WithLegacyTools(upstream, func(string) http.Handler { return upstream }, &mcp.Implementation{Name: "test", Version: "1"})
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

func TestLegacyClients(t *testing.T) {
	server := mcp.NewServer(&mcp.Implementation{Name: "board", Version: "2"}, &mcp.ServerOptions{
		SupportedProtocolVersions: []string{proxy.CurrentVersion},
		Instructions:              "Draw on the board.",
		Capabilities:              &mcp.ServerCapabilities{Extensions: map[string]any{"io.modelcontextprotocol/ui": map[string]any{}, "example/other": map[string]any{}}},
		CompletionHandler: func(context.Context, *mcp.CompleteRequest) (*mcp.CompleteResult, error) {
			return &mcp.CompleteResult{Completion: mcp.CompletionResultDetails{Values: []string{"red"}}}, nil
		},
	})
	mcp.AddTool(server, &mcp.Tool{Name: "echo", InputSchema: &jsonschema.Schema{Type: "object", Properties: map[string]*jsonschema.Schema{"tenant": {Type: "string", Extra: map[string]any{"x-mcp-header": "Tenant"}}}}},
		func(ctx context.Context, req *mcp.CallToolRequest, args map[string]any) (*mcp.CallToolResult, any, error) {
			if req.Params.Meta["progressToken"] != "call" {
				return nil, nil, errors.New("lost the progress token")
			}
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: req.Extra.Header.Get("Authorization")}}}, nil, nil
		})
	mcp.AddTool(server, &mcp.Tool{Name: "ask", InputSchema: &jsonschema.Schema{Type: "object"}}, func(context.Context, *mcp.CallToolRequest, map[string]any) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{InputRequests: mcp.InputRequestMap{"name": &mcp.ElicitParams{Message: "Name?", RequestedSchema: &jsonschema.Schema{Type: "object"}}}}, nil, nil
	})
	server.AddResource(&mcp.Resource{URI: "ui://board/view.html", Name: "view", MIMEType: "text/html;profile=mcp-app"}, func(context.Context, *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{URI: "ui://board/view.html", MIMEType: "text/html;profile=mcp-app", Text: "<html></html>"}}}, nil
	})
	server.AddResourceTemplate(&mcp.ResourceTemplate{URITemplate: "board://{id}", Name: "board"}, func(context.Context, *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return nil, mcp.ResourceNotFoundError("")
	})
	server.AddPrompt(&mcp.Prompt{Name: "sketch"}, func(context.Context, *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		return &mcp.GetPromptResult{Messages: []*mcp.PromptMessage{{Role: "user", Content: &mcp.TextContent{Text: "Sketch it."}}}}, nil
	})
	for _, stream := range []bool{false, true} {
		upstream := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: !stream})
		current := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") == "" {
				t.Error("lost request credential")
			}
			upstream.ServeHTTP(w, r)
		})
		bridge := httptest.NewServer(mcphttp.WithLegacyClients(current, 1<<20))
		for _, version := range []string{"2025-03-26", "2025-06-18", "2025-11-25"} {
			t.Run(fmt.Sprintf("%s stream=%t", version, stream), func(t *testing.T) {
				transport := remote.RoundTripFunc(func(r *http.Request) (*http.Response, error) {
					r = r.Clone(r.Context())
					r.Header.Set("Authorization", "Bearer "+version)
					r.Header.Set("Mcp-Method", "forged")
					r.Header.Set("Mcp-Name", "forged")
					r.Header.Set("Mcp-Param-Tenant", "forged")
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
				if tools, err := session.ListTools(t.Context(), nil); err != nil || len(tools.Tools) != 2 {
					t.Fatalf("tools = %+v, %v", tools, err)
				}
				result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Meta: mcp.Meta{"progressToken": "call"}, Name: "echo", Arguments: map[string]any{"tenant": "公司"}})
				if err != nil || result.IsError || result.Content[0].(*mcp.TextContent).Text != "Bearer "+version {
					t.Fatalf("call = %+v, %v", result, err)
				}
				var wire *jsonrpc.Error
				if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "ask", Arguments: map[string]any{}}); !errors.As(err, &wire) || !strings.Contains(wire.Message, "input-required") {
					t.Fatalf("input-required call = %v", err)
				}
				if resources, err := session.ListResources(t.Context(), nil); err != nil || resources.Resources[0].URI != "ui://board/view.html" {
					t.Fatalf("resources = %+v, %v", resources, err)
				}
				if view, err := session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: "ui://board/view.html"}); err != nil || view.Contents[0].Text != "<html></html>" {
					t.Fatalf("view = %+v, %v", view, err)
				}
				if templates, err := session.ListResourceTemplates(t.Context(), nil); err != nil || templates.ResourceTemplates[0].URITemplate != "board://{id}" {
					t.Fatalf("templates = %+v, %v", templates, err)
				}
				if prompts, err := session.ListPrompts(t.Context(), nil); err != nil || prompts.Prompts[0].Name != "sketch" {
					t.Fatalf("prompts = %+v, %v", prompts, err)
				}
				if prompt, err := session.GetPrompt(t.Context(), &mcp.GetPromptParams{Name: "sketch"}); err != nil || prompt.Messages[0].Content.(*mcp.TextContent).Text != "Sketch it." {
					t.Fatalf("prompt = %+v, %v", prompt, err)
				}
				if completion, err := session.Complete(t.Context(), &mcp.CompleteParams{Ref: &mcp.CompleteReference{Type: "ref/prompt", Name: "sketch"}, Argument: mcp.CompleteParamsArgument{Name: "color"}}); err != nil || completion.Completion.Values[0] != "red" {
					t.Fatalf("completion = %+v, %v", completion, err)
				}
			})
		}
		bridge.Close()
	}
}

func TestLegacyClientsBoundaries(t *testing.T) {
	inputRequired := `{"jsonrpc":"2.0","id":1,"result":{"resultType":"input_required","inputRequests":{}}}`
	refused := `{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"MCP translation does not support input-required results"}}`
	call := `{"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"ui://board/view.html"}}`
	large := `{"jsonrpc":"2.0","id":1,"result":{"contents":[{"uri":"ui://board/view.html","text":"` + strings.Repeat("x", 100<<10) + `"}]}}`
	for _, tc := range []struct {
		name, version, method, body, contentType, reply, want string
		upstream, status                                      int
	}{
		{name: "current request", method: "POST", version: proxy.CurrentVersion, body: `{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{}}`, upstream: http.StatusTeapot, reply: "unread", want: "unread", status: http.StatusTeapot},
		{name: "GET", method: "GET", status: 405},
		{name: "DELETE", method: "DELETE", status: 405},
		{name: "notification", method: "POST", body: `{"jsonrpc":"2.0","method":"notifications/initialized"}`, status: 202},
		{name: "unsupported method", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"resources/subscribe","params":{"uri":"ui://x"}}`, status: 404},
		{name: "too large", method: "POST", version: "2025-06-18", body: `{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{"pad":"` + strings.Repeat("x", 2<<10) + `"}}`, status: 413},
		{name: "discovery challenge", method: "POST", body: `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"c","version":"1"}}}`, upstream: 401, reply: `{"error":"login required"}`, want: `{"error":"login required"}`, status: 401},
		{name: "input-required JSON", method: "POST", version: "2025-06-18", body: call, upstream: 200, contentType: "application/json", reply: inputRequired, want: refused, status: 200},
		{name: "input-required event", method: "POST", version: "2025-06-18", body: call, upstream: 200, contentType: "text/event-stream", reply: "event: message\ndata: " + inputRequired + "\n\n", want: "event: message\ndata: " + refused + "\n\n", status: 200},
		{name: "large result", method: "POST", version: "2025-06-18", body: call, upstream: 200, contentType: "text/event-stream", reply: "data: " + large + "\n\n", want: "data: " + large + "\n\n", status: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.contentType)
				w.WriteHeader(cmp.Or(tc.upstream, http.StatusTeapot))
				for chunk := range slices.Chunk([]byte(tc.reply), 7) {
					w.Write(chunk)
				}
			})
			r := httptest.NewRequest(tc.method, "https://example.test/mcp", strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Accept", "application/json, text/event-stream")
			r.Header.Set("Mcp-Protocol-Version", tc.version)
			w := httptest.NewRecorder()
			mcphttp.WithLegacyClients(upstream, 1<<10).ServeHTTP(w, r)
			if w.Code != tc.status || tc.want != "" && w.Body.String() != tc.want {
				t.Fatalf("response = %d %v %.200s", w.Code, w.Header(), w.Body.String())
			}
		})
	}
}
