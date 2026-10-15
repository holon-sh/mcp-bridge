package proxy_test

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mcphttp "github.com/hughhan1/mcp-bridge/http"
	"github.com/hughhan1/mcp-bridge/internal/testutil"
	"github.com/hughhan1/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/auth"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func legacyTestTransport(t *testing.T, handler http.HandlerFunc) (http.RoundTripper, string) {
	t.Helper()
	upstream := httptest.NewTLSServer(handler)
	t.Cleanup(upstream.Close)
	return proxy.NewRemoteTransport(upstream.Client().Transport), upstream.URL
}

func legacyTestRejectCurrent(handler http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Mcp-Protocol-Version") == proxy.CurrentVersion {
			req, ok := mcphttp.ReadMessage(w, r, mcp.DefaultMaxLineLength).(*jsonrpc.Request)
			if !ok {
				return
			}
			if req.Method != "notifications/initialized" {
				mcphttp.WriteError(w, http.StatusBadRequest, req.ID, proxy.UnsupportedVersion(proxy.CurrentVersion, []string{"2025-06-18"}))
				return
			}
		}
		handler.ServeHTTP(w, r)
	}
}

func legacyTestProvider(version string, call func(http.ResponseWriter, *http.Request, *jsonrpc.Request)) http.HandlerFunc {
	return legacyTestRejectCurrent(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		req, ok := mcphttp.ReadMessage(w, r, mcp.DefaultMaxLineLength).(*jsonrpc.Request)
		if !ok {
			return
		}
		switch req.Method {
		case "initialize":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0", "id": req.ID.Raw(),
				"result": &mcp.InitializeResult{
					ProtocolVersion: version, ServerInfo: &mcp.Implementation{Name: "legacy", Version: "1"},
					Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}},
				},
			})
		case "notifications/initialized", "notifications/cancelled":
			w.WriteHeader(http.StatusAccepted)
		case "tools/call":
			call(w, r, req)
		default:
			http.NotFound(w, r)
		}
	}))
}

func legacyRequest(t *testing.T, endpoint, method, token string) *http.Request {
	t.Helper()
	params := map[string]any{"_meta": map[string]any{
		"workspace.example/request": "retained",
		"progressToken":             "stateless-progress",
	}}
	if method == "tools/call" {
		params["name"] = "profile"
		params["arguments"] = json.RawMessage(`{"format":"concise","number":9007199254740993}`)
	}
	r := testutil.Request(t, method, params)
	r.URL.Scheme, r.URL.Host = "https", strings.TrimPrefix(endpoint, "https://")
	r.GetBody = nil
	r.Header.Set("Authorization", "Bearer "+token)
	return r
}

func TestLegacyToolCompatibility(t *testing.T) {
	var created, executed, deleted atomic.Int32
	check := func(req mcp.Request) {
		executed.Add(1)
		caps := req.GetSession().(*mcp.ServerSession).InitializeParams().Capabilities
		if !reflect.DeepEqual(caps, &mcp.ClientCapabilities{}) {
			t.Error("advertised unsupported client capabilities")
		}
		if !reflect.DeepEqual(req.GetParams().GetMeta(), map[string]any{"workspace.example/request": "retained"}) {
			t.Errorf("incorrect metadata translation: %v", req.GetParams().GetMeta())
		}
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "legacy", Version: "1"}, &mcp.ServerOptions{
		SupportedProtocolVersions: []string{"2025-06-18"},
		GetSessionID:              func() string { return strconv.Itoa(int(created.Add(1))) },
	})
	t.Cleanup(func() {
		for session := range server.Sessions() {
			session.Close()
		}
	})
	tool := &mcp.Tool{
		Name:        "profile",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{"format": map[string]any{"type": "string", "enum": []string{"concise"}}}},
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}
	server.AddTool(tool, func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		check(req)
		var arguments struct {
			Format string
			Number json.Number
		}
		if json.Unmarshal(req.Params.Arguments, &arguments) != nil || arguments.Format != "concise" || arguments.Number != "9007199254740993" {
			t.Error("tool arguments changed")
		}
		owner := req.Extra.Header.Get("Authorization")
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: owner}}, IsError: true,
			StructuredContent: map[string]any{"owner": owner},
		}, nil
	})
	server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if method == "tools/list" {
				check(req)
				return &mcp.ListToolsResult{
					Tools:      []*mcp.Tool{tool},
					NextCursor: "next-page",
				}, nil
			}
			return next(ctx, method, req)
		}
	})
	provider := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{JSONResponse: true})
	authenticate := auth.RequireBearerToken(func(_ context.Context, token string, _ *http.Request) (*auth.TokenInfo, error) {
		return &auth.TokenInfo{UserID: token}, nil
	}, &auth.RequireBearerTokenOptions{AllowMissingExpiration: true})
	transport, endpoint := legacyTestTransport(t, legacyTestRejectCurrent(authenticate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, key := range []string{"Mcp-Method", "Mcp-Name", "Mcp-Param-Workspace"} {
			if r.Header.Get(key) != "" {
				t.Errorf("retained stateless header %s", key)
			}
		}
		recorder := httptest.NewRecorder()
		provider.ServeHTTP(recorder, r)
		if r.Method == http.MethodDelete && recorder.Code == http.StatusNoContent {
			deleted.Add(1)
		}
		maps.Copy(w.Header(), recorder.Header())
		w.Header().Del("Content-Length")
		w.Header().Set("Set-Cookie", "must-not-leak=1")
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(recorder.Code)
		compressed := gzip.NewWriter(w)
		compressed.Write(recorder.Body.Bytes())
		compressed.Close()
	}))))
	var workers sync.WaitGroup
	for _, tc := range []struct{ method, result string }{
		{"tools/list", `{"tools":[{"name":"profile","inputSchema":{"type":"object","properties":{"format":{"type":"string","enum":["concise"]}}},"annotations":{"readOnlyHint":true}}],"nextCursor":"next-page","resultType":"complete","cacheScope":"private"}`},
		{"tools/call", `{"content":[{"type":"text","text":"$OWNER"}],"structuredContent":{"owner":"$OWNER"},"isError":true,"resultType":"complete","cacheScope":"private"}`},
	} {
		for _, owner := range []string{"alice", "bob"} {
			workers.Go(func() {
				r := legacyRequest(t, endpoint, tc.method, owner)
				r.Header.Set("Mcp-Session-Id", "untrusted")
				r.Header.Set("Mcp-Param-Workspace", "untrusted")
				r.Header.Set("Accept-Encoding", "gzip")
				response, err := transport.RoundTrip(r)
				if err != nil {
					t.Error(err)
					return
				}
				defer response.Body.Close()
				var reply, want struct {
					JSONRPC string
					ID      int
					Result  struct {
						ResultType, CacheScope, NextCursor string
						Content                            []map[string]any
						StructuredContent                  map[string]any
						IsError                            bool
						Tools                              []*mcp.Tool
					}
				}
				_ = json.Unmarshal([]byte(strings.ReplaceAll(`{"jsonrpc":"2.0","id":1,"result":`+tc.result+`}`, "$OWNER", "Bearer "+owner)), &want)
				if err := json.NewDecoder(response.Body).Decode(&reply); err != nil {
					t.Error(err)
					return
				}
				if response.StatusCode != 200 || !reflect.DeepEqual(reply, want) {
					t.Errorf("%s: HTTP %d, got %+v, want %+v", tc.method, response.StatusCode, reply, want)
				}
				if response.Header.Get("Set-Cookie") != "" || response.Header.Get("Mcp-Session-Id") != "" {
					t.Error("leaked legacy state")
				}
			})
		}
	}
	workers.Wait()
	if created.Load() != 4 || executed.Load() != 4 || deleted.Load() != 4 {
		t.Fatalf("session lifecycle: created=%d executed=%d deleted=%d", created.Load(), executed.Load(), deleted.Load())
	}
}

func TestFallbackRequiresExplicitVersionRejection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		body    string
		headers http.Header
	}{
		{name: "authentication", status: 401, body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32022,"message":"unsupported"}}`},
		{name: "invalid parameters", status: 400, body: `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"invalid params"}}`},
		{name: "response ID mismatch", status: 400, body: `{"jsonrpc":"2.0","id":"other-id","error":{"code":-32022,"message":"unsupported"}}`},
		{name: "oversized ordinary error", status: 400, body: strings.Repeat("ordinary upstream error", mcp.DefaultMaxLineLength/10)},
		{
			name:    "request header mismatch",
			status:  400,
			body:    `{"jsonrpc":"2.0","id":1,"error":{"code":-32022,"message":"unsupported"}}`,
			headers: http.Header{"Mcp-Name": {"other-tool"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			transport, endpoint := legacyTestTransport(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("WWW-Authenticate", `Bearer resource_metadata="https://provider.example/metadata"`)
				w.Header().Set("Retry-After", "30")
				w.WriteHeader(tc.status)
				io.WriteString(w, tc.body)
			})
			r := legacyRequest(t, endpoint, "tools/call", "alice")
			maps.Copy(r.Header, tc.headers)
			response, err := transport.RoundTrip(r)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != tc.status || string(body) != tc.body || calls.Load() != 1 || response.Header.Get("Retry-After") != "30" || response.Header.Get("WWW-Authenticate") != `Bearer resource_metadata="https://provider.example/metadata"` {
				t.Fatalf("unexpected retry or response: %d, calls %d, error %v", response.StatusCode, calls.Load(), err)
			}
		})
	}
}

func TestLegacyCancellationDoesNotReplayTool(t *testing.T) {
	cancelled := make(chan struct{})
	var executions atomic.Int32
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	transport, endpoint := legacyTestTransport(t, legacyTestProvider("2025-06-18", func(_ http.ResponseWriter, r *http.Request, _ *jsonrpc.Request) {
		if executions.Add(1) != 1 {
			t.Error("cancelled tool was replayed")
			return
		}
		cancel()
		<-r.Context().Done()
		close(cancelled)
	}))
	done := make(chan error, 1)
	go func() {
		response, err := transport.RoundTrip(legacyRequest(t, endpoint, "tools/call", "alice").WithContext(ctx))
		if response != nil {
			response.Body.Close()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("proxy did not stop")
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream request was not cancelled")
	}
}

func TestLegacyFailuresDoNotReplayTools(t *testing.T) {
	for _, tc := range []struct {
		name, version, wantError string
		status                   int
		executions               int32
	}{
		{name: "current negotiated version", version: proxy.CurrentVersion, wantError: "upstream did not negotiate a legacy MCP protocol"},
		{name: "HTTP failure after dispatch", version: "2025-06-18", status: 500, executions: 1},
		{name: "RPC failure after dispatch", version: "2025-06-18", executions: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var executions atomic.Int32
			transport, endpoint := legacyTestTransport(t, legacyTestProvider(tc.version, func(w http.ResponseWriter, _ *http.Request, req *jsonrpc.Request) {
				executions.Add(1)
				if tc.status != 0 {
					http.Error(w, "transport failure", tc.status)
					return
				}
				mcphttp.WriteError(w, http.StatusOK, req.ID, &jsonrpc.Error{
					Code: -32603, Message: "tool execution failed", Data: json.RawMessage(`{"detail":"preserved"}`),
				})
			}))
			response, err := transport.RoundTrip(legacyRequest(t, endpoint, "tools/call", "alice"))
			if response != nil {
				defer response.Body.Close()
			}
			if executions.Load() != tc.executions {
				t.Fatalf("unexpected tool executions %d", executions.Load())
			}
			if tc.wantError != "" {
				if err == nil || err.Error() != tc.wantError {
					t.Fatalf("expected %q, got %v", tc.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var reply struct {
				ID    int
				Error *struct {
					Code int
					Data struct{ Detail string }
				}
			}
			if json.NewDecoder(response.Body).Decode(&reply) != nil || response.StatusCode != 200 || reply.ID != 1 || reply.Error == nil {
				t.Fatalf("lost RPC failure: %+v", reply)
			}
			if tc.status == 0 && (reply.Error.Code != -32603 || reply.Error.Data.Detail != "preserved") {
				t.Fatal("lost RPC error data")
			}
		})
	}
}
