package mcpstdio_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	mcpbridge "github.com/hughhan1/mcp-bridge/stdio"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/oauth2"
)

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }
func (failingWriter) Close() error                { return nil }

func TestStdioLifecycle(t *testing.T) {
	for _, mode := range []string{"streamable-http", "sse"} {
		t.Run("cancel/"+mode, func(t *testing.T) {
			started, cancelled := make(chan struct{}), make(chan struct{})
			s := mcp.NewServer(&mcp.Implementation{Name: "cancel", Version: "1"}, nil)
			mcp.AddTool(s, &mcp.Tool{Name: "slow"}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
				close(started)
				<-ctx.Done()
				close(cancelled)
				return nil, nil, ctx.Err()
			})
			var handler http.Handler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, nil)
			if mode == "sse" {
				handler = mcp.NewSSEHandler(func(*http.Request) *mcp.Server { return s }, nil)
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			var remote mcp.Transport = &mcp.StreamableClientTransport{Endpoint: server.URL}
			if mode == "sse" {
				remote = &mcp.SSEClientTransport{Endpoint: server.URL}
			}
			local, clientTransport := mcp.NewInMemoryTransports()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			finished := make(chan error, 1)
			go func() { finished <- mcpbridge.Run(ctx, remote, local) }()
			client := mcp.NewClient(&mcp.Implementation{Name: "cancel-test", Version: "1"}, nil)
			session, err := client.Connect(ctx, clientTransport, &mcp.ClientSessionOptions{ProtocolVersion: "2025-11-25"})
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			go func() {
				_, _ = session.CallTool(t.Context(), &mcp.CallToolParams{Name: "slow", Arguments: map[string]any{}})
			}()
			<-started
			cancel()
			if err := <-finished; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			<-cancelled
		})
	}
	for _, failure := range []struct {
		name    string
		local   bool
		readEOF bool
		err     error
		wantErr bool
	}{
		{name: "remote read EOF", readEOF: true, wantErr: true},
		{name: "remote write EOF", err: io.EOF, wantErr: true},
		{name: "local write EOF", local: true, err: io.EOF},
		{name: "local write closed pipe", local: true, err: io.ErrClosedPipe},
		{name: "local connection closed", local: true, err: mcp.ErrConnectionClosed},
		{name: "local write failure", local: true, err: errors.New("local output failed"), wantErr: true},
	} {
		t.Run(failure.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reader, writer := io.Pipe()
			defer writer.Close()
			if failure.readEOF {
				writer.Close()
			}
			memory, client := mcp.NewInMemoryTransports()
			var remote, local mcp.Transport = &mcp.IOTransport{Reader: reader, Writer: failingWriter{failure.err}}, memory
			if failure.local {
				remote, local = local, remote
			}
			finished := make(chan error, 1)
			go func() { finished <- mcpbridge.Run(ctx, remote, local) }()
			conn, err := client.Connect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if !failure.readEOF {
				if err := conn.Write(ctx, &jsonrpc.Request{Method: "notifications/initialized", Params: json.RawMessage(`{}`)}); err != nil {
					t.Fatal(err)
				}
			}
			if err := <-finished; (err != nil) != failure.wantErr {
				t.Fatalf("shutdown: %v, want error: %t", err, failure.wantErr)
			}
		})
	}
}

func TestStdioRequestIDs(t *testing.T) {
	for _, value := range []any{float64(1), "client-request"} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			upstream := mcp.NewServer(&mcp.Implementation{Name: "fixture", Version: "1"}, nil)
			remote, upstreamTransport := mcp.NewInMemoryTransports()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			session, err := upstream.Connect(ctx, upstreamTransport, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			local, client := mcp.NewInMemoryTransports()
			finished := make(chan error, 1)
			go func() {
				finished <- mcpbridge.Run(ctx, remote, local)
			}()
			conn, err := client.Connect(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			id, _ := jsonrpc.MakeID(value)
			for _, request := range []*jsonrpc.Request{
				{ID: id, Method: "initialize", Params: json.RawMessage(`{"protocolVersion":"2025-11-25","clientInfo":{"name":"ids","version":"1"},"capabilities":{}}`)},
				{Method: "notifications/initialized", Params: json.RawMessage(`{}`)},
				{ID: id, Method: "tools/list", Params: json.RawMessage(`{}`)},
			} {
				if err := conn.Write(ctx, request); err != nil {
					t.Fatal(err)
				}
				if request.IsCall() {
					message, err := conn.Read(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if response, ok := message.(*jsonrpc.Response); !ok || response.Error != nil || response.ID != id {
						t.Fatalf("%s: %v", request.Method, message)
					}
				}
			}
			conn.Close()
			if err := <-finished; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func connectClient(t *testing.T, handler http.Handler, opts *mcp.ClientOptions, version string, configure ...func(*mcp.StreamableClientTransport)) (*mcp.ClientSession, <-chan error) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	local, transport := mcp.NewInMemoryTransports()
	finished := make(chan error, 1)
	remote := &mcp.StreamableClientTransport{Endpoint: server.URL}
	for _, apply := range configure {
		apply(remote)
	}
	go func() { finished <- mcpbridge.Run(ctx, remote, local) }()
	client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, opts)
	session, err := client.Connect(ctx, transport, &mcp.ClientSessionOptions{ProtocolVersion: version})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { session.Close() })
	return session, finished
}

func modernHandler(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true})
}

func TestStdioSubscriptions(t *testing.T) {
	for _, ending := range []string{"unsubscribe", "disconnect", "rejected"} {
		t.Run(ending, func(t *testing.T) {
			stopped := make(chan struct{})
			server := mcp.NewServer(&mcp.Implementation{Name: "chat", Version: "1"}, &mcp.ServerOptions{
				SupportedProtocolVersions: []string{"2026-07-28"},
				Capabilities:              &mcp.ServerCapabilities{Resources: &mcp.ResourceCapabilities{Subscribe: true}},
				SubscribeHandler: func(context.Context, *mcp.SubscribeRequest) error {
					if ending == "rejected" {
						return errors.New("subscription denied")
					}
					return nil
				},
				UnsubscribeHandler: func(context.Context, *mcp.UnsubscribeRequest) error { close(stopped); return nil },
			})
			updates := make(chan *mcp.ResourceUpdatedNotificationParams, 1)
			stopStream := make(chan context.CancelFunc, 1)
			handler := modernHandler(server)
			session, finished := connectClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Mcp-Method") == "subscriptions/listen" {
					ctx, cancel := context.WithCancel(r.Context())
					defer cancel()
					stopStream <- cancel
					r = r.WithContext(ctx)
				}
				handler.ServeHTTP(w, r)
			}), &mcp.ClientOptions{ResourceUpdatedHandler: func(_ context.Context, r *mcp.ResourceUpdatedNotificationRequest) { updates <- r.Params }}, "2025-11-25")
			err := session.Subscribe(t.Context(), &mcp.SubscribeParams{URI: "chat://messages"})
			if ending == "rejected" {
				if err == nil || !strings.Contains(err.Error(), "upstream subscription ended") {
					t.Fatalf("subscription rejection: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			if ending == "disconnect" {
				(<-stopStream)()
				if err := <-finished; err == nil || !strings.Contains(err.Error(), "upstream subscription ended") {
					t.Fatalf("stream loss: %v", err)
				}
			} else {
				if err := server.ResourceUpdated(t.Context(), &mcp.ResourceUpdatedNotificationParams{URI: "chat://messages"}); err != nil {
					t.Fatal(err)
				}
				update := <-updates
				if update.URI != "chat://messages" || update.Meta[mcp.MetaKeySubscriptionID] != nil {
					t.Fatalf("legacy notification: %v", update)
				}
				if err := session.Unsubscribe(t.Context(), &mcp.UnsubscribeParams{URI: "chat://messages"}); err != nil {
					t.Fatal(err)
				}
				<-stopped
				if _, err := session.ListTools(t.Context(), nil); err != nil {
					t.Fatalf("unsubscribe disconnected client: %v", err)
				}
			}
		})
	}
}

func TestStdioRequestCancellation(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	server := mcp.NewServer(&mcp.Implementation{Name: "job", Version: "1"}, &mcp.ServerOptions{SupportedProtocolVersions: []string{"2026-07-28"}})
	handler := modernHandler(server)
	remote := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		if bytes.Contains(body, []byte(`"notifications/cancelled"`)) {
			t.Error("forwarded a cancellation notification to an HTTP remote")
		}
		if r.Header.Get("Mcp-Method") != "tools/call" {
			handler.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, ": running\n\n")
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(stopped)
	})
	session, _ := connectClient(t, remote, nil, "2025-11-25")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "run", Arguments: map[string]any{}})
		finished <- err
	}()
	<-started
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled request: %v", err)
	}
	<-stopped
	if _, err := session.ListTools(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestStdioCurrentStreamRemote(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	server := mcp.NewServer(&mcp.Implementation{Name: "stream", Version: "1"}, &mcp.ServerOptions{SupportedProtocolVersions: []string{"2026-07-28"}})
	server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if method == "tools/list" {
				return nil, errors.New("tool catalog unavailable")
			}
			return next(ctx, method, req)
		}
	})
	mcp.AddTool(server, &mcp.Tool{Name: "echo"}, func(context.Context, *mcp.CallToolRequest, struct{}) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{}, nil, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "wait"}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return nil, nil, ctx.Err()
	})
	remote, upstream := mcp.NewInMemoryTransports()
	if _, err := server.Connect(t.Context(), upstream, nil); err != nil {
		t.Fatal(err)
	}
	local, downstream := mcp.NewInMemoryTransports()
	go func() { _ = mcpbridge.Run(t.Context(), remote, local) }()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "client", Version: "1"}, nil).Connect(t.Context(), downstream, &mcp.ClientSessionOptions{ProtocolVersion: "2026-07-28"})
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	go func() { _, _ = session.CallTool(ctx, &mcp.CallToolParams{Name: "wait", Arguments: map[string]any{}}) }()
	<-started
	cancel()
	<-stopped
}

func TestStdioInvalidHeaderArgument(t *testing.T) {
	server := mcp.NewServer(&mcp.Implementation{Name: "tenant", Version: "1"}, &mcp.ServerOptions{SupportedProtocolVersions: []string{"2026-07-28"}})
	mcp.AddTool(server, &mcp.Tool{Name: "echo", InputSchema: map[string]any{"type": "object", "properties": map[string]any{"tenant": map[string]any{"type": "string", "x-mcp-header": "Tenant"}}}}, func(context.Context, *mcp.CallToolRequest, map[string]any) (*mcp.CallToolResult, any, error) {
		return &mcp.CallToolResult{}, nil, nil
	})
	handler := modernHandler(server)
	var calls atomic.Int32
	session, _ := connectClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Mcp-Method") == "tools/call" {
			calls.Add(1)
		}
		handler.ServeHTTP(w, r)
	}), nil, "2025-11-25")
	var wire *jsonrpc.Error
	if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"tenant": 1.5}}); !errors.As(err, &wire) || wire.Code != jsonrpc.CodeInvalidParams || calls.Load() != 0 {
		t.Fatalf("invalid header argument = %v after %d upstream calls", err, calls.Load())
	}
	if _, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "echo", Arguments: "not-an-object"}); err != nil && !errors.As(err, &wire) {
		t.Fatalf("non-object arguments = %v", err)
	}
}

func TestStdioUnsupportedResults(t *testing.T) {
	for _, method := range []string{"tools/call", "resources/read", "prompts/get"} {
		t.Run(method, func(t *testing.T) {
			server := mcp.NewServer(&mcp.Implementation{Name: "input", Version: "1"}, &mcp.ServerOptions{SupportedProtocolVersions: []string{"2026-07-28"}})
			handler := modernHandler(server)
			session, _ := connectClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Mcp-Method") != method {
					handler.ServeHTTP(w, r)
					return
				}
				var request struct {
					ID any `json:"id"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					t.Error(err)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": map[string]any{"resultType": "input_required", "inputRequests": map[string]any{}}})
			}), nil, "2025-11-25")
			var err error
			switch method {
			case "tools/call":
				_, err = session.CallTool(t.Context(), &mcp.CallToolParams{Name: "run", Arguments: map[string]any{}})
			case "resources/read":
				_, err = session.ReadResource(t.Context(), &mcp.ReadResourceParams{URI: "resource://input"})
			case "prompts/get":
				_, err = session.GetPrompt(t.Context(), &mcp.GetPromptParams{Name: "input"})
			}
			if err == nil || !strings.Contains(err.Error(), "input-required") {
				t.Fatalf("unsupported multi-round-trip response: %v", err)
			}
		})
	}
}

type fixtureOAuth struct {
	token atomic.Pointer[oauth2.Token]
}

func (a *fixtureOAuth) TokenSource(context.Context) (oauth2.TokenSource, error) {
	if token := a.token.Load(); token != nil {
		return oauth2.StaticTokenSource(token), nil
	}
	return nil, nil
}

func (a *fixtureOAuth) Authorize(_ context.Context, _ *http.Request, res *http.Response) error {
	res.Body.Close()
	a.token.Store(&oauth2.Token{AccessToken: "fixture", TokenType: "Bearer"})
	return nil
}

func TestStdioAuthorization(t *testing.T) {
	for _, version := range []string{"2025-11-25", "2026-07-28"} {
		t.Run(version, func(t *testing.T) {
			server := mcp.NewServer(&mcp.Implementation{Name: "protected", Version: "1"}, &mcp.ServerOptions{SupportedProtocolVersions: []string{"2026-07-28"}})
			server.AddTool(&mcp.Tool{Name: "read", InputSchema: json.RawMessage(`{"type":"object"}`)}, func(context.Context, *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "authorized"}}}, nil
			})
			handler := modernHandler(server)
			oauth := new(fixtureOAuth)
			session, _ := connectClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer fixture" {
					w.Header().Set("WWW-Authenticate", `Bearer resource_metadata="https://auth.example/resource"`)
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
				handler.ServeHTTP(w, r)
			}), nil, version, func(remote *mcp.StreamableClientTransport) { remote.OAuthHandler = oauth })
			result, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "read", Arguments: map[string]any{}})
			if err != nil || result.Content[0].(*mcp.TextContent).Text != "authorized" {
				t.Fatalf("authenticated call: %v %v", result, err)
			}
		})
	}
}

func TestStdioDiscoveryFailures(t *testing.T) {
	for _, status := range []int{401, 403, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var initialized atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Method string `json:"method"`
				}
				_ = json.NewDecoder(r.Body).Decode(&request)
				if request.Method == "initialize" {
					initialized.Store(true)
				}
				http.Error(w, http.StatusText(status), status)
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			local, transport := mcp.NewInMemoryTransports()
			finished := make(chan error, 1)
			go func() { finished <- mcpbridge.Run(ctx, &mcp.StreamableClientTransport{Endpoint: server.URL}, local) }()
			client := mcp.NewClient(&mcp.Implementation{Name: "legacy", Version: "1"}, nil)
			session, err := client.Connect(ctx, transport, &mcp.ClientSessionOptions{ProtocolVersion: "2025-11-25"})
			if session != nil {
				session.Close()
			}
			if err == nil {
				t.Fatal("failed discovery allowed connection")
			}
			if err := <-finished; err == nil || !strings.Contains(err.Error(), http.StatusText(status)) || initialized.Load() {
				t.Fatalf("discovery failure: %v, downgraded: %t", err, initialized.Load())
			}
		})
	}
}
