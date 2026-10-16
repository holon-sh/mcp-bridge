package mcphttp

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hughhan1/mcp-bridge/internal/testutil"
	"github.com/hughhan1/mcp-bridge/proxy"
	mcpbridge "github.com/hughhan1/mcp-bridge/stdio"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestProcess(t *testing.T) {
	mode := os.Getenv("BRIDGE_TEST_PROCESS")
	if mode == "" {
		return
	}
	if mode == "descendant-child" {
		lifetime := os.NewFile(3, "lifetime")
		defer lifetime.Close()
		_, _ = lifetime.Write([]byte{1})
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(0)
	}
	if mode == "descendant" {
		lifetime, input := os.NewFile(3, "lifetime"), os.NewFile(4, "input")
		child := exec.Command(os.Args[0], "-test.run=^TestProcess$")
		child.Env = append(os.Environ(), "BRIDGE_TEST_PROCESS=descendant-child")
		child.ExtraFiles, child.Stdin, child.Stderr = []*os.File{lifetime}, input, os.Stderr
		if child.Start() != nil {
			os.Exit(2)
		}
		lifetime.Close()
		input.Close()
	}
	conn, _ := (&mcp.StdioTransport{}).Connect(context.Background())
	var concurrent []*jsonrpc.Request
	echo := func(req *jsonrpc.Request) {
		var params map[string]any
		_ = json.Unmarshal(req.Params, &params)
		meta, _ := params["_meta"].(map[string]any)
		if token := meta["progressToken"]; token != nil {
			raw, _ := json.Marshal(map[string]any{"progressToken": token, "progress": 1})
			_ = conn.Write(context.Background(), &jsonrpc.Request{Method: "notifications/progress", Params: raw})
		}
		raw, _ := json.Marshal(map[string]any{"resultType": "complete", "echo": params, "content": []any{map[string]any{"type": "text", "text": "ok"}}})
		_ = conn.Write(context.Background(), &jsonrpc.Response{ID: req.ID, Result: raw})
	}
	var cancelled int64
	for {
		message, err := conn.Read(context.Background())
		if err != nil {
			os.Exit(0)
		}
		req := message.(*jsonrpc.Request)
		var params map[string]any
		_ = json.Unmarshal(req.Params, &params)
		if !req.IsCall() {
			if req.Method == "notifications/cancelled" {
				cancelled++
			}
			continue
		}
		var result any
		switch req.Method {
		case "server/discover":
			result = map[string]any{"supportedVersions": []string{cmp.Or(os.Getenv("BRIDGE_TEST_VERSION"), ProtocolVersion)}, "capabilities": map[string]any{"extensions": map[string]any{"io.modelcontextprotocol/tasks": map[string]any{}}}}
		case "tools/list":
			result = map[string]any{"tools": []any{
				map[string]any{"name": "echo", "inputSchema": map[string]any{"type": "object"}},
				map[string]any{"name": "headers", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{
					"tenant": map[string]any{"type": "string", "x-mcp-header": "Tenant"},
					"details": map[string]any{"type": "object", "properties": map[string]any{
						"count":   map[string]any{"type": "integer", "x-mcp-header": "Count"},
						"enabled": map[string]any{"type": "boolean", "x-mcp-header": "Enabled"},
					}},
				}}},
				map[string]any{"name": "duplicate-headers", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{
					"first":  map[string]any{"type": "string", "x-mcp-header": "Tenant"},
					"second": map[string]any{"type": "string", "x-mcp-header": "Tenant"},
				}}},
			}}
		case "tools/call":
			switch params["name"] {
			case "slow":
				token := params["_meta"].(map[string]any)["progressToken"]
				if token != nil {
					raw, _ := json.Marshal(map[string]any{"progressToken": token, "progress": 0})
					_ = conn.Write(context.Background(), &jsonrpc.Request{Method: "notifications/progress", Params: raw})
				}
				continue
			case "flood":
				token := params["_meta"].(map[string]any)["progressToken"]
				for i := 0; i < 256; i++ {
					raw, _ := json.Marshal(map[string]any{"progressToken": token, "progress": i, "message": strings.Repeat("x", 4096)})
					_ = conn.Write(context.Background(), &jsonrpc.Request{Method: "notifications/progress", Params: raw})
				}
				result = map[string]any{}
			case "crash":
				os.Exit(3)
			case "malformed":
				fmt.Println("not JSON")
				continue
			case "oversized":
				fmt.Println(strings.Repeat("x", 5<<20))
				continue
			case "create":
				if token := params["_meta"].(map[string]any)["progressToken"]; token != nil {
					raw, _ := json.Marshal(map[string]any{"progressToken": token, "progress": 1})
					_ = conn.Write(context.Background(), &jsonrpc.Request{Method: "notifications/progress", Params: raw})
				}
				result = map[string]any{"resultType": "task", "task": map[string]any{"taskId": "task-1", "status": "working", "ttlMs": 60000, "pollIntervalMs": 10}}
			case "interact":
				if params["requestState"] == "opaque-state" {
					result = map[string]any{"resultType": "complete", "inputResponses": params["inputResponses"]}
				} else {
					result = map[string]any{"resultType": "input_required", "requestState": "opaque-state", "inputRequests": map[string]any{"input-1": map[string]any{"method": "elicitation/create", "params": map[string]any{"message": "input"}}}}
				}
			case "concurrent":
				concurrent = append(concurrent, req)
				if len(concurrent) < 2 {
					continue
				}
				for _, pending := range concurrent {
					echo(pending)
				}
				concurrent = nil
				continue
			case "cancelled":
				result = map[string]any{"count": cancelled}
			default:
				echo(req)
				continue
			}
		case "tasks/get":
			result = map[string]any{"taskId": "task-1", "status": "working", "result": map[string]any{"opaque": "preserved"}}
		case "subscriptions/listen":
			raw, _ := json.Marshal(map[string]any{"_meta": map[string]any{"io.modelcontextprotocol/subscriptionId": req.ID.Raw()}})
			_ = conn.Write(context.Background(), &jsonrpc.Request{Method: "notifications/tools/list_changed", Params: raw})
			continue
		case "custom/error":
			_ = conn.Write(context.Background(), &jsonrpc.Response{ID: req.ID, Error: &jsonrpc.Error{Code: -32601, Message: "missing", Data: json.RawMessage(`{"custom":true}`)}})
			continue
		default:
			result = map[string]any{"opaque": params}
		}
		raw, _ := json.Marshal(result)
		_ = conn.Write(context.Background(), &jsonrpc.Response{ID: req.ID, Result: raw})
	}
}

func start(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	b, err := Start(t.Context(), func() *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestProcess$")
		cmd.Env = append(os.Environ(), "BRIDGE_TEST_PROCESS=raw")
		return cmd
	}, Options{ProtocolVersion: ProtocolVersion})
	if err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer(b)
	t.Cleanup(func() { _ = b.Close(); s.Close() })
	return b, s
}

func send(t *testing.T, server *httptest.Server, method string, params map[string]any) (int, map[string]any) {
	t.Helper()
	req := testutil.Request(t, method, params)
	req.URL.Scheme, req.URL.Host = "http", strings.TrimPrefix(server.URL, "http://")
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var value map[string]any
	if err := json.NewDecoder(res.Body).Decode(&value); err != nil {
		t.Fatal(err)
	}
	return res.StatusCode, value
}

func TestConcurrentIDsAndProgress(t *testing.T) {
	_, server := start(t)
	var group sync.WaitGroup
	// The fixture withholds replies until both requests are pending upstream.
	for i := range 2 {
		group.Go(func() {
			params := map[string]any{"name": "concurrent", "arguments": map[string]any{"value": i}, "_meta": map[string]any{"progressToken": "same-token"}}
			req := testutil.Request(t, "tools/call", params)
			req.URL.Host = strings.TrimPrefix(server.URL, "http://")
			res, err := server.Client().Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			defer res.Body.Close()
			progress, response := false, false
			scanner := bufio.NewScanner(res.Body)
			for scanner.Scan() {
				data, ok := strings.CutPrefix(scanner.Text(), "data: ")
				if !ok {
					continue
				}
				var message map[string]any
				if err := json.Unmarshal([]byte(data), &message); err != nil {
					t.Error(err)
					return
				}
				if message["method"] == "notifications/progress" {
					progress = message["params"].(map[string]any)["progressToken"] == "same-token"
				} else if result, ok := message["result"].(map[string]any); ok {
					response = message["id"] == float64(1) && result["echo"].(map[string]any)["arguments"].(map[string]any)["value"] == float64(i)
				}
			}
			if scanner.Err() != nil || !progress || !response {
				t.Errorf("request %d: progress=%t response=%t error=%v", i, progress, response, scanner.Err())
			}
		})
	}
	group.Wait()
}

func TestPayloadForwarding(t *testing.T) {
	for _, direction := range []string{"serve", "connect"} {
		t.Run(direction, func(t *testing.T) {
			_, server := start(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var conn mcp.Connection
			var finished chan error
			if direction == "connect" {
				local, client := mcp.NewInMemoryTransports()
				finished = make(chan error, 1)
				go func() {
					finished <- mcpbridge.Run(ctx, &mcp.StreamableClientTransport{Endpoint: server.URL}, local)
				}()
				var err error
				conn, err = client.Connect(ctx)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { conn.Close() })
			}
			invoke := func(method string, params map[string]any) *jsonrpc.Response {
				t.Helper()
				req := testutil.Request(t, method, params).WithContext(ctx)
				var msg jsonrpc.Message
				if conn == nil {
					req.URL.Host = strings.TrimPrefix(server.URL, "http://")
					res, err := server.Client().Do(req)
					if err != nil {
						t.Fatal(err)
					}
					defer res.Body.Close()
					want := 200
					if method == "custom/error" {
						want = 404
					}
					if res.StatusCode != want {
						t.Fatalf("%s: HTTP %d", method, res.StatusCode)
					}
					data, err := io.ReadAll(res.Body)
					if err == nil {
						msg, err = jsonrpc.DecodeMessage(data)
					}
					if err != nil {
						t.Fatal(err)
					}
				} else {
					data, _ := io.ReadAll(req.Body)
					request, err := jsonrpc.DecodeMessage(data)
					if err != nil {
						t.Fatal(err)
					}
					if err := conn.Write(ctx, request); err != nil {
						t.Fatal(err)
					}
					msg, err = conn.Read(ctx)
					if err != nil {
						t.Fatal(err)
					}
				}
				response, ok := msg.(*jsonrpc.Response)
				if !ok || response.ID.Raw() != int64(1) {
					t.Fatalf("%s: %v", method, msg)
				}
				return response
			}
			call := func(method string, params map[string]any) map[string]any {
				t.Helper()
				response := invoke(method, params)
				if response.Error != nil {
					t.Fatal(response.Error)
				}
				var value map[string]any
				if err := json.Unmarshal(response.Result, &value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			created := call("tools/call", map[string]any{"name": "create"})
			if created["resultType"] != "task" {
				t.Fatal(created)
			}
			if state := call("tasks/get", map[string]any{"taskId": "task-1"}); state["status"] != "working" || state["result"].(map[string]any)["opaque"] != "preserved" {
				t.Fatal(state)
			}
			state := call("tools/call", map[string]any{"name": "interact"})
			if state["resultType"] != "input_required" || state["requestState"] != "opaque-state" {
				t.Fatal(state)
			}
			result := call("tools/call", map[string]any{"name": "interact", "requestState": state["requestState"], "inputResponses": map[string]any{"input-1": "answer"}})
			if result["inputResponses"].(map[string]any)["input-1"] != "answer" {
				t.Fatal(result)
			}
			extension := call("custom/extension", map[string]any{"unknown": map[string]any{"nested": []any{1, "two"}}})
			if got := extension["opaque"].(map[string]any)["unknown"].(map[string]any)["nested"]; !reflect.DeepEqual(got, []any{float64(1), "two"}) {
				t.Fatal(extension)
			}
			if conn != nil {
				for _, arguments := range []map[string]any{{"tenant": "日本語"}, {"tenant": "  tenant  "}, {"details": map[string]any{"count": 42, "enabled": true}}} {
					call("tools/call", map[string]any{"name": "headers", "arguments": arguments})
				}
			}
			response := invoke("custom/error", nil)
			var rpcErr *jsonrpc.Error
			if !errors.As(response.Error, &rpcErr) || rpcErr.Code != -32601 || string(rpcErr.Data) != `{"custom":true}` {
				t.Fatal(response)
			}
			if conn != nil {
				conn.Close()
				if err := <-finished; err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestSubscriptions(t *testing.T) {
	_, server := start(t)
	req := testutil.Request(t, "subscriptions/listen", nil)
	req.URL.Host = strings.TrimPrefix(server.URL, "http://")
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	reader := bufio.NewReader(res.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "data:") {
			if !strings.Contains(line, `"io.modelcontextprotocol/subscriptionId":1`) {
				t.Fatal(line)
			}
			break
		}
	}
	res.Body.Close()
	_, _ = send(t, server, "tools/list", nil)
}

func TestValidation(t *testing.T) {
	b, _ := start(t)
	for _, test := range []struct {
		name, tool, method string
		arguments          map[string]any
		headers            http.Header
		body               string
		status             int
	}{
		{name: "GET", method: http.MethodGet, status: 405},
		{name: "JSON-only Accept", headers: http.Header{"Accept": {"application/json"}}, status: 406},
		{name: "stream-only Accept", headers: http.Header{"Accept": {"text/event-stream"}}, status: 406},
		{name: "client response", body: `{"jsonrpc":"2.0","id":1,"result":{}}`, status: 400},
		{name: "cancellation notification", body: `{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}`, status: 400},
		{name: "wrong method", headers: http.Header{"Mcp-Method": {"wrong"}}, status: 400},
		{name: "wrong name", headers: http.Header{"Mcp-Name": {"wrong"}}, status: 400},
		{name: "missing argument header", arguments: map[string]any{"tenant": "tenant-a"}, status: 400},
		{name: "wrong argument header", arguments: map[string]any{"tenant": "tenant-a"}, headers: http.Header{"Mcp-Param-Tenant": {"wrong"}}, status: 400},
		{name: "argument header", arguments: map[string]any{"tenant": "tenant-a"}, headers: http.Header{"Mcp-Param-Tenant": {"tenant-a"}}, status: 200},
		{name: "missing nested", arguments: map[string]any{"details": map[string]any{"count": 42}}, status: 400},
		{name: "nested", arguments: map[string]any{"details": map[string]any{"count": 42, "enabled": true}}, headers: http.Header{"Mcp-Param-Count": {"42.0"}, "Mcp-Param-Enabled": {"true"}}, status: 200},
		{name: "wrong nested", arguments: map[string]any{"details": map[string]any{"count": 42}}, headers: http.Header{"Mcp-Param-Count": {"43"}}, status: 400},
		{name: "absent nested", headers: http.Header{"Mcp-Param-Count": {"42"}}, status: 400},
		{name: "duplicate bindings", tool: "duplicate-headers", arguments: map[string]any{"first": "a", "second": "b"}, headers: http.Header{"Mcp-Param-Tenant": {"a"}}, status: 400},
		{name: "invalid JSON", body: "invalid", status: 400},
		{name: "batch", body: `[]`, status: 400},
		{name: "oversized", body: strings.Repeat("x", 5<<20), status: 413},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := testutil.Request(t, "tools/call", map[string]any{"name": cmp.Or(test.tool, "headers"), "arguments": test.arguments})
			req.Method = cmp.Or(test.method, req.Method)
			for name, values := range test.headers {
				req.Header[name] = values
			}
			if test.body != "" {
				req.Body = io.NopCloser(strings.NewReader(test.body))
			}
			w := httptest.NewRecorder()
			b.ServeHTTP(w, req)
			if w.Code != test.status {
				t.Fatalf("HTTP %d: %s", w.Code, w.Body)
			}
		})
	}
}

func TestProcessFailures(t *testing.T) {
	for _, name := range []string{"crash", "malformed", "oversized"} {
		t.Run(name, func(t *testing.T) {
			b, s := start(t)
			status, _ := send(t, s, "tools/call", map[string]any{"name": name})
			if status < 500 {
				t.Fatal(status)
			}
			if b.Wait() == nil {
				t.Fatal("missing process failure")
			}
		})
	}
	t.Run("startup", func(t *testing.T) {
		if _, err := Start(t.Context(), func() *exec.Cmd { return exec.Command("/no/such/mcp-server") }, Options{ProtocolVersion: ProtocolVersion}); err == nil {
			t.Fatal("missing startup failure")
		}
	})
}

type responseWriter struct {
	http.ResponseWriter
	write func([]byte) (int, error)
	flush func()
}

func (w *responseWriter) Write(data []byte) (int, error) {
	if w.write != nil {
		return w.write(data)
	}
	return w.ResponseWriter.Write(data)
}

func (w *responseWriter) Flush() {
	if w.flush != nil {
		w.flush()
		return
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func TestHTTPConsumerLifecycle(t *testing.T) {
	for _, test := range []struct {
		name, tool                          string
		waitTask, cancelRequest, failBridge bool
		wantCancelled                       float64
	}{
		{name: "cancel request", tool: "slow", cancelRequest: true, wantCancelled: 1},
		{name: "slow consumer", tool: "flood", failBridge: true},
		{name: "disconnect before task response", tool: "create"},
		{name: "disconnect during task response", tool: "create", waitTask: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, server := start(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var enteredOnce, releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			writer := &responseWriter{ResponseWriter: httptest.NewRecorder(), write: func(data []byte) (int, error) {
				if test.waitTask && !bytes.Contains(data, []byte(`"resultType":"task"`)) {
					return len(data), nil
				}
				enteredOnce.Do(func() { close(entered) })
				<-release
				if test.failBridge || test.cancelRequest {
					return len(data), nil
				}
				return 0, io.ErrClosedPipe
			}}
			req := testutil.Request(t, "tools/call", map[string]any{"name": test.tool, "_meta": map[string]any{"progressToken": "progress"}}).WithContext(ctx)
			done := make(chan struct{})
			go func() { b.ServeHTTP(writer, req); close(done) }()
			<-entered
			if test.failBridge {
				switch err := b.Wait(); fmt.Sprint(err) {
				case "MCP consumer too slow", "MCP output buffer limit exceeded":
				default:
					t.Fatalf("slow consumer shutdown: %v", err)
				}
			} else if test.cancelRequest {
				cancel()
			} else {
				// A later reply proves the task response has reached the bridge.
				_, _ = send(t, server, "tools/call", map[string]any{"name": "cancelled"})
			}
			releaseOnce.Do(func() { close(release) })
			<-done
			if !test.failBridge {
				_, result := send(t, server, "tools/call", map[string]any{"name": "cancelled"})
				if result["result"].(map[string]any)["count"] != test.wantCancelled {
					t.Fatal(result)
				}
			}
		})
	}
}

func TestShutdownReapsDescendants(t *testing.T) {
	lifetime, childLifetime, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer lifetime.Close()
	defer childLifetime.Close()
	childInput, holdOpen, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer childInput.Close()
	defer holdOpen.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestProcess$")
	cmd.Env = append(os.Environ(), "BRIDGE_TEST_PROCESS=descendant")
	cmd.ExtraFiles = []*os.File{childLifetime, childInput}
	b, err := Start(t.Context(), func() *exec.Cmd { return cmd }, Options{ProtocolVersion: ProtocolVersion})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	childLifetime.Close()
	childInput.Close()
	// The descendant holds this pipe open while blocked on input owned by
	// this test. Parent exit alone cannot release it or produce EOF here.
	var ready [1]byte
	if _, err := io.ReadFull(lifetime, ready[:]); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if n, err := lifetime.Read(ready[:]); n != 0 || err != io.EOF {
		t.Fatalf("descendant lifetime: read %d bytes, %v", n, err)
	}
}

func TestProxyProcess(t *testing.T) {
	if os.Getenv("MCP_PROXY_FIXTURE") == "" {
		return
	}
	versions := []string(nil)
	if value := os.Getenv("MCP_PROXY_VERSIONS"); value != "" {
		versions = strings.Split(value, ",")
	}
	server := fixtureServer(versions...)
	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func fixtureServer(versions ...string) *mcp.Server {
	cancelled := make(chan struct{}, 1)
	options := &mcp.ServerOptions{
		PageSize:                  1,
		SupportedProtocolVersions: versions,
		Capabilities:              &mcp.ServerCapabilities{Extensions: map[string]any{"io.modelcontextprotocol/ui": map[string]any{}, "example.com/custom": map[string]any{}}},
		SubscribeHandler:          func(_ context.Context, _ *mcp.SubscribeRequest) error { return nil },
		UnsubscribeHandler:        func(_ context.Context, _ *mcp.UnsubscribeRequest) error { return nil },
	}
	server := mcp.NewServer(&mcp.Implementation{Name: "fixture", Version: "1"}, options)
	mcp.AddTool(server, &mcp.Tool{Name: "echo", Meta: mcp.Meta{"ui": map[string]any{"resourceUri": "ui://fixture"}}, InputSchema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "string", "x-mcp-header": "Value"}}, "required": []string{"value"}}}, func(ctx context.Context, req *mcp.CallToolRequest, input struct {
		Value string `json:"value"`
	}) (*mcp.CallToolResult, any, error) {
		if token := req.Params.GetProgressToken(); token != nil {
			_ = req.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{ProgressToken: token, Progress: 1})
		}
		caps := any(nil)
		if init := req.Session.InitializeParams(); init != nil {
			caps = init.Capabilities
		}
		return nil, map[string]any{"value": input.Value, "pid": os.Getpid(), "caps": caps, "meta": req.Params.Meta}, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "slow"}, func(ctx context.Context, req *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		if token := req.Params.GetProgressToken(); token != nil {
			_ = req.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{ProgressToken: token})
		}
		<-ctx.Done()
		cancelled <- struct{}{}
		return nil, nil, ctx.Err()
	})
	mcp.AddTool(server, &mcp.Tool{Name: "wait-cancel"}, func(ctx context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		select {
		case <-cancelled:
			return nil, struct{}{}, nil
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	})
	mcp.AddTool(server, &mcp.Tool{Name: "ask"}, func(ctx context.Context, req *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		result, err := req.Session.Elicit(ctx, &mcp.ElicitParams{Message: "answer", RequestedSchema: map[string]any{"type": "object", "properties": map[string]any{"answer": map[string]any{"type": "string"}}}})
		if err != nil {
			return nil, nil, err
		}
		return nil, result.Content, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "crash"}, func(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		os.Exit(3)
		return nil, nil, nil
	})
	mcp.AddTool(server, &mcp.Tool{Name: "notify"}, func(ctx context.Context, req *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		if err := req.Session.Log(ctx, &mcp.LoggingMessageParams{Level: "info", Data: "fixture-log"}); err != nil {
			return nil, nil, err
		}
		if err := server.ResourceUpdated(ctx, &mcp.ResourceUpdatedNotificationParams{URI: "ui://fixture"}); err != nil {
			return nil, nil, err
		}
		return nil, map[string]any{}, nil
	})
	server.AddResource(&mcp.Resource{URI: "ui://fixture", Name: "app", MIMEType: "text/html;profile=mcp-app"}, func(context.Context, *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{URI: "ui://fixture", MIMEType: "text/html;profile=mcp-app", Text: "<main>app</main>", Meta: mcp.Meta{"ui": map[string]any{"prefersBorder": true}}}}}, nil
	})
	server.AddPrompt(&mcp.Prompt{Name: "greet"}, func(_ context.Context, _ *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
		return &mcp.GetPromptResult{Messages: []*mcp.PromptMessage{{Role: "user", Content: &mcp.TextContent{Text: "hello"}}}}, nil
	})
	return server
}

func commandFactory() *exec.Cmd {
	cmd := exec.Command(os.Args[0], "-test.run=^TestProxyProcess$")
	cmd.Env = append(os.Environ(), "MCP_PROXY_FIXTURE=1")
	cmd.Stderr = os.Stderr
	return cmd
}

func clientOptions(progress chan<- any) *mcp.ClientOptions {
	return &mcp.ClientOptions{
		Capabilities:           &mcp.ClientCapabilities{Extensions: map[string]any{"io.modelcontextprotocol/ui": map[string]any{}}},
		LoggingMessageHandler:  func(_ context.Context, _ *mcp.LoggingMessageRequest) { progress <- "log" },
		ResourceUpdatedHandler: func(_ context.Context, _ *mcp.ResourceUpdatedNotificationRequest) { progress <- "resource" },
		ElicitationHandler: func(_ context.Context, _ *mcp.ElicitRequest) (*mcp.ElicitResult, error) {
			return &mcp.ElicitResult{Action: "accept", Content: map[string]any{"answer": "yes"}}, nil
		},
		ProgressNotificationHandler: func(_ context.Context, req *mcp.ProgressNotificationClientRequest) {
			progress <- req.Params.ProgressToken
		},
	}
}

func exercise(t *testing.T, session *mcp.ClientSession, legacy, translated bool, progress <-chan any) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// The SDK needs every tool descriptor to send schema-derived headers.
	var tools []string
	for tool, err := range session.Tools(ctx, nil) {
		if err != nil {
			t.Fatal(err)
		}
		if tool.Name == "echo" && tool.Meta["ui"].(map[string]any)["resourceUri"] != "ui://fixture" {
			t.Fatalf("MCP App tool metadata: %v", tool.Meta)
		}
		tools = append(tools, tool.Name)
	}
	slices.Sort(tools)
	if !slices.Equal(tools, []string{"ask", "crash", "echo", "notify", "slow", "wait-cancel"}) {
		t.Fatalf("forwarded tools: %v", tools)
	}
	meta := mcp.Meta{"progressToken": "caller-token"}
	if translated {
		meta[mcp.MetaKeyProtocolVersion] = "2025-11-25"
		meta[mcp.MetaKeyClientInfo] = map[string]any{"name": "spoofed"}
	}
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Meta: meta, Name: "echo", Arguments: map[string]any{"value": "hello"}})
	if err != nil || result.IsError {
		t.Fatalf("echo: %v %v", result, err)
	}
	value, ok := result.StructuredContent.(map[string]any)
	if !ok || value["value"] != "hello" {
		t.Fatalf("echo payload: %#v", result)
	}
	if len(result.Content) == 0 {
		t.Fatal("tool response lost readable content")
	}
	if translated {
		meta := value["meta"].(map[string]any)
		caps := meta[mcp.MetaKeyClientCapabilities].(map[string]any)
		if meta[mcp.MetaKeyProtocolVersion] != ProtocolVersion || meta[mcp.MetaKeyClientInfo].(map[string]any)["name"] != "test" || caps["elicitation"] != nil {
			t.Fatalf("upstream identity and capabilities: %v", meta)
		}
	}
	if token := <-progress; token != "caller-token" {
		t.Fatalf("progress token: %v", token)
	}
	app, err := session.ReadResource(ctx, &mcp.ReadResourceParams{URI: "ui://fixture"})
	if err != nil || len(app.Contents) != 1 || app.Contents[0].Text != "<main>app</main>" || app.Contents[0].MIMEType != "text/html;profile=mcp-app" || app.Contents[0].Meta["ui"].(map[string]any)["prefersBorder"] != true {
		t.Fatalf("MCP App resource: %v %v", app, err)
	}
	prompt, err := session.GetPrompt(ctx, &mcp.GetPromptParams{Name: "greet"})
	if err != nil || len(prompt.Messages) != 1 {
		t.Fatalf("prompt: %v %v", prompt, err)
	}
	if _, err := session.ReadResource(ctx, &mcp.ReadResourceParams{URI: "test://missing"}); err == nil {
		t.Fatal("missing resource error lost")
	}
	if legacy && !translated {
		answer, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "ask", Arguments: map[string]any{}})
		if err != nil || answer.IsError || !reflect.DeepEqual(answer.StructuredContent, map[string]any{"answer": "yes"}) {
			t.Fatalf("elicitation: %v %v", answer, err)
		}
		if err := session.SetLoggingLevel(ctx, &mcp.SetLoggingLevelParams{Level: "debug"}); err != nil {
			t.Fatal(err)
		}
		if err := session.Subscribe(ctx, &mcp.SubscribeParams{URI: "ui://fixture"}); err != nil {
			t.Fatal(err)
		}
		if _, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "notify", Arguments: map[string]any{}}); err != nil {
			t.Fatal(err)
		}
		seen := make(map[any]bool)
		for !seen["log"] || !seen["resource"] {
			notification := <-progress
			seen[notification] = true
		}
		if err := session.Unsubscribe(ctx, &mcp.UnsubscribeParams{URI: "ui://fixture"}); err != nil {
			t.Fatal(err)
		}
		slow, stop := context.WithCancel(ctx)
		finished := make(chan error, 1)
		go func() {
			_, err := session.CallTool(slow, &mcp.CallToolParams{Name: "slow", Arguments: map[string]any{}, Meta: mcp.Meta{"progressToken": "slow-started"}})
			finished <- err
		}()
		if token := <-progress; token != "slow-started" {
			t.Fatalf("slow request progress: %v", token)
		}
		stop()
		if err := <-finished; !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
		if _, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "wait-cancel", Arguments: map[string]any{}}); err != nil {
			t.Fatalf("remote cancellation: %v", err)
		}
	}
}

func TestProxyTransports(t *testing.T) {
	type mode struct {
		name, version              string
		sse, fallback, noDiscovery bool
	}
	pinnedModes := []mode{{name: "sse-oldest", version: "2024-11-05", sse: true}}
	modes := []mode{
		{name: "sse", version: "2025-11-25", sse: true},
		{name: "fallback", version: "2025-11-25", fallback: true},
	}
	var translatedModes []mode
	for _, version := range mcp.SupportedProtocolVersions() {
		m := mode{name: version, version: version}
		modes = append(modes, m)
		pinnedModes = append(pinnedModes, m)
		if version != ProtocolVersion {
			translatedModes = append(translatedModes, m)
		}
	}
	connectModes := append(slices.Clone(modes), mode{name: "no-discovery", version: "2025-11-25", fallback: true, noDiscovery: true})
	for _, direction := range []struct {
		name                           string
		serve, connect, pin, translate bool
		modes                          []mode
	}{
		{name: "serve", serve: true, modes: modes},
		{name: "serve-pinned", serve: true, pin: true, modes: pinnedModes},
		{name: "connect", connect: true, modes: connectModes},
		{name: "connect-translated", connect: true, translate: true, modes: translatedModes},
	} {
		for _, mode := range direction.modes {
			t.Run(direction.name+"/"+mode.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var handler http.Handler
				path := ""
				if direction.serve {
					options := Options{}
					if direction.pin {
						options.ProtocolVersion = mode.version
					}
					b, err := Start(ctx, func() *exec.Cmd {
						cmd := commandFactory()
						if mode.fallback {
							cmd.Env = append(cmd.Env, "MCP_PROXY_VERSIONS=2025-11-25")
						}
						return cmd
					}, options)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { b.Close() })
					handler, path = b, "/mcp"
					if mode.sse {
						path = "/sse"
					}
				} else {
					var versions []string
					if mode.fallback {
						versions = []string{mode.version}
					}
					if direction.translate {
						versions = []string{ProtocolVersion}
					}
					upstream := fixtureServer(versions...)
					getServer := func(*http.Request) *mcp.Server { return upstream }
					handler = mcp.NewStreamableHTTPHandler(getServer, &mcp.StreamableHTTPOptions{Stateless: mode.version == ProtocolVersion || direction.translate})
					if mode.sse {
						handler = mcp.NewSSEHandler(getServer, nil)
					}
				}
				posted := make(chan struct{})
				var postedOnce sync.Once
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if mode.noDiscovery && r.Header.Get("Mcp-Method") == "server/discover" {
						http.NotFound(w, r)
						return
					}
					if direction.translate && r.Header.Get("Mcp-Name") == "echo" && (r.Header.Get("Mcp-Protocol-Version") != ProtocolVersion || r.Header.Get("Mcp-Param-Value") != "hello") {
						t.Errorf("translated HTTP headers: %v", r.Header)
					}
					if mode.sse && r.Method == http.MethodGet {
						response := w
						w = &responseWriter{ResponseWriter: w, flush: func() {
							_ = http.NewResponseController(response).Flush()
							select {
							case <-posted:
							case <-r.Context().Done():
							}
						}}
					}
					handler.ServeHTTP(w, r)
					if mode.sse && r.Method == http.MethodPost {
						postedOnce.Do(func() { close(posted) })
					}
				}))
				t.Cleanup(server.Close)
				var transport mcp.Transport = &mcp.StreamableClientTransport{Endpoint: server.URL + path}
				if mode.sse {
					transport = &mcp.SSEClientTransport{Endpoint: server.URL + path}
				}
				var finished chan error
				if direction.connect {
					local, client := mcp.NewInMemoryTransports()
					finished = make(chan error, 1)
					go func(remote mcp.Transport) { finished <- mcpbridge.Run(ctx, remote, local) }(transport)
					transport = client
				}
				progress := make(chan any, 10)
				client := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, clientOptions(progress))
				opts := &mcp.ClientSessionOptions{ProtocolVersion: mode.version}
				if direction.pin && mode.sse {
					opts.ProtocolVersion = "2025-11-25"
				}
				if mode.fallback && !mode.noDiscovery || direction.pin && !mode.sse {
					opts = nil
				}
				session, err := client.Connect(ctx, transport, opts)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { session.Close() })
				if result := session.InitializeResult(); result.ProtocolVersion != mode.version {
					t.Fatalf("negotiated protocol: %v, want %s", result, mode.version)
				}
				caps := session.InitializeResult().Capabilities.Extensions
				if caps["io.modelcontextprotocol/ui"] == nil || (caps["example.com/custom"] != nil) == direction.translate {
					t.Fatalf("negotiated capabilities: %v", caps)
				}
				exercise(t, session, mode.version != ProtocolVersion, direction.translate, progress)
				if direction.pin && !mode.sse {
					if mode.version == ProtocolVersion {
						status, discovered := send(t, server, "server/discover", nil)
						if status != 200 || !reflect.DeepEqual(discovered["result"].(map[string]any)["supportedVersions"], []any{mode.version}) {
							t.Fatalf("pinned discovery: %v", discovered)
						}
					} else {
						for _, method := range []string{"ping", "notifications/initialized"} {
							message := &jsonrpc.Request{Method: method, Params: json.RawMessage(`{}`)}
							if method == "ping" {
								message.ID, _ = jsonrpc.MakeID(int64(999))
							}
							data, _ := jsonrpc.EncodeMessage(message)
							req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+path, strings.NewReader(string(data)))
							req.Header.Set("Content-Type", "application/json")
							req.Header.Set("Accept", "application/json, text/event-stream")
							req.Header.Set("Mcp-Protocol-Version", ProtocolVersion)
							req.Header.Set("Mcp-Session-Id", session.ID())
							res, err := server.Client().Do(req)
							if err != nil {
								t.Fatal(err)
							}
							var rejected struct {
								Error *jsonrpc.Error `json:"error"`
							}
							err = json.NewDecoder(res.Body).Decode(&rejected)
							res.Body.Close()
							if err != nil || res.StatusCode != http.StatusBadRequest || rejected.Error == nil || rejected.Error.Code != mcp.CodeUnsupportedProtocolVersion {
								t.Fatalf("version header: HTTP %d %+v %v", res.StatusCode, rejected, err)
							}
						}
						if _, err := session.ListTools(ctx, &mcp.ListToolsParams{}); err != nil {
							t.Fatalf("rejected request broke session: %v", err)
						}
					}
				}
				session.Close()
				if finished != nil {
					if err := <-finished; err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}

func TestLegacySessionsAreIsolated(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	b, err := Start(ctx, commandFactory, Options{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(b)
	t.Cleanup(func() { b.Close(); server.Close() })
	var clients []*mcp.ClientSession
	for i := range 2 {
		progress := make(chan any, 10)
		options := clientOptions(progress)
		if i == 1 {
			options.ElicitationHandler = nil
		}
		c := mcp.NewClient(&mcp.Implementation{Name: fmt.Sprint(i), Version: "1"}, options)
		session, err := c.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: server.URL + "/mcp"}, &mcp.ClientSessionOptions{ProtocolVersion: "2025-11-25"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { session.Close() })
		clients = append(clients, session)
	}
	var results [2]*mcp.CallToolResult
	var errs [2]error
	var calls sync.WaitGroup
	for i, session := range clients {
		calls.Go(func() {
			results[i], errs[i] = session.CallTool(ctx, &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"value": fmt.Sprint(i)}})
		})
	}
	calls.Wait()
	pids := make(map[any]bool)
	for i, result := range results {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		value := result.StructuredContent.(map[string]any)
		if value["value"] != fmt.Sprint(i) || pids[value["pid"]] {
			t.Fatalf("sessions crossed: %v", value)
		}
		pids[value["pid"]] = true
		caps := value["caps"].(map[string]any)
		if _, present := caps["elicitation"]; present != (i == 0) {
			t.Fatalf("capabilities crossed clients: %v", caps)
		}
	}
	if _, err := clients[0].CallTool(ctx, &mcp.CallToolParams{Name: "crash", Arguments: map[string]any{}}); err == nil {
		t.Fatal("missing process failure")
	}
	clients[0].Close()
	if _, err := clients[1].ListTools(ctx, &mcp.ListToolsParams{}); err != nil {
		t.Fatalf("one crash broke the other session: %v", err)
	}
}

func TestLegacyNotificationsBeforeGET(t *testing.T) {
	b, err := Start(t.Context(), commandFactory, Options{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(b)
	t.Cleanup(func() { b.Close(); server.Close() })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	id := ""
	post := func(method string, params any) {
		t.Helper()
		data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/mcp", strings.NewReader(string(data)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set("Mcp-Protocol-Version", "2025-11-25")
		if id != "" {
			req.Header.Set("Mcp-Session-Id", id)
		}
		res, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		if res.StatusCode != 200 || !strings.Contains(string(body), `"result"`) {
			t.Fatalf("%s: HTTP %d %s", method, res.StatusCode, body)
		}
		if id == "" {
			id = res.Header.Get("Mcp-Session-Id")
		}
	}
	post("initialize", map[string]any{"protocolVersion": "2025-11-25", "clientInfo": map[string]any{"name": "delayed-get", "version": "1"}, "capabilities": map[string]any{}})
	post("logging/setLevel", map[string]any{"level": "debug"})
	post("resources/subscribe", map[string]any{"uri": "ui://fixture"})
	post("tools/call", map[string]any{"name": "notify", "arguments": map[string]any{}})
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/mcp", nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Mcp-Session-Id", id)
	req.Header.Set("Mcp-Protocol-Version", "2025-11-25")
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	seen := make(map[string]bool)
	scanner := bufio.NewScanner(res.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, `"method":"notifications/message"`) {
			seen["log"] = true
		}
		if strings.Contains(line, `"method":"notifications/resources/updated"`) {
			seen["resource"] = true
		}
		if len(seen) == 2 {
			return
		}
	}
	t.Fatalf("early notifications lost: %v %v", seen, scanner.Err())
}

func TestStartRequiresDiscoveredVersion(t *testing.T) {
	_, err := Start(t.Context(), func() *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestProcess$")
		cmd.Env = append(os.Environ(), "BRIDGE_TEST_PROCESS=raw", "BRIDGE_TEST_VERSION=2099-01-01")
		return cmd
	}, Options{ProtocolVersion: ProtocolVersion})
	if err == nil {
		t.Fatal("started a process that does not support the configured version")
	}
}

func TestProtocolAdmission(t *testing.T) {
	for _, test := range []struct {
		name, configured, requested, upstream string
		sse                                   bool
	}{
		{name: "unknown configuration", configured: "2099-01-01"},
		{name: "current startup mismatch", configured: ProtocolVersion, upstream: "2025-11-25"},
		{name: "current rejects legacy", configured: ProtocolVersion, requested: "2025-11-25"},
		{name: "legacy negotiation mismatch", configured: "2025-11-25", requested: "2025-11-25", upstream: "2025-06-18"},
		{name: "SSE negotiation mismatch", configured: "2025-11-25", requested: "2025-11-25", upstream: "2025-06-18", sse: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			server, err := Start(ctx, func() *exec.Cmd {
				cmd := commandFactory()
				if test.upstream != "" {
					cmd.Env = append(cmd.Env, "MCP_PROXY_VERSIONS="+test.upstream)
				}
				return cmd
			}, Options{ProtocolVersion: test.configured})
			if test.requested != "" {
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { server.Close() })
				host := httptest.NewServer(server)
				t.Cleanup(host.Close)
				var transport mcp.Transport = &mcp.StreamableClientTransport{Endpoint: host.URL + "/mcp"}
				if test.sse {
					transport = &mcp.SSEClientTransport{Endpoint: host.URL + "/sse"}
				}
				client := mcp.NewClient(&mcp.Implementation{Name: "admission", Version: "1"}, nil)
				var session *mcp.ClientSession
				session, err = client.Connect(ctx, transport, &mcp.ClientSessionOptions{ProtocolVersion: test.requested})
				if session != nil {
					session.Close()
				}
			} else if server != nil {
				server.Close()
			}
			var issue *jsonrpc.Error
			if !errors.As(err, &issue) || issue.Code != mcp.CodeUnsupportedProtocolVersion {
				t.Fatalf("expected unsupported protocol error, got %v", err)
			}
			var data mcp.UnsupportedProtocolVersionData
			if err := json.Unmarshal(issue.Data, &data); err != nil {
				t.Fatal(err)
			}
			wanted := test.configured
			if test.requested != "" && test.upstream == "" {
				wanted = test.requested
			}
			if data.Requested != wanted || len(data.Supported) == 0 {
				t.Fatalf("protocol error data: %+v", data)
			}
		})
	}
}

func TestLegacySessionLimit(t *testing.T) {
	var processes []*exec.Cmd
	b, err := Start(t.Context(), func() *exec.Cmd {
		cmd := commandFactory()
		processes = append(processes, cmd)
		return cmd
	}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(b)
	t.Cleanup(func() { b.Close(); server.Close() })
	for i := range 65 {
		request, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","clientInfo":{"name":"limit","version":"1"},"capabilities":{}}}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json, text/event-stream")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		status := 200
		if i == 64 {
			status = 503
		}
		if response.StatusCode != status {
			t.Fatalf("session %d: HTTP %d", i, response.StatusCode)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if len(processes) != 64 {
		t.Fatalf("started %d processes", len(processes))
	}
	for _, cmd := range processes {
		if cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
			t.Fatal("legacy process was not reaped")
		}
	}
}

// fakeIdleTimer advances only when the test requests it. It drives the same
// reset and expiry callback used by the live HTTP session and subprocess.
type fakeIdleTimer struct {
	mu            sync.Mutex
	now, deadline time.Duration
	active        bool
	fire          func()
}

func (c *fakeIdleTimer) Reset(d time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	active := c.active
	c.deadline, c.active = c.now+d, true
	return active
}

func (c *fakeIdleTimer) Stop() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	active := c.active
	c.active = false
	return active
}

func (c *fakeIdleTimer) advance(d time.Duration) bool {
	c.mu.Lock()
	c.now += d
	if !c.active || c.now < c.deadline {
		c.mu.Unlock()
		return false
	}
	c.active = false
	c.mu.Unlock()
	c.fire()
	return true
}

func TestLegacySessionExpiry(t *testing.T) {
	for _, mode := range []string{"streamable-http", "sse"} {
		t.Run(mode, func(t *testing.T) {
			b, err := Start(t.Context(), commandFactory, Options{})
			if err != nil {
				t.Fatal(err)
			}
			clock := new(fakeIdleTimer)
			b.afterFunc = func(d time.Duration, fire func()) idleTimer {
				clock.fire = fire
				clock.Reset(d)
				return clock
			}
			server := httptest.NewServer(b)
			t.Cleanup(func() { b.Close(); server.Close() })
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			method, path, data := http.MethodPost, "/mcp", `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","clientInfo":{"name":"expiry","version":"1"},"capabilities":{}}}`
			if mode == "sse" {
				method, path, data = http.MethodGet, "/sse", ""
			}
			request, _ := http.NewRequestWithContext(ctx, method, server.URL+path, strings.NewReader(data))
			request.Header.Set("Accept", "application/json, text/event-stream")
			request.Header.Set("Content-Type", "application/json")
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != 200 {
				t.Fatal(response.Status)
			}
			id := response.Header.Get("Mcp-Session-Id")
			endpoint := server.URL + "/mcp"
			stream := bufio.NewReader(response.Body)
			if mode == "sse" {
				for {
					line, err := stream.ReadString('\n')
					if err != nil {
						t.Fatal(err)
					}
					if path, ok := strings.CutPrefix(line, "data: "); ok {
						endpoint = server.URL + strings.TrimSpace(path)
						break
					}
				}
			} else {
				_, err = io.Copy(io.Discard, response.Body)
				response.Body.Close()
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
				req.Header.Set("Mcp-Session-Id", id)
				req.Header.Set("Accept", "text/event-stream")
				response, err = server.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				stream = bufio.NewReader(response.Body)
			}
			defer response.Body.Close()
			// SSE publishes its endpoint before the idle timer is installed.
			b.work.Wait()
			b.mu.Lock()
			var session *legacySession
			for _, candidate := range b.sessions {
				session = candidate
			}
			b.mu.Unlock()
			ping := func() {
				t.Helper()
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"ping","params":{}}`))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Accept", "application/json, text/event-stream")
				req.Header.Set("Mcp-Session-Id", id)
				res, err := server.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer res.Body.Close()
				reader, want := bufio.NewReader(res.Body), http.StatusOK
				if mode == "sse" {
					reader, want = stream, http.StatusAccepted
				}
				if res.StatusCode != want {
					t.Fatal(res.Status)
				}
				var raw []byte
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						t.Fatal(err)
					}
					if data, ok := strings.CutPrefix(line, "data: "); ok {
						raw = []byte(data)
						break
					}
				}
				var reply struct {
					ID     int
					Result json.RawMessage
					Error  *jsonrpc.Error
				}
				if err := json.Unmarshal(raw, &reply); err != nil || reply.ID != 2 || reply.Error != nil || len(reply.Result) == 0 {
					t.Fatalf("live session ping: %s (%v)", raw, err)
				}
			}
			if clock.advance(5*time.Minute - time.Nanosecond) {
				t.Fatal("session expired before its idle deadline")
			}
			ping() // Activity renews the full five-minute idle period.
			if clock.advance(time.Nanosecond) {
				t.Fatal("activity did not renew the idle deadline")
			}
			ping() // The original deadline must no longer expire this session.
			if clock.advance(5*time.Minute - time.Nanosecond) {
				t.Fatal("session expired before its renewed idle deadline")
			}
			if !clock.advance(time.Nanosecond) {
				t.Fatal("session did not expire at its idle deadline")
			}
			_, err = io.Copy(io.Discard, stream)
			response.Body.Close()
			if err != nil {
				t.Fatalf("idle session did not close: %v", err)
			}
			<-session.done
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
			req.Header.Set("Mcp-Session-Id", id)
			res, err := server.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			res.Body.Close()
			if res.StatusCode != http.StatusNotFound {
				t.Fatalf("expired session: HTTP %d", res.StatusCode)
			}
			if err := b.Close(); err != nil {
				t.Fatalf("session failure affected bridge: %v", err)
			}
		})
	}
}

func TestProtocolHeaderErrors(t *testing.T) {
	_, server := start(t)
	for _, test := range []struct {
		name               string
		versions           []string
		code               int64
		notificationStatus int
	}{
		{"missing", nil, mcp.CodeHeaderMismatch, http.StatusAccepted},
		{"empty", []string{""}, mcp.CodeHeaderMismatch, http.StatusAccepted},
		{"duplicate", []string{ProtocolVersion, "2099-01-01"}, mcp.CodeHeaderMismatch, http.StatusBadRequest},
		{"duplicate unsupported first", []string{"2099-01-01", ProtocolVersion}, mcp.CodeHeaderMismatch, http.StatusBadRequest},
		{"unsupported", []string{"2099-01-01"}, mcp.CodeUnsupportedProtocolVersion, http.StatusBadRequest},
	} {
		t.Run(test.name, func(t *testing.T) {
			headers := http.Header{
				"Content-Type":         {"application/json"},
				"Accept":               {"application/json, text/event-stream"},
				"Mcp-Protocol-Version": test.versions,
			}
			check := func(issue *jsonrpc.Error) {
				t.Helper()
				if issue == nil || issue.Code != test.code {
					t.Fatalf("protocol rejection = %+v, want code %d", issue, test.code)
				}
				if test.code == mcp.CodeUnsupportedProtocolVersion {
					var data mcp.UnsupportedProtocolVersionData
					if json.Unmarshal(issue.Data, &data) != nil || data.Requested != "2099-01-01" || !reflect.DeepEqual(data.Supported, []string{ProtocolVersion}) {
						t.Fatalf("negotiation data = %s", issue.Data)
					}
				}
			}
			// The public validator requires a version even when notifications
			// may omit it. Both boundaries must reject malformed versions before
			// parsing params or forwarding messages.
			check(proxy.ValidateHeaders(headers, &jsonrpc.Request{}, ProtocolVersion))
			for _, request := range []struct {
				name   string
				body   string
				status int
			}{
				{"call", `{"jsonrpc":"2.0","id":1,"method":"server/discover"}`, http.StatusBadRequest},
				{"notification", `{"jsonrpc":"2.0","method":"notifications/initialized"}`, test.notificationStatus},
			} {
				t.Run(request.name, func(t *testing.T) {
					r, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL, strings.NewReader(request.body))
					if err != nil {
						t.Fatal(err)
					}
					r.Header = headers
					response, err := server.Client().Do(r)
					if err != nil {
						t.Fatal(err)
					}
					defer response.Body.Close()
					if response.StatusCode != request.status {
						t.Fatalf("HTTP status = %d, want %d", response.StatusCode, request.status)
					}
					if response.StatusCode == http.StatusAccepted {
						return
					}
					var body struct {
						Error *jsonrpc.Error `json:"error"`
					}
					if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
						t.Fatal(err)
					}
					check(body.Error)
				})
			}
		})
	}
}
