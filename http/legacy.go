package mcphttp

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"mime"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"

	"github.com/holon-sh/mcp-bridge/internal/translate"
	"github.com/holon-sh/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// WithLegacyTools adds sessionless 2025 Streamable HTTP clients to a current
// tools-only handler. Authentication must wrap this handler. Each translated
// request retains its own context and credentials; no session IDs are issued.
// The current handler must return JSON for tools/list. Tool responses, including
// streams and HTTP authorization failures, pass through unchanged.
// Legacy clients cannot receive server-initiated requests or subscriptions.
func WithLegacyTools(next http.Handler, catalog func(tool string) http.Handler, info *mcp.Implementation) http.Handler {
	server := mcp.NewServer(info, &mcp.ServerOptions{SupportedProtocolVersions: legacyHTTPVersions, Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}}})
	lifecycle := legacyLifecycle(server)
	return withLegacy(next, proxy.MaxMessageBytes, func(w http.ResponseWriter, r *http.Request, request *jsonrpc.Request) {
		switch request.Method {
		case "", "initialize", "ping":
			lifecycle.ServeHTTP(w, r)
		case "tools/list", "tools/call":
			forwardLegacy(w, r, next, catalog, request)
		default:
			WriteError(w, 404, request.ID, &jsonrpc.Error{Code: jsonrpc.CodeMethodNotFound, Message: "method not supported by the sessionless tools bridge"})
		}
	})
}

// WithLegacyClients adds sessionless 2025 Streamable HTTP clients to a current
// handler for one MCP server. It answers initialize from the server's
// server/discover result and forwards the tools, resources, prompts and
// completion methods as current requests, with the request metadata headers
// and _meta those require. next answers those requests, including the
// server/discover and tools/list requests the bridge sends to answer
// initialize and to find a tool's header arguments.
//
// Authentication must wrap this handler. Each translated request retains its
// own context and credentials; no session IDs are issued. A legacy request
// body may hold at most maxMessageBytes; current requests reach next unread.
// Responses, including streams and HTTP authorization failures, pass through
// unchanged, except that an input-required result becomes an error.
//
// Legacy clients cannot receive server-initiated requests or subscriptions.
// The server sees the bridge, not the legacy client, as its client: the
// capabilities a legacy client declares at initialize do not reach it.
func WithLegacyClients(next http.Handler, maxMessageBytes int64) http.Handler {
	lifecycle := legacyLifecycle(mcp.NewServer(clientInfo, &mcp.ServerOptions{SupportedProtocolVersions: legacyHTTPVersions}))
	catalog := func(string) http.Handler { return next }
	return withLegacy(next, maxMessageBytes, func(w http.ResponseWriter, r *http.Request, request *jsonrpc.Request) {
		switch request.Method {
		case "", "ping":
			lifecycle.ServeHTTP(w, r)
		case "initialize":
			initializeLegacy(w, r, next, request)
		case "tools/list", "resources/list", "resources/templates/list", "prompts/list", "completion/complete":
			forwardLegacy(w, r, next, catalog, request)
		case "tools/call", "resources/read", "prompts/get":
			// [Multi round-trip requests] may answer these with input requests,
			// which a legacy client has no way to answer.
			//
			// [Multi round-trip requests]: https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/mrtr#supported-requests
			results := &legacyResults{ResponseWriter: w}
			forwardLegacy(results, r, next, catalog, request)
			results.finish()
		default:
			WriteError(w, 404, request.ID, &jsonrpc.Error{Code: jsonrpc.CodeMethodNotFound, Message: "method not supported by the sessionless bridge"})
		}
	})
}

var legacyHTTPVersions = []string{"2025-11-25", "2025-06-18", "2025-03-26"}

func LegacyToolsVersions() []string {
	return slices.Clone(legacyHTTPVersions)
}

func legacyLifecycle(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
}

// withLegacy sends legacy requests to serve, with the method of a call or an
// empty method for anything else, and current requests to next.
func withLegacy(next http.Handler, maxMessageBytes int64, serve func(http.ResponseWriter, *http.Request, *jsonrpc.Request)) http.Handler {
	origins := http.NewCrossOriginProtection()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A request without a [protocol version header] uses 2025-03-26.
		//
		// [protocol version header]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#request-metadata
		if !slices.Contains(legacyHTTPVersions, cmp.Or(r.Header.Get("Mcp-Protocol-Version"), "2025-03-26")) {
			next.ServeHTTP(w, r)
			return
		}
		if err := origins.Check(r); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		call := &jsonrpc.Request{}
		if r.Method == http.MethodPost {
			msg := ReadMessage(w, r, maxMessageBytes)
			if msg == nil {
				return
			}
			if request, _ := msg.(*jsonrpc.Request); request != nil && request.IsCall() {
				call = request
			}
		}
		serve(w, r, call)
	})
}

// initializeLegacy answers initialize with what server/discover reports.
func initializeLegacy(w http.ResponseWriter, r *http.Request, next http.Handler, request *jsonrpc.Request) {
	params, _ := json.Marshal(&mcp.DiscoverParams{Meta: clientMeta(proxy.CurrentVersion)})
	query := currentRequest(r, &jsonrpc.Request{ID: request.ID, Method: "server/discover", Params: params})
	query.Header.Set("Mcp-Method", "server/discover")
	response := httptest.NewRecorder()
	next.ServeHTTP(response, query)
	reply := recordedReply(response)
	if reply == nil || reply.Error != nil {
		relay(w, response)
		return
	}
	var discovered mcp.DiscoverResult
	if json.Unmarshal(reply.Result, &discovered) != nil {
		WriteError(w, 502, request.ID, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "invalid discovery response"})
		return
	}
	var info *mcp.Implementation
	if raw, err := json.Marshal(discovered.Meta[mcp.MetaKeyServerInfo]); err == nil {
		_ = json.Unmarshal(raw, &info)
	}
	if info == nil || info.Name == "" {
		info = clientInfo
	}
	legacyLifecycle(mcp.NewServer(info, &mcp.ServerOptions{
		SupportedProtocolVersions: legacyHTTPVersions,
		Capabilities:              translate.Capabilities(discovered.Capabilities),
		Instructions:              discovered.Instructions,
	})).ServeHTTP(w, r)
}

func forwardLegacy(w http.ResponseWriter, r *http.Request, next http.Handler, catalog func(string) http.Handler, request *jsonrpc.Request) {
	params := map[string]json.RawMessage{}
	var meta map[string]json.RawMessage
	if _ = json.Unmarshal(request.Params, &params); params == nil {
		WriteError(w, 400, request.ID, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "params must be an object"})
		return
	}
	_ = json.Unmarshal(params["_meta"], &meta)
	bridge, _ := json.Marshal(clientMeta(proxy.CurrentVersion))
	_ = json.Unmarshal(bridge, &meta)
	params["_meta"], _ = json.Marshal(meta)
	request.Params, _ = json.Marshal(params)
	var tool string
	_ = json.Unmarshal(params["name"], &tool)
	r = currentRequest(r, request)
	var rejected *httptest.ResponseRecorder
	err := proxy.SetHeaders(r.Context(), r.Header, request, func(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
		query := r.Clone(ctx)
		query.Header.Set("Mcp-Method", method)
		setBody(query, &jsonrpc.Request{ID: request.ID, Method: method, Params: params})
		response := httptest.NewRecorder()
		catalog(tool).ServeHTTP(response, query)
		if reply := recordedReply(response); reply != nil && reply.Error == nil {
			return reply.Result, nil
		}
		rejected = response
		return nil, errors.New("tool catalog rejected the request")
	})
	switch {
	case rejected != nil:
		relay(w, rejected)
	case err != nil:
		writeHeaderError(w, request.ID, err)
	default:
		next.ServeHTTP(w, r)
	}
}

// currentRequest returns a copy of r that carries request with the current
// protocol version and no other MCP headers.
func currentRequest(r *http.Request, request *jsonrpc.Request) *http.Request {
	r = r.Clone(r.Context())
	maps.DeleteFunc(r.Header, func(key string, _ []string) bool { return strings.HasPrefix(strings.ToLower(key), "mcp-") })
	r.Header.Set("Mcp-Protocol-Version", proxy.CurrentVersion)
	setBody(r, request)
	return r
}

// recordedReply returns the JSON-RPC response in a successful reply, which is
// JSON or an event stream, or nil if there is none.
func recordedReply(response *httptest.ResponseRecorder) *jsonrpc.Response {
	if response.Code != http.StatusOK {
		return nil
	}
	media, _, _ := mime.ParseMediaType(response.Header().Get("Content-Type"))
	if media != "text/event-stream" {
		reply, _ := decodeResponse(response.Body.Bytes())
		return reply
	}
	lines := bufio.NewScanner(bytes.NewReader(response.Body.Bytes()))
	lines.Buffer(nil, proxy.MaxMessageBytes)
	for lines.Scan() {
		if data, ok := bytes.CutPrefix(lines.Bytes(), []byte("data:")); ok {
			if reply, _ := decodeResponse(data); reply != nil {
				return reply
			}
		}
	}
	return nil
}

func decodeResponse(data []byte) (*jsonrpc.Response, error) {
	message, err := jsonrpc.DecodeMessage(bytes.TrimSpace(data))
	reply, _ := message.(*jsonrpc.Response)
	return reply, err
}

func relay(w http.ResponseWriter, response *httptest.ResponseRecorder) {
	maps.Copy(w.Header(), response.Header())
	w.WriteHeader(response.Code)
	_, _ = response.Body.WriteTo(w)
}

func setBody(r *http.Request, request *jsonrpc.Request) {
	body, _ := jsonrpc.EncodeMessage(request)
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
}

// legacyResults replaces an input-required result in a JSON response or in a
// line of an event stream with an error. It holds at most inspectBytes of a
// message: a larger one passes through, since an input-required result carries
// only its input requests.
type legacyResults struct {
	http.ResponseWriter
	started, inspect, stream, overflow bool
	pending                            []byte
}

const inspectBytes = 64 << 10

func (w *legacyResults) WriteHeader(status int) {
	if w.started {
		return
	}
	w.started = true
	media, _, _ := mime.ParseMediaType(w.Header().Get("Content-Type"))
	w.stream = media == "text/event-stream"
	w.inspect = status == http.StatusOK && (w.stream || media == "application/json")
	if w.inspect && !w.stream {
		w.Header().Del("Content-Length")
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *legacyResults) Write(p []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	if !w.inspect {
		return w.ResponseWriter.Write(p)
	}
	written := len(p)
	for len(p) > 0 {
		end := len(p)
		if i := bytes.IndexByte(p, '\n'); w.stream && i >= 0 {
			end = i + 1
		}
		chunk, complete := p[:end], w.stream && p[end-1] == '\n'
		p = p[end:]
		var err error
		switch {
		case w.overflow:
			_, err = w.ResponseWriter.Write(chunk)
			w.overflow = !complete
		case complete:
			w.pending = append(w.pending, chunk...)
			err = w.finish()
		default:
			w.pending = append(w.pending, chunk...)
			if len(w.pending) > inspectBytes {
				_, err = w.ResponseWriter.Write(w.pending)
				w.pending, w.overflow = nil, true
			}
		}
		if err != nil {
			return 0, err
		}
	}
	return written, nil
}

// finish writes the message held so far.
func (w *legacyResults) finish() error {
	if len(w.pending) == 0 {
		return nil
	}
	data := w.pending
	w.pending = nil
	payload, prefixed := bytes.CutPrefix(data, []byte("data:"))
	if w.stream && !prefixed {
		_, err := w.ResponseWriter.Write(data)
		return err
	}
	reply, _ := decodeResponse(payload)
	var result struct {
		ResultType string `json:"resultType"`
	}
	if reply != nil && reply.Error == nil && json.Unmarshal(reply.Result, &result) == nil && result.ResultType == "input_required" {
		data, _ = jsonrpc.EncodeMessage(&jsonrpc.Response{ID: reply.ID, Error: &jsonrpc.Error{
			Code: jsonrpc.CodeInternalError, Message: "MCP translation does not support input-required results",
		}})
		if w.stream {
			data = append(append([]byte("data: "), data...), '\n')
		}
	}
	_, err := w.ResponseWriter.Write(data)
	return err
}

func (w *legacyResults) Flush() {
	w.WriteHeader(http.StatusOK)
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func (w *legacyResults) Unwrap() http.ResponseWriter { return w.ResponseWriter }
