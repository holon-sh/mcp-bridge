package mcphttp

import (
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

// WithLegacyClients adds sessionless 2025 Streamable HTTP clients to next, a
// current handler. It answers initialize from next's server/discover result and
// forwards the tools, resources, prompts and completion methods as current
// requests, with the request metadata headers and _meta those require.
// catalog(tool) answers the tools/list requests that find a tool's header
// arguments.
//
// Authentication must wrap this handler. Each translated request retains its
// own context and credentials; no session IDs are issued. A legacy request body
// may hold at most maxMessageBytes; current requests reach next unread.
// Responses, including streams and HTTP authorization failures, pass through
// unchanged.
//
// Legacy clients cannot receive server-initiated requests or subscriptions. The
// server sees the bridge as a client without capabilities: what a legacy client
// declares at initialize does not reach it, and a server that needs a client
// capability answers with an error instead of an input request.
func WithLegacyClients(next http.Handler, catalog func(tool string) http.Handler, maxMessageBytes int64) http.Handler {
	lifecycle := legacyLifecycle(mcp.NewServer(clientInfo, &mcp.ServerOptions{SupportedProtocolVersions: legacyHTTPVersions}))
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
		request := &jsonrpc.Request{}
		if r.Method == http.MethodPost {
			msg := ReadMessage(w, r, maxMessageBytes)
			if msg == nil {
				return
			}
			if call, _ := msg.(*jsonrpc.Request); call != nil && call.IsCall() {
				request = call
			}
		}
		switch request.Method {
		case "", "ping":
			lifecycle.ServeHTTP(w, r)
		case "initialize":
			initializeLegacy(w, r, next, request)
		case "tools/list", "tools/call", "resources/list", "resources/templates/list", "resources/read", "prompts/list", "prompts/get", "completion/complete":
			forwardLegacy(w, r, next, catalog, request)
		default:
			WriteError(w, 404, request.ID, &jsonrpc.Error{Code: jsonrpc.CodeMethodNotFound, Message: "method not supported by the sessionless bridge"})
		}
	})
}

var legacyHTTPVersions = []string{"2025-11-25", "2025-06-18", "2025-03-26"}

func legacyLifecycle(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
}

func initializeLegacy(w http.ResponseWriter, r *http.Request, next http.Handler, request *jsonrpc.Request) {
	params, _ := json.Marshal(&mcp.DiscoverParams{Meta: clientMeta(proxy.CurrentVersion)})
	result, err := query(w, r, next, request.ID, "server/discover", params)
	if err != nil {
		return
	}
	var discovered struct {
		mcp.DiscoverResult
		Meta struct {
			ServerInfo *mcp.Implementation `json:"io.modelcontextprotocol/serverInfo"`
		} `json:"_meta"`
	}
	if json.Unmarshal(result, &discovered) != nil {
		WriteError(w, 502, request.ID, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "invalid server/discover result"})
		return
	}
	legacyLifecycle(mcp.NewServer(cmp.Or(discovered.Meta.ServerInfo, clientInfo), &mcp.ServerOptions{
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
	r = currentRequest(r.Context(), r, request)
	err := proxy.SetHeaders(r.Context(), r.Header, request, func(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
		return query(w, r.WithContext(ctx), catalog(tool), request.ID, method, params)
	})
	switch {
	case errors.Is(err, errAnswered):
	case err != nil:
		writeHeaderError(w, request.ID, err)
	default:
		next.ServeHTTP(w, r)
	}
}

var errAnswered = errors.New("answered by the current handler")

func query(w http.ResponseWriter, r *http.Request, handler http.Handler, id jsonrpc.ID, method string, params json.RawMessage) (json.RawMessage, error) {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, currentRequest(r.Context(), r, &jsonrpc.Request{ID: id, Method: method, Params: params}))
	reply := recordedReply(response)
	switch {
	case reply != nil && reply.Error == nil:
		return reply.Result, nil
	case reply == nil && response.Code == http.StatusOK:
		WriteError(w, 502, id, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "invalid " + method + " reply"})
	default:
		maps.Copy(w.Header(), response.Header())
		w.WriteHeader(response.Code)
		_, _ = response.Body.WriteTo(w)
	}
	return nil, errAnswered
}

func currentRequest(ctx context.Context, r *http.Request, request *jsonrpc.Request) *http.Request {
	r = r.Clone(ctx)
	maps.DeleteFunc(r.Header, func(key string, _ []string) bool { return strings.HasPrefix(strings.ToLower(key), "mcp-") })
	r.Header.Set("Mcp-Protocol-Version", proxy.CurrentVersion)
	r.Header.Set("Mcp-Method", request.Method)
	setBody(r, request)
	return r
}

func setBody(r *http.Request, request *jsonrpc.Request) {
	body, _ := jsonrpc.EncodeMessage(request)
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
}

func recordedReply(response *httptest.ResponseRecorder) *jsonrpc.Response {
	if response.Code != http.StatusOK {
		return nil
	}
	body := response.Body.Bytes()
	if media, _, _ := mime.ParseMediaType(response.Header().Get("Content-Type")); media != "text/event-stream" {
		return decodeResponse(body)
	}
	var data []byte
	for line := range bytes.Lines(body) {
		if line = bytes.TrimRight(line, "\r\n"); len(line) == 0 {
			if reply := decodeResponse(data); reply != nil {
				return reply
			}
			data = nil
		} else if value, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			data = append(append(data, bytes.TrimPrefix(value, []byte(" "))...), '\n')
		}
	}
	return decodeResponse(data)
}

func decodeResponse(data []byte) *jsonrpc.Response {
	message, _ := jsonrpc.DecodeMessage(bytes.TrimSpace(data))
	reply, _ := message.(*jsonrpc.Response)
	return reply
}
