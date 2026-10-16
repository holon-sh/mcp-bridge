package mcphttp

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"

	"github.com/hughhan1/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// WithLegacyTools adds sessionless 2025 Streamable HTTP clients to a current
// tools-only handler. Authentication must wrap this handler. Each translated
// request retains its own context and credentials; no session IDs are issued.
// The current handler must return JSON for tools/list. Tool responses, including
// streams and HTTP authorization failures, pass through unchanged.
// Legacy clients cannot receive server-initiated requests or subscriptions.
func WithLegacyTools(next http.Handler, info *mcp.Implementation) http.Handler {
	server := mcp.NewServer(info, &mcp.ServerOptions{SupportedProtocolVersions: legacyHTTPVersions, Capabilities: &mcp.ServerCapabilities{Tools: &mcp.ToolCapabilities{}}})
	lifecycle := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
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
		var request *jsonrpc.Request
		method := ""
		if r.Method == http.MethodPost {
			msg := ReadMessage(w, r, proxy.MaxMessageBytes)
			if msg == nil {
				return
			}
			if request, _ = msg.(*jsonrpc.Request); request != nil && request.IsCall() {
				method = request.Method
			}
		}
		switch method {
		case "", "initialize", "ping":
			lifecycle.ServeHTTP(w, r)
		case "tools/list", "tools/call":
			forwardLegacyTool(w, r, next, request)
		default:
			WriteError(w, 404, request.ID, &jsonrpc.Error{Code: jsonrpc.CodeMethodNotFound, Message: "method not supported by the sessionless tools bridge"})
		}
	})
}

var legacyHTTPVersions = []string{"2025-11-25", "2025-06-18", "2025-03-26"}

func forwardLegacyTool(w http.ResponseWriter, r *http.Request, next http.Handler, request *jsonrpc.Request) {
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
	r = r.Clone(r.Context())
	maps.DeleteFunc(r.Header, func(key string, _ []string) bool { return strings.HasPrefix(strings.ToLower(key), "mcp-") })
	r.Header.Set("Mcp-Protocol-Version", proxy.CurrentVersion)
	var rejected *httptest.ResponseRecorder
	err := proxy.SetHeaders(r.Context(), r.Header, request, func(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
		query := r.Clone(ctx)
		query.Header.Set("Mcp-Method", method)
		setBody(query, &jsonrpc.Request{ID: request.ID, Method: method, Params: params})
		response := httptest.NewRecorder()
		next.ServeHTTP(response, query)
		message, _ := jsonrpc.DecodeMessage(response.Body.Bytes())
		if reply, ok := message.(*jsonrpc.Response); ok && reply.Error == nil {
			return reply.Result, nil
		}
		rejected = response
		return nil, errors.New("tool catalog rejected the request")
	})
	switch {
	case rejected != nil:
		maps.Copy(w.Header(), rejected.Header())
		w.WriteHeader(rejected.Code)
		_, _ = rejected.Body.WriteTo(w)
	case err != nil:
		writeHeaderError(w, request.ID, err)
	default:
		setBody(r, request)
		next.ServeHTTP(w, r)
	}
}

func setBody(r *http.Request, request *jsonrpc.Request) {
	body, _ := jsonrpc.EncodeMessage(request)
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
}
