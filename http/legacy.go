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

// WithLegacyClients lets clients of the 2025 Streamable HTTP protocol versions
// use next, a handler for the current version, without sessions. next receives
// each of their requests as a current request with the same credentials.
// catalog(tool) must answer tools/list with the named tool's definition.
// Current requests reach next unchanged.
//
// Authentication must wrap this handler. A 2025 request body larger than
// maxMessageBytes is rejected. Responses from next, including streams and
// HTTP errors, reach the client unchanged.
//
// 2025 clients cannot receive server-initiated requests or subscriptions, and
// next does not see the capabilities they declare.
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
		default:
			forwardLegacy(w, r, next, catalog, request)
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
	// An [event's data] is its data fields joined by newlines.
	//
	// [event's data]: https://html.spec.whatwg.org/multipage/server-sent-events.html#dispatchMessage
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
