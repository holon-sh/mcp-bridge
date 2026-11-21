package mcphttp

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
// Current requests reach next unchanged.
//
// Authentication must wrap this handler. A 2025 request body larger than
// maxMessageBytes is rejected. Responses from next, including streams and
// HTTP errors, reach the client.
//
// 2025 clients cannot receive server-initiated requests or subscriptions, and
// next does not see the capabilities they declare.
func WithLegacyClients(next http.Handler, maxMessageBytes int64) http.Handler {
	lifecycle := legacyLifecycle(mcp.NewServer(clientInfo, &mcp.ServerOptions{SupportedProtocolVersions: legacyHTTPVersions}))
	origins := http.NewCrossOriginProtection()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A request without a [protocol version header] uses 2025-03-26.
		//
		// [protocol version header]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#request-metadata
		if !slices.Contains(legacyHTTPVersions, cmp.Or(r.Header.Get(proxy.HeaderProtocolVersion), "2025-03-26")) {
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
			forwardLegacy(w, r, next, request)
		}
	})
}

var legacyHTTPVersions = []string{"2025-11-25", "2025-06-18", "2025-03-26"}

func legacyLifecycle(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{Stateless: true, JSONResponse: true})
}

func initializeLegacy(w http.ResponseWriter, r *http.Request, next http.Handler, request *jsonrpc.Request) {
	params, _ := json.Marshal(&mcp.DiscoverParams{Meta: clientMeta(proxy.CurrentVersion)})
	result, err := query(w, r, next, request.ID, proxy.MethodDiscover, params)
	if err != nil {
		return
	}
	var discovered struct {
		mcp.DiscoverResult
		Meta struct {
			ServerInfo *mcp.Implementation `json:"io.modelcontextprotocol/serverInfo"`
		} `json:"_meta"`
	}
	var upstream struct {
		Capabilities map[string]json.RawMessage `json:"capabilities"`
	}
	if json.Unmarshal(result, &discovered) != nil || json.Unmarshal(result, &upstream) != nil {
		WriteError(w, 502, request.ID, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "invalid server/discover result"})
		return
	}
	capabilities := translate.Capabilities(discovered.Capabilities)
	if discovered.Capabilities != nil && discovered.Capabilities.Extensions[eventsExtension] != nil {
		capabilities.AddExtension(eventsExtension, nil)
	}
	response := httptest.NewRecorder()
	legacyLifecycle(mcp.NewServer(cmp.Or(discovered.Meta.ServerInfo, clientInfo), &mcp.ServerOptions{
		SupportedProtocolVersions: legacyHTTPVersions,
		Capabilities:              capabilities,
		Instructions:              discovered.Instructions,
	})).ServeHTTP(response, r)
	if reply := recordedReply(response); reply != nil && reply.Error == nil && upstream.Capabilities["events"] != nil {
		var fields, served map[string]json.RawMessage
		_ = json.Unmarshal(reply.Result, &fields)
		_ = json.Unmarshal(fields["capabilities"], &served)
		served["events"] = json.RawMessage(`{}`)
		fields["capabilities"], _ = json.Marshal(served)
		reply.Result, _ = json.Marshal(fields)
		data, _ := jsonrpc.EncodeMessage(reply)
		response.Body = bytes.NewBuffer(data)
	}
	copyResponse(w, response)
}

const eventsExtension = "io.modelcontextprotocol/events"

func forwardLegacy(w http.ResponseWriter, r *http.Request, next http.Handler, request *jsonrpc.Request) {
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
	r = currentRequest(r.Context(), r, request)
	err := proxy.SetHeaders(r.Context(), r.Header, request, func(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
		return query(w, r.WithContext(ctx), next, request.ID, method, params)
	})
	switch {
	case errors.Is(err, errAnswered):
	case err != nil:
		writeHeaderError(w, request.ID, err)
	case request.Method == proxy.MethodListTools || request.Method == proxy.MethodCallTool:
		results := &legacyResults{ResponseWriter: w}
		next.ServeHTTP(results, r)
		results.close()
	default:
		next.ServeHTTP(w, r)
	}
}

type legacyResults struct {
	http.ResponseWriter
	body []byte
}

func (w *legacyResults) WriteHeader(status int) {
	w.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(status)
}

func (w *legacyResults) Write(data []byte) (int, error) {
	w.body = append(w.body, data...)
	if !stream(w.Header()) || bytes.IndexByte(data, '\n') < 0 {
		return len(data), nil
	}
	var events, event []byte
	for line := range bytes.Lines(w.body) {
		event = append(event, line...)
		if string(bytes.TrimLeft(line, "\r")) == "\n" {
			events = append(events, w.legacy(event)...)
			event = nil
		}
	}
	w.body = event
	_, err := w.ResponseWriter.Write(events)
	return len(data), err
}

func (w *legacyResults) Flush() { _ = http.NewResponseController(w.ResponseWriter).Flush() }

func (w *legacyResults) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *legacyResults) close() { _, _ = w.ResponseWriter.Write(w.legacy(w.body)) }

func stream(header http.Header) bool {
	media, _, _ := mime.ParseMediaType(header.Get("Content-Type"))
	return media == "text/event-stream"
}

func (w *legacyResults) legacy(message []byte) []byte {
	decode, frame := decodeResponse, "%s"
	if stream(w.Header()) {
		decode, frame = streamReply, "data: %s\n\n"
	}
	reply := decode(message)
	if reply == nil {
		return message
	}
	result := translate.StructuredOutput(reply.Result)
	if result == nil {
		return message
	}
	reply.Result = result
	data, _ := jsonrpc.EncodeMessage(reply)
	return fmt.Appendf(nil, frame, data)
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
		copyResponse(w, response)
	}
	return nil, errAnswered
}

func copyResponse(w http.ResponseWriter, response *httptest.ResponseRecorder) {
	maps.Copy(w.Header(), response.Header())
	w.WriteHeader(response.Code)
	_, _ = response.Body.WriteTo(w)
}

func currentRequest(ctx context.Context, r *http.Request, request *jsonrpc.Request) *http.Request {
	r = r.Clone(ctx)
	maps.DeleteFunc(r.Header, func(key string, _ []string) bool { return strings.HasPrefix(strings.ToLower(key), "mcp-") })
	r.Header.Set(proxy.HeaderProtocolVersion, proxy.CurrentVersion)
	r.Header.Set(proxy.HeaderMethod, request.Method)
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
	if !stream(response.Header()) {
		return decodeResponse(body)
	}
	return streamReply(body)
}

func streamReply(body []byte) *jsonrpc.Response {
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
