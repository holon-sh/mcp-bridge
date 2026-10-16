package mcphttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/hughhan1/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func (b *currentBridge) serveHTTP(w http.ResponseWriter, r *http.Request, msg jsonrpc.Message) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", 405)
		return
	}
	select {
	case <-b.stopping:
		http.Error(w, "MCP process stopped", 503)
		return
	default:
	}
	if !accepts(r.Header, "application/json") || !accepts(r.Header, "text/event-stream") {
		http.Error(w, "Accept must include application/json and text/event-stream", 406)
		return
	}
	if msg == nil {
		msg = ReadMessage(w, r, proxy.MaxMessageBytes)
		if msg == nil {
			return
		}
	}
	req, ok := msg.(*jsonrpc.Request)
	if !ok {
		WriteError(w, 400, jsonrpc.ID{}, &jsonrpc.Error{Code: jsonrpc.CodeInvalidRequest, Message: "expected a request or notification"})
		return
	}
	if req.Method == "notifications/cancelled" {
		WriteError(w, 400, req.ID, &jsonrpc.Error{Code: jsonrpc.CodeInvalidRequest, Message: "HTTP requests are cancelled by closing their response stream"})
		return
	}
	if issue := proxy.ValidateProtocolHeader(r.Header, b.version); issue != nil {
		WriteError(w, 400, req.ID, issue)
		return
	}
	if !req.IsCall() {
		if err := b.write(r.Context(), req); err != nil {
			http.Error(w, "MCP process unavailable", 503)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		return
	}
	if req.Method == "initialize" {
		var params mcp.InitializeParams
		_ = json.Unmarshal(req.Params, &params)
		WriteError(w, 400, req.ID, proxy.UnsupportedVersion(params.ProtocolVersion, []string{b.version}))
		return
	}
	if issue := proxy.ValidateHeaders(r.Header, req, b.version); issue != nil {
		WriteError(w, 400, req.ID, issue)
		return
	}
	if req.Method == "tools/call" {
		arguments, err := proxy.ToolHeaderArguments(r.Context(), req.Params, b.request)
		if err = errors.Join(err, proxy.ValidateArgumentHeaders(r.Header, arguments)); err != nil {
			writeHeaderError(w, req.ID, err)
			return
		}
	}
	c, err := b.begin(r.Context(), req)
	if err != nil {
		WriteError(w, 503, req.ID, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: err.Error()})
		return
	}
	defer b.finish(c, true)
	streaming := false
	startStream := func() error {
		if streaming {
			return nil
		}
		streaming = true
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("X-Accel-Buffering", "no")
		return writeEvent(w, ": connected\n\n")
	}
	if req.Method == "subscriptions/listen" && startStream() != nil {
		return
	}
	for {
		wait, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		message, err := b.receive(wait, c)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) && r.Context().Err() == nil {
			if streaming && writeEvent(w, ": keepalive\n\n") != nil {
				return
			}
			continue
		}
		if err != nil {
			if !streaming {
				WriteError(w, 502, req.ID, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "MCP process stopped"})
			} else {
				data, _ := jsonrpc.EncodeMessage(&jsonrpc.Response{ID: req.ID, Error: &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "MCP process stopped"}})
				_ = writeEvent(w, "event: message\ndata: "+string(data)+"\n\n")
			}
			return
		}
		response, isResponse := message.(*jsonrpc.Response)
		if isResponse && response.Error == nil && b.pinned && req.Method == "server/discover" {
			var result map[string]json.RawMessage
			if json.Unmarshal(response.Result, &result) != nil || result == nil {
				WriteError(w, 502, req.ID, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "invalid discovery response"})
				return
			}
			result["supportedVersions"], _ = json.Marshal([]string{b.version})
			response.Result, _ = json.Marshal(result)
		}
		data, _ := jsonrpc.EncodeMessage(message)
		if isResponse && !streaming {
			w.Header().Set("Content-Type", "application/json")
			status := http.StatusOK
			var wire *jsonrpc.Error
			if errors.As(response.Error, &wire) {
				status = errorStatus(wire.Code, status)
			}
			w.WriteHeader(status)
			_, _ = w.Write(data)
			return
		}
		if startStream() != nil || writeEvent(w, "event: message\ndata: "+string(data)+"\n\n") != nil {
			return
		}
		if isResponse || message.(*jsonrpc.Request).Method == "notifications/cancelled" {
			return
		}
	}
}

// ReadMessage reads one JSON-RPC message from an application/json request body,
// limited to maxBytes. The body remains readable on success. On failure it writes
// an HTTP error response and returns nil.
func ReadMessage(w http.ResponseWriter, r *http.Request, maxBytes int64) jsonrpc.Message {
	if typ, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type")); typ != "application/json" {
		http.Error(w, "Content-Type must be application/json", 415)
		return nil
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBytes))
	if err != nil {
		var limit *http.MaxBytesError
		status := http.StatusBadRequest
		if errors.As(err, &limit) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, "invalid request body", status)
		return nil
	}
	r.Body = io.NopCloser(bytes.NewReader(data))
	msg, err := jsonrpc.DecodeMessage(data)
	if err != nil {
		WriteError(w, 400, jsonrpc.ID{}, &jsonrpc.Error{Code: jsonrpc.CodeParseError, Message: "invalid JSON-RPC message"})
		return nil
	}
	return msg
}

func writeEvent(w http.ResponseWriter, value string) error {
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(10 * time.Second))
	defer func() { _ = controller.SetWriteDeadline(time.Time{}) }()
	_, err := io.WriteString(w, value)
	return errors.Join(err, controller.Flush())
}

// WriteError sends a JSON-RPC error response with the supplied HTTP status and
// request ID. Use a zero jsonrpc.ID when the ID is unknown. Call it before writing
// any other response headers or body.
func WriteError(w http.ResponseWriter, status int, id jsonrpc.ID, issue *jsonrpc.Error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	encoded, _ := jsonrpc.EncodeMessage(&jsonrpc.Response{ID: id, Error: issue})
	_, _ = w.Write(encoded)
}

func errorStatus(code int64, fallback int) int {
	switch code {
	case jsonrpc.CodeMethodNotFound:
		return http.StatusNotFound
	case mcp.CodeHeaderMismatch, mcp.CodeUnsupportedProtocolVersion, jsonrpc.CodeInvalidParams:
		return http.StatusBadRequest
	}
	return fallback
}

func writeHeaderError(w http.ResponseWriter, id jsonrpc.ID, err error) {
	var wire *jsonrpc.Error
	if errors.As(err, &wire) {
		WriteError(w, 400, id, wire)
		return
	}
	WriteError(w, 502, id, &jsonrpc.Error{Code: jsonrpc.CodeInternalError, Message: "could not read upstream tool headers"})
}

func accepts(headers http.Header, target string) bool {
	category, _, _ := strings.Cut(target, "/")
	for _, value := range headers.Values("Accept") {
		for _, entry := range strings.Split(value, ",") {
			media, params, err := mime.ParseMediaType(strings.TrimSpace(entry))
			if err == nil && params["q"] != "0" && (media == target || media == "*/*" || media == category+"/*") {
				return true
			}
		}
	}
	return false
}
