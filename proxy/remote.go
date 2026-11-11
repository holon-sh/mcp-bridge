package proxy

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"mime"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"

	"github.com/holon-sh/mcp-bridge/internal/remote"
	"github.com/holon-sh/mcp-bridge/internal/translate"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewRemoteTransport wraps base so current requests, which callers have checked
// with [ValidateHeaders], can use legacy Streamable HTTP servers. A legacy
// server's result gets the fields of a current result, and a tools/list,
// tools/call or server/discover request the server refuses is retried in a
// legacy session.
func NewRemoteTransport(base http.RoundTripper) http.RoundTripper {
	return legacyFallback{base: cmp.Or(base, http.DefaultTransport)}
}

type legacyFallback struct{ base http.RoundTripper }

func (t legacyFallback) RoundTrip(r *http.Request) (*http.Response, error) {
	sent, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		return nil, err
	}
	r = r.Clone(r.Context())
	r.Header.Del("Accept-Encoding")
	r.Body = io.NopCloser(bytes.NewReader(sent))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(sent)), nil }
	response, err := t.base.RoundTrip(r)
	if err != nil {
		return response, err
	}
	media, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusBadRequest || media != "application/json" {
		return response, nil
	}
	originalBody := response.Body
	body, err := io.ReadAll(io.LimitReader(originalBody, mcp.DefaultMaxLineLength+1))
	if err != nil {
		_ = originalBody.Close()
		return nil, err
	}
	response.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(body), originalBody), originalBody}
	message, _ := jsonrpc.DecodeMessage(body)
	reply, ok := message.(*jsonrpc.Response)
	if !ok {
		return response, nil
	}
	method := r.Header.Get("Mcp-Method")
	if errors.Is(reply.Error, &jsonrpc.Error{Code: mcp.CodeUnsupportedProtocolVersion}) && slices.Contains([]string{"server/discover", "tools/list", "tools/call"}, method) ||
		method == "server/discover" && errors.Is(reply.Error, &jsonrpc.Error{Code: jsonrpc.CodeMethodNotFound}) {
		message, _ = jsonrpc.DecodeMessage(sent)
		if request, ok := message.(*jsonrpc.Request); ok {
			_ = originalBody.Close()
			return t.legacy(r, request)
		}
	}
	if result := translate.Result(reply.Result); result != nil {
		reply.Result = result
		body, _ = jsonrpc.EncodeMessage(reply)
		_ = originalBody.Close()
		response.Body, response.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
		response.Header.Del("Content-Length")
	}
	return response, nil
}

func (t legacyFallback) legacy(r *http.Request, request *jsonrpc.Request) (*http.Response, error) {
	params := map[string]json.RawMessage{"arguments": json.RawMessage(`{}`)}
	_ = json.Unmarshal(request.Params, &params)
	var meta map[string]json.RawMessage
	_ = json.Unmarshal(params["_meta"], &meta)
	var info mcp.Implementation
	_ = json.Unmarshal(meta[mcp.MetaKeyClientInfo], &info)
	for _, key := range []string{mcp.MetaKeyProtocolVersion, mcp.MetaKeyClientInfo, mcp.MetaKeyClientCapabilities, "progressToken"} {
		delete(meta, key)
	}
	params["_meta"], _ = json.Marshal(meta)
	raw, _ := json.Marshal(params)
	versions := mcp.SupportedProtocolVersions()
	version := versions[slices.IndexFunc(versions, IsLegacyVersion)]
	client := mcp.NewClient(&info, &mcp.ClientOptions{
		Capabilities: &mcp.ClientCapabilities{}, MultiRoundTrip: &mcp.MultiRoundTripOptions{Disabled: true},
		Logger: slog.New(slog.DiscardHandler),
	})
	headers := r.Header.Clone()
	maps.DeleteFunc(headers, func(key string, _ []string) bool {
		lower := strings.ToLower(key)
		return strings.HasPrefix(lower, "mcp-") || slices.Contains([]string{"last-event-id", "content-length", "accept", "content-type"}, lower)
	})
	var failure atomic.Pointer[http.Response]
	session, err := client.Connect(r.Context(), remote.Streamable(mcp.StreamableClientTransport{
		Endpoint: r.URL.String(),
		HTTPClient: &http.Client{
			Transport:     t.base,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, mcp.DefaultMaxLineLength, func(base http.RoundTripper, next *http.Request) (*http.Response, error) {
		maps.Copy(next.Header, headers.Clone())
		response, err := base.RoundTrip(next)
		if err != nil || next.Method != http.MethodPost || response.StatusCode < 400 || r.Context().Err() != nil {
			return response, err
		}
		response.Header.Del("Set-Cookie")
		response.Header.Del("Mcp-Session-Id")
		response.Request = r
		failure.Store(response)
		return nil, errors.New(response.Status)
	}), &mcp.ClientSessionOptions{ProtocolVersion: version})
	if err != nil {
		if response := failure.Load(); response != nil {
			return response, nil
		}
		return nil, err
	}
	defer session.Close()
	initialized := session.InitializeResult()
	var result any
	switch request.Method {
	case "server/discover":
		capabilities := &mcp.ServerCapabilities{}
		if initialized.Capabilities != nil && initialized.Capabilities.Tools != nil {
			capabilities.Tools = &mcp.ToolCapabilities{}
		}
		result = &mcp.DiscoverResult{
			SupportedVersions: []string{CurrentVersion},
			Capabilities:      capabilities,
			Instructions:      initialized.Instructions,
			Meta:              mcp.Meta{mcp.MetaKeyServerInfo: initialized.ServerInfo},
		}
	case "tools/list":
		var p mcp.ListToolsParams
		if err = json.Unmarshal(raw, &p); err == nil {
			result, err = session.ListTools(r.Context(), &p)
		}
	default:
		var p mcp.CallToolParams
		if err = json.Unmarshal(raw, &p); err == nil {
			p.Arguments = params["arguments"]
			result, err = session.CallTool(r.Context(), &p)
		}
	}
	reply := &jsonrpc.Response{ID: request.ID}
	if err != nil {
		if response := failure.Load(); response != nil {
			return response, nil
		}
		var wire *jsonrpc.Error
		if !errors.As(err, &wire) {
			return nil, err
		}
		reply.Error = wire
	} else {
		reply.Result, _ = json.Marshal(result)
		if current := translate.Result(reply.Result); current != nil {
			reply.Result = current
		}
	}
	body, _ := jsonrpc.EncodeMessage(reply)
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}},
		Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Request: r,
	}, nil
}
