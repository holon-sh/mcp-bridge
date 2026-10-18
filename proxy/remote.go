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

	"github.com/hughhan1/mcp-bridge/internal/remote"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// NewRemoteTransport wraps base so current tools/list and tools/call requests
// can use legacy Streamable HTTP servers after an explicit protocol-version
// rejection. It also translates server/discover after a protocol-version or
// method-not-found rejection.
func NewRemoteTransport(base http.RoundTripper) http.RoundTripper {
	return legacyFallback{base: cmp.Or(base, http.DefaultTransport)}
}

type legacyFallback struct{ base http.RoundTripper }

func (t legacyFallback) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Body == nil || !slices.Contains([]string{"server/discover", "tools/list", "tools/call"}, r.Header.Get("Mcp-Method")) {
		return t.base.RoundTrip(r)
	}
	sent, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, mcp.DefaultMaxLineLength))
	_ = r.Body.Close()
	if err != nil {
		return nil, err
	}
	r = r.Clone(r.Context())
	r.Body = io.NopCloser(bytes.NewReader(sent))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(sent)), nil }
	response, err := t.base.RoundTrip(r)
	if err != nil {
		return response, err
	}
	discovery := r.Header.Get("Mcp-Method") == "server/discover"
	media, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != http.StatusBadRequest && !(discovery && response.StatusCode == http.StatusOK && media == "application/json") {
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
	rejected, ok := message.(*jsonrpc.Response)
	if !ok || !(errors.Is(rejected.Error, &jsonrpc.Error{Code: mcp.CodeUnsupportedProtocolVersion}) ||
		discovery && errors.Is(rejected.Error, &jsonrpc.Error{Code: jsonrpc.CodeMethodNotFound})) {
		return response, nil
	}
	message, _ = jsonrpc.DecodeMessage(sent)
	request, ok := message.(*jsonrpc.Request)
	if !ok || rejected.ID != request.ID ||
		ValidateHeaders(r.Header, request, CurrentVersion) != nil {
		return response, nil
	}
	_ = response.Body.Close()
	return t.legacy(r, request)
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
		return strings.HasPrefix(lower, "mcp-") || slices.Contains([]string{"last-event-id", "content-length", "accept", "accept-encoding", "content-type"}, lower)
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
	if initialized.ProtocolVersion >= CurrentVersion {
		return nil, errors.New("upstream did not negotiate a legacy MCP protocol")
	}
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
		data, _ := json.Marshal(result)
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(data, &fields)
		fields["resultType"] = json.RawMessage(`"complete"`)
		fields["cacheScope"] = json.RawMessage(`"private"`)
		reply.Result, _ = json.Marshal(fields)
	}
	body, _ := jsonrpc.EncodeMessage(reply)
	return &http.Response{
		StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}, "Cache-Control": {"no-store"}},
		Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body)), Request: r,
	}, nil
}
