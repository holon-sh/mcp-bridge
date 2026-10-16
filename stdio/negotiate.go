package mcpstdio

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	internalremote "github.com/hughhan1/mcp-bridge/internal/remote"
	"github.com/hughhan1/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var errLegacyUpstream = errors.New("upstream uses legacy MCP")

func requiresTranslation(ctx context.Context, remote *mcp.StreamableClientTransport, initialize *mcp.InitializeParams) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	transport := *remote
	transport.DisableStandaloneSSE = true
	transport.MaxRetries = -1
	transport.MaxEventSize = proxy.MaxMessageBytes
	clientCopy := *cmp.Or(remote.HTTPClient, http.DefaultClient)
	rt := cmp.Or(clientCopy.Transport, http.DefaultTransport)
	var status atomic.Int32
	clientCopy.Transport = internalremote.RoundTripFunc(func(req *http.Request) (*http.Response, error) {
		res, err := rt.RoundTrip(req)
		if res != nil {
			status.Store(int32(res.StatusCode))
			res.Body = http.MaxBytesReader(nil, res.Body, proxy.MaxMessageBytes)
		}
		return res, err
	})
	transport.HTTPClient = &clientCopy
	client := mcp.NewClient(initialize.ClientInfo, &mcp.ClientOptions{Capabilities: initialize.Capabilities, MultiRoundTrip: &mcp.MultiRoundTripOptions{Disabled: true}})
	var versions []string
	var discoveryError error
	client.AddSendingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			if method == "initialize" {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				if slices.ContainsFunc(versions, proxy.IsLegacyVersion) {
					return nil, errLegacyUpstream
				}
				if discoveryError == nil {
					return nil, proxy.UnsupportedVersion(proxy.CurrentVersion, versions)
				}
				var rpcErr *jsonrpc.Error
				hasRPCError := errors.As(discoveryError, &rpcErr)
				modernError := hasRPCError && (rpcErr.Code == mcp.CodeHeaderMismatch || rpcErr.Code == mcp.CodeMissingRequiredClientCapabilities || rpcErr.Code == mcp.CodeUnsupportedProtocolVersion)
				code := status.Load()
				if modernError || code == http.StatusUnauthorized || code == http.StatusForbidden || code == http.StatusTooManyRequests || code < 200 || code >= 500 {
					return nil, discoveryError
				}
				if code >= 400 || (hasRPCError && rpcErr.Code == jsonrpc.CodeMethodNotFound) {
					return nil, errLegacyUpstream
				}
				return nil, discoveryError
			}
			// Connect only sends discovery here: initialize is intercepted above,
			// and this probe has no notification handlers that open subscriptions.
			result, err := next(ctx, method, req)
			discoveryError = err
			var rpcErr *jsonrpc.Error
			if errors.As(err, &rpcErr) && rpcErr.Code == mcp.CodeUnsupportedProtocolVersion {
				var data mcp.UnsupportedProtocolVersionData
				if json.Unmarshal(rpcErr.Data, &data) == nil {
					versions = data.Supported
				}
			}
			if err == nil {
				versions = result.(*mcp.DiscoverResult).SupportedVersions
			}
			return result, err
		}
	})
	session, err := client.Connect(ctx, &transport, &mcp.ClientSessionOptions{ProtocolVersion: proxy.CurrentVersion})
	if errors.Is(err, errLegacyUpstream) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer session.Close()
	return !slices.ContainsFunc(versions, proxy.IsLegacyVersion), nil
}

type replayConnection struct {
	mcp.Connection
	first jsonrpc.Message
}

func (c *replayConnection) Read(ctx context.Context) (jsonrpc.Message, error) {
	if c.first != nil {
		message := c.first
		c.first = nil
		return message, nil
	}
	return c.Connection.Read(ctx)
}
