package mcpstdio

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"

	"github.com/holon-sh/mcp-bridge/internal/translate"
	"github.com/holon-sh/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// runLegacy serves a 2025 client on downstream from a current upstream session.
// It forwards tools, resources, prompts, completions and progress; resource
// subscriptions, tasks and input-required results do not survive translation.
func runLegacy(ctx context.Context, remote *mcp.StreamableClientTransport, downstream mcp.Connection, initialize *mcp.InitializeParams) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	session, err := connectCurrent(ctx, cancel, remote, downstream, initialize)
	if err != nil {
		return err
	}
	defer session.Close()
	upstream := session.InitializeResult()
	server := mcp.NewServer(cmp.Or(upstream.ServerInfo, translate.Bridge), &mcp.ServerOptions{
		Instructions:              upstream.Instructions,
		Capabilities:              translate.Capabilities(upstream.Capabilities),
		SupportedProtocolVersions: slices.DeleteFunc(mcp.SupportedProtocolVersions(), func(v string) bool { return !proxy.IsLegacyVersion(v) }),
	})
	server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			switch method {
			case "initialize", "notifications/initialized", "notifications/cancelled", "ping":
				return next(ctx, method, req)
			}
			result, err := forward(ctx, session, method, req.GetParams())
			if r, ok := result.(interface{ NeedsInput() bool }); ok && err == nil && r.NeedsInput() {
				return nil, errors.New("MCP translation does not support input-required results")
			}
			return result, err
		}
	})
	err = server.Run(ctx, connectionTransport{legacyClient{downstream}})
	return cmp.Or(context.Cause(ctx), err)
}

type legacyClient struct{ mcp.Connection }

func (c legacyClient) Write(ctx context.Context, msg jsonrpc.Message) error {
	if response, ok := msg.(*jsonrpc.Response); ok {
		if result := translate.StructuredOutput(response.Result); result != nil {
			response.Result = result
		}
	}
	return c.Connection.Write(ctx, msg)
}

type connectionTransport struct{ mcp.Connection }

func (t connectionTransport) Connect(context.Context) (mcp.Connection, error) {
	return t.Connection, nil
}

// connectCurrent opens a current session with remote for the client that sent
// initialize, and relays upstream progress notifications to local.
func connectCurrent(ctx context.Context, cancel context.CancelCauseFunc, remote *mcp.StreamableClientTransport, local mcp.Connection, initialize *mcp.InitializeParams) (*mcp.ClientSession, error) {
	upstream, err := connectRemote(ctx, remote)
	if err != nil {
		return nil, err
	}
	client := mcp.NewClient(initialize.ClientInfo, &mcp.ClientOptions{
		Capabilities:   &mcp.ClientCapabilities{Extensions: translate.AppExtension(initialize.Capabilities.Extensions)},
		MultiRoundTrip: &mcp.MultiRoundTripOptions{Disabled: true},
		ProgressNotificationHandler: func(ctx context.Context, req *mcp.ProgressNotificationClientRequest) {
			raw, _ := json.Marshal(req.Params)
			if err := local.Write(ctx, &jsonrpc.Request{Method: "notifications/progress", Params: raw}); err != nil {
				cancel(err)
			}
		},
	})
	session, err := client.Connect(ctx, connectionTransport{currentOnly{upstream}}, &mcp.ClientSessionOptions{ProtocolVersion: proxy.CurrentVersion})
	if err != nil {
		return nil, err
	}
	go func() { cancel(cmp.Or(session.Wait(), errors.New("upstream connection closed"))) }()
	return session, nil
}

// currentOnly keeps the upstream session from falling back to initialize.
type currentOnly struct{ mcp.Connection }

func (c currentOnly) Write(ctx context.Context, msg jsonrpc.Message) error {
	if req, ok := msg.(*jsonrpc.Request); ok && req.Method == "initialize" {
		return errors.New("MCP translation requires a modern upstream; initialize fallback is disabled")
	}
	return c.Connection.Write(ctx, msg)
}

func forward(ctx context.Context, session *mcp.ClientSession, method string, params mcp.Params) (mcp.Result, error) {
	// Replace client-supplied protocol metadata with the negotiated upstream identity.
	// The SDK supplies typed params, including typed nil for optional params.
	if !reflect.ValueOf(params).IsNil() {
		meta := params.GetMeta()
		delete(meta, mcp.MetaKeyProtocolVersion)
		delete(meta, mcp.MetaKeyClientInfo)
		delete(meta, mcp.MetaKeyClientCapabilities)
	}
	switch method {
	case "tools/list":
		return session.ListTools(ctx, params.(*mcp.ListToolsParams))
	case "tools/call":
		call := params.(*mcp.CallToolParamsRaw)
		return session.CallTool(ctx, &mcp.CallToolParams{Meta: call.Meta, Name: call.Name, Arguments: call.Arguments})
	case "resources/list":
		return session.ListResources(ctx, params.(*mcp.ListResourcesParams))
	case "resources/templates/list":
		return session.ListResourceTemplates(ctx, params.(*mcp.ListResourceTemplatesParams))
	case "resources/read":
		return session.ReadResource(ctx, params.(*mcp.ReadResourceParams))
	case "prompts/list":
		return session.ListPrompts(ctx, params.(*mcp.ListPromptsParams))
	case "prompts/get":
		return session.GetPrompt(ctx, params.(*mcp.GetPromptParams))
	case "completion/complete":
		return session.Complete(ctx, params.(*mcp.CompleteParams))
	default:
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeMethodNotFound, Message: "unsupported in MCP translation: " + method}
	}
}
