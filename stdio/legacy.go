package mcpstdio

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/hughhan1/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func runLegacy(ctx context.Context, remote *mcp.StreamableClientTransport, downstream mcp.Connection, initialize *mcp.InitializeParams) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	b := &legacyBridge{local: downstream, cancel: cancel, subscriptions: make(map[string]*legacySubscription), listens: make(map[jsonrpc.ID]*legacySubscription)}
	if err := b.connect(ctx, remote, initialize); err != nil {
		return err
	}
	defer b.session.Close()
	upstream := b.session.InitializeResult()
	info := cmp.Or(upstream.ServerInfo, &mcp.Implementation{Name: "mcp-bridge", Version: "preview"})
	caps := &mcp.ServerCapabilities{Completions: upstream.Capabilities.Completions, Extensions: appExtension(upstream.Capabilities.Extensions)}
	if upstream.Capabilities.Tools != nil {
		caps.Tools = &mcp.ToolCapabilities{}
	}
	if upstream.Capabilities.Prompts != nil {
		caps.Prompts = &mcp.PromptCapabilities{}
	}
	if upstream.Capabilities.Resources != nil {
		caps.Resources = &mcp.ResourceCapabilities{Subscribe: upstream.Capabilities.Resources.Subscribe}
	}
	server := mcp.NewServer(info, &mcp.ServerOptions{
		Instructions: upstream.Instructions,
		Capabilities: caps,
		SubscribeHandler: func(ctx context.Context, req *mcp.SubscribeRequest) error {
			return b.subscribe(ctx, req.Params)
		},
		UnsubscribeHandler: func(ctx context.Context, req *mcp.UnsubscribeRequest) error {
			b.subscriptionOps.Lock()
			defer b.subscriptionOps.Unlock()
			return b.unsubscribe(ctx, req.Params.URI)
		},
		SupportedProtocolVersions: slices.DeleteFunc(mcp.SupportedProtocolVersions(), func(v string) bool { return !proxy.IsLegacyVersion(v) }),
	})
	server.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			switch method {
			case "initialize", "notifications/initialized", "notifications/cancelled", "ping", "resources/subscribe", "resources/unsubscribe":
				return next(ctx, method, req)
			}
			result, err := b.forward(ctx, method, req.GetParams())
			if err == nil {
				if r, ok := result.(interface{ NeedsInput() bool }); ok && r.NeedsInput() {
					return nil, errors.New("MCP translation does not support input-required results")
				}
			}
			return result, err
		}
	})
	err := server.Run(ctx, connectionTransport{downstream})
	return cmp.Or(context.Cause(ctx), err)
}

type connectionTransport struct{ mcp.Connection }

func (t connectionTransport) Connect(context.Context) (mcp.Connection, error) {
	return t.Connection, nil
}

type legacyBridge struct {
	local           mcp.Connection
	cancel          context.CancelCauseFunc
	session         *mcp.ClientSession
	subMu           sync.Mutex
	subscriptions   map[string]*legacySubscription
	subscriptionOps sync.Mutex
	listens         map[jsonrpc.ID]*legacySubscription
}

func appExtension(extensions map[string]any) map[string]any {
	if settings, ok := extensions["io.modelcontextprotocol/ui"]; ok {
		return map[string]any{"io.modelcontextprotocol/ui": settings}
	}
	return nil
}

func (b *legacyBridge) connect(ctx context.Context, remote *mcp.StreamableClientTransport, params *mcp.InitializeParams) error {
	upstream, err := connectRemote(ctx, remote)
	if err != nil {
		return err
	}
	observer := &legacyUpstream{Connection: upstream, bridge: b}
	client := mcp.NewClient(params.ClientInfo, &mcp.ClientOptions{
		Capabilities:   &mcp.ClientCapabilities{Extensions: appExtension(params.Capabilities.Extensions)},
		MultiRoundTrip: &mcp.MultiRoundTripOptions{Disabled: true},
	})
	client.AddReceivingMiddleware(func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(ctx context.Context, method string, req mcp.Request) (mcp.Result, error) {
			result, err := next(ctx, method, req)
			if err != nil {
				return nil, err
			}
			switch method {
			case "notifications/resources/updated", "notifications/progress":
				if method == "notifications/resources/updated" {
					id, _ := jsonrpc.MakeID(req.GetParams().GetMeta()[mcp.MetaKeySubscriptionID])
					b.subMu.Lock()
					sub := b.listens[id]
					active := sub != nil && sub.acknowledged && sub == b.subscriptions[req.GetParams().(*mcp.ResourceUpdatedNotificationParams).URI]
					b.subMu.Unlock()
					if !active {
						return result, nil
					}
				}
				delete(req.GetParams().GetMeta(), mcp.MetaKeySubscriptionID)
				raw, err := json.Marshal(req.GetParams())
				if err == nil {
					err = b.local.Write(ctx, &jsonrpc.Request{Method: method, Params: raw})
				}
				if err != nil {
					b.cancel(err)
				}
			}
			return result, nil
		}
	})
	b.session, err = client.Connect(ctx, connectionTransport{observer}, &mcp.ClientSessionOptions{ProtocolVersion: proxy.CurrentVersion})
	if err != nil {
		return err
	}
	go func() {
		b.cancel(cmp.Or(b.session.Wait(), errors.New("upstream connection closed")))
	}()
	return nil
}

func (b *legacyBridge) forward(ctx context.Context, method string, params mcp.Params) (mcp.Result, error) {
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
		return b.session.ListTools(ctx, params.(*mcp.ListToolsParams))
	case "tools/call":
		call := params.(*mcp.CallToolParamsRaw)
		return b.session.CallTool(ctx, &mcp.CallToolParams{Meta: call.Meta, Name: call.Name, Arguments: call.Arguments})
	case "resources/list":
		return b.session.ListResources(ctx, params.(*mcp.ListResourcesParams))
	case "resources/templates/list":
		return b.session.ListResourceTemplates(ctx, params.(*mcp.ListResourceTemplatesParams))
	case "resources/read":
		return b.session.ReadResource(ctx, params.(*mcp.ReadResourceParams))
	case "prompts/list":
		return b.session.ListPrompts(ctx, params.(*mcp.ListPromptsParams))
	case "prompts/get":
		return b.session.GetPrompt(ctx, params.(*mcp.GetPromptParams))
	case "completion/complete":
		return b.session.Complete(ctx, params.(*mcp.CompleteParams))
	default:
		return nil, &jsonrpc.Error{Code: jsonrpc.CodeMethodNotFound, Message: "unsupported in MCP translation: " + method}
	}
}

type legacySubscription struct {
	uri          string
	ready        chan error
	acknowledged bool
}

func (b *legacyBridge) subscribe(ctx context.Context, params *mcp.SubscribeParams) error {
	b.subscriptionOps.Lock()
	defer b.subscriptionOps.Unlock()
	if caps := b.session.InitializeResult().Capabilities.Resources; caps == nil || !caps.Subscribe {
		return &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "upstream does not support this resource subscription"}
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	b.subMu.Lock()
	if b.subscriptions[params.URI] != nil {
		b.subMu.Unlock()
		return nil
	}
	if len(b.subscriptions) >= proxy.MaxRequests {
		b.subMu.Unlock()
		return errors.New("resource subscription limit exceeded")
	}
	sub := &legacySubscription{uri: params.URI, ready: make(chan error, 1)}
	b.subscriptions[params.URI] = sub
	b.subMu.Unlock()
	err := b.session.Subscribe(ctx, params)
	if err == nil {
		select {
		case err = <-sub.ready:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	if err != nil {
		_ = b.unsubscribe(context.WithoutCancel(ctx), params.URI)
	}
	return err
}

func (b *legacyBridge) unsubscribe(ctx context.Context, uri string) error {
	b.subMu.Lock()
	delete(b.subscriptions, uri)
	for id, sub := range b.listens {
		if sub.uri == uri {
			delete(b.listens, id)
		}
	}
	b.subMu.Unlock()
	return b.session.Unsubscribe(ctx, &mcp.UnsubscribeParams{URI: uri})
}

type legacyUpstream struct {
	mcp.Connection
	bridge *legacyBridge
}

func (u *legacyUpstream) Write(ctx context.Context, msg jsonrpc.Message) error {
	if req, ok := msg.(*jsonrpc.Request); ok {
		if req.Method == "initialize" {
			return errors.New("MCP translation requires a modern upstream; initialize fallback is disabled")
		}
		if req.Method == "subscriptions/listen" {
			var params mcp.SubscriptionsListenParams
			if err := json.Unmarshal(req.Params, &params); err != nil {
				return err
			}
			u.bridge.subMu.Lock()
			// ClientSession.Subscribe opens one stream for the URI registered by
			// subscribe; subscriptionOps holds it in place until acknowledgement.
			uri := params.Notifications.ResourceSubscriptions[0]
			u.bridge.listens[req.ID] = u.bridge.subscriptions[uri]
			u.bridge.subMu.Unlock()
		}
	}
	return u.Connection.Write(ctx, msg)
}

func (u *legacyUpstream) Read(ctx context.Context) (jsonrpc.Message, error) {
	msg, err := u.Connection.Read(ctx)
	if err != nil {
		return msg, err
	}
	u.bridge.subMu.Lock()
	defer u.bridge.subMu.Unlock()
	switch msg := msg.(type) {
	case *jsonrpc.Request:
		if msg.Method != "notifications/subscriptions/acknowledged" {
			break
		}
		var params mcp.SubscriptionsAcknowledgedParams
		if err := json.Unmarshal(msg.Params, &params); err != nil {
			return nil, err
		}
		id, _ := jsonrpc.MakeID(params.Meta[mcp.MetaKeySubscriptionID])
		if sub := u.bridge.listens[id]; sub != nil && !sub.acknowledged {
			if slices.Contains(params.Notifications.ResourceSubscriptions, sub.uri) {
				sub.acknowledged = true
				sub.ready <- nil
			} else {
				delete(u.bridge.listens, id)
				sub.ready <- fmt.Errorf("upstream declined resource subscription %q", sub.uri)
			}
		}
	case *jsonrpc.Response:
		if sub := u.bridge.listens[msg.ID]; sub != nil {
			delete(u.bridge.listens, msg.ID)
			issue := fmt.Errorf("upstream subscription ended for %q; reconnect and resubscribe", sub.uri)
			if sub.acknowledged {
				u.bridge.cancel(issue)
			} else {
				sub.ready <- issue
			}
		}
	}
	return msg, nil
}
