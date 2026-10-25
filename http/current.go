package mcphttp

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"sync"
	"time"

	"github.com/holon-sh/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ProtocolVersion is the current MCP revision available through [Options].
const ProtocolVersion = proxy.CurrentVersion

const (
	maxQueuedBytes = 16 << 20
	shutdownGrace  = 5 * time.Second
)

type currentBridge struct {
	conn     mcp.Connection
	version  string
	pinned   bool
	log      io.Writer
	mu       sync.Mutex
	pending  map[string]*call
	queued   int
	err      error
	stopOnce sync.Once
	stopping chan struct{}
	done     chan struct{}
}

type call struct {
	id        string
	original  jsonrpc.ID
	progress  json.RawMessage
	messages  chan delivery
	completed bool
}

type delivery struct {
	message jsonrpc.Message
	size    int
}

type lockedWriter struct {
	sync.Mutex
	io.Writer
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.Lock()
	defer w.Unlock()
	return w.Writer.Write(p)
}

func startCurrent(ctx context.Context, cmd *exec.Cmd, version string) (*currentBridge, error) {
	conn, err := startProcess(ctx, cmd)
	if err != nil {
		return nil, err
	}
	pinned := version != ""
	version = cmp.Or(version, ProtocolVersion)
	b := &currentBridge{conn: conn, version: version, pinned: pinned, log: conn.log, pending: map[string]*call{}, stopping: make(chan struct{}), done: make(chan struct{})}
	go b.read()
	probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	params, _ := json.Marshal(&mcp.DiscoverParams{Meta: clientMeta(version)})
	result, err := b.request(probeCtx, "server/discover", params)
	if err == nil {
		var discovered mcp.DiscoverResult
		err = json.Unmarshal(result, &discovered)
		if err == nil && !slices.Contains(discovered.SupportedVersions, version) {
			err = proxy.UnsupportedVersion(version, discovered.SupportedVersions)
		}
	}
	if err != nil {
		b.stop(err)
		<-b.done
		return nil, fmt.Errorf("discover MCP server: %w", err)
	}
	return b, nil
}

func clientMeta(version string) mcp.Meta {
	return mcp.Meta{
		mcp.MetaKeyProtocolVersion:    version,
		mcp.MetaKeyClientInfo:         &mcp.Implementation{Name: "mcp-bridge", Version: "0.0.2"},
		mcp.MetaKeyClientCapabilities: map[string]any{},
	}
}

func (b *currentBridge) stop(err error) {
	b.stopOnce.Do(func() {
		b.mu.Lock()
		b.err = err
		b.mu.Unlock()
		close(b.stopping)
		go func() {
			_ = b.conn.Close()
			close(b.done)
		}()
	})
}

func (b *currentBridge) write(ctx context.Context, msg jsonrpc.Message) error {
	select {
	case <-b.stopping:
		return errors.New("MCP process stopped")
	default:
	}
	err := b.conn.Write(ctx, msg)
	if err != nil && ctx.Err() == nil {
		b.stop(fmt.Errorf("write MCP message: %w", err))
	}
	return err
}

func (b *currentBridge) begin(ctx context.Context, req *jsonrpc.Request) (*call, error) {
	var params, meta map[string]json.RawMessage
	_ = json.Unmarshal(req.Params, &params)
	_ = json.Unmarshal(params["_meta"], &meta)
	c := &call{id: rand.Text(), original: req.ID, progress: meta["progressToken"], messages: make(chan delivery, 8)}
	if c.progress != nil {
		meta["progressToken"], _ = json.Marshal(c.id)
	}
	params["_meta"], _ = json.Marshal(meta)
	raw, _ := json.Marshal(params)
	id, _ := jsonrpc.MakeID(c.id)
	b.mu.Lock()
	if len(b.pending) >= proxy.MaxRequests {
		b.mu.Unlock()
		return nil, errors.New("too many concurrent MCP requests")
	}
	b.pending[c.id] = c
	b.mu.Unlock()
	if err := b.write(ctx, &jsonrpc.Request{ID: id, Method: req.Method, Params: raw}); err != nil {
		b.finish(c, false)
		return nil, err
	}
	return c, nil
}

func (b *currentBridge) finish(c *call, cancel bool) {
	b.mu.Lock()
	cancel = cancel && !c.completed
	delete(b.pending, c.id)
	for {
		select {
		case message := <-c.messages:
			b.queued -= message.size
		default:
			b.mu.Unlock()
			if cancel {
				raw, _ := json.Marshal(&mcp.CancelledParams{RequestID: c.id, Reason: "HTTP request closed"})
				_ = b.write(context.Background(), &jsonrpc.Request{Method: "notifications/cancelled", Params: raw})
			}
			return
		}
	}
}

func (b *currentBridge) receive(ctx context.Context, c *call) (jsonrpc.Message, error) {
	select {
	case d := <-c.messages:
		b.mu.Lock()
		b.queued -= d.size
		b.mu.Unlock()
		return d.message, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.stopping:
		return nil, errors.New("MCP process stopped")
	}
}

func (b *currentBridge) request(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
	id, _ := jsonrpc.MakeID("internal")
	c, err := b.begin(ctx, &jsonrpc.Request{ID: id, Method: method, Params: params})
	if err != nil {
		return nil, err
	}
	defer b.finish(c, true)
	for {
		message, err := b.receive(ctx, c)
		if err != nil {
			return nil, err
		}
		if response, ok := message.(*jsonrpc.Response); ok {
			return response.Result, response.Error
		}
	}
}

func (b *currentBridge) read() {
	for {
		msg, err := b.conn.Read(context.Background())
		if err != nil {
			b.stop(fmt.Errorf("read MCP stdout: %w", err))
			return
		}
		if req, ok := msg.(*jsonrpc.Request); ok && req.IsCall() {
			b.stop(errors.New("upstream sent a server request unsupported by MCP 2026-07-28"))
			return
		}
		b.route(msg)
	}
}

func (b *currentBridge) route(msg jsonrpc.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var c *call
	switch message := msg.(type) {
	case *jsonrpc.Response:
		id, _ := message.ID.Raw().(string)
		c = b.pending[id]
		if c != nil {
			message.ID = c.original
			c.completed = true
		}
	case *jsonrpc.Request:
		var params, meta map[string]json.RawMessage
		_ = json.Unmarshal(message.Params, &params)
		_ = json.Unmarshal(params["_meta"], &meta)
		var id string
		switch message.Method {
		case "notifications/progress":
			_ = json.Unmarshal(params["progressToken"], &id)
			c = b.pending[id]
			if c != nil && c.progress != nil {
				params["progressToken"] = c.progress
			} else {
				c = nil
			}
		case "notifications/cancelled":
			_ = json.Unmarshal(params["requestId"], &id)
			c = b.pending[id]
			if c != nil {
				params["requestId"], _ = json.Marshal(c.original.Raw())
				c.completed = true
			}
		default:
			_ = json.Unmarshal(meta[mcp.MetaKeySubscriptionID], &id)
			c = b.pending[id]
			if c != nil {
				meta[mcp.MetaKeySubscriptionID], _ = json.Marshal(c.original.Raw())
				params["_meta"], _ = json.Marshal(meta)
			}
		}
		message.Params, _ = json.Marshal(params)
	}
	if c == nil {
		_, _ = fmt.Fprintln(b.log, "mcp-bridge: discarded uncorrelated upstream message")
		return
	}
	data, _ := jsonrpc.EncodeMessage(msg)
	if b.queued+len(data) > maxQueuedBytes {
		go b.stop(errors.New("MCP output buffer limit exceeded"))
		return
	}
	select {
	case c.messages <- delivery{message: msg, size: len(data)}:
		b.queued += len(data)
	default:
		go b.stop(errors.New("MCP consumer too slow"))
	}
}
