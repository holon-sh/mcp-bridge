package mcphttp

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/holon-sh/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Options controls which MCP protocol versions clients may use.
type Options struct {
	// ProtocolVersion restricts clients to one supported version. Empty allows negotiation.
	ProtocolVersion string
}

// Server makes a stdio MCP server available over HTTP.
type Server struct {
	ctx          context.Context
	cancel       context.CancelFunc
	command      func() *exec.Cmd
	version      string
	mu           sync.Mutex
	current      *currentBridge
	currentErr   error
	currentOnce  sync.Once
	sessions     map[string]*legacySession
	closed       bool
	err          error
	once         sync.Once
	done         chan struct{}
	work         sync.WaitGroup
	legacyEvents *mcp.MemoryEventStore
	afterFunc    func(time.Duration, func()) idleTimer
}

// idleTimer is the resettable session clock; production uses time.AfterFunc.
type idleTimer interface {
	Reset(time.Duration) bool
	Stop() bool
}

type legacySession struct {
	transport http.Handler
	version   string
	cancel    context.CancelFunc
	done      chan struct{}
	timer     idleTimer
	timeout   time.Duration
	mu        sync.Mutex
	closing   bool
}

// Start returns a handler that serves the MCP command over Streamable HTTP and
// legacy SSE. Empty options allow protocol negotiation; ProtocolVersion restricts
// clients to one supported revision. Cancelling ctx stops the server.
//
// command must return a fresh, unstarted command with stdin and stdout unset.
// It must be safe to call concurrently, and any shared stderr writer must also
// support concurrent use.
func Start(ctx context.Context, command func() *exec.Cmd, opts Options) (*Server, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if command == nil {
		return nil, errors.New("command factory is required")
	}
	version := opts.ProtocolVersion
	if version != "" && !slices.Contains(mcp.SupportedProtocolVersions(), version) {
		return nil, proxy.UnsupportedVersion(version, mcp.SupportedProtocolVersions())
	}
	ctx, cancel := context.WithCancel(ctx)
	b := &Server{ctx: ctx, cancel: cancel, command: command, version: version, sessions: make(map[string]*legacySession), done: make(chan struct{})}
	b.afterFunc = func(d time.Duration, f func()) idleTimer { return time.AfterFunc(d, f) }
	b.legacyEvents = mcp.NewMemoryEventStore(nil)
	b.legacyEvents.SetMaxBytes(maxQueuedBytes)
	if version != "" && !proxy.IsLegacyVersion(version) {
		if _, err := b.getCurrent(); err != nil {
			b.stop(err)
			<-b.done
			return nil, err
		}
	}
	go func() {
		select {
		case <-ctx.Done():
			b.stop(ctx.Err())
		case <-b.done:
		}
	}()
	return b, nil
}

// Wait blocks until the server stops and returns the error that stopped it,
// or nil after a normal shutdown.
func (b *Server) Wait() error {
	<-b.done
	return b.err
}

// Close stops the server and waits for shutdown to complete.
func (b *Server) Close() error { b.stop(nil); return b.Wait() }

func (b *Server) stop(err error) {
	b.once.Do(func() {
		b.mu.Lock()
		b.closed = true
		b.err = err
		b.mu.Unlock()
		b.cancel()
		go func() {
			b.work.Wait()
			b.mu.Lock()
			current := b.current
			sessions := slices.Collect(maps.Values(b.sessions))
			b.mu.Unlock()
			var wg sync.WaitGroup
			if current != nil {
				wg.Go(func() { current.stop(nil); <-current.done })
			}
			for _, s := range sessions {
				wg.Go(func() { s.close(); <-s.done })
			}
			wg.Wait()
			close(b.done)
		}()
	})
}

func (b *Server) getCurrent() (*currentBridge, error) {
	b.currentOnce.Do(func() {
		b.mu.Lock()
		if b.closed {
			b.currentErr = errors.New("MCP bridge stopped")
			b.mu.Unlock()
			return
		}
		b.work.Add(1)
		b.mu.Unlock()
		defer b.work.Done()
		current, err := startCurrent(b.ctx, b.command(), b.version)
		b.mu.Lock()
		b.current, b.currentErr = current, err
		b.mu.Unlock()
		if err != nil {
			return
		}
		go func() { <-current.done; b.stop(current.err) }()
	})
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, errors.New("MCP bridge stopped")
	}
	return b.current, b.currentErr
}

// ServeHTTP serves /mcp, /sse, and /messages. When Options.ProtocolVersion is set
// to [ProtocolVersion], the handler accepts requests at any path.
func (b *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if closed {
		http.Error(w, "MCP bridge stopped", 503)
		return
	}
	if b.version != "" && !proxy.IsLegacyVersion(b.version) {
		current, err := b.getCurrent()
		if err != nil {
			http.Error(w, "MCP process unavailable", 503)
			return
		}
		current.serveHTTP(w, r, nil)
		return
	}
	switch strings.TrimSuffix(r.URL.Path, "/") {
	case "/mcp":
	case "/sse":
		b.serveSSE(w, r)
		return
	case "/messages":
		b.serveLegacy(w, r, r.URL.Query().Get("sessionid"), true)
		return
	default:
		http.NotFound(w, r)
		return
	}
	if id := r.Header.Get("Mcp-Session-Id"); id != "" {
		b.serveLegacy(w, r, id, false)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "session ID required", 405)
		return
	}
	msg := ReadMessage(w, r, proxy.MaxMessageBytes)
	if msg == nil {
		return
	}
	if req, ok := msg.(*jsonrpc.Request); ok && req.Method == "initialize" {
		var params mcp.InitializeParams
		_ = json.Unmarshal(req.Params, &params)
		if header := r.Header.Get("Mcp-Protocol-Version"); header != "" && header != params.ProtocolVersion {
			WriteError(w, 400, req.ID, proxy.Mismatch("Mcp-Protocol-Version"))
			return
		}
		if issue := prepareInitialize(req, b.version); issue != nil {
			WriteError(w, 400, req.ID, issue)
			return
		}
		setBody(r, req)
		r.Header.Set("Mcp-Protocol-Version", b.version)
		id := rand.Text()
		t := &mcp.StreamableServerTransport{SessionID: id, EventStore: b.legacyEvents}
		s, err := b.newSession(id, t)
		if err != nil {
			http.Error(w, "could not start MCP session", 503)
			return
		}
		s.transport.ServeHTTP(w, r)
		return
	}
	if b.version != "" {
		issue := proxy.UnsupportedVersion(r.Header.Get("Mcp-Protocol-Version"), []string{b.version})
		id := jsonrpc.ID{}
		if req, ok := msg.(*jsonrpc.Request); ok {
			id = req.ID
		}
		WriteError(w, 400, id, issue)
		return
	}
	current, err := b.getCurrent()
	if err != nil {
		var rpcErr *jsonrpc.Error
		if req, ok := msg.(*jsonrpc.Request); ok && errors.As(err, &rpcErr) {
			WriteError(w, errorStatus(rpcErr.Code, http.StatusBadRequest), req.ID, rpcErr)
			return
		}
		b.stop(err)
		http.Error(w, "MCP process unavailable", 503)
		return
	}
	current.serveHTTP(w, r, msg)
}

func prepareInitialize(req *jsonrpc.Request, version string) *jsonrpc.Error {
	var params mcp.InitializeParams
	if !req.IsCall() || json.Unmarshal(req.Params, &params) != nil || !proxy.IsLegacyVersion(params.ProtocolVersion) {
		return &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "initialize requires a supported legacy protocol"}
	}
	if version != "" {
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(req.Params, &fields)
		fields["protocolVersion"], _ = json.Marshal(version)
		req.Params, _ = json.Marshal(fields)
	}
	return nil
}

func (b *Server) newSession(id string, transport interface {
	mcp.Transport
	http.Handler
}) (s *legacySession, err error) {
	b.mu.Lock()
	if b.closed || len(b.sessions) >= proxy.MaxRequests {
		b.mu.Unlock()
		return nil, errors.New("MCP session limit exceeded or bridge stopped")
	}
	ctx, cancel := context.WithCancel(b.ctx)
	s = &legacySession{transport: transport, version: b.version, cancel: cancel, done: make(chan struct{}), timeout: 5 * time.Minute}
	b.sessions[id] = s
	b.work.Add(1)
	b.mu.Unlock()
	defer func() {
		if err != nil {
			cancel()
			b.mu.Lock()
			delete(b.sessions, id)
			b.mu.Unlock()
		}
		b.work.Done()
	}()
	process, err := startProcess(ctx, b.command())
	if err != nil {
		return nil, err
	}
	conn, err := transport.Connect(ctx)
	if err != nil {
		process.Close()
		return nil, err
	}
	s.mu.Lock()
	s.timer = b.afterFunc(s.timeout, s.close)
	s.mu.Unlock()
	go func() {
		err := proxy.Relay(ctx, process, &activeConnection{Connection: conn, session: s, pending: make(map[jsonrpc.ID]bool)})
		if err != nil && ctx.Err() == nil {
			_, _ = fmt.Fprintln(process.log, "mcp-bridge:", err)
		}
		s.close()
		b.mu.Lock()
		delete(b.sessions, id)
		b.mu.Unlock()
		close(s.done)
	}()
	return s, nil
}

type activeConnection struct {
	mcp.Connection
	session    *legacySession
	mu         sync.Mutex
	pending    map[jsonrpc.ID]bool
	initialize jsonrpc.ID
}

func (c *activeConnection) Read(ctx context.Context) (jsonrpc.Message, error) {
	for {
		msg, err := c.Connection.Read(ctx)
		if err != nil {
			return msg, err
		}
		c.session.touch()
		if req, ok := msg.(*jsonrpc.Request); ok {
			if req.Method == "initialize" {
				if issue := prepareInitialize(req, c.session.version); issue != nil {
					if err := c.Connection.Write(ctx, &jsonrpc.Response{ID: req.ID, Error: issue}); err != nil {
						return nil, err
					}
					continue
				}
			}
			c.mu.Lock()
			if req.IsCall() {
				if len(c.pending) >= proxy.MaxRequests {
					c.mu.Unlock()
					return nil, errors.New("MCP request limit exceeded")
				}
				c.pending[req.ID] = false
				if req.Method == "initialize" {
					c.initialize = req.ID
				}
			} else if req.Method == "notifications/cancelled" {
				var params mcp.CancelledParams
				_ = json.Unmarshal(req.Params, &params)
				id, _ := jsonrpc.MakeID(params.RequestID)
				if _, exists := c.pending[id]; exists {
					c.pending[id] = true
				}
			}
			c.mu.Unlock()
		}
		return msg, nil
	}
}

func (c *activeConnection) Write(ctx context.Context, msg jsonrpc.Message) error {
	if response, ok := msg.(*jsonrpc.Response); ok {
		c.mu.Lock()
		cancelled := c.pending[response.ID]
		initialize := response.ID == c.initialize
		if initialize {
			c.initialize = jsonrpc.ID{}
		}
		delete(c.pending, response.ID)
		c.mu.Unlock()
		if cancelled {
			return nil
		}
		if initialize && response.Error == nil && c.session.version != "" {
			var result mcp.InitializeResult
			if err := json.Unmarshal(response.Result, &result); err != nil {
				return err
			}
			if result.ProtocolVersion != c.session.version {
				rejected := *response
				rejected.Result = nil
				rejected.Error = proxy.UnsupportedVersion(c.session.version, []string{result.ProtocolVersion})
				msg = &rejected
			}
		}
	}
	c.session.touch()
	return c.Connection.Write(ctx, msg)
}

func (s *legacySession) touch() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closing {
		s.timer.Reset(s.timeout)
	}
}
func (s *legacySession) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closing = true
	s.timer.Stop()
	s.cancel()
}

func (b *Server) serveLegacy(w http.ResponseWriter, r *http.Request, id string, sse bool) {
	if version := r.Header.Get("Mcp-Protocol-Version"); version != "" {
		supported := mcp.SupportedProtocolVersions()
		if b.version != "" {
			supported = []string{b.version}
		}
		if !proxy.IsLegacyVersion(version) || b.version != "" && version != b.version {
			WriteError(w, 400, jsonrpc.ID{}, proxy.UnsupportedVersion(version, supported))
			return
		}
	}
	b.mu.Lock()
	s := b.sessions[id]
	b.mu.Unlock()
	if s == nil {
		http.Error(w, "MCP session not found", 404)
		return
	}
	_, isSSE := s.transport.(*mcp.SSEServerTransport)
	if isSSE != sse {
		http.Error(w, "MCP transport mismatch", 400)
		return
	}
	if !sse && r.Method == http.MethodDelete {
		s.close()
		<-s.done
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if sse && r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "method not allowed", 405)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, proxy.MaxMessageBytes)
	s.transport.ServeHTTP(w, r)
}

func (b *Server) serveSSE(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", 405)
		return
	}
	if !accepts(r.Header, "text/event-stream") {
		http.Error(w, "Accept must include text/event-stream", 406)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	id := rand.Text()
	t := &mcp.SSEServerTransport{Endpoint: "/messages?sessionid=" + id, Response: w, MaxRequestBodyBytes: proxy.MaxMessageBytes}
	s, err := b.newSession(id, t)
	if err != nil {
		http.Error(w, "could not start MCP session", 503)
		return
	}
	select {
	case <-r.Context().Done():
		s.close()
		<-s.done
	case <-s.done:
	}
}
