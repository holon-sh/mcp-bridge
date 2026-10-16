package mcpstdio

import (
	"bufio"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hughhan1/mcp-bridge/internal/remote"
	"github.com/hughhan1/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type remoteDelivery struct {
	message jsonrpc.Message
	err     error
}

type remoteConnection struct {
	mcp.Connection
	ctx        context.Context
	cancel     context.CancelFunc
	closeCtx   context.CancelFunc
	client     *http.Client
	endpoint   string
	incoming   chan remoteDelivery
	mu         sync.Mutex
	version    string
	initialize jsonrpc.ID
	calls      map[jsonrpc.ID]context.CancelFunc
	once       sync.Once
	writes     sync.WaitGroup
	closed     bool
	queries    map[jsonrpc.ID]chan *jsonrpc.Response
}

type remoteHeadersKey struct{}

func connectRemote(ctx context.Context, transport mcp.Transport) (mcp.Connection, error) {
	ctx, cancel := context.WithCancel(ctx)
	r := &remoteConnection{ctx: ctx, cancel: cancel, incoming: make(chan remoteDelivery, 8), calls: make(map[jsonrpc.ID]context.CancelFunc), queries: make(map[jsonrpc.ID]chan *jsonrpc.Response)}
	connCtx, closeCtx := context.WithCancel(context.WithoutCancel(ctx))
	r.closeCtx = closeCtx
	stopConnect := context.AfterFunc(ctx, closeCtx)
	defer stopConnect()
	switch t := transport.(type) {
	case *mcp.StreamableClientTransport:
		r.endpoint = t.Endpoint
		copyTransport := remote.Streamable(*t, proxy.MaxMessageBytes, func(base http.RoundTripper, req *http.Request) (*http.Response, error) {
			r.mu.Lock()
			version := r.version
			r.mu.Unlock()
			if version != "" {
				req.Header.Set("Mcp-Protocol-Version", version)
			}
			headers, _ := req.Context().Value(remoteHeadersKey{}).(http.Header)
			maps.Copy(req.Header, headers)
			return base.RoundTrip(req)
		})
		r.client = copyTransport.HTTPClient
		transport = copyTransport
	case *mcp.SSEClientTransport:
		copyTransport := *t
		copyTransport.MaxEventSize = remote.EventSize(copyTransport.MaxEventSize, proxy.MaxMessageBytes)
		transport = &copyTransport
	}
	conn, err := transport.Connect(connCtx)
	if err != nil {
		cancel()
		closeCtx()
		return nil, err
	}
	r.Connection = conn
	go r.read()
	return r, nil
}

func (r *remoteConnection) deliver(msg jsonrpc.Message, err error) {
	select {
	case r.incoming <- remoteDelivery{msg, err}:
	case <-r.ctx.Done():
	}
}

func (r *remoteConnection) read() {
	for {
		msg, err := r.Connection.Read(r.ctx)
		var cancel context.CancelFunc
		if response, ok := msg.(*jsonrpc.Response); ok {
			r.mu.Lock()
			query := r.queries[response.ID]
			if query != nil {
				r.mu.Unlock()
				query <- response
				continue
			}
			cancel = r.calls[response.ID]
			delete(r.calls, response.ID)
			initialize := response.ID == r.initialize
			if initialize {
				r.initialize = jsonrpc.ID{}
			}
			r.mu.Unlock()
			if initialize && response.Error == nil {
				var result mcp.InitializeResult
				if e := json.Unmarshal(response.Result, &result); e != nil {
					err = e
				} else if !proxy.IsLegacyVersion(result.ProtocolVersion) {
					err = errors.New("upstream initialize did not negotiate a supported legacy protocol")
				} else {
					r.mu.Lock()
					r.version = result.ProtocolVersion
					r.mu.Unlock()
					if r.client != nil {
						go r.listen()
					}
				}
			}
		}
		r.deliver(msg, err)
		if cancel != nil {
			cancel()
		}
		if err != nil {
			return
		}
	}
}

func (r *remoteConnection) listen() {
	req, _ := http.NewRequestWithContext(r.ctx, http.MethodGet, r.endpoint, nil)
	req.Header.Set("Accept", "text/event-stream")
	if id := r.Connection.SessionID(); id != "" {
		req.Header.Set("Mcp-Session-Id", id)
	}
	res, err := r.client.Do(req)
	if err != nil {
		r.deliver(nil, err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode == http.StatusMethodNotAllowed {
		return
	}
	if !strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream") {
		r.deliver(nil, fmt.Errorf("MCP notification stream: HTTP %d", res.StatusCode))
		return
	}
	err = readEvents(res.Body, func(msg jsonrpc.Message) error { r.deliver(msg, nil); return r.ctx.Err() })
	r.deliver(nil, cmp.Or(err, io.EOF))
}

func (r *remoteConnection) Read(ctx context.Context) (jsonrpc.Message, error) {
	select {
	case d := <-r.incoming:
		return d.message, d.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}
}

func (r *remoteConnection) Write(ctx context.Context, msg jsonrpc.Message) error {
	req, ok := msg.(*jsonrpc.Request)
	if !ok {
		return r.Connection.Write(ctx, msg)
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return mcp.ErrConnectionClosed
	}
	if req.Method == "initialize" {
		var params mcp.InitializeParams
		if err := json.Unmarshal(req.Params, &params); err != nil || !proxy.IsLegacyVersion(params.ProtocolVersion) {
			r.mu.Unlock()
			return errors.New("initialize requires a supported legacy protocol version")
		}
		r.version, r.initialize = params.ProtocolVersion, req.ID
	}
	if req.Method == "notifications/cancelled" {
		var params mcp.CancelledParams
		_ = json.Unmarshal(req.Params, &params)
		id, _ := jsonrpc.MakeID(params.RequestID)
		cancel := r.calls[id]
		delete(r.calls, id)
		notify := r.notifiesCancellation()
		r.mu.Unlock()
		var err error
		if notify {
			err = r.Connection.Write(ctx, msg)
		}
		if cancel != nil {
			cancel()
		}
		return err
	}
	if !req.IsCall() {
		r.mu.Unlock()
		return r.Connection.Write(ctx, msg)
	}
	if len(r.calls) >= proxy.MaxRequests {
		r.mu.Unlock()
		return errors.New("MCP request limit exceeded")
	}
	ctx, cancel := context.WithCancel(ctx)
	r.calls[req.ID] = cancel
	r.writes.Add(1)
	r.mu.Unlock()
	go func() {
		defer r.writes.Done()
		var err error
		var params struct {
			Meta mcp.Meta `json:"_meta"`
		}
		_ = json.Unmarshal(req.Params, &params)
		version, _ := params.Meta[mcp.MetaKeyProtocolVersion].(string)
		if r.client != nil && version >= proxy.CurrentVersion {
			headers := make(http.Header)
			err = proxy.SetHeaders(ctx, headers, req, r.request)
			ctx = context.WithValue(ctx, remoteHeadersKey{}, headers)
		}
		if err == nil {
			err = r.Connection.Write(ctx, msg)
		}
		if err != nil && ctx.Err() == nil {
			var rpcErr *jsonrpc.Error
			if errors.As(err, &rpcErr) {
				r.mu.Lock()
				delete(r.calls, req.ID)
				r.mu.Unlock()
				r.deliver(&jsonrpc.Response{ID: req.ID, Error: rpcErr}, nil)
				cancel()
			} else {
				r.deliver(nil, err)
			}
		}
	}()
	return nil
}

func (r *remoteConnection) request(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
	id, _ := jsonrpc.MakeID(rand.Text())
	response := make(chan *jsonrpc.Response, 1)
	r.mu.Lock()
	r.queries[id] = response
	r.mu.Unlock()
	defer func() { r.mu.Lock(); delete(r.queries, id); r.mu.Unlock() }()
	if err := r.Connection.Write(ctx, &jsonrpc.Request{ID: id, Method: method, Params: params}); err != nil {
		return nil, err
	}
	select {
	case result := <-response:
		return result.Result, result.Error
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}
}

func (r *remoteConnection) Close() error {
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		calls := r.calls
		r.calls = nil
		notify := r.notifiesCancellation()
		r.mu.Unlock()
		ctx, stop := context.WithTimeout(context.WithoutCancel(r.ctx), 5*time.Second)
		defer stop()
		for id, cancel := range calls {
			if notify {
				params, _ := json.Marshal(&mcp.CancelledParams{RequestID: id.Raw()})
				_ = r.Connection.Write(ctx, &jsonrpc.Request{Method: "notifications/cancelled", Params: params})
			}
			cancel()
		}
		_ = r.Connection.Close()
		r.closeCtx()
		r.cancel()
		r.writes.Wait()
	})
	return nil
}

func (r *remoteConnection) notifiesCancellation() bool {
	return r.client == nil || proxy.IsLegacyVersion(r.version)
}

func readEvents(reader io.Reader, deliver func(jsonrpc.Message) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), proxy.MaxMessageBytes)
	var data strings.Builder
	event := ""
	dispatch := func() error {
		name := event
		event = ""
		value := strings.TrimSuffix(data.String(), "\n")
		data.Reset()
		if value == "" || name != "" && name != "message" {
			return nil
		}
		msg, err := jsonrpc.DecodeMessage([]byte(value))
		if err != nil {
			return err
		}
		return deliver(msg)
	}
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "event":
			event = value
		case "data":
			if data.Len()+len(value)+1 > proxy.MaxMessageBytes {
				return errors.New("MCP event limit exceeded")
			}
			data.WriteString(value)
			data.WriteByte('\n')
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return dispatch()
}
