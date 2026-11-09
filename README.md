# mcp-bridge

[![CI](https://github.com/holon-sh/mcp-bridge/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/holon-sh/mcp-bridge/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/holon-sh/mcp-bridge.svg)](https://pkg.go.dev/github.com/holon-sh/mcp-bridge)
[![MIT License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

A Go library and CLI that forwards MCP between stdio and HTTP using the
[official Go MCP SDK](https://github.com/modelcontextprotocol/go-sdk).

## Install

```sh
curl -LsSf https://raw.githubusercontent.com/holon-sh/mcp-bridge/main/install.sh | sh
```

## CLI

### stdio to HTTP

Launch the MCP server as a subprocess and serve streamable HTTP at `/mcp` and
legacy SSE at `/sse`.

```mermaid
flowchart LR
    client["MCP client"]
    subgraph host["Server machine"]
        bridge["mcp-bridge"] <-->|stdio| server["MCP server"]
    end
    client <-->|"streamable HTTP or SSE"| bridge
```

```sh
mcp-bridge serve --listen 127.0.0.1:8080 -- /path/to/mcp-server --stdio
```

### HTTP to stdio

Launch the bridge as a subprocess and expose a remote streamable HTTP or legacy
SSE MCP server over stdio.

```mermaid
flowchart LR
    subgraph host["Client machine"]
        client["MCP client"] <-->|stdio| bridge["mcp-bridge"]
    end
    bridge <-->|"streamable HTTP or SSE"| server["MCP server"]
```

```sh
mcp-bridge connect https://example.com/mcp
mcp-bridge connect --transport sse https://example.com/sse
mcp-bridge connect --header 'Authorization: Bearer example' https://example.com/mcp
```

## MCP client configuration

For clients using an `mcpServers` configuration:

```json
{
  "mcpServers": {
    "example-mcp": {
      "command": "mcp-bridge",
      "args": ["connect", "https://example.com/mcp", "--header", "Authorization: Bearer TOKEN"]
    }
  }
}
```

## Go API

```sh
go get github.com/holon-sh/mcp-bridge@latest
```

- [`github.com/holon-sh/mcp-bridge/http`](https://pkg.go.dev/github.com/holon-sh/mcp-bridge/http) serves stdio MCP servers over HTTP.
- [`github.com/holon-sh/mcp-bridge/stdio`](https://pkg.go.dev/github.com/holon-sh/mcp-bridge/stdio) exposes a remote HTTP MCP server through a local transport, usually stdio.
- [`github.com/holon-sh/mcp-bridge/proxy`](https://pkg.go.dev/github.com/holon-sh/mcp-bridge/proxy) provides raw tool pagination, MCP header encoding, and connection relaying for embedding proxies.

### stdio to HTTP

```go
import (
    "context"
    "errors"
    "net/http"
    "os/exec"

    mcpbridge "github.com/holon-sh/mcp-bridge/http"
)

func serve(ctx context.Context) error {
    bridge, err := mcpbridge.Start(ctx, func() *exec.Cmd {
        return exec.Command("/path/to/mcp-server", "--stdio")
    }, mcpbridge.Options{})
    if err != nil {
        return err
    }
    defer bridge.Close()

    // Serve locally, with cross-origin browser protection.
    server := &http.Server{
        Addr:    "127.0.0.1:8080",
        Handler: http.NewCrossOriginProtection().Handler(bridge),
    }
    defer server.Close()

    go func() { bridge.Wait(); server.Close() }()
    if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
        return err
    }
    return bridge.Wait()
}
```

### HTTP to stdio

```go
import (
    "context"

    "github.com/holon-sh/mcp-bridge/proxy"
    mcpbridge "github.com/holon-sh/mcp-bridge/stdio"
    "github.com/modelcontextprotocol/go-sdk/mcp"
    "golang.org/x/oauth2"
)

func connect(ctx context.Context, token string) error {
    client := oauth2.NewClient(ctx, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token}))
    client.Transport = proxy.NewRemoteTransport(client.Transport)
    return mcpbridge.Run(ctx,
        &mcp.StreamableClientTransport{Endpoint: "https://example.com/mcp", HTTPClient: client},
        &mcp.StdioTransport{},
    )
}
```

For a legacy SSE server, use `mcp.SSEClientTransport` instead.

### Custom proxies

Use [`proxy`](https://pkg.go.dev/github.com/holon-sh/mcp-bridge/proxy) to relay
between existing MCP connections, page through tools, or encode MCP HTTP headers:

```go
err := proxy.Relay(ctx, upstream, downstream)
```

Both connections close when forwarding ends.
