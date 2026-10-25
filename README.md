# mcp-bridge

[![CI](https://github.com/holon-sh/mcp-bridge/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/holon-sh/mcp-bridge/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.27%2B-00ADD8?logo=go)](https://go.dev/doc/install)
[![Go Reference](https://pkg.go.dev/badge/github.com/holon-sh/mcp-bridge.svg)](https://pkg.go.dev/github.com/holon-sh/mcp-bridge@v0.0.0-20261025141317-cc9108a764eb)
[![Downloads](https://img.shields.io/badge/releases-download-blue)](https://github.com/holon-sh/mcp-bridge/releases/latest)
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

<details open>
<summary><code>mcp-bridge serve --help</code></summary>

```text
Expose a stdio MCP server over Streamable HTTP and legacy SSE

Usage:
  mcp-bridge serve [flags] -- SERVER [ARG...]

Flags:
      --allow-origin origin       Trusted browser origin (repeatable)
      --cwd string                Server working directory
      --env KEY=VALUE             Server environment override KEY=VALUE (repeatable)
  -h, --help                      help for serve
      --listen string             HTTP listening address (default "127.0.0.1:8080")
      --protocol-version string   Require one MCP protocol version (default: automatic negotiation)
```

</details>

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

<details open>
<summary><code>mcp-bridge connect --help</code></summary>

```text
Expose a remote Streamable HTTP or legacy SSE MCP server over stdio

Usage:
  mcp-bridge connect [flags] URL

Flags:
      --header header      Remote header as Name: value (repeatable)
  -h, --help               help for connect
      --transport string   Remote transport: streamable-http or sse (default "streamable-http")
```

</details>

## MCP client configuration

For clients using an `mcpServers` configuration:

<details open>
<summary><h4>Connect a stdio client to a streamable HTTP server</h4></summary>

```json
{
  "mcpServers": {
    "example-mcp": {
      "command": "mcp-bridge",
      "args": ["connect", "https://example.com/mcp"]
    }
  }
}
```

</details>

<details open>
<summary><h4>Connect to a legacy SSE server</h4></summary>

```json
{
  "mcpServers": {
    "example-mcp": {
      "command": "mcp-bridge",
      "args": ["connect", "--transport", "sse", "https://example.com/sse"]
    }
  }
}
```

</details>

<details open>
<summary><h4>Include authentication and an additional HTTP header</h4></summary>

```json
{
  "mcpServers": {
    "example-mcp": {
      "command": "mcp-bridge",
      "args": [
        "connect", "https://example.com/mcp",
        "--header", "Authorization: Bearer TOKEN",
        "--header", "X-Tenant: example"
      ]
    }
  }
}
```

</details>

## Go API

Use the exact source commit of GitHub release `v0.0.1`:

```sh
go get github.com/holon-sh/mcp-bridge@v0.0.0-20261025141317-cc9108a764eb
```

The Go module proxy cached the previous module declaration for `v0.0.1` during
this repository's transfer. Go module versions are immutable, so Go consumers
must use this commit pin. The CLI release remains `0.0.1`.

This repository provides these Go packages:

- [`github.com/holon-sh/mcp-bridge/http`](https://pkg.go.dev/github.com/holon-sh/mcp-bridge@v0.0.0-20261025141317-cc9108a764eb/http) serves stdio MCP servers over HTTP.
- [`github.com/holon-sh/mcp-bridge/stdio`](https://pkg.go.dev/github.com/holon-sh/mcp-bridge@v0.0.0-20261025141317-cc9108a764eb/stdio) exposes a remote HTTP MCP server through a local transport, usually stdio.
- [`github.com/holon-sh/mcp-bridge/proxy`](https://pkg.go.dev/github.com/holon-sh/mcp-bridge@v0.0.0-20261025141317-cc9108a764eb/proxy) provides raw tool pagination, MCP header encoding, and connection relaying for embedding proxies.

They run inside your Go program without invoking the CLI.

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

<details open>
<summary><h4>Streamable HTTP</h4></summary>

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

</details>

<details open>
<summary><h4>Legacy SSE</h4></summary>

```go
import (
    "context"

    mcpbridge "github.com/holon-sh/mcp-bridge/stdio"
    "github.com/modelcontextprotocol/go-sdk/mcp"
    "golang.org/x/oauth2"
)

func connectSSE(ctx context.Context, token string) error {
    client := oauth2.NewClient(ctx, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token}))
    return mcpbridge.Run(ctx,
        &mcp.SSEClientTransport{Endpoint: "https://example.com/sse", HTTPClient: client},
        &mcp.StdioTransport{},
    )
}
```

</details>

### Custom proxies

Use [`proxy`](https://pkg.go.dev/github.com/holon-sh/mcp-bridge@v0.0.0-20261025141317-cc9108a764eb/proxy) to forward
messages between existing MCP connections, list tools across pages, or handle MCP
HTTP headers in your own bridge. For example, relay an upstream server connection
to a downstream client connection:

```go
err := proxy.Relay(ctx, upstream, downstream)
```

Both connections close when forwarding ends. See the
[API reference](https://pkg.go.dev/github.com/holon-sh/mcp-bridge@v0.0.0-20261025141317-cc9108a764eb/proxy) for details.
