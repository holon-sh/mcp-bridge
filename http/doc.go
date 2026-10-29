// Package mcphttp makes stdio MCP servers available over Streamable HTTP and
// legacy SSE. [Start] returns an HTTP handler that accepts supported protocol
// versions automatically or restricts clients to the version selected in [Options].
// Current clients also reach servers that predate MCP 2026-07-28: the bridge
// initializes them with the legacy handshake and answers server/discover itself.
package mcphttp
