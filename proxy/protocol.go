package proxy

import (
	"encoding/json"
	"net/http"
	"slices"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	// CurrentVersion is the current MCP protocol revision supported by the bridge.
	CurrentVersion = "2026-07-28"
	// MaxMessageBytes is the maximum accepted JSON-RPC message size in bytes.
	MaxMessageBytes = 4 << 20
	// MaxRequests is the maximum number of concurrent calls or legacy sessions.
	MaxRequests = 64
)

// The [MCP methods] that the bridge sends or that carry a name in the
// [standard request headers].
//
// [MCP methods]: https://modelcontextprotocol.io/specification/2026-07-28/schema
// [standard request headers]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#standard-request-headers
const (
	MethodDiscover     = "server/discover"
	MethodListTools    = "tools/list"
	MethodCallTool     = "tools/call"
	MethodReadResource = "resources/read"
	MethodGetPrompt    = "prompts/get"
)

// The methods of the Tasks extension that carry their task's ID in the
// Mcp-Name header, as its [routing headers] require.
//
// [routing headers]: https://tasks.extensions.modelcontextprotocol.io/specification/2026-07-28/tasks#streamable-http-routing-headers
const (
	MethodGetTask    = "tasks/get"
	MethodUpdateTask = "tasks/update"
	MethodCancelTask = "tasks/cancel"
)

// IsLegacyVersion reports whether version is a supported MCP revision older
// than [CurrentVersion].
func IsLegacyVersion(version string) bool {
	return version < CurrentVersion && slices.Contains(mcp.SupportedProtocolVersions(), version)
}

// ErrorStatus returns the HTTP status a current server answers a request with
// when it refuses it with code: 404 for an [unknown method], and 400 for a
// [HeaderMismatch], an [UnsupportedProtocolVersionError], or a missing client
// capability or invalid params under [request _meta]. It returns 0 for any other
// code.
//
// [unknown method]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#protocol-version-header
// [HeaderMismatch]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#server-validation
// [UnsupportedProtocolVersionError]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#protocol-version-header
// [request _meta]: https://modelcontextprotocol.io/specification/2026-07-28/basic/index#_meta
func ErrorStatus(code int64) int {
	switch code {
	case jsonrpc.CodeMethodNotFound:
		return http.StatusNotFound
	case mcp.CodeHeaderMismatch, mcp.CodeUnsupportedProtocolVersion, mcp.CodeMissingRequiredClientCapabilities, jsonrpc.CodeInvalidParams:
		return http.StatusBadRequest
	}
	return 0
}

// UnsupportedVersion returns an MCP error identifying the rejected version
// and the versions clients may use instead.
func UnsupportedVersion(requested string, supported []string) *jsonrpc.Error {
	data, _ := json.Marshal(&mcp.UnsupportedProtocolVersionData{Requested: requested, Supported: supported})
	return &jsonrpc.Error{Code: mcp.CodeUnsupportedProtocolVersion, Message: "unsupported MCP protocol version: " + requested, Data: data}
}
