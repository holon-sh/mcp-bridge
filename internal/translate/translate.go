// Package translate decides which MCP features survive translation between a
// current and a legacy session.
package translate

import (
	"cmp"
	"encoding/json"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Result returns a legacy result with the fields every current result has, or
// nil if result already has them. A legacy result is complete and carries no
// caching hints, so it is private to the caller and immediately stale.
func Result(result json.RawMessage) json.RawMessage {
	var fields map[string]json.RawMessage
	if json.Unmarshal(result, &fields) != nil || fields == nil || fields["resultType"] != nil {
		return nil
	}
	fields["resultType"] = json.RawMessage(`"complete"`)
	fields["cacheScope"] = json.RawMessage(`"private"`)
	fields["ttlMs"] = json.RawMessage(`0`)
	current, _ := json.Marshal(fields)
	return current
}

const appExtension = "io.modelcontextprotocol/ui"

// AppExtension keeps only the MCP Apps extension, which works through plain
// resource reads.
func AppExtension(extensions map[string]any) map[string]any {
	if settings, ok := extensions[appExtension]; ok {
		return map[string]any{appExtension: settings}
	}
	return nil
}

// Capabilities keeps whether upstream serves tools, prompts and resources, its
// completions, and the MCP Apps extension. List-changed and subscribe flags are
// dropped: the two protocols deliver those notifications differently.
func Capabilities(upstream *mcp.ServerCapabilities) *mcp.ServerCapabilities {
	upstream = cmp.Or(upstream, &mcp.ServerCapabilities{})
	caps := &mcp.ServerCapabilities{Completions: upstream.Completions, Extensions: AppExtension(upstream.Extensions)}
	if upstream.Tools != nil {
		caps.Tools = &mcp.ToolCapabilities{}
	}
	if upstream.Prompts != nil {
		caps.Prompts = &mcp.PromptCapabilities{}
	}
	if upstream.Resources != nil {
		caps.Resources = &mcp.ResourceCapabilities{}
	}
	return caps
}
