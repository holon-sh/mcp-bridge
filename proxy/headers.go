package proxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"math"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type headerProperty struct {
	Header     string                    `json:"x-mcp-header"`
	Properties map[string]headerProperty `json:"properties"`
}

// HeaderArgument associates a tool argument value with its MCP HTTP header.
// Obtain values from [ToolHeaderArguments] and pass them to [SetArgumentHeaders]
// or [ValidateArgumentHeaders].
type HeaderArgument struct {
	name  string
	value any
}

// ToolHeaderArguments identifies the HTTP headers required by a tools/call request.
// Schema properties marked x-mcp-header select the arguments.
//
// The params argument contains the call parameters. The request callback sends
// catalog requests and returns their JSON results.
//
// It returns nil if the tool is not found, or an error if the lookup fails.
func ToolHeaderArguments(ctx context.Context, params json.RawMessage, request func(context.Context, string, json.RawMessage) (json.RawMessage, error)) ([]HeaderArgument, error) {
	var call struct {
		Name      string                     `json:"name"`
		Arguments map[string]any             `json:"arguments"`
		Meta      map[string]json.RawMessage `json:"_meta"`
	}
	_ = json.Unmarshal(params, &call)
	delete(call.Meta, "progressToken")
	raw, _ := json.Marshal(map[string]any{"_meta": call.Meta})
	pageCount := 0
	for tools, err := range ToolPages(ctx, raw, func(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
		if pageCount == 100 {
			return nil, errors.New("upstream tool pagination limit exceeded")
		}
		pageCount++
		return request(ctx, method, params)
	}) {
		if err != nil {
			return nil, err
		}
		for _, tool := range tools {
			var name string
			if _ = json.Unmarshal(tool["name"], &name); name == call.Name {
				var schema headerProperty
				_ = json.Unmarshal(tool["inputSchema"], &schema)
				var headers []HeaderArgument
				var collect func(map[string]headerProperty, map[string]any)
				collect = func(properties map[string]headerProperty, arguments map[string]any) {
					for name, property := range properties {
						value := arguments[name]
						if property.Header != "" {
							headers = append(headers, HeaderArgument{"Mcp-Param-" + property.Header, value})
						}
						nested, _ := value.(map[string]any)
						collect(property.Properties, nested)
					}
				}
				collect(schema.Properties, call.Arguments)
				return headers, nil
			}
		}
	}
	return nil, nil
}

// ToolPages lists all pages of a tool catalog, preserving tool schemas and
// extension fields.
//
// The params argument contains tools/list parameters; use {} for no options.
// The request callback sends each catalog request and returns its JSON result.
//
// Iteration ends after the last page, cancellation, or an error.
func ToolPages(ctx context.Context, params json.RawMessage, request func(context.Context, string, json.RawMessage) (json.RawMessage, error)) iter.Seq2[[]map[string]json.RawMessage, error] {
	return func(yield func([]map[string]json.RawMessage, error) bool) {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(params, &fields); err != nil || fields == nil {
			yield(nil, errors.New("tools/list parameters must be an object"))
			return
		}
		seen := map[string]bool{}
		for {
			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}
			raw, _ := json.Marshal(fields)
			result, err := request(ctx, "tools/list", raw)
			if err != nil {
				yield(nil, err)
				return
			}
			var page struct {
				Tools      []map[string]json.RawMessage `json:"tools"`
				NextCursor string                       `json:"nextCursor"`
			}
			if json.Unmarshal(result, &page) != nil || page.Tools == nil {
				yield(nil, errors.New("invalid tools/list result"))
				return
			}
			if !yield(page.Tools, nil) || page.NextCursor == "" {
				return
			}
			if seen[page.NextCursor] {
				yield(nil, errors.New("repeated catalog cursor"))
				return
			}
			seen[page.NextCursor] = true
			fields["cursor"], _ = json.Marshal(page.NextCursor)
		}
	}
}

// SetArgumentHeaders sets encoded MCP headers for arguments obtained from
// [ToolHeaderArguments]. headers must be non-nil. Missing or null arguments leave
// existing headers unchanged.
//
// Unsupported values return an error. Headers already set before the error remain set.
func SetArgumentHeaders(headers http.Header, arguments []HeaderArgument) error {
	for _, argument := range arguments {
		name, value := argument.name, argument.value
		if value == nil {
			continue
		}
		text := fmt.Sprint(value)
		if !primitiveMatches(value, text) {
			return &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "invalid MCP header argument"}
		}
		headers.Set(name, EncodeHeader(text))
	}
	return nil
}

// EncodeHeader formats text for use in an MCP HTTP header, preserving Unicode
// characters and whitespace. Use [DecodeHeader] to recover the original text.
func EncodeHeader(text string) string {
	if strings.HasPrefix(text, "=?base64?") || strings.TrimSpace(text) != text || strings.IndexFunc(text, func(c rune) bool { return c < 0x20 || c > 0x7e }) >= 0 {
		return "=?base64?" + base64.StdEncoding.EncodeToString([]byte(text)) + "?="
	}
	return text
}

// ValidateArgumentHeaders checks that headers match the tool arguments returned
// by [ToolHeaderArguments]. Missing or null arguments require no header.
// It returns an MCP header-mismatch error for missing, duplicate, or invalid
// argument headers. Unrelated headers are ignored.
func ValidateArgumentHeaders(headers http.Header, arguments []HeaderArgument) error {
	for _, argument := range arguments {
		name, value := argument.name, argument.value
		if value == nil {
			if len(headers.Values(name)) != 0 {
				return Mismatch(name)
			}
		} else if header, err := HeaderValue(headers, name); err != nil || !primitiveMatches(value, header) {
			return Mismatch(name)
		}
	}
	return nil
}

func primitiveMatches(value any, header string) bool {
	switch v := value.(type) {
	case string:
		return header == v
	case bool:
		return header == strconv.FormatBool(v)
	case float64:
		number, err := strconv.ParseFloat(header, 64)
		return err == nil && v == math.Trunc(v) && math.Abs(v) <= 1<<53-1 && v == number
	default:
		return false
	}
}

// HeaderValue returns the decoded text of the named MCP header.
// Missing, duplicate, or invalid values return an error.
func HeaderValue(headers http.Header, name string) (string, error) {
	values := headers.Values(name)
	if len(values) != 1 {
		return "", fmt.Errorf("expected one %s header", name)
	}
	return DecodeHeader(values[0])
}

// DecodeHeader recovers the text of an MCP HTTP header value.
// Invalid header values return an error.
func DecodeHeader(value string) (string, error) {
	if encoded, ok := strings.CutPrefix(value, "=?base64?"); ok {
		encoded, ok = strings.CutSuffix(encoded, "?=")
		if !ok {
			return "", errors.New("invalid encoded header")
		}
		decoded, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil || base64.StdEncoding.EncodeToString(decoded) != encoded || !utf8.Valid(decoded) {
			return "", errors.New("invalid encoded header")
		}
		return string(decoded), nil
	}
	if strings.TrimSpace(value) != value || strings.ContainsFunc(value, func(c rune) bool { return c < 0x20 && c != '\t' || c > 0x7e }) {
		return "", errors.New("invalid unencoded header")
	}
	return value, nil
}

// Mismatch returns an MCP header-mismatch error naming the offending header.
func Mismatch(name string) *jsonrpc.Error {
	return &jsonrpc.Error{Code: mcp.CodeHeaderMismatch, Message: "header mismatch: " + name}
}

// ValidateProtocolHeader checks an optional MCP-Protocol-Version header.
// Duplicate headers return HeaderMismatch; an explicit unsupported version
// returns UnsupportedProtocolVersionError with negotiation data. Absence or an
// empty value is allowed for legacy initialization and notifications; calls that
// require the header use [ValidateHeaders].
func ValidateProtocolHeader(headers http.Header, expected string) *jsonrpc.Error {
	if len(headers.Values("Mcp-Protocol-Version")) > 1 {
		return Mismatch("Mcp-Protocol-Version")
	}
	if version := headers.Get("Mcp-Protocol-Version"); version != "" && version != expected {
		return UnsupportedVersion(version, []string{expected})
	}
	return nil
}

// ValidateHeaders checks that MCP headers match req and the expected protocol
// version. req must be non-nil. It returns nil on success or a protocol error
// describing the mismatch. Tool argument headers require
// [ValidateArgumentHeaders] separately.
func ValidateHeaders(headers http.Header, req *jsonrpc.Request, expected string) *jsonrpc.Error {
	if issue := ValidateProtocolHeader(headers, expected); issue != nil {
		return issue
	}
	if headers.Get("Mcp-Protocol-Version") == "" {
		return Mismatch("Mcp-Protocol-Version")
	}
	var params, meta map[string]json.RawMessage
	_ = json.Unmarshal(req.Params, &params)
	_ = json.Unmarshal(params["_meta"], &meta)
	var version string
	_ = json.Unmarshal(meta[mcp.MetaKeyProtocolVersion], &version)
	if version != expected {
		return Mismatch("Mcp-Protocol-Version")
	}
	var info mcp.Implementation
	var capabilities map[string]json.RawMessage
	_ = json.Unmarshal(meta[mcp.MetaKeyClientInfo], &info)
	_ = json.Unmarshal(meta[mcp.MetaKeyClientCapabilities], &capabilities)
	if info.Name == "" || info.Version == "" || capabilities == nil {
		return &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "clientInfo and clientCapabilities are required"}
	}
	if len(headers.Values("Mcp-Method")) != 1 || headers.Get("Mcp-Method") != req.Method {
		return Mismatch("Mcp-Method")
	}
	name, issue := requestName(req.Method, params)
	if issue != nil {
		return issue
	}
	if value, _ := HeaderValue(headers, "Mcp-Name"); value != name {
		return Mismatch("Mcp-Name")
	}
	return nil
}

// SetHeaders sets the [request metadata headers] for req, looking up tools/call
// schemas with request as [ToolHeaderArguments] does.
//
// [request metadata headers]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#request-metadata
func SetHeaders(ctx context.Context, headers http.Header, req *jsonrpc.Request, request func(context.Context, string, json.RawMessage) (json.RawMessage, error)) error {
	var params map[string]json.RawMessage
	_ = json.Unmarshal(req.Params, &params)
	name, issue := requestName(req.Method, params)
	if issue != nil {
		return issue
	}
	var arguments []HeaderArgument
	if req.Method == "tools/call" {
		var err error
		if arguments, err = ToolHeaderArguments(ctx, req.Params, request); err != nil {
			return err
		}
	}
	headers.Set("Mcp-Method", req.Method)
	if name != "" {
		headers.Set("Mcp-Name", EncodeHeader(name))
	}
	return SetArgumentHeaders(headers, arguments)
}

var nameFields = map[string]string{"tools/call": "name", "prompts/get": "name", "resources/read": "uri"}

func requestName(method string, params map[string]json.RawMessage) (string, *jsonrpc.Error) {
	field, ok := nameFields[method]
	if !ok {
		return "", nil
	}
	var name string
	if _ = json.Unmarshal(params[field], &name); name == "" {
		return "", &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "missing name or uri"}
	}
	return name, nil
}
