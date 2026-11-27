package proxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"maps"
	"math"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// The [standard request headers] of the Streamable HTTP transport.
//
// [standard request headers]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#standard-request-headers
const (
	HeaderProtocolVersion = "Mcp-Protocol-Version"
	HeaderMethod          = "Mcp-Method"
	HeaderName            = "Mcp-Name"
)

// paramPrefix begins the headers that carry [x-mcp-header] tool arguments.
//
// [x-mcp-header]: https://modelcontextprotocol.io/specification/2026-07-28/server/tools#x-mcp-header
const paramPrefix = "Mcp-Param-"

type headerProperty struct {
	Header     string                    `json:"x-mcp-header"`
	Properties map[string]headerProperty `json:"properties"`
}

// headerArgument is a request argument and the MCP HTTP header that carries it.
type headerArgument struct {
	name  string
	value any
}

// toolArguments returns the header arguments of a tools/call, as declared by
// the called tool's input schema. request sends the tools/list requests that
// find the tool. It returns nil if the tool is not listed.
func toolArguments(ctx context.Context, call Request, request func(context.Context, string, json.RawMessage) (json.RawMessage, error)) ([]headerArgument, error) {
	meta := maps.Clone(call.Meta)
	delete(meta, "progressToken")
	raw, _ := json.Marshal(map[string]any{"_meta": meta})
	pageCount := 0
	for tools, err := range Pages[map[string]json.RawMessage](ctx, MethodListTools, "tools", raw, func(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
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
				return schemaArguments(tool["inputSchema"], call.Params["arguments"])
			}
		}
	}
	return nil, nil
}

func schemaArguments(inputSchema, rawArguments json.RawMessage) ([]headerArgument, error) {
	var schema headerProperty
	if err := json.Unmarshal(inputSchema, &schema); err != nil {
		return nil, err
	}
	var arguments map[string]any
	_ = json.Unmarshal(rawArguments, &arguments)
	var headers []headerArgument
	var collect func(map[string]headerProperty, map[string]any)
	collect = func(properties map[string]headerProperty, arguments map[string]any) {
		for name, property := range properties {
			value := arguments[name]
			if property.Header != "" {
				headers = append(headers, headerArgument{paramPrefix + property.Header, value})
			}
			nested, _ := value.(map[string]any)
			collect(property.Properties, nested)
		}
	}
	collect(schema.Properties, arguments)
	return headers, nil
}

// Pages lists all pages of an MCP list method, such as tools/list or the skills
// extension's skills/list, preserving the fields of each item that T retains.
//
// The params argument contains the method's parameters; use {} for no options.
// The request callback sends each request and returns its JSON result, whose
// field names the array of items on that page.
//
// Iteration ends after the last page, cancellation, or an error.
func Pages[T any](ctx context.Context, method, field string, params json.RawMessage, request func(context.Context, string, json.RawMessage) (json.RawMessage, error)) iter.Seq2[[]T, error] {
	return func(yield func([]T, error) bool) {
		var fields map[string]json.RawMessage
		if _ = json.Unmarshal(params, &fields); fields == nil {
			yield(nil, errors.New(method+" parameters must be an object"))
			return
		}
		seen := map[string]bool{}
		for {
			if err := ctx.Err(); err != nil {
				yield(nil, err)
				return
			}
			raw, _ := json.Marshal(fields)
			result, err := request(ctx, method, raw)
			if err != nil {
				yield(nil, err)
				return
			}
			var page map[string]json.RawMessage
			var items []T
			var next struct {
				Cursor string `json:"nextCursor"`
			}
			if json.Unmarshal(result, &page) != nil || json.Unmarshal(page[field], &items) != nil || items == nil || json.Unmarshal(result, &next) != nil {
				yield(nil, errors.New("invalid "+method+" result"))
				return
			}
			if !yield(items, nil) || next.Cursor == "" {
				return
			}
			if seen[next.Cursor] {
				yield(nil, errors.New("repeated "+method+" cursor"))
				return
			}
			seen[next.Cursor] = true
			fields["cursor"], _ = json.Marshal(next.Cursor)
		}
	}
}

// EncodeHeader formats text for use in an MCP HTTP header, preserving Unicode
// characters and whitespace.
func EncodeHeader(text string) string {
	if strings.HasPrefix(text, "=?base64?") || strings.TrimSpace(text) != text || strings.IndexFunc(text, func(c rune) bool { return c < 0x20 || c > 0x7e }) >= 0 {
		return "=?base64?" + base64.StdEncoding.EncodeToString([]byte(text)) + "?="
	}
	return text
}

// ValidateArgumentHeaders checks the [x-mcp-header] argument headers of call,
// a tools/call checked with [ParseRequest], against the input schema of the
// tool it names; other methods have none. request sends the tools/list requests
// that find the tool, and a tool that is not listed has none.
//
// Missing or null arguments require no header. It returns an MCP
// header-mismatch error for a missing, duplicate or invalid argument header,
// and ignores unrelated headers.
//
// [x-mcp-header]: https://modelcontextprotocol.io/specification/2026-07-28/server/tools#x-mcp-header
func ValidateArgumentHeaders(ctx context.Context, headers http.Header, call Request, request func(context.Context, string, json.RawMessage) (json.RawMessage, error)) error {
	if call.Message.Method != MethodCallTool {
		return nil
	}
	arguments, err := toolArguments(ctx, call, request)
	if err != nil {
		return err
	}
	for _, argument := range arguments {
		name, value := argument.name, argument.value
		if value == nil {
			if len(headers.Values(name)) != 0 {
				return Mismatch(name)
			}
		} else if header, err := headerValue(headers, name); err != nil || !primitiveMatches(value, header) {
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

// headerValue returns the decoded text of the named MCP header, as
// [EncodeHeader] encodes it. Missing, duplicate, or invalid values return an error.
func headerValue(headers http.Header, name string) (string, error) {
	values := headers.Values(name)
	if len(values) != 1 {
		return "", fmt.Errorf("expected one %s header", name)
	}
	value := values[0]
	if encoded, ok := strings.CutPrefix(value, "=?base64?"); ok {
		encoded, ok = strings.CutSuffix(encoded, "?=")
		if !ok {
			return "", errors.New("invalid encoded header")
		}
		decoded, _ := base64.StdEncoding.DecodeString(encoded)
		if base64.StdEncoding.EncodeToString(decoded) != encoded || !utf8.Valid(decoded) {
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
// require the header use [ParseRequest].
func ValidateProtocolHeader(headers http.Header, expected string) *jsonrpc.Error {
	if len(headers.Values(HeaderProtocolVersion)) > 1 {
		return Mismatch(HeaderProtocolVersion)
	}
	if version := headers.Get(HeaderProtocolVersion); version != "" && version != expected {
		return UnsupportedVersion(version, []string{expected})
	}
	return nil
}

// ParseHeaders checks the [standard request headers] of a request without its
// body, for intermediaries that route on them. It returns the method and the
// decoded name, empty for methods without one, or a protocol error describing
// the invalid header.
//
// [standard request headers]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#standard-request-headers
func ParseHeaders(headers http.Header, expected string) (method, name string, issue *jsonrpc.Error) {
	if issue := ValidateProtocolHeader(headers, expected); issue != nil {
		return "", "", issue
	}
	if headers.Get(HeaderProtocolVersion) == "" {
		return "", "", Mismatch(HeaderProtocolVersion)
	}
	if len(headers.Values(HeaderMethod)) != 1 {
		return "", "", Mismatch(HeaderMethod)
	}
	method = headers.Get(HeaderMethod)
	if _, named := nameFields[method]; !named {
		if len(headers.Values(HeaderName)) != 0 {
			return "", "", Mismatch(HeaderName)
		}
		return method, "", nil
	}
	if name, err := headerValue(headers, HeaderName); err == nil && name != "" {
		return method, name, nil
	}
	return "", "", Mismatch(HeaderName)
}

// Request is an MCP request whose body agrees with its [standard request
// headers]. Name is the value of the Mcp-Name header, and Params and Meta hold
// the request's params and its [_meta].
//
// [standard request headers]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#standard-request-headers
// [_meta]: https://modelcontextprotocol.io/specification/2026-07-28/basic#_meta
type Request struct {
	Message *jsonrpc.Request
	Name    string
	Params  map[string]json.RawMessage
	Meta    map[string]json.RawMessage
}

// ParseRequest parses an MCP request for an intermediary, checking it against
// its headers as [server validation] requires. [x-mcp-header] arguments are
// checked by [ValidateArgumentHeaders].
//
// On failure it returns the protocol error to answer with, and a Request
// holding as much of the message as it could parse, so the answer can carry
// the request's ID.
//
// [server validation]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#server-validation
// [x-mcp-header]: https://modelcontextprotocol.io/specification/2026-07-28/server/tools#x-mcp-header
func ParseRequest(headers http.Header, message jsonrpc.Message, expected string) (Request, *jsonrpc.Error) {
	req, _ := message.(*jsonrpc.Request)
	if req == nil || !req.IsCall() {
		return Request{Message: req}, &jsonrpc.Error{Code: jsonrpc.CodeInvalidRequest, Message: "expected an MCP request"}
	}
	parsed := Request{Message: req, Params: map[string]json.RawMessage{}}
	if req.Params != nil && (json.Unmarshal(req.Params, &parsed.Params) != nil || parsed.Params == nil) {
		return Request{Message: req}, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "params must be an object"}
	}
	_ = json.Unmarshal(parsed.Params["_meta"], &parsed.Meta)
	method, name, issue := ParseHeaders(headers, expected)
	if issue != nil {
		return parsed, issue
	}
	var version string
	_ = json.Unmarshal(parsed.Meta[mcp.MetaKeyProtocolVersion], &version)
	if version != expected {
		return parsed, Mismatch(HeaderProtocolVersion)
	}
	var info mcp.Implementation
	var capabilities map[string]json.RawMessage
	_ = json.Unmarshal(parsed.Meta[mcp.MetaKeyClientCapabilities], &capabilities)
	rawInfo := parsed.Meta[mcp.MetaKeyClientInfo]
	if capabilities == nil || rawInfo != nil && (json.Unmarshal(rawInfo, &info) != nil || info.Name == "" || info.Version == "") {
		return parsed, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "invalid clientInfo or clientCapabilities"}
	}
	if method != req.Method {
		return parsed, Mismatch(HeaderMethod)
	}
	body, issue := requestName(method, parsed.Params)
	if issue != nil {
		return parsed, issue
	}
	if body.value != nil && body.value != name {
		return parsed, Mismatch(HeaderName)
	}
	parsed.Name = name
	return parsed, nil
}

// SetHeaders sets the [request metadata headers] for req, looking up tools/call
// schemas with request as [ValidateArgumentHeaders] does.
//
// [request metadata headers]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#request-metadata
func SetHeaders(ctx context.Context, headers http.Header, req *jsonrpc.Request, request func(context.Context, string, json.RawMessage) (json.RawMessage, error)) error {
	return setHeaders(headers, req, func(call Request) ([]headerArgument, error) {
		if req.Method != MethodCallTool {
			return nil, nil
		}
		_ = json.Unmarshal(call.Params["_meta"], &call.Meta)
		return toolArguments(ctx, call, request)
	})
}

// SetToolHeaders sets the [request metadata headers] for req as [SetHeaders]
// does, with inputSchema as the input schema of the tool a tools/call names.
// A nil inputSchema sets no argument headers.
//
// [request metadata headers]: https://modelcontextprotocol.io/specification/2026-07-28/basic/transports/streamable-http#request-metadata
func SetToolHeaders(headers http.Header, req *jsonrpc.Request, inputSchema json.RawMessage) error {
	return setHeaders(headers, req, func(call Request) ([]headerArgument, error) {
		if inputSchema == nil {
			return nil, nil
		}
		return schemaArguments(inputSchema, call.Params["arguments"])
	})
}

func setHeaders(headers http.Header, req *jsonrpc.Request, tool func(Request) ([]headerArgument, error)) error {
	call := Request{Message: req}
	_ = json.Unmarshal(req.Params, &call.Params)
	name, issue := requestName(req.Method, call.Params)
	if issue != nil {
		return issue
	}
	call.Name, _ = name.value.(string)
	arguments, err := tool(call)
	if err != nil {
		return err
	}
	headers.Set(HeaderMethod, req.Method)
	for _, argument := range append([]headerArgument{name}, arguments...) {
		if argument.value == nil {
			continue
		}
		text := fmt.Sprint(argument.value)
		if !primitiveMatches(argument.value, text) {
			return &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "invalid MCP header argument"}
		}
		headers.Set(argument.name, EncodeHeader(text))
	}
	return nil
}

// nameFields holds, for each method that carries a name in the Mcp-Name
// header, the params field that holds it. The Tasks extension's methods carry
// their task's ID, as its [routing headers] require.
//
// [routing headers]: https://tasks.extensions.modelcontextprotocol.io/specification/2026-07-28/tasks#streamable-http-routing-headers
var nameFields = map[string]string{
	MethodCallTool:     "name",
	MethodGetPrompt:    "name",
	MethodReadResource: "uri",
	"tasks/get":        "taskId",
	"tasks/update":     "taskId",
	"tasks/cancel":     "taskId",
}

func requestName(method string, params map[string]json.RawMessage) (headerArgument, *jsonrpc.Error) {
	field, ok := nameFields[method]
	if !ok {
		return headerArgument{name: HeaderName}, nil
	}
	var name string
	if _ = json.Unmarshal(params[field], &name); name == "" {
		return headerArgument{}, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "missing " + field}
	}
	return headerArgument{HeaderName, name}, nil
}
