package proxy_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/holon-sh/mcp-bridge/proxy"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestParseRequest(t *testing.T) {
	version := `"io.modelcontextprotocol/protocolVersion":"2026-07-28"`
	info := `"io.modelcontextprotocol/clientInfo":{"name":"client","version":"1"}`
	capabilities := `"io.modelcontextprotocol/clientCapabilities":{}`
	meta := func(fields ...string) string { return `"_meta":{` + strings.Join(fields, ",") + `}` }
	named := func(field, value string) string {
		return `{"` + field + `":"` + value + `",` + meta(version, info, capabilities) + `}`
	}
	id, _ := jsonrpc.MakeID("1")
	for _, test := range []struct {
		name, method, params, want string
		headers                    http.Header
		header, notification       bool
		code                       int64
	}{
		{name: "call", method: "tools/call", params: named("name", "echo"), headers: http.Header{"Mcp-Name": {"echo"}}, want: "echo"},
		{name: "resource", method: "resources/read", params: named("uri", "file:///a"), headers: http.Header{"Mcp-Name": {"file:///a"}}, want: "file:///a"},
		{name: "encoded name", method: "tools/call", params: named("name", "发布"), headers: http.Header{"Mcp-Name": {"=?base64?5Y+R5biD?="}}, want: "发布"},
		{name: "list without client info", params: `{` + meta(version, capabilities) + `}`},
		{name: "notification", notification: true, code: jsonrpc.CodeInvalidRequest},
		{name: "array params", params: `[]`, code: jsonrpc.CodeInvalidParams},
		{name: "missing version header", headers: http.Header{"Mcp-Protocol-Version": nil}, header: true, code: mcp.CodeHeaderMismatch},
		{name: "duplicate version header", headers: http.Header{"Mcp-Protocol-Version": {"2026-07-28", "2099-01-01"}}, header: true, code: mcp.CodeHeaderMismatch},
		{name: "unsupported version header", headers: http.Header{"Mcp-Protocol-Version": {"2099-01-01"}}, header: true, code: mcp.CodeUnsupportedProtocolVersion},
		{name: "metadata version", params: `{` + meta(`"io.modelcontextprotocol/protocolVersion":"2025-11-25"`, info, capabilities) + `}`, code: mcp.CodeHeaderMismatch},
		{name: "client name", params: `{` + meta(version, `"io.modelcontextprotocol/clientInfo":{"version":"1"}`, capabilities) + `}`, code: jsonrpc.CodeInvalidParams},
		{name: "malformed client info", params: `{` + meta(version, `"io.modelcontextprotocol/clientInfo":{"name":"client","version":"1","icons":1}`, capabilities) + `}`, code: jsonrpc.CodeInvalidParams},
		{name: "client version", params: `{` + meta(version, `"io.modelcontextprotocol/clientInfo":{"name":"client"}`, capabilities) + `}`, code: jsonrpc.CodeInvalidParams},
		{name: "client capabilities", params: `{` + meta(version, info) + `}`, code: jsonrpc.CodeInvalidParams},
		{name: "duplicate method", headers: http.Header{"Mcp-Method": {"tools/list", "tools/list"}}, header: true, code: mcp.CodeHeaderMismatch},
		{name: "other method", headers: http.Header{"Mcp-Method": {"prompts/list"}}, code: mcp.CodeHeaderMismatch},
		{name: "missing name", method: "tools/call", headers: http.Header{"Mcp-Name": {"echo"}}, code: jsonrpc.CodeInvalidParams},
		{name: "other name", method: "tools/call", params: named("name", "echo"), headers: http.Header{"Mcp-Name": {"other"}}, code: mcp.CodeHeaderMismatch},
		{name: "empty name", method: "tools/call", params: named("name", "echo"), headers: http.Header{"Mcp-Name": {""}}, header: true, code: mcp.CodeHeaderMismatch},
		{name: "unexpected name", headers: http.Header{"Mcp-Name": {"echo"}}, header: true, code: mcp.CodeHeaderMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			method := cmp.Or(test.method, "tools/list")
			headers := http.Header{"Mcp-Protocol-Version": {proxy.CurrentVersion}, "Mcp-Method": {method}}
			maps.Copy(headers, test.headers)
			message := &jsonrpc.Request{ID: id, Method: method, Params: json.RawMessage(cmp.Or(test.params, `{`+meta(version, info, capabilities)+`}`))}
			if test.notification {
				message.ID = jsonrpc.ID{}
			}
			_, _, headerErr := proxy.ParseHeaders(headers, proxy.CurrentVersion)
			request, err := proxy.ParseRequest(headers, message, proxy.CurrentVersion)
			if (headerErr != nil) != test.header || cmp.Or(err, &jsonrpc.Error{}).Code != test.code {
				t.Fatalf("ParseHeaders = %v, ParseRequest = %v", headerErr, err)
			}
			if request.Message != message || request.Name != test.want || (request.Params == nil) != (test.notification || test.params == `[]`) {
				t.Fatalf("ParseRequest = %+v", request)
			}
		})
	}
}

func TestToolHeaderArgumentsBoundsPagination(t *testing.T) {
	pages := 0
	_, err := proxy.ToolHeaderArguments(t.Context(), json.RawMessage(`{"name":"echo"}`), func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
		pages++
		return json.Marshal(map[string]any{"tools": []any{}, "nextCursor": strings.Repeat("x", pages%101)})
	})
	if err == nil {
		t.Fatal("unbounded tool catalog pagination")
	}
}

func TestSetToolHeaders(t *testing.T) {
	headers := http.Header{}
	err := proxy.SetToolHeaders(headers, &jsonrpc.Request{Method: "tools/call", Params: json.RawMessage(`{"name":"echo","arguments":{"region":"eu"}}`)},
		json.RawMessage(`{"type":"object","properties":{"region":{"type":"string","x-mcp-header":"Region"}}}`))
	want := http.Header{"Mcp-Method": {"tools/call"}, "Mcp-Name": {"echo"}, "Mcp-Param-Region": {"eu"}}
	if err != nil || !reflect.DeepEqual(headers, want) {
		t.Fatalf("headers = %v, %v; want %v", headers, err, want)
	}
}

func TestPages(t *testing.T) {
	const tool = `{"name":"echo","inputSchema":{"const":9007199254740993},"extension":{"retained":true}}`
	var wantTools []map[string]json.RawMessage
	_ = json.Unmarshal([]byte("["+tool+"]"), &wantTools)
	for _, stop := range []string{"complete", "early", "cancelled"} {
		t.Run(stop, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls, pages := 0, 0
			cancelled := false
			for tools, err := range proxy.Pages[map[string]json.RawMessage](ctx, "skills/list", "skills", json.RawMessage(`{"_meta":{"trace":"caller"},"extension":true}`), func(ctx context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
				var params map[string]json.RawMessage
				if ctx.Err() != nil || method != "skills/list" || json.Unmarshal(raw, &params) != nil ||
					string(params["_meta"]) != `{"trace":"caller"}` || string(params["extension"]) != "true" {
					t.Fatalf("request changed: %s %s", method, raw)
				}
				wantCursor := ""
				if calls > 0 {
					wantCursor = strconv.Quote(strconv.Itoa(calls))
				}
				if string(params["cursor"]) != wantCursor {
					t.Fatalf("cursor = %s, want %s", params["cursor"], wantCursor)
				}
				calls++
				cursor := ""
				if calls < 101 {
					cursor = strconv.Itoa(calls)
				}
				return fmt.Appendf(nil, `{"skills":[%s],"nextCursor":%q}`, tool, cursor), nil
			}) {
				if err != nil {
					if stop == "cancelled" && errors.Is(err, context.Canceled) {
						cancelled = true
						break
					}
					t.Fatal(err)
				}
				pages++
				if !reflect.DeepEqual(tools, wantTools) {
					t.Fatalf("tools changed: %v", tools)
				}
				if stop == "early" {
					break
				}
				if stop == "cancelled" {
					cancel()
				}
			}
			want := 101
			if stop != "complete" {
				want = 1
			}
			if stop == "cancelled" && !cancelled {
				t.Fatal("cancellation was not reported")
			}
			if calls != want || pages != want {
				t.Fatalf("requests/pages = %d/%d, want %d", calls, pages, want)
			}
		})
	}
}

func TestPageFailures(t *testing.T) {
	upstream := errors.New("unavailable")
	for _, test := range []struct {
		name string
		body string
		err  error
	}{
		{"missing tools", `{}`, nil},
		{"invalid cursor", `{"tools":[],"nextCursor":1}`, nil},
		{"cycle", `{"tools":[],"nextCursor":"again"}`, nil},
		{"upstream", "", upstream},
	} {
		t.Run(test.name, func(t *testing.T) {
			failed := false
			calls := 0
			for _, err := range proxy.Pages[map[string]json.RawMessage](t.Context(), "tools/list", "tools", json.RawMessage(`{}`), func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
				calls++
				if calls > 2 {
					t.Fatal("pagination did not stop")
				}
				return json.RawMessage(test.body), test.err
			}) {
				if err != nil {
					failed = true
					if test.err != nil && !errors.Is(err, test.err) {
						t.Fatalf("upstream error lost: %v", err)
					}
				}
			}
			if !failed {
				t.Fatal("invalid catalog accepted")
			}
		})
	}
}

func TestHeaderEncoding(t *testing.T) {
	for _, test := range []struct{ value, wire string }{
		{"echo", "echo"},
		{"发布", "=?base64?5Y+R5biD?="},
		{" spaced ", "=?base64?IHNwYWNlZCA=?="},
		{"=?base64?literal?=", "=?base64?PT9iYXNlNjQ/bGl0ZXJhbD89?="},
	} {
		if got := proxy.EncodeHeader(test.value); got != test.wire {
			t.Errorf("encode %q = %q, want %q", test.value, got, test.wire)
		}
		if got, err := proxy.HeaderValue(http.Header{"Mcp-Name": {test.wire}}, "Mcp-Name"); err != nil || got != test.value {
			t.Errorf("decode %q = %q, %v", test.wire, got, err)
		}
	}
	for _, values := range [][]string{nil, {"a", "b"}, {" unencoded "}, {"发布"}, {"=?base64?Zg=="}, {"=?base64?Zh==?="}, {"=?base64?/w==?="}, {"=?base64?Zg==\n?="}} {
		if _, err := proxy.HeaderValue(http.Header{"Mcp-Name": values}, "Mcp-Name"); err == nil {
			t.Errorf("invalid header accepted: %q", values)
		}
	}
}
