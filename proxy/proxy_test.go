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
)

func TestValidateHeaders(t *testing.T) {
	version := `"io.modelcontextprotocol/protocolVersion":"2026-07-28"`
	info := `"io.modelcontextprotocol/clientInfo":{"name":"client","version":"1"}`
	capabilities := `"io.modelcontextprotocol/clientCapabilities":{}`
	meta := func(fields ...string) string { return `"_meta":{` + strings.Join(fields, ",") + `}` }
	call := `{"name":"echo",` + meta(version, info, capabilities) + `}`
	for _, test := range []struct {
		name, method, params string
		headers              http.Header
		fault                string
	}{
		{name: "call", method: "tools/call", params: call, headers: http.Header{"Mcp-Name": {"echo"}}},
		{name: "encoded name", method: "tools/call", params: `{"name":"发布",` + meta(version, info, capabilities) + `}`, headers: http.Header{"Mcp-Name": {"=?base64?5Y+R5biD?="}}},
		{name: "list without client info", params: `{` + meta(version, capabilities) + `}`},
		{name: "missing version header", headers: http.Header{"Mcp-Protocol-Version": nil}, fault: "header"},
		{name: "duplicate version header", headers: http.Header{"Mcp-Protocol-Version": {"2026-07-28", "2099-01-01"}}, fault: "header"},
		{name: "unsupported version header", headers: http.Header{"Mcp-Protocol-Version": {"2099-01-01"}}, fault: "header"},
		{name: "metadata version", params: `{` + meta(`"io.modelcontextprotocol/protocolVersion":"2025-11-25"`, info, capabilities) + `}`, fault: "body"},
		{name: "client name", params: `{` + meta(version, `"io.modelcontextprotocol/clientInfo":{"version":"1"}`, capabilities) + `}`, fault: "body"},
		{name: "malformed client info", params: `{` + meta(version, `"io.modelcontextprotocol/clientInfo":{"name":"client","version":"1","icons":1}`, capabilities) + `}`, fault: "body"},
		{name: "client version", params: `{` + meta(version, `"io.modelcontextprotocol/clientInfo":{"name":"client"}`, capabilities) + `}`, fault: "body"},
		{name: "client capabilities", params: `{` + meta(version, info) + `}`, fault: "body"},
		{name: "duplicate method", headers: http.Header{"Mcp-Method": {"tools/list", "tools/list"}}, fault: "header"},
		{name: "other method", headers: http.Header{"Mcp-Method": {"prompts/list"}}, fault: "body"},
		{name: "missing name", method: "tools/call", headers: http.Header{"Mcp-Name": {"echo"}}, fault: "body"},
		{name: "empty name", method: "tools/call", params: call, headers: http.Header{"Mcp-Name": {""}}, fault: "header"},
		{name: "unexpected name", headers: http.Header{"Mcp-Name": {"echo"}}, fault: "header"},
		{name: "duplicate unexpected name", headers: http.Header{"Mcp-Name": {"echo", "echo"}}, fault: "header"},
		{name: "empty unexpected name", headers: http.Header{"Mcp-Name": {""}}, fault: "header"},
	} {
		t.Run(test.name, func(t *testing.T) {
			method := cmp.Or(test.method, "tools/list")
			params := cmp.Or(test.params, `{`+meta(version, info, capabilities)+`}`)
			headers := http.Header{"Mcp-Protocol-Version": {proxy.CurrentVersion}, "Mcp-Method": {method}}
			maps.Copy(headers, test.headers)
			_, _, headerErr := proxy.ParseHeaders(headers, proxy.CurrentVersion)
			err := proxy.ValidateHeaders(headers, &jsonrpc.Request{Method: method, Params: json.RawMessage(params)}, proxy.CurrentVersion)
			if (headerErr != nil) != (test.fault == "header") || (err != nil) != (test.fault != "") {
				t.Fatalf("ParseHeaders = %v, ValidateHeaders = %v", headerErr, err)
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
