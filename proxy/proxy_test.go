package proxy_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strconv"
	"testing"

	"github.com/hughhan1/mcp-bridge/proxy"
)

func TestToolPages(t *testing.T) {
	const tool = `{"name":"echo","inputSchema":{"const":9007199254740993},"extension":{"retained":true}}`
	var wantTools []map[string]json.RawMessage
	_ = json.Unmarshal([]byte("["+tool+"]"), &wantTools)
	for _, stop := range []string{"complete", "early", "cancelled"} {
		t.Run(stop, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			calls, pages := 0, 0
			cancelled := false
			for tools, err := range proxy.ToolPages(ctx, json.RawMessage(`{"_meta":{"trace":"caller"},"extension":true}`), func(ctx context.Context, method string, raw json.RawMessage) (json.RawMessage, error) {
				var params map[string]json.RawMessage
				if ctx.Err() != nil || method != "tools/list" || json.Unmarshal(raw, &params) != nil ||
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
				return fmt.Appendf(nil, `{"tools":[%s],"nextCursor":%q}`, tool, cursor), nil
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

func TestToolPageFailures(t *testing.T) {
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
			for _, err := range proxy.ToolPages(t.Context(), json.RawMessage(`{}`), func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
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
