package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestCLIProcess(t *testing.T) {
	switch os.Getenv("MCP_CLI_FIXTURE") {
	case "server":
		if err := cliServer().Run(context.Background(), &mcp.StdioTransport{}); err != nil {
			os.Exit(1)
		}
		os.Exit(0)
	case "cli":
		for i, arg := range os.Args {
			if arg == "--" {
				os.Args = append(os.Args[:1], os.Args[i+1:]...)
				main()
				os.Exit(0)
			}
		}
	}
}

func cliServer() *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "cli-fixture", Version: "1"}, nil)
	mcp.AddTool(s, &mcp.Tool{Name: "inspect"}, func(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, any, error) {
		cwd, _ := os.Getwd()
		return nil, map[string]any{"cwd": cwd, "env": os.Getenv("MCP_CLI_VALUE"), "args": os.Args}, nil
	})
	return s
}

type endpointWriter chan string

func (d endpointWriter) Write(p []byte) (int, error) {
	if text, ok := strings.CutPrefix(string(p), "MCP endpoint: "); ok {
		d <- strings.Fields(text)[0]
	}
	return len(p), nil
}

func TestServeCommand(t *testing.T) {
	for _, boundary := range []struct {
		name      string
		separator []string
	}{{"separator", []string{"--"}}, {"executable", nil}} {
		t.Run(boundary.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			d := make(endpointWriter, 1)
			cwd := t.TempDir()
			cwd, err := filepath.EvalSymlinks(cwd)
			if err != nil {
				t.Fatal(err)
			}
			finished := make(chan error, 1)
			serverArgs := []string{os.Args[0], "-test.run=^TestCLIProcess$", "--", "server-argument", "--help", "--cwd", "server-dir", "--env", "MCP_CLI_VALUE=child-argument"}
			args := []string{"serve", "--listen", "127.0.0.1:0", "--cwd", cwd, "--protocol-version", "2025-06-18", "--env", "MCP_CLI_FIXTURE=server", "--env", "MCP_CLI_VALUE=original", "--env", "MCP_CLI_VALUE=one,two=three", "--allow-origin", "https://client.example"}
			args = append(args, boundary.separator...)
			args = append(args, serverArgs...)
			go func() {
				finished <- run(ctx, args, io.NopCloser(strings.NewReader("")), nopWriter{io.Discard}, d)
			}()
			endpoint := <-d
			client := mcp.NewClient(&mcp.Implementation{Name: "cli-test", Version: "1"}, nil)
			session, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint}, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "inspect", Arguments: map[string]any{}})
			if err != nil {
				t.Fatal(err)
			}
			value := result.StructuredContent.(map[string]any)
			gotArgs, ok := value["args"].([]any)
			if value["cwd"] != cwd || value["env"] != "one,two=three" || !ok || !slices.EqualFunc(gotArgs, serverArgs, func(got any, want string) bool { return got == want }) {
				t.Fatalf("command configuration: %v", value)
			}
			for _, origin := range []string{"https://client.example", "https://untrusted.example"} {
				method := http.MethodOptions
				if origin == "https://untrusted.example" {
					method = http.MethodPost
				}
				req, _ := http.NewRequestWithContext(ctx, method, endpoint, nil)
				req.Header.Set("Origin", origin)
				req.Header.Set("Access-Control-Request-Headers", "Content-Type, Mcp-Protocol-Version")
				response, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
				if origin == "https://client.example" && (response.StatusCode != 204 || response.Header.Get("Access-Control-Allow-Origin") != origin) {
					t.Fatalf("trusted origin: %v", response)
				}
				if origin == "https://untrusted.example" && response.StatusCode != 403 {
					t.Fatalf("untrusted origin: %d", response.StatusCode)
				}
			}
			session.Close()
			cancel()
			if err := <-finished; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}

type nopWriter struct{ io.Writer }

func (nopWriter) Close() error { return nil }

func TestConnectCommand(t *testing.T) {
	for _, mode := range []struct {
		name, transport string
		redirect        bool
	}{{"http", "streamable-http", false}, {"sse", "sse", false}, {"http-redirect", "streamable-http", true}, {"sse-redirect", "sse", true}} {
		t.Run(mode.name, func(t *testing.T) {
			s := cliServer()
			var handler http.Handler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return s }, &mcp.StreamableHTTPOptions{Stateless: true})
			if mode.transport == "sse" {
				handler = mcp.NewSSEHandler(func(*http.Request) *mcp.Server { return s }, nil)
			}
			var contacted atomic.Bool
			if mode.redirect {
				other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { contacted.Store(true) }))
				t.Cleanup(other.Close)
				handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
				})
			}
			var received atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				received.Add(1)
				if r.Header.Get("Authorization") != "Bearer fixture" || !slices.Equal(r.Header.Values("X-Test"), []string{"one,two", "three"}) {
					t.Errorf("remote headers: %v", r.Header)
				}
				handler.ServeHTTP(w, r)
			}))
			defer server.Close()
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			finished := make(chan error, 1)
			var log bytes.Buffer
			go func() {
				finished <- run(ctx, []string{"connect", "--header", "Authorization: Bearer fixture", server.URL, "--transport", mode.transport, "--header", "X-Test: one,two", "--header", "X-Test: three"}, inR, outW, &log)
			}()
			client := mcp.NewClient(&mcp.Implementation{Name: "cli-test", Version: "1"}, nil)
			version := "2026-07-28"
			if mode.transport == "sse" {
				version = "2025-11-25"
			}
			session, err := client.Connect(ctx, &mcp.IOTransport{Reader: outR, Writer: inW}, &mcp.ClientSessionOptions{ProtocolVersion: version})
			if mode.redirect {
				if err == nil {
					session.Close()
					t.Fatal("followed another origin")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				if _, err := session.ListTools(ctx, &mcp.ListToolsParams{}); err != nil {
					t.Fatal(err)
				}
				session.Close()
			}
			if err := <-finished; !mode.redirect && err != nil {
				t.Fatalf("connect: %v", err)
			}
			if log.Len() != 0 || received.Load() == 0 || contacted.Load() {
				t.Fatalf("diagnostics=%q requests=%d cross-origin=%t", &log, received.Load(), contacted.Load())
			}
		})
	}
}

func TestCLISignalAndStdout(t *testing.T) {
	server := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return cliServer() }, &mcp.StreamableHTTPOptions{Stateless: true}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCLIProcess$", "--", "connect", server.URL)
	cmd.Env = append(os.Environ(), "MCP_CLI_FIXTURE=cli")
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	_, _ = io.WriteString(input, `{"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"test","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}}}`+"\n")
	var message map[string]any
	if err := json.NewDecoder(output).Decode(&message); err != nil || message["result"] == nil {
		t.Fatalf("stdout is not MCP JSON: %v %v", message, err)
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatalf("signal: %v %s", err, &stderr)
	}
}

func TestCLIArguments(t *testing.T) {
	for _, test := range []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{"no-command", nil, true},
		{"invalid-env", []string{"serve", "--env", "bad", "--", "unused"}, true},
		{"empty-env-name", []string{"serve", "--env", "=bad", "--", "unused"}, true},
		{"invalid-origin", []string{"serve", "--allow-origin", "bad", "--", "unused"}, true},
		{"unsupported-protocol", []string{"serve", "--protocol-version", "2099-01-01", "--", "unused"}, true},
		{"invalid-transport", []string{"connect", "--transport", "bad", "http://localhost"}, true},
		{"invalid-header", []string{"connect", "--header", "bad", "http://localhost"}, true},
		{"empty-header-name", []string{"connect", "--header", ":bad", "http://localhost"}, true},
		{"root-help", []string{"--help"}, false},
		{"serve-help", []string{"serve", "--help"}, false},
		{"connect-help", []string{"help", "connect"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout, diagnostics bytes.Buffer
			err := run(t.Context(), test.args, io.NopCloser(strings.NewReader("")), nopWriter{&stdout}, &diagnostics)
			if (err != nil) != test.wantErr {
				t.Fatalf("arguments %v: %v", test.args, err)
			}
			if stdout.Len() != 0 {
				t.Fatalf("non-MCP output on stdout: %q", &stdout)
			}
		})
	}
}
