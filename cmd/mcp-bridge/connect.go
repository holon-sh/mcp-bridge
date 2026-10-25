package main

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"

	"github.com/holon-sh/mcp-bridge/proxy"
	mcpbridge "github.com/holon-sh/mcp-bridge/stdio"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"
)

func newConnectCommand(input io.ReadCloser, output io.WriteCloser) *cobra.Command {
	var transport string
	var headers []string
	command := &cobra.Command{
		Use:   "connect [flags] URL",
		Short: "Expose a remote Streamable HTTP or legacy SSE MCP server over stdio",
		Args:  cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			if transport != "streamable-http" && transport != "sse" {
				return fmt.Errorf("unsupported remote transport %q; use streamable-http or sse", transport)
			}
			values := make(http.Header)
			for _, value := range headers {
				name, content, ok := strings.Cut(value, ":")
				if !ok || strings.TrimSpace(name) == "" {
					return errors.New("--header requires Name: value")
				}
				values.Add(strings.TrimSpace(name), strings.TrimSpace(content))
			}
			client := &http.Client{Transport: headerTransport{headers: values}, CheckRedirect: func(r *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return errors.New("too many remote redirects")
				}
				if len(via) > 0 && (r.URL.Scheme != via[0].URL.Scheme || !strings.EqualFold(r.URL.Host, via[0].URL.Host)) {
					return errors.New("remote redirect changes origin")
				}
				return nil
			}}
			var remote mcp.Transport = &mcp.StreamableClientTransport{Endpoint: args[0], HTTPClient: client}
			if transport == "sse" {
				remote = &mcp.SSEClientTransport{Endpoint: args[0], HTTPClient: client}
			}
			return mcpbridge.Run(command.Context(), remote, &mcp.IOTransport{Reader: input, Writer: output, MaxLineLength: proxy.MaxMessageBytes})
		},
	}
	flags := command.Flags()
	flags.StringVar(&transport, "transport", "streamable-http", "Remote transport: streamable-http or sse")
	flags.StringArrayVar(&headers, "header", nil, "Remote `header` as Name: value (repeatable)")
	return command
}

type headerTransport struct{ headers http.Header }

func (t headerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	maps.Copy(r.Header, t.headers.Clone())
	return http.DefaultTransport.RoundTrip(r)
}
