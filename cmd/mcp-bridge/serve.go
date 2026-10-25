package main

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	mcpbridge "github.com/holon-sh/mcp-bridge/http"
	"github.com/spf13/cobra"
)

func newServeCommand() *cobra.Command {
	var listen, dir, protocolVersion string
	var env, origins []string
	command := &cobra.Command{
		Use:   "serve [flags] -- SERVER [ARG...]",
		Short: "Expose a stdio MCP server over Streamable HTTP and legacy SSE",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			for _, value := range env {
				key, _, ok := strings.Cut(value, "=")
				if !ok || key == "" {
					return errors.New("--env requires KEY=VALUE")
				}
			}
			protection := http.NewCrossOriginProtection()
			for _, origin := range origins {
				if err := protection.AddTrustedOrigin(origin); err != nil {
					return err
				}
			}
			ctx := command.Context()
			diagnostics := command.ErrOrStderr()
			b, err := mcpbridge.Start(ctx, func() *exec.Cmd {
				cmd := exec.Command(args[0], args[1:]...)
				cmd.Env = append(os.Environ(), env...)
				cmd.Dir, cmd.Stderr = dir, diagnostics
				return cmd
			}, mcpbridge.Options{ProtocolVersion: protocolVersion})
			if err != nil {
				return err
			}
			defer b.Close()
			handler := protection.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				origin := r.Header.Get("Origin")
				for _, allowed := range origins {
					if origin == allowed {
						w.Header().Set("Access-Control-Allow-Origin", origin)
						w.Header().Add("Vary", "Origin")
						w.Header().Set("Access-Control-Expose-Headers", "Mcp-Session-Id")
						if r.Method == http.MethodOptions {
							w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE")
							w.Header().Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
							w.WriteHeader(http.StatusNoContent)
							return
						}
						break
					}
				}
				b.ServeHTTP(w, r)
			}))
			listener, err := net.Listen("tcp", listen)
			if err != nil {
				return err
			}
			server := &http.Server{Handler: handler, ReadHeaderTimeout: 10 * time.Second}
			_, _ = fmt.Fprintf(diagnostics, "MCP endpoint: http://%s/mcp (legacy SSE: /sse)\n", listener.Addr())
			exited := make(chan error, 2)
			go func() { exited <- server.Serve(listener) }()
			go func() { exited <- b.Wait() }()
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case err = <-exited:
			}
			_ = server.Close()
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		},
	}
	flags := command.Flags()
	flags.SetInterspersed(false)
	flags.StringVar(&listen, "listen", "127.0.0.1:8080", "HTTP listening address")
	flags.StringVar(&dir, "cwd", "", "Server working directory")
	flags.StringVar(&protocolVersion, "protocol-version", "", "Require one MCP protocol version (default: automatic negotiation)")
	flags.StringArrayVar(&env, "env", nil, "Server environment override `KEY=VALUE` (repeatable)")
	flags.StringArrayVar(&origins, "allow-origin", nil, "Trusted browser `origin` (repeatable)")
	return command
}
