package remote

import (
	"cmp"
	"mime"
	"net/http"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type RoundTripFunc func(*http.Request) (*http.Response, error)

func (f RoundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func Streamable(t mcp.StreamableClientTransport, maxBytes int, roundTrip func(http.RoundTripper, *http.Request) (*http.Response, error)) *mcp.StreamableClientTransport {
	copyClient := *cmp.Or(t.HTTPClient, http.DefaultClient)
	base := cmp.Or(copyClient.Transport, http.DefaultTransport)
	copyClient.Transport = RoundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := roundTrip(base, r.Clone(r.Context()))
		if err != nil {
			return nil, err
		}
		if media, _, _ := mime.ParseMediaType(response.Header.Get("Content-Type")); media != "text/event-stream" {
			response.Body = http.MaxBytesReader(nil, response.Body, int64(maxBytes))
		}
		return response, nil
	})
	t.HTTPClient = &copyClient
	t.DisableStandaloneSSE = true
	t.MaxRetries = -1
	t.MaxEventSize = EventSize(t.MaxEventSize, maxBytes)
	return &t
}

func EventSize(configured, limit int) int {
	if configured <= 0 {
		return limit
	}
	return min(configured, limit)
}
