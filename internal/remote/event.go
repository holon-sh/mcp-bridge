package remote

import (
	"bufio"
	"bytes"
	"io"
	"iter"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// Messages yields the JSON-RPC message of each [event] of the event stream r whose data, its data fields joined by
// newlines, holds one.
//
// [event]: https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
func Messages(r io.Reader) iter.Seq[jsonrpc.Message] {
	return func(yield func(jsonrpc.Message) bool) {
		lines := bufio.NewReader(r)
		var data []byte
		for {
			line, err := lines.ReadBytes('\n')
			line = bytes.TrimRight(line, "\r\n")
			if value, ok := bytes.CutPrefix(line, []byte("data:")); ok {
				data = append(append(data, bytes.TrimPrefix(value, []byte(" "))...), '\n')
			}
			if len(line) == 0 || err != nil {
				if message, invalid := jsonrpc.DecodeMessage(bytes.TrimSpace(data)); invalid == nil && !yield(message) {
					return
				}
				data = nil
			}
			if err != nil {
				return
			}
		}
	}
}
