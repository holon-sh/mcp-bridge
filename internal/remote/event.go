package remote

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"iter"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// Events yields the JSON-RPC message in the data of each message [event] in
// the event stream r, its data fields joined by newlines. An event whose data
// does not decode yields an error and iteration continues; an event larger than
// maxBytes or a failed read ends it.
//
// [event]: https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
func Events(r io.Reader, maxBytes int) iter.Seq2[jsonrpc.Message, error] {
	return func(yield func(jsonrpc.Message, error) bool) {
		lines := bufio.NewScanner(r)
		lines.Buffer(nil, maxBytes)
		var data []byte
		event := ""
		for {
			more := lines.Scan()
			line := lines.Bytes()
			if !more || len(line) == 0 {
				if data = bytes.TrimSuffix(data, []byte("\n")); len(data) > 0 && (event == "" || event == "message") {
					if !yield(jsonrpc.DecodeMessage(data)) {
						return
					}
				}
				if !more {
					if err := lines.Err(); err != nil {
						yield(nil, err)
					}
					return
				}
				data, event = nil, ""
				continue
			}
			field, value, _ := bytes.Cut(line, []byte(":"))
			value = bytes.TrimPrefix(value, []byte(" "))
			switch string(field) {
			case "event":
				event = string(value)
			case "data":
				if len(data)+len(value)+1 > maxBytes {
					yield(nil, errors.New("MCP event limit exceeded"))
					return
				}
				data = append(append(data, value...), '\n')
			}
		}
	}
}

// Reply is the first JSON-RPC response in the event stream r.
func Reply(r io.Reader, maxBytes int) *jsonrpc.Response {
	for message := range Events(r, maxBytes) {
		if reply, ok := message.(*jsonrpc.Response); ok {
			return reply
		}
	}
	return nil
}
