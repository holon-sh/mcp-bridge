package remote

import (
	"bufio"
	"bytes"
	"io"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
)

// Reply is the first JSON-RPC response in the event stream r: the data of an [event], its data fields joined by
// newlines, that decodes as one. It reads r no further.
//
// [event]: https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
func Reply(r io.Reader) *jsonrpc.Response {
	lines := bufio.NewReader(r)
	var data []byte
	for {
		line, err := lines.ReadBytes('\n')
		line = bytes.TrimRight(line, "\r\n")
		if value, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			data = append(append(data, bytes.TrimPrefix(value, []byte(" "))...), '\n')
		}
		if len(line) == 0 || err != nil {
			message, _ := jsonrpc.DecodeMessage(bytes.TrimSpace(data))
			if reply, ok := message.(*jsonrpc.Response); ok || err != nil {
				return reply
			}
			data = nil
		}
	}
}
