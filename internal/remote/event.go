package remote

import (
	"bufio"
	"bytes"
)

// NextEvent reads an [event] of an event stream from r. It returns the bytes it read and the event's data, its data
// fields joined by newlines.
//
// [event]: https://html.spec.whatwg.org/multipage/server-sent-events.html#event-stream-interpretation
func NextEvent(r *bufio.Reader) (read, data []byte, err error) {
	for {
		var line []byte
		line, err = r.ReadBytes('\n')
		read = append(read, line...)
		line = bytes.TrimRight(line, "\r\n")
		if value, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			data = append(append(data, bytes.TrimPrefix(value, []byte(" "))...), '\n')
		}
		if len(line) == 0 || err != nil {
			return read, data, err
		}
	}
}
