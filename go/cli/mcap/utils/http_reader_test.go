package utils

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
)

// helper to create a test server supporting ranges
func newRangeServer(data []byte, supportRanges bool) *httptest.Server {
	handler := func(w http.ResponseWriter, req *http.Request) {
		if req.Method == "HEAD" {
			if supportRanges {
				w.Header().Set("Accept-Ranges", "bytes")
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			return
		}
		rng := req.Header.Get("Range")
		if supportRanges && rng != "" {
			// parse "bytes=start-end"
			var start, end int
			_, err := fmt.Sscanf(rng, "bytes=%d-%d", &start, &end)
			if err != nil {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			if end >= len(data) {
				end = len(data) - 1
			}
			w.Header().Set("Content-Range",
				fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(data[start : end+1])
			return
		}
		// full response
		w.Write(data)
	}
	return httptest.NewServer(http.HandlerFunc(handler))
}

func TestReadSequential(t *testing.T) {
	content := []byte("Hello, 世界—Go!")
	ts := newRangeServer(content, true)
	defer ts.Close()

	rs, err := NewHTTPReader(http.DefaultClient, ts.URL)
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	buf := make([]byte, len(content))
	n, err := rs.Read(buf[:5])
	if err != nil {
		t.Fatalf("Read() first error: %v", err)
	}
	if string(buf[:5]) != string(content[:5]) {
		t.Errorf("got %q, want %q", buf[:5], content[:5])
	}
	n2, err := rs.Read(buf[5:])
	if err != nil && err != io.EOF {
		t.Fatalf("Read() second error: %v", err)
	}
	if string(buf) != string(content) {
		t.Errorf("complete read mismatch: got %q", buf)
	}
	if n+n2 != len(content) {
		t.Errorf("read bytes count mismatch: got %d, want %d", n+n2, len(content))
	}
}
