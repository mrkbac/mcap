package utils

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// HTTPReader enables io.ReadSeekCloser over HTTP using byte-range requests.
// It makes no internal buffering; reads issue an HTTP range request per call.
type HTTPReader struct {
	client Client // HTTP client or mock
	url    string // resource URL
	size   int64  // total size in bytes
	pos    int64  // current read offset
}

// Client abstraction for HTTP operations.
type Client interface {
	Head(url string) (*http.Response, error)
	Do(req *http.Request) (*http.Response, error)
}

// NewHTTPReader creates an HTTPReader, checking server range support and size.
func NewHTTPReader(client Client, url string) (*HTTPReader, error) {
	// Attempt HEAD to get Accept-Ranges and Content-Length
	resp, err := client.Head(url)
	if err != nil {
		return nil, fmt.Errorf("HEAD request failed: %w", err)
	}
	resp.Body.Close()

	// If server advertises byte ranges, use Content-Length
	if strings.EqualFold(resp.Header.Get("Accept-Ranges"), "bytes") {
		cl := resp.Header.Get("Content-Length")
		if cl == "" {
			return nil, fmt.Errorf("missing Content-Length header")
		}
		size, err := strconv.ParseInt(cl, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid Content-Length: %w", err)
		}
		return &HTTPReader{client: client, url: url, size: size}, nil
	}

	// Fallback: request first byte to infer size from Content-Range
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create GET request: %w", err)
	}
	req.Header.Set("Range", "bytes=0-0")

	rresp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("range test GET failed: %w", err)
	}
	defer rresp.Body.Close()

	if rresp.StatusCode != http.StatusPartialContent {
		return nil, fmt.Errorf("range requests not supported")
	}
	cr := rresp.Header.Get("Content-Range")
	parts := strings.Split(cr, "/")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid Content-Range header: %s", cr)
	}
	size, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid size in Content-Range: %w", err)
	}
	return &HTTPReader{client: client, url: url, size: size}, nil
}

// Read issues a HTTP range GET for [pos, pos+len(p)-1] and advances position.
// It returns io.EOF when all data has been read.
func (r *HTTPReader) Read(p []byte) (int, error) {
	if r.pos >= r.size {
		return 0, io.EOF
	}
	end := r.pos + int64(len(p)) - 1
	if end >= r.size {
		end = r.size - 1
	}

	req, err := http.NewRequest(http.MethodGet, r.url, nil)
	if err != nil {
		return 0, err
	}
	rangeHeader := fmt.Sprintf("bytes=%d-%d", r.pos, end)
	req.Header.Set("Range", rangeHeader)

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusPartialContent {
		return 0, fmt.Errorf("unexpected status: %s", resp.Status)
	}
	n, err := io.ReadFull(resp.Body, p)
	if err != nil && err != io.ErrUnexpectedEOF {
		return n, err
	}
	r.pos += int64(n)
	if r.pos >= r.size {
		err = io.EOF
	}
	return n, err
}

// Seek moves the read position according to io.Seeker semantics.
func (r *HTTPReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		abs = r.size + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}
	if abs < 0 {
		return 0, fmt.Errorf("negative position: %d", abs)
	}
	r.pos = abs
	return abs, nil
}

// Size returns the total size of the resource.
func (r *HTTPReader) Size() int64 { return r.size }

// Close implements io.Closer but has no resources to free.
func (r *HTTPReader) Close() error { return nil }
