package utils

import (
	"bufio"
	"io"
)

type PeekableReadSeeker interface {
	io.ReadSeeker
	Peek(n int) ([]byte, error)
}

// Ensure debugReadSeekCloser implements the interfaces we expect.
var _ io.ReadSeekCloser = (*bufferedReadSeekerCloser)(nil)
var _ PeekableReadSeeker = (*bufferedReadSeekerCloser)(nil)

// bufferedReadSeekerCloser wraps a ReadSeekCloser with a buffer.
type bufferedReadSeekerCloser struct {
	inner io.ReadSeekCloser
	buf   *bufio.Reader
}

// NewBufferedReadSeekerCloser creates a new wrapper with buffer size 'size'.
func NewBufferedReadSeekerCloser(inner io.ReadSeekCloser, size int) *bufferedReadSeekerCloser {
	return &bufferedReadSeekerCloser{
		inner: inner,
		buf:   bufio.NewReaderSize(inner, size),
	}
}

// Read reads from the internal buffer.
func (b *bufferedReadSeekerCloser) Read(p []byte) (int, error) {
	return b.buf.Read(p)
}

// Seek handles all whence modes, with optimized logic for SeekCurrent.
func (b *bufferedReadSeekerCloser) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekCurrent:
		unread := int64(b.buf.Buffered())

		// Special-case: querying current position
		if offset == 0 {
			underlyingPos, err := b.inner.Seek(0, io.SeekCurrent)
			if err != nil {
				return 0, err
			}
			// logical position = underlying − unread buffered bytes
			return underlyingPos - unread, nil
		}

		// Single-call reposition: move by (offset − unread)
		newPos, err := b.inner.Seek(offset-unread, io.SeekCurrent)
		if err != nil {
			return 0, err
		}
		// discard the old buffer so reads start fresh at newPos
		b.buf.Reset(b.inner)
		return newPos, nil

	default:
		// SeekStart & SeekEnd: delegate and reset buffer
		newPos, err := b.inner.Seek(offset, whence)
		if err != nil {
			return 0, err
		}
		b.buf.Reset(b.inner)
		return newPos, nil
	}
}

func (b *bufferedReadSeekerCloser) Peek(n int) ([]byte, error) {
	// Peek reads from the internal buffer without consuming it.
	return b.buf.Peek(n)
}

// Close closes the underlying resource.
func (b *bufferedReadSeekerCloser) Close() error {
	return b.inner.Close()
}
