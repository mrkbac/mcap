package utils

import (
	"bufio"
	"fmt"
	"io"
)

type debugReadSeekCloser struct {
	inner io.ReadSeekCloser
	// debug
	totalBytes int64 // total bytes read (for debugging)
	totalReads int64 // total requests made (for debugging)
}

var _ io.ReadSeekCloser = (*debugReadSeekCloser)(nil)

func (d *debugReadSeekCloser) Read(p []byte) (int, error) {
	n, err := d.inner.Read(p)
	if err == nil {
		d.totalBytes += int64(n)
		d.totalReads++

		fmt.Printf("Read %d bytes, total %d bytes read, %d reads made\n", n, d.totalBytes, d.totalReads)
	}
	return n, err
}

func (d *debugReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	if offset != 0 || whence != io.SeekCurrent {
		fmt.Printf("Seek called with offset %d, whence %d\n", offset, whence)
	}
	return d.inner.Seek(offset, whence)
}

// Close closes the underlying resource.
func (d *debugReadSeekCloser) Close() error {
	return d.inner.Close()
}

type peek interface {
	Peek(n int) ([]byte, error)
}

func (d *debugReadSeekCloser) Peek(n int) ([]byte, error) {
	// Peek is not supported by ReadSeekCloser, return an error.
	if pr, ok := d.inner.(peek); ok {
		data, err := pr.Peek(n)
		if err != nil {
			return nil, err
		}
		if len(data) < n {
			return nil, io.ErrUnexpectedEOF
		}
		return data, nil
	}
	return nil, fmt.Errorf("Peek not supported by %T", d.inner)
}

// bufferedReadSeekerCloser wraps a ReadSeekCloser with a buffer.
type bufferedReadSeekerCloser struct {
	inner io.ReadSeekCloser
	buf   *bufio.Reader
}

// NewBufferedReadSeekerCloser creates a new wrapper with buffer size 'size'.
func NewBufferedReadSeekerCloser(rsc io.ReadSeekCloser, size int) *bufferedReadSeekerCloser {
	inner := &debugReadSeekCloser{inner: rsc}
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
