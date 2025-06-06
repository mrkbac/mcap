package utils

import (
	"hash/crc32"
	"io"
)

type ChecksummingWriteCounter struct {
	w     io.Writer
	count int64
	crc   uint32
}

func (cw *ChecksummingWriteCounter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.count += int64(n)
	cw.crc = crc32.Update(cw.crc, crc32.IEEETable, p)
	return n, err
}

func (cw *ChecksummingWriteCounter) Count() int64 {
	return cw.count
}

func (cw *ChecksummingWriteCounter) CRC() uint32 {
	return cw.crc
}

func NewChecksummingWriteCounter(w io.Writer, initialCRC uint32) *ChecksummingWriteCounter {
	return &ChecksummingWriteCounter{
		w:   w,
		crc: initialCRC,
	}
}
