package utils

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// LazyChunk represents a chunk where Records are loaded on demand
type LazyChunk struct {
	mcap.Chunk
	// Custom
	recordsOffset int64  // Offset in the reader where Records start
	recordsLength uint64 // Length of the Records data
	reader        io.ReadSeeker
}

// LoadRecords loads the Records data if not already loaded
func (lc *LazyChunk) LoadRecords() error {
	if lc.Records != nil {
		return nil // Already loaded
	}

	currentPosition, err := lc.reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("failed to get current position: %w", err)
	}

	// Seek to the records offset
	_, err = lc.reader.Seek(lc.recordsOffset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to records: %w", err)
	}

	// Read the records
	lc.Records = make([]byte, lc.recordsLength)
	_, err = io.ReadFull(lc.reader, lc.Records)
	if err != nil {
		return fmt.Errorf("failed to read records: %w", err)
	}

	// Reset the reader to the original position after reading the header
	fmt.Printf("Resetting reader position to %d\n", currentPosition)
	_, err = lc.reader.Seek(currentPosition, io.SeekStart)
	if err != nil {
		fmt.Printf("Failed to reset reader position: %s\n", err)
	}

	return nil
}

// ToChunk converts LazyChunk to mcap.Chunk, loading Records if needed
func (lc *LazyChunk) ToChunk() (*mcap.Chunk, error) {
	if err := lc.LoadRecords(); err != nil {
		return nil, err
	}

	return &lc.Chunk, nil
}

// ParseChunkLazy parses chunk header without reading Records
func ParseChunkLazy(reader io.ReadSeeker) (*LazyChunk, error) {
	messageStartTime, err := getUint64(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read start time: %w", err)
	}
	messageEndTime, err := getUint64(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read end time: %w", err)
	}
	uncompressedSize, err := getUint64(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read uncompressed size: %w", err)
	}
	uncompressedCRC, err := getUint32(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read uncompressed CRC: %w", err)
	}
	compression, err := getPrefixedString(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read compression: %w", err)
	}
	recordsLength, err := getUint64(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read records length: %w", err)
	}
	recordsOffset, err := reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, fmt.Errorf("failed to get current offset: %w", err)
	}

	return &LazyChunk{
		Chunk: mcap.Chunk{
			MessageStartTime: messageStartTime,
			MessageEndTime:   messageEndTime,
			UncompressedSize: uncompressedSize,
			UncompressedCRC:  uncompressedCRC,
			Compression:      compression,
		},
		recordsLength: recordsLength,
		recordsOffset: recordsOffset,
		reader:        reader,
	}, nil
}

// UpdateInfoFromChunk updates the info object with the information from the chunk.
// If chunk contains new unseen channels, add them to the info.
// If messageIndex is nil, it will be generated from the chunk and returned.
func UpdateInfoFromChunk(
	info *mcap.Info, c *mcap.Chunk, messageIndexes []*mcap.MessageIndex,
) ([]*mcap.MessageIndex, error) {
	containsNewChannel := false
	recreateMessageIndexes := false
	var messageIndexesByChannelID map[uint16]*mcap.MessageIndex

	if messageIndexes == nil {
		recreateMessageIndexes = true
		messageIndexesByChannelID = make(map[uint16]*mcap.MessageIndex)
	} else {
		for _, messageIndex := range messageIndexes {
			if messageIndex.IsEmpty() {
				continue
			}
			if _, ok := info.Channels[messageIndex.ChannelID]; !ok {
				containsNewChannel = true
			}
			info.Statistics.MessageCount += uint64(len(messageIndex.Records))
			info.Statistics.ChannelMessageCounts[messageIndex.ChannelID] += uint64(len(messageIndex.Records))
		}
	}

	if containsNewChannel || recreateMessageIndexes {
		var uncompressedBytes []byte

		switch mcap.CompressionFormat(c.Compression) {
		case mcap.CompressionNone:
			uncompressedBytes = c.Records
		case mcap.CompressionZSTD:
			compressedDataReader := bytes.NewReader(c.Records)
			chunkDataReader, err := zstd.NewReader(compressedDataReader)
			if err != nil {
				return nil, err
			}
			defer chunkDataReader.Close()
			uncompressedBytes, err = io.ReadAll(chunkDataReader)
			if err != nil {
				return nil, err
			}
		case mcap.CompressionLZ4:
			var err error
			compressedDataReader := bytes.NewReader(c.Records)
			chunkDataReader := lz4.NewReader(compressedDataReader)
			uncompressedBytes, err = io.ReadAll(chunkDataReader)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported compression format: %s", c.Compression)
		}

		uncompressedBytesReader := bytes.NewReader(uncompressedBytes)

		lexer, err := mcap.NewLexer(uncompressedBytesReader, &mcap.LexerOptions{
			SkipMagic: true,
		})
		if err != nil {
			return nil, err
		}
		defer lexer.Close()

		msg := make([]byte, 1024)
		for {
			position, err := uncompressedBytesReader.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, err
			}
			token, data, err := lexer.Next(msg)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return nil, err
			}
			if len(data) > len(msg) {
				msg = data
			}

			switch token {
			case mcap.TokenSchema:
				schema, err := mcap.ParseSchema(data)
				if err != nil {
					return nil, err
				}
				info.Schemas[schema.ID] = schema
			case mcap.TokenChannel:
				channel, err := mcap.ParseChannel(data)
				if err != nil {
					return nil, err
				}
				info.Channels[channel.ID] = channel
			case mcap.TokenMessage:
				if recreateMessageIndexes {
					m, err := mcap.ParseMessage(data)
					if err != nil {
						return nil, err
					}
					idx, ok := messageIndexesByChannelID[m.ChannelID]
					if !ok {
						idx = &mcap.MessageIndex{
							ChannelID: m.ChannelID,
							Records:   nil,
						}
						messageIndexesByChannelID[m.ChannelID] = idx
					}
					idx.Add(m.LogTime, uint64(position))

					// Also update stats if recreating indexes
					info.Statistics.MessageCount++
					info.Statistics.ChannelMessageCounts[m.ChannelID]++
				}
			}
		}
	}

	if recreateMessageIndexes {
		messageIndexes = make([]*mcap.MessageIndex, 0, len(messageIndexesByChannelID))
		for _, idx := range messageIndexesByChannelID {
			messageIndexes = append(messageIndexes, idx)
		}
	}

	return messageIndexes, nil
}

type RebuildData struct {
	// Contains everything needed to write a new summary section
	Info *mcap.Info
	// Offset of DataEnd, writing at this offset a DataEnd record to recover it.
	DataEndOffset int64
	// ContainsFaultyChunks indicates if the file contains chunks that are not
	ContainsFaultyChunks bool
	// MessageIndexes for the last chunk, if any.
	MessageIndexes []*mcap.MessageIndex

	DataSectionCRC uint32
}

// RebuildInfo reads an MCAP file and rebuilds the info from it.
func RebuildInfo(bufReader PeekableReadSeeker, includeCRC bool) (*RebuildData, error) {
	// bufReader := bufio
	// readerCounter := NewChecksummingReaderCounter(bufReader, includeCRC)

	info := &mcap.Info{
		Statistics: &mcap.Statistics{
			ChannelMessageCounts: make(map[uint16]uint64),
		},
		Channels:          make(map[uint16]*mcap.Channel),
		Schemas:           make(map[uint16]*mcap.Schema),
		ChunkIndexes:      make([]*mcap.ChunkIndex, 0),
		MetadataIndexes:   make([]*mcap.MetadataIndex, 0),
		AttachmentIndexes: make([]*mcap.AttachmentIndex, 0),
		Header: &mcap.Header{
			Profile: "",
			Library: "",
		},
		Footer: &mcap.Footer{},
	}

	rebuildData := &RebuildData{
		Info: info,
	}

	var currentPos int64

	lexer, err := mcap.NewLexer(bufReader, &mcap.LexerOptions{
		ValidateChunkCRCs: true,
		EmitChunks:        true,
		EmitInvalidChunks: true,
		AttachmentCallback: func(ar *mcap.AttachmentReader) error {
			bufferLen := 1 + // opcode
				8 + // record length
				8 + // log time
				8 + // create time
				4 + len(ar.Name) + // name
				4 + len(ar.MediaType) + // media type
				8 + // content length
				4 // CRC

			info.AttachmentIndexes = append(info.AttachmentIndexes, &mcap.AttachmentIndex{
				Offset:     uint64(currentPos),
				Length:     uint64(bufferLen) + ar.DataSize,
				DataSize:   ar.DataSize,
				LogTime:    ar.LogTime,
				CreateTime: ar.CreateTime,
				Name:       ar.Name,
				MediaType:  ar.MediaType,
			})
			return nil
		},
	})
	if err != nil {
		return nil, err
	}

	var lastLazyChunk *LazyChunk
	var lastIndexes []*mcap.MessageIndex
	var messageIndexOffsets map[uint16]uint64
	var chunkStartOffset int64
	var chunkEndOffset int64

	finalizeChunk := func() error {
		if lastLazyChunk != nil {
			// Only convert to full chunk if we need to process the records
			needsRecords := lastIndexes == nil
			if !needsRecords {
				// Check if we have new channels
				for _, idx := range lastIndexes {
					if _, ok := info.Channels[idx.ChannelID]; !ok {
						needsRecords = true
						break
					}
				}
			}

			if needsRecords {
				// Convert to full chunk and process
				fullChunk, err := lastLazyChunk.ToChunk()
				if err != nil {
					return err
				}
				_, err = UpdateInfoFromChunk(info, fullChunk, lastIndexes)
				if err != nil {
					return err
				}
			} else {
				// Just update statistics from indexes
				for _, messageIndex := range lastIndexes {
					if !messageIndex.IsEmpty() {
						info.Statistics.MessageCount += uint64(len(messageIndex.Records))
						info.Statistics.ChannelMessageCounts[messageIndex.ChannelID] += uint64(len(messageIndex.Records))
					}
				}
			}

			messageIndexEnd := uint64(currentPos)
			info.ChunkIndexes = append(info.ChunkIndexes, &mcap.ChunkIndex{
				MessageStartTime:    lastLazyChunk.MessageStartTime,
				MessageEndTime:      lastLazyChunk.MessageEndTime,
				ChunkStartOffset:    uint64(chunkStartOffset),
				ChunkLength:         uint64(chunkEndOffset - chunkStartOffset),
				MessageIndexOffsets: messageIndexOffsets,
				MessageIndexLength:  messageIndexEnd - uint64(chunkEndOffset),
				Compression:         mcap.CompressionFormat(lastLazyChunk.Compression),
				CompressedSize:      lastLazyChunk.recordsLength,
				UncompressedSize:    lastLazyChunk.UncompressedSize,
			})

			lastIndexes = nil
			lastLazyChunk = nil
			messageIndexOffsets = nil
		}
		return nil
	}

	buf := make([]byte, 1024)
	doneReading := false
	for !doneReading {
		// The offset of the previous read it the last valid position
		rebuildData.DataEndOffset = currentPos

		currentPos, err = bufReader.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, fmt.Errorf("failed to get current position: %w", err)
		}

		// rebuildData.DataSectionCRC = readerCounter.CRC()

		peek, err := bufReader.Peek(1)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		opCodePeeked := mcap.OpCode(peek[0])
		// fmt.Printf("Peeked opcode: %s at position %d\n", opCodePeeked, currentPos)

		if strings.Contains(opCodePeeked.String(), "unrecognized opcode") {
			return nil, fmt.Errorf("unrecognized opcode %s at position %d", opCodePeeked, currentPos)
		}

		if opCodePeeked != mcap.OpMessageIndex {
			if err := finalizeChunk(); err != nil {
				fmt.Printf("Failed to finalize chunk: %s\n", err)
				rebuildData.ContainsFaultyChunks = true
				lastLazyChunk = nil
			}
		}

		// Handle chunks specially to avoid reading Records unnecessarily
		if opCodePeeked == mcap.OpChunk {
			if lastLazyChunk != nil {
				return nil, fmt.Errorf("got chunk opcode but already have a lazy chunk")
			}
			// Read chunk header manually
			buf = make([]byte, 9) // opcode + length
			n, err := io.ReadFull(bufReader, buf)
			if err != nil {
				return nil, fmt.Errorf("failed to read chunk header: %w", err)
			}
			if n != 9 {
				return nil, fmt.Errorf("expected to read 9 bytes for chunk header, got %d", n)
			}

			opcode := mcap.OpCode(buf[0])
			if opcode != mcap.OpChunk {
				return nil, fmt.Errorf("expected chunk opcode, got %s", opcode)
			}

			// Parse chunk lazily
			lazyChunk, err := ParseChunkLazy(bufReader)
			if err != nil {
				fmt.Printf("Failed to parse chunk, skipping: %s\n", err)
				rebuildData.ContainsFaultyChunks = true
				continue
			}

			if info.Statistics.MessageCount == 0 {
				info.Statistics.MessageStartTime = lazyChunk.MessageStartTime
			}
			if info.Statistics.MessageEndTime < lazyChunk.MessageEndTime {
				info.Statistics.MessageEndTime = lazyChunk.MessageEndTime
			}

			// seek to after chunk
			_, err = bufReader.Seek(lazyChunk.recordsOffset+int64(lazyChunk.recordsLength), io.SeekStart)
			if err != nil {
				return nil, fmt.Errorf("failed to seek after chunk: %w", err)
			}

			lastLazyChunk = lazyChunk
			messageIndexOffsets = make(map[uint16]uint64)
			chunkStartOffset = currentPos
			chunkEndOffset, err = bufReader.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, fmt.Errorf("failed to get chunk end offset: %w", err)
			}
			continue
		}

		token, data, err := lexer.Next(buf)
		if err != nil {
			if token == mcap.TokenInvalidChunk {
				fmt.Printf("Invalid chunk encountered, skipping: %s\n", err)
				continue
			}
			if errors.Is(err, io.EOF) {
				break
			}
			var expected *mcap.ErrTruncatedRecord
			if errors.As(err, &expected) {
				fmt.Println(expected.Error())
				break
			}
			break
		}
		if len(data) > len(buf) {
			buf = data
		}

		switch token {
		case mcap.TokenHeader:
			header, err := mcap.ParseHeader(data)
			if err != nil {
				return nil, err
			}
			info.Header = header
		case mcap.TokenMessageIndex:
			if lastLazyChunk == nil {
				return nil, fmt.Errorf("got message index but not chunk before it")
			}
			index, err := mcap.ParseMessageIndex(data)
			if err != nil {
				return nil, err
			}
			lastIndexes = append(lastIndexes, index)
			messageIndexOffsets[index.ChannelID] = uint64(currentPos)

		case mcap.TokenMetadata:
			metadata, err := mcap.ParseMetadata(data)
			if err != nil {
				return nil, err
			}
			info.MetadataIndexes = append(info.MetadataIndexes, &mcap.MetadataIndex{
				Offset: uint64(currentPos),
				Length: uint64(len(data) + 8 + 1),
				Name:   metadata.Name,
			})

		case mcap.TokenDataEnd, mcap.TokenFooter:
			// data section is over, either because the file is over or the summary section starts.
			doneReading = true
			// Set the end offset for the data section
			rebuildData.DataEndOffset = currentPos

		case mcap.TokenSchema, mcap.TokenChannel, mcap.TokenMessage:
			return nil, fmt.Errorf("rebuilding info only supports chunked mcaps")

		case mcap.TokenError:
			return nil, errors.New("received error token but lexer did not return error on Next")
		}
	}

	if lastLazyChunk != nil {
		// For the last chunk, we need to load records to create indexes
		fullChunk, err := lastLazyChunk.ToChunk()
		if err != nil {
			fmt.Printf("Failed to read last chunk: %s\n", err)
			rebuildData.ContainsFaultyChunks = true
		} else {
			idx, err := UpdateInfoFromChunk(info, fullChunk, nil)
			if err != nil {
				fmt.Printf("Failed to process last chunk: %s\n", err)
				rebuildData.ContainsFaultyChunks = true
			} else {
				rebuildData.MessageIndexes = idx
			}
		}
	}

	info.Statistics.ChannelCount = uint32(len(info.Channels))
	info.Statistics.SchemaCount = uint16(len(info.Schemas))
	info.Statistics.ChunkCount = uint32(len(info.ChunkIndexes))
	info.Statistics.AttachmentCount = uint32(len(info.AttachmentIndexes))
	info.Statistics.MetadataCount = uint32(len(info.MetadataIndexes))

	return rebuildData, nil
}

// WriteInfo writes the summary section to the given writer using the provided info.
// Ensure that the cursor is just behind the DataEnd record.
func WriteInfo(w io.WriteSeeker, info *mcap.Info) error {
	position, err := w.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	section := &summarySection{
		Channels:          make([]*mcap.Channel, 0),
		Schemas:           make([]*mcap.Schema, 0),
		AttachmentIndexes: info.AttachmentIndexes,
		MetadataIndexes:   info.MetadataIndexes,
		ChunkIndexes:      info.ChunkIndexes,

		Statistics: info.Statistics,
		Footer: &mcap.Footer{
			SummaryCRC: 1, // Dummy value, so `writeSummaryBytes` write it
		},
	}

	for _, channel := range info.Channels {
		section.Channels = append(section.Channels, channel)
	}
	for _, schema := range info.Schemas {
		section.Schemas = append(section.Schemas, schema)
	}

	return writeSummaryBytes(w, section, position)
}

// Helper functions that were referenced but not included in the original code
func getUint64(reader io.Reader) (uint64, error) {
	var value uint64
	if err := binary.Read(reader, binary.LittleEndian, &value); err != nil {
		return 0, fmt.Errorf("failed to read uint64: %w", err)
	}
	return value, nil
}

func getUint32(reader io.Reader) (uint32, error) {
	var value uint32
	if err := binary.Read(reader, binary.LittleEndian, &value); err != nil {
		return 0, fmt.Errorf("failed to read uint32: %w", err)
	}
	return value, nil
}

func getPrefixedString(reader io.Reader) (string, error) {
	var length uint32
	if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
		return "", fmt.Errorf("failed to read string length: %w", err)
	}
	if length > 100 {
		return "", fmt.Errorf("string length too large: %d", length)
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(reader, buf); err != nil {
		return "", fmt.Errorf("failed to read string data: %w", err)
	}
	return string(buf), nil
}
