package utils

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

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

// BuildInfo reads an MCAP file and builds basic info from it.
func BuildInfo(reader io.ReadSeeker) (*mcap.Info, error) {
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

	lexer, err := mcap.NewLexer(reader, &mcap.LexerOptions{
		ValidateChunkCRCs: true,
		EmitChunks:        true,
		EmitInvalidChunks: true,
	})
	if err != nil {
		return nil, err
	}

	buf := make([]byte, 1024)
	done := false
	for !done {
		token, data, err := lexer.Next(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			// Skip invalid chunks and continue
			if token == mcap.TokenInvalidChunk {
				continue
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
		case mcap.TokenChunk:
			chunk, err := mcap.ParseChunk(data)
			if err != nil {
				continue // Skip malformed chunks
			}
			// Update info with chunk data
			_, err = UpdateInfoFromChunk(info, chunk, nil)
			if err != nil {
				continue // Skip chunks that can't be processed
			}
		case mcap.TokenMetadata:
			metadata, err := mcap.ParseMetadata(data)
			if err != nil {
				continue
			}
			info.MetadataIndexes = append(info.MetadataIndexes, &mcap.MetadataIndex{
				Name: metadata.Name,
			})
		case mcap.TokenDataEnd, mcap.TokenFooter:
			// End of data section
			done = true
		case mcap.TokenSchema, mcap.TokenChannel, mcap.TokenMessage:
			return nil, fmt.Errorf("non-chunked mcap not supported by BuildInfo")
		}
	}
	// Update final statistics
	info.Statistics.ChannelCount = uint32(len(info.Channels))
	info.Statistics.SchemaCount = uint16(len(info.Schemas))
	info.Statistics.ChunkCount = uint32(len(info.ChunkIndexes))
	info.Statistics.AttachmentCount = uint32(len(info.AttachmentIndexes))
	info.Statistics.MetadataCount = uint32(len(info.MetadataIndexes))

	return info, nil
}
