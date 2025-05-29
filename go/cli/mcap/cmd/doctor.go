package cmd

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"reflect"

	"github.com/fatih/color"
	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/spf13/cobra"
)

var (
	verbose            bool
	strictMessageOrder bool
)

type mcapDoctor struct {
	reader *utils.ChecksummingReaderCounter

	channelsInDataSection              map[uint16]*mcap.Channel
	schemasInDataSection               map[uint16]*mcap.Schema
	channelsReferencedInChunksByOffset map[uint64][]uint16
	channelIDsInSummarySection         map[uint16]bool
	schemaIDsInSummarySection          map[uint16]bool

	// Map from chunk offset to chunk index
	chunkIndexes map[uint64]*mcap.ChunkIndex

	inSummarySection bool

	messageCount         uint64
	channelMessageCounts map[uint16]uint64
	minLogTime           uint64
	maxLogTime           uint64
	statistics           *mcap.Statistics
	expectedChunkIndexes []*mcap.ChunkIndex

	diagnosis Diagnosis
}

func (doctor *mcapDoctor) warn(format string, v ...any) {
	color.Yellow(format, v...)
	doctor.diagnosis.Warnings = append(doctor.diagnosis.Warnings, fmt.Sprintf(format, v...))
}

func (doctor *mcapDoctor) error(format string, v ...any) {
	color.Red(format, v...)
	doctor.diagnosis.Errors = append(doctor.diagnosis.Errors, fmt.Sprintf(format, v...))
}

func (doctor *mcapDoctor) fatal(v ...any) {
	color.Set(color.FgRed)
	fmt.Println(v...)
	color.Unset()
	os.Exit(1)
}

func (doctor *mcapDoctor) fatalf(format string, v ...any) {
	color.Red(format, v...)
	os.Exit(1)
}

func (doctor *mcapDoctor) examineSchema(schema *mcap.Schema) {
	if schema.Encoding == "" {
		if len(schema.Data) == 0 {
			doctor.warn("Schema with ID: %d, Name: %q has empty Encoding and Data fields", schema.ID, schema.Name)
		} else {
			doctor.error("Schema with ID: %d has empty Encoding but Data contains: %q", schema.ID, string(schema.Data))
		}
	}

	if schema.ID == 0 {
		doctor.error("Schema.ID 0 is reserved. Do not make Schema records with ID 0.")
	}
	previous := doctor.schemasInDataSection[schema.ID]
	if previous != nil {
		if schema.Name != previous.Name {
			doctor.error("Two schema records with same ID %d but different names (%q != %q)",
				schema.ID,
				schema.Name,
				previous.Name,
			)
		}
		if schema.Encoding != previous.Encoding {
			doctor.error("Two schema records with same ID %d but different encodings (%q != %q)",
				schema.ID,
				schema.Encoding,
				previous.Encoding,
			)
		}
		if !bytes.Equal(schema.Data, previous.Data) {
			doctor.error("Two schema records with different data present with same ID %d", schema.ID)
		}
	}
	if doctor.inSummarySection {
		if previous == nil {
			doctor.error("Schema with id %d in summary section does not exist in data section", schema.ID)
		}
		doctor.schemaIDsInSummarySection[schema.ID] = true
	} else {
		if previous != nil {
			doctor.warn("Duplicate schema records in data section with ID %d", schema.ID)
		}
		doctor.schemasInDataSection[schema.ID] = schema
	}
}

func (doctor *mcapDoctor) examineChannel(channel *mcap.Channel) {
	previous := doctor.channelsInDataSection[channel.ID]
	if previous != nil {
		if channel.SchemaID != previous.SchemaID {
			doctor.error("Two channel records with same ID %d but different schema IDs (%d != %d)",
				channel.ID,
				channel.SchemaID,
				previous.SchemaID,
			)
		}
		if channel.Topic != previous.Topic {
			doctor.error("Two channel records with same ID %d but different topics (%q != %q)",
				channel.ID,
				channel.Topic,
				previous.Topic,
			)
		}
		if channel.MessageEncoding != previous.MessageEncoding {
			doctor.error("Two channel records with same ID %d but different message encodings (%q != %q)",
				channel.ID,
				channel.MessageEncoding,
				previous.MessageEncoding,
			)
		}
		if !reflect.DeepEqual(channel.Metadata, previous.Metadata) {
			doctor.error("Two channel records with different metadata present with same ID %d",
				channel.ID)
		}
	}
	if doctor.inSummarySection {
		if previous == nil {
			doctor.error("Channel with ID %d in summary section does not exist in data section", channel.ID)
		}
		doctor.channelIDsInSummarySection[channel.ID] = true
	} else {
		if previous != nil {
			doctor.warn("Duplicate channel records in data section with ID %d", channel.ID)
		}
		doctor.channelsInDataSection[channel.ID] = channel
	}

	if channel.SchemaID != 0 {
		if _, ok := doctor.schemasInDataSection[channel.SchemaID]; !ok {
			doctor.error("Encountered Channel (%d) with unknown Schema (%d)", channel.ID, channel.SchemaID)
		}
	}
}

func (doctor *mcapDoctor) examineChunk(
	chunk *mcap.Chunk,
	startOffset uint64,
	endOffset uint64,
	messageIndexOffsets map[uint16]uint64,
	messageIndexEnd uint64,
	messageIndexes map[uint16]*mcap.MessageIndex,
) {
	referencedChannels := make(map[uint16]bool)
	compressionFormat := mcap.CompressionFormat(chunk.Compression)
	var uncompressedBytes []byte

	switch compressionFormat {
	case mcap.CompressionNone:
		uncompressedBytes = chunk.Records
	case mcap.CompressionZSTD:
		compressedDataReader := bytes.NewReader(chunk.Records)
		chunkDataReader, err := zstd.NewReader(compressedDataReader)
		if err != nil {
			doctor.error("Could not make zstd decoder: %s", err)
			return
		}
		uncompressedBytes, err = io.ReadAll(chunkDataReader)
		if err != nil {
			doctor.error("Could not decompress: %s", err)
			return
		}
	case mcap.CompressionLZ4:
		var err error
		compressedDataReader := bytes.NewReader(chunk.Records)
		chunkDataReader := lz4.NewReader(compressedDataReader)
		uncompressedBytes, err = io.ReadAll(chunkDataReader)
		if err != nil {
			doctor.error("Could not decompress: %s", err)
			return
		}
	default:
		doctor.error("Unsupported compression format: %q", chunk.Compression)
		return
	}

	if uint64(len(uncompressedBytes)) != chunk.UncompressedSize {
		doctor.error("Uncompressed chunk data size != Chunk.uncompressed_size")
		return
	}

	if chunk.UncompressedCRC != 0 {
		crc := crc32.ChecksumIEEE(uncompressedBytes)
		if crc != chunk.UncompressedCRC {
			doctor.error("invalid CRC: %x != %x", crc, chunk.UncompressedCRC)
			return
		}
	}

	uncompressedBytesReader := bytes.NewReader(uncompressedBytes)

	lexer, err := mcap.NewLexer(uncompressedBytesReader, &mcap.LexerOptions{
		SkipMagic:         true,
		ValidateChunkCRCs: true,
		EmitChunks:        true,
	})
	if err != nil {
		doctor.error("Failed to make lexer for chunk bytes: %s", err)
		return
	}
	defer lexer.Close()

	var minLogTime uint64 = math.MaxUint64
	var maxLogTime uint64
	var chunkMessageCount uint64

	msg := make([]byte, 1024)
	for {
		currentPosition, err := uncompressedBytesReader.Seek(0, io.SeekCurrent)
		if err != nil {
			doctor.error("Failed to determine read cursor: %s", err)
			return
		}
		tokenType, data, err := lexer.Next(msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			doctor.fatalf("Failed to read next token: %s", err)
		}
		if len(data) > len(msg) {
			msg = data
		}

		if len(data) > len(msg) {
			msg = data
		}
		switch tokenType {
		case mcap.TokenSchema:
			schema, err := mcap.ParseSchema(data)
			if err != nil {
				doctor.error("Failed to parse schema: %s", err)
			}
			doctor.examineSchema(schema)
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				doctor.error("Error parsing Channel: %s", err)
			}
			doctor.examineChannel(channel)
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(data)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}
			referencedChannels[message.ChannelID] = true

			channel := doctor.channelsInDataSection[message.ChannelID]
			if channel == nil {
				doctor.error("Got a Message record for channel: %d before a channel record.", message.ChannelID)
			}

			if message.LogTime < doctor.maxLogTime {
				errStr := fmt.Sprintf("Message.log_time %d on %q is less than the latest log time %d",
					message.LogTime, channel.Topic, doctor.maxLogTime)
				if strictMessageOrder {
					doctor.error(errStr)
				} else {
					doctor.warn(errStr)
				}
			}

			if message.LogTime < minLogTime {
				minLogTime = message.LogTime
			}

			if message.LogTime > maxLogTime {
				maxLogTime = message.LogTime
			}

			if message.LogTime > doctor.maxLogTime {
				doctor.maxLogTime = message.LogTime
			}

			chunkMessageCount++
			doctor.messageCount++
			doctor.channelMessageCounts[message.ChannelID]++

			if messageIndex, ok := messageIndexes[message.ChannelID]; ok {
				if messageIndex.ChannelID == message.ChannelID {
					foundIndexEntry := false
					for _, index := range messageIndex.Records {
						if index.Offset == uint64(currentPosition) {
							if index.Timestamp != message.LogTime {
								doctor.error(
									"MessageIndex entry for channel %d at offset %d has timestamp %d, but message has timestamp %d",
									message.ChannelID,
									currentPosition,
									index.Timestamp,
									message.LogTime,
								)
							}
							foundIndexEntry = true
							break
						}
					}

					if !foundIndexEntry {
						doctor.warn(
							"No MessageIndex entry found for message channel %d with log time %d at offset %d",
							message.ChannelID,
							message.LogTime,
							currentPosition,
						)
					}
				}
			} else {
				doctor.warn(
					"No MessageIndex record found for channel %d",
					message.ChannelID,
				)
			}

		default:
			doctor.error("Illegal record in chunk: %d", tokenType)
		}
	}

	if chunkMessageCount != 0 {
		if minLogTime != chunk.MessageStartTime {
			doctor.error(
				"Chunk.message_start_time %d does not match the earliest message log time %d",
				chunk.MessageStartTime,
				minLogTime,
			)
		}

		if maxLogTime != chunk.MessageEndTime && chunkMessageCount != 0 {
			doctor.error(
				"Chunk.message_end_time %d does not match the latest message log time %d",
				chunk.MessageEndTime,
				maxLogTime,
			)
		}

		if minLogTime < doctor.minLogTime {
			doctor.minLogTime = minLogTime
		}
		if maxLogTime > doctor.maxLogTime {
			doctor.maxLogTime = maxLogTime
		}
	}
	asArray := make([]uint16, 0, len(referencedChannels))
	for id := range referencedChannels {
		asArray = append(asArray, id)
	}
	doctor.channelsReferencedInChunksByOffset[startOffset] = asArray

	doctor.expectedChunkIndexes = append(doctor.expectedChunkIndexes, &mcap.ChunkIndex{
		MessageStartTime:    chunk.MessageStartTime,
		MessageEndTime:      chunk.MessageEndTime,
		ChunkStartOffset:    startOffset,
		ChunkLength:         endOffset - startOffset,
		MessageIndexOffsets: messageIndexOffsets,
		MessageIndexLength:  messageIndexEnd - endOffset,
		Compression:         mcap.CompressionFormat(chunk.Compression),
		CompressedSize:      uint64(len(chunk.Records)),
		UncompressedSize:    chunk.UncompressedSize,
	})

}

type Diagnosis struct {
	Errors   []string
	Warnings []string
}

func (doctor *mcapDoctor) Examine() Diagnosis {
	lexer, err := mcap.NewLexer(doctor.reader, &mcap.LexerOptions{
		SkipMagic:         false,
		ValidateChunkCRCs: true,
		EmitChunks:        true,
	})
	if err != nil {
		doctor.fatal(err)
	}
	defer lexer.Close()

	var lastMessageTime uint64
	var lastToken mcap.TokenType
	var dataEnd *mcap.DataEnd
	var footer *mcap.Footer
	var messageOutsideChunk bool
	var lastChunk *mcap.Chunk
	var lastChunkStartOffset uint64
	var lastChunkEndOffset uint64
	var lastChunkMessageIndexes map[uint16]*mcap.MessageIndex
	var lastChunkMessageIndexOffsets map[uint16]uint64
	var lastChunkMessageIndexEnd uint64

	msg := make([]byte, 1024)
	for {
		// Read the crc first since the crc does not contain DataEnd
		previousCRC := doctor.reader.CRC()
		recordStartPos := doctor.reader.Count()
		tokenType, data, err := lexer.Next(msg)
		if err != nil {
			if errors.Is(err, io.EOF) {
				if dataEnd == nil {
					doctor.error("File does not contain a DataEnd record (last record was %s)", lastToken.String())
				}
				if footer == nil {
					doctor.error("File does not contain a Footer record (last record was %s)", lastToken.String())
				}
				break
			}
			doctor.fatalf("Failed to read next token: %s", err)
		}
		lastToken = tokenType
		if len(data) > len(msg) {
			msg = data
		}

		if tokenType != mcap.TokenMessageIndex {
			if lastChunk != nil {
				doctor.examineChunk(lastChunk, lastChunkStartOffset, lastChunkEndOffset, lastChunkMessageIndexOffsets, lastChunkMessageIndexEnd, lastChunkMessageIndexes)
				lastChunk = nil
				lastChunkMessageIndexes = nil
				lastChunkMessageIndexOffsets = nil
			}
		}
		switch tokenType {
		case mcap.TokenHeader:
			header, err := mcap.ParseHeader(data)
			if err != nil {
				doctor.error("Error parsing Header: %s", err)
			}

			if header.Library == "" {
				doctor.warn("Set the Header.library field to a value that identifies the software that produced the file.")
			}

			if header.Profile != "" && header.Profile != "ros1" && header.Profile != "ros2" {
				doctor.warn(`Header.profile field %q is not a well-known profile.`, header.Profile)
			}
		case mcap.TokenFooter:
			footer, err = mcap.ParseFooter(data)
			if err != nil {
				doctor.error("Failed to parse footer: %s", err)
			}
			// Everything in the footer besides the CRC is contained in the CRC calculation
			footerHeader := make([]byte, 9)
			footerHeader[0] = byte(mcap.OpFooter)
			binary.LittleEndian.PutUint64(footerHeader[1:], 8+8+4)
			previousCRC = crc32.Update(previousCRC, crc32.IEEETable, footerHeader)
			previousCRC = crc32.Update(previousCRC, crc32.IEEETable, data[:len(data)-4])
			if footer.SummaryCRC != 0 && footer.SummaryCRC != previousCRC {
				doctor.error("Summary CRC mismatch: %x != %x", footer.SummaryCRC, previousCRC)
			}
		case mcap.TokenSchema:
			schema, err := mcap.ParseSchema(data)
			if err != nil {
				doctor.error("Failed to parse schema: %s", err)
			}
			doctor.examineSchema(schema)
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				doctor.error("Error parsing Channel: %s", err)
			}
			doctor.examineChannel(channel)
		case mcap.TokenMessage:
			message, err := mcap.ParseMessage(data)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}
			messageOutsideChunk = true
			channel := doctor.channelsInDataSection[message.ChannelID]
			if channel == nil {
				doctor.error("Got a Message record for channel: %d before a channel info.", message.ChannelID)
			}
			if message.LogTime < lastMessageTime {
				doctor.error("Message.log_time %d on %q is less than the previous message record time %d",
					message.LogTime, channel.Topic, lastMessageTime)
			}
			lastMessageTime = message.LogTime

			if message.LogTime < doctor.minLogTime {
				doctor.minLogTime = message.LogTime
			}
			if message.LogTime > doctor.maxLogTime {
				doctor.maxLogTime = message.LogTime
			}

			doctor.messageCount++
			doctor.channelMessageCounts[message.ChannelID]++

		case mcap.TokenChunk:
			chunk, err := mcap.ParseChunk(data)
			if err != nil {
				doctor.error("Error parsing Message: %s", err)
			}
			lastChunkStartOffset = uint64(recordStartPos)
			lastChunkEndOffset = uint64(doctor.reader.Count())

			lastChunk = chunk
			// copy the records, since it is referenced and the buffer will be reused
			recordsCopy := make([]byte, len(lastChunk.Records))
			copy(recordsCopy, lastChunk.Records)
			lastChunk.Records = recordsCopy
		case mcap.TokenMessageIndex:
			messageIndex, err := mcap.ParseMessageIndex(data)
			if err != nil {
				doctor.error("Failed to parse message index: %s", err)
			}
			if messageOutsideChunk {
				doctor.warn("Message index in file has message records outside chunks. Indexed readers will miss these messages.")
			}
			if lastChunk == nil {
				doctor.error("Message index for channel %d found before a chunk record", messageIndex.ChannelID)
			} else {
				if lastChunkMessageIndexes == nil {
					lastChunkMessageIndexes = make(map[uint16]*mcap.MessageIndex)
				}
				if _, ok := lastChunkMessageIndexes[messageIndex.ChannelID]; ok {
					doctor.warn("Duplicate message index found for channel %d", messageIndex.ChannelID)
				} else {
					lastChunkMessageIndexes[messageIndex.ChannelID] = messageIndex
				}

				if lastChunkMessageIndexOffsets == nil {
					lastChunkMessageIndexOffsets = make(map[uint16]uint64)
				}
				lastChunkMessageIndexOffsets[messageIndex.ChannelID] = uint64(recordStartPos)
			}
		case mcap.TokenChunkIndex:
			chunkIndex, err := mcap.ParseChunkIndex(data)
			if err != nil {
				doctor.error("Failed to parse chunk index: %s", err)
			}
			if messageOutsideChunk {
				doctor.warn("Message index in file has message records outside chunks. Indexed readers will miss these messages.")
			}
			if _, ok := doctor.chunkIndexes[chunkIndex.ChunkStartOffset]; ok {
				doctor.error("Multiple chunk indexes found for chunk at offset %d", chunkIndex.ChunkStartOffset)
			}
			doctor.chunkIndexes[chunkIndex.ChunkStartOffset] = chunkIndex
		case mcap.TokenAttachmentIndex:
			_, err := mcap.ParseAttachmentIndex(data)
			if err != nil {
				doctor.error("Failed to parse attachment index: %s", err)
			}
		case mcap.TokenStatistics:
			statistics, err := mcap.ParseStatistics(data)
			if err != nil {
				doctor.error("Failed to parse statistics: %s", err)
			}
			if doctor.statistics != nil {
				doctor.error("File contains multiple Statistics records")
			}
			doctor.statistics = statistics
		case mcap.TokenMetadata:
			_, err := mcap.ParseMetadata(data)
			if err != nil {
				doctor.error("Failed to parse metadata: %s", err)
			}
		case mcap.TokenMetadataIndex:
			_, err := mcap.ParseMetadataIndex(data)
			if err != nil {
				doctor.error("Failed to parse metadata index: %s", err)
			}
		case mcap.TokenSummaryOffset:
			_, err := mcap.ParseSummaryOffset(data)
			if err != nil {
				doctor.error("Failed to parse summary offset: %s", err)
			}
		case mcap.TokenDataEnd:
			dataEnd, err = mcap.ParseDataEnd(data)
			if err != nil {
				doctor.error("Failed to parse data end: %s", err)
			}
			doctor.inSummarySection = true

			if dataEnd.DataSectionCRC != 0 && dataEnd.DataSectionCRC != previousCRC {
				doctor.error("Data section CRC mismatch: %x != %x", dataEnd.DataSectionCRC, previousCRC)
			}
			doctor.reader.ResetCRC()
		case mcap.TokenError:
			// this is the value of the tokenType when there is an error
			// from the lexer, which we caught at the top.
			doctor.fatalf("Failed to parse:", err)
		}
	}

	if lastChunk != nil {
		doctor.examineChunk(lastChunk, lastChunkStartOffset, lastChunkEndOffset, lastChunkMessageIndexOffsets, lastChunkMessageIndexEnd, lastChunkMessageIndexes)
	}

	for chunkOffset, chunkIndex := range doctor.chunkIndexes {
		channelsReferenced := doctor.channelsReferencedInChunksByOffset[chunkOffset]
		for _, id := range channelsReferenced {
			if present := doctor.channelIDsInSummarySection[id]; !present {
				doctor.error(
					"Indexed chunk at offset %d contains messages referencing channel (%d) not duplicated in summary section",
					chunkOffset,
					id,
				)
			}
			channel := doctor.channelsInDataSection[id]
			if channel == nil {
				// message with unknown channel, this is checked when that message is scanned
				continue
			}
			if present := doctor.schemaIDsInSummarySection[channel.SchemaID]; !present {
				doctor.error(
					"Indexed chunk at offset %d contains messages referencing schema (%d) not duplicated in summary section",
					chunkOffset,
					channel.SchemaID,
				)
			}
		}

		var foundIndex *mcap.ChunkIndex
		for _, expected := range doctor.expectedChunkIndexes {
			if expected.ChunkStartOffset == chunkOffset {
				// This is an expected chunk index, so we don't need to check it.
				foundIndex = expected
				break
			}
		}

		if foundIndex != nil {
			if chunkIndex.ChunkLength != foundIndex.ChunkLength {
				doctor.error(
					"Chunk index %d length mismatch: %d vs %d.",
					chunkOffset,
					chunkIndex.ChunkLength,
					foundIndex.ChunkLength,
				)
				continue
			}

			if foundIndex.MessageStartTime != chunkIndex.MessageStartTime {
				doctor.error(
					"Chunk at offset %d has message start time %d, but its chunk index has message start time %d",
					chunkOffset,
					foundIndex.MessageStartTime,
					chunkIndex.MessageStartTime,
				)
			}
			if foundIndex.MessageEndTime != chunkIndex.MessageEndTime {
				doctor.error(
					"Chunk at offset %d has message end time %d, but its chunk index has message end time %d",
					chunkOffset,
					foundIndex.MessageEndTime,
					chunkIndex.MessageEndTime,
				)
			}
			if foundIndex.Compression != chunkIndex.Compression {
				doctor.error(
					"Chunk at offset %d has compression %q, but its chunk index has compression %q",
					chunkOffset,
					foundIndex.Compression,
					chunkIndex.Compression,
				)
			}
			if foundIndex.CompressedSize != chunkIndex.CompressedSize {
				doctor.error(
					"Chunk at offset %d has data length %d, but its chunk index has compressed size %d",
					chunkOffset,
					foundIndex.CompressedSize,
					chunkIndex.CompressedSize,
				)
			}
			if foundIndex.UncompressedSize != chunkIndex.UncompressedSize {
				doctor.error(
					"Chunk at offset %d has uncompressed size %d, but its chunk index has uncompressed size %d",
					chunkOffset,
					foundIndex.UncompressedSize,
					chunkIndex.UncompressedSize,
				)
			}
		} else {
			doctor.error(
				"Chunk index at offset %d does not point to an chunk",
				chunkOffset,
			)
		}
	}

	if doctor.statistics != nil {
		if doctor.messageCount > 0 {
			if doctor.statistics.MessageStartTime != doctor.minLogTime {
				doctor.error(
					"Statistics has message start time %d, but the minimum message start time is %d",
					doctor.statistics.MessageStartTime,
					doctor.minLogTime,
				)
			}
			if doctor.statistics.MessageEndTime != doctor.maxLogTime {
				doctor.error(
					"Statistics has message end time %d, but the maximum message end time is %d",
					doctor.statistics.MessageEndTime,
					doctor.maxLogTime,
				)
			}
		}
		if doctor.statistics.MessageCount != doctor.messageCount {
			doctor.error(
				"Statistics has message count %d, but actual number of messages is %d",
				doctor.statistics.MessageCount,
				doctor.messageCount,
			)
		}
		for channelID, count := range doctor.channelMessageCounts {
			if count != doctor.statistics.ChannelMessageCounts[channelID] {
				doctor.error(
					"Statistics has message count %d for channel %d, but actual number of messages is %d",
					doctor.statistics.ChannelMessageCounts[channelID],
					channelID,
					count,
				)
			}
		}
	}
	return doctor.diagnosis
}

func newMcapDoctor(reader io.ReadSeeker) *mcapDoctor {
	return &mcapDoctor{
		reader:                             utils.NewChecksummingReaderCounter(reader, true),
		channelsInDataSection:              make(map[uint16]*mcap.Channel),
		channelsReferencedInChunksByOffset: make(map[uint64][]uint16),
		channelIDsInSummarySection:         make(map[uint16]bool),
		schemaIDsInSummarySection:          make(map[uint16]bool),
		schemasInDataSection:               make(map[uint16]*mcap.Schema),
		chunkIndexes:                       make(map[uint64]*mcap.ChunkIndex),
		minLogTime:                         math.MaxUint64,
		channelMessageCounts:               make(map[uint16]uint64),
	}
}

func main(_ *cobra.Command, args []string) {
	ctx := context.Background()
	if len(args) != 1 {
		fmt.Println("An MCAP file argument is required.")
		os.Exit(1)
	}
	filename := args[0]
	err := utils.WithReader(ctx, filename, func(remote bool, rs io.ReadSeeker) error {
		doctor := newMcapDoctor(rs)
		if remote {
			doctor.warn("Will read full remote file")
		}
		if verbose {
			fmt.Printf("Examining %s\n", args[0])
		}
		diagnosis := doctor.Examine()
		if len(diagnosis.Errors) > 0 {
			return fmt.Errorf("encountered %d errors", len(diagnosis.Errors))
		}
		return nil
	})
	if err != nil {
		die("Doctor command failed: %s", err)
	}
}

var doctorCommand = &cobra.Command{
	Use:   "doctor <file>",
	Short: "Check an MCAP file structure",
	Run:   main,
}

func init() {
	rootCmd.AddCommand(doctorCommand)

	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
	rootCmd.PersistentFlags().BoolVarP(&strictMessageOrder, "strict-message-order", "",
		false, "Require that messages have a monotonic log time")
}
