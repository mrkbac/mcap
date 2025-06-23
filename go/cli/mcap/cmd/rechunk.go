package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/foxglove/mcap/go/cli/mcap/utils"
	"github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"github.com/spf13/cobra"
)

type rechunkOptions struct {
	chunkSize        int64
	compression      mcap.CompressionFormat
	topicGroups      []string
	individualChunks bool
}

type topicGroup struct {
	name     string
	patterns []regexp.Regexp
}

type chunkWriter struct {
	buf          *bytes.Buffer
	mcapWriter   *mcap.Writer
	startTime    uint64
	endTime      uint64
	messageIndex map[uint16]*mcap.MessageIndex

	w *utils.ChecksummingWriteCounter

	// Track what's been written to this chunk
	writtenSchemas  map[uint16]bool
	writtenChannels map[uint16]bool

	//debug
	topics []string
	group  string
}

func newChunkWriter(ops *rechunkOptions) (*chunkWriter, error) {
	buf := &bytes.Buffer{}
	mcapWriter, err := mcap.NewWriter(buf, &mcap.WriterOptions{
		SkipMagic: true,
		Chunked:   false,
	})
	if err != nil {
		return nil, err
	}

	return &chunkWriter{
		buf:             buf,
		mcapWriter:      mcapWriter,
		startTime:       0,
		endTime:         0,
		messageIndex:    make(map[uint16]*mcap.MessageIndex),
		writtenSchemas:  make(map[uint16]bool),
		writtenChannels: make(map[uint16]bool),
	}, nil
}

func (w *chunkWriter) writeSchema(s *mcap.Schema) error {
	return w.mcapWriter.WriteSchema(s)
}
func (w *chunkWriter) writeChannel(c *mcap.Channel) error {
	w.topics = append(w.topics, c.Topic)
	return w.mcapWriter.WriteChannel(c)
}

func (w *chunkWriter) writeMessage(m *mcap.Message) error {
	if w.startTime == 0 {
		w.startTime = m.LogTime
	}
	if w.endTime < m.LogTime {
		w.endTime = m.LogTime
	}

	position := len(w.buf.Bytes())
	if _, ok := w.messageIndex[m.ChannelID]; !ok {
		w.messageIndex[m.ChannelID] = &mcap.MessageIndex{
			ChannelID: m.ChannelID,
		}
	}
	w.messageIndex[m.ChannelID].Add(m.LogTime, uint64(position))
	return w.mcapWriter.WriteMessage(m)
}

func (w *chunkWriter) reset() {
	w.buf.Reset()
	w.startTime = 0
	w.endTime = 0
	w.messageIndex = make(map[uint16]*mcap.MessageIndex)
	w.writtenSchemas = make(map[uint16]bool)
	w.writtenChannels = make(map[uint16]bool)
}

func (w *chunkWriter) finalize(mcapWriter *mcap.Writer) error {
	uncompressedBytes := w.buf.Bytes()
	uncompressedSize := len(uncompressedBytes)
	uncompressedCRC := crc32.ChecksumIEEE(uncompressedBytes)

	// Try compression to see if it's beneficial
	compressedWriter := bytes.Buffer{}
	zw, err := zstd.NewWriter(&compressedWriter, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return err
	}

	io.Copy(zw, w.buf)
	err = zw.Close()
	if err != nil {
		return fmt.Errorf("failed to close zstd writer: %w", err)
	}

	compressedBytes := compressedWriter.Bytes()
	compressedSize := len(compressedBytes)

	// Calculate compression ratio and check if it provides at least 5% savings
	compressionRatio := float64(compressedSize) / float64(uncompressedSize)
	useCompression := compressionRatio <= 0.95 // 5% or more savings

	var chunk *mcap.Chunk
	if useCompression {
		chunk = &mcap.Chunk{
			MessageStartTime: w.startTime,
			MessageEndTime:   w.endTime,
			UncompressedSize: uint64(uncompressedSize),
			UncompressedCRC:  uncompressedCRC,
			Compression:      "zstd",
			Records:          compressedBytes,
		}
	} else {
		// Use uncompressed data if compression doesn't provide sufficient benefit
		chunk = &mcap.Chunk{
			MessageStartTime: w.startTime,
			MessageEndTime:   w.endTime,
			UncompressedSize: uint64(uncompressedSize),
			UncompressedCRC:  uncompressedCRC,
			Compression:      "",
			Records:          uncompressedBytes,
		}
	}

	w.reset()

	idxList := make([]*mcap.MessageIndex, 0, len(w.messageIndex))
	for _, idx := range w.messageIndex {
		idxList = append(idxList, idx)
	}

	if err := mcapWriter.WriteChunkWithIndexes(chunk, idxList); err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
	}

	mcapWriter.Statistics.MessageCount += w.mcapWriter.Statistics.MessageCount

	for channelID, count := range w.mcapWriter.Statistics.ChannelMessageCounts {
		mcapWriter.Statistics.ChannelMessageCounts[channelID] += count
	}

	// group,topic,count,final size, uncompressed size, ratio, compression used
	finalSize := uncompressedSize
	if useCompression {
		finalSize = compressedSize
	}
	fmt.Printf("%s,'%s',%d,%d,%d,%f,%t\n",
		w.group,
		w.topics,
		w.mcapWriter.Statistics.MessageCount,
		finalSize,
		uncompressedSize,
		float64(uncompressedSize)/float64(finalSize),
		useCompression,
	)

	return nil
}

// parseTopicGroups parses the topic group specifications
// Each groupSpec contains space-separated regex patterns that belong to the same group
func parseTopicGroups(groupSpecs []string) ([]topicGroup, error) {
	var groups []topicGroup
	for i, spec := range groupSpecs {
		patternStrings := strings.Fields(spec)
		if len(patternStrings) > 0 {
			patterns, err := compileMatchers(patternStrings)
			if err != nil {
				return nil, fmt.Errorf("failed to compile regex patterns for group %d: %w", i+1, err)
			}
			groupName := fmt.Sprintf("group_%d", i+1)
			groups = append(groups, topicGroup{
				name:     groupName,
				patterns: patterns,
			})
		}
	}
	return groups, nil
}

// getTopicGroup returns the group name for a topic, or "rest" if no group matches
func getTopicGroup(topic string, groups []topicGroup) string {
	for _, group := range groups {
		for _, pattern := range group.patterns {
			if pattern.MatchString(topic) {
				return group.name
			}
		}
	}
	return "rest"
}

func rechunkRun(
	r io.Reader,
	w io.Writer,
	ops *rechunkOptions,
) error {
	fmt.Println("group,topic,count,final_size,uncompressed,ratio,compressed")
	mcapWriter, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked:     true,
		ChunkSize:   ops.chunkSize,
		Compression: ops.compression,
	})
	if err != nil {
		return err
	}

	defer func() {
		err := mcapWriter.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to close mcap writer: %v\n", err)
			return
		}
	}()

	lexer, err := mcap.NewLexer(r, &mcap.LexerOptions{
		ValidateChunkCRCs: true,
		AttachmentCallback: func(ar *mcap.AttachmentReader) error {
			err = mcapWriter.WriteAttachment(&mcap.Attachment{
				LogTime:    ar.LogTime,
				CreateTime: ar.CreateTime,
				Name:       ar.Name,
				MediaType:  ar.MediaType,
				DataSize:   ar.DataSize,
				Data:       ar.Data(),
			})
			if err != nil {
				return err
			}
			return nil
		},
	})
	if err != nil {
		return err
	}

	buf := make([]byte, 1024)

	schemas := make(map[uint16]*mcap.Schema)
	channels := make(map[uint16]*mcap.Channel)

	// Parse topic groups from command line options
	topicGroups, err := parseTopicGroups(ops.topicGroups)
	if err != nil {
		return fmt.Errorf("failed to parse topic groups: %w", err)
	}

	// Map from group name to chunk writer
	groupChunks := make(map[string]*chunkWriter)
	// Map from channel ID to group name for quick lookup
	channelGroups := make(map[uint16]string)

	defer func() {
		// cleanup and finalize any remaining chunks
		for groupName, chunkWriter := range groupChunks {
			err := chunkWriter.finalize(mcapWriter)
			if err != nil {
				fmt.Println("ERROR", err)
			}
			delete(groupChunks, groupName)
		}
	}()

	for {
		token, data, err := lexer.Next(buf)
		if err != nil {
			if token == mcap.TokenInvalidChunk {
				fmt.Printf("Invalid chunk encountered, skipping: %s\n", err)
				continue
			}

			if errors.Is(err, io.EOF) {
				return nil
			}
			var expected *mcap.ErrTruncatedRecord
			if errors.As(err, &expected) {
				fmt.Println(expected.Error())
				return nil
			}
			return err
		}
		if len(data) > len(buf) {
			buf = data
		}

		switch token {
		case mcap.TokenHeader:
			header, err := mcap.ParseHeader(data)
			if err != nil {
				return err
			}
			if err := mcapWriter.WriteHeader(header); err != nil {
				return err
			}
		case mcap.TokenMetadata:
			metadata, err := mcap.ParseMetadata(data)
			if err != nil {
				return err
			}
			if err := mcapWriter.WriteMetadata(metadata); err != nil {
				return err
			}
		case mcap.TokenSchema:
			schema, err := mcap.ParseSchema(data)
			if err != nil {
				return err
			}
			if _, ok := schemas[schema.ID]; !ok {
				schemas[schema.ID] = schema
				mcapWriter.AddSchema(schema)
			}
		case mcap.TokenChannel:
			channel, err := mcap.ParseChannel(data)
			if err != nil {
				return err
			}
			if _, ok := channels[channel.ID]; !ok {
				channels[channel.ID] = channel
				mcapWriter.AddChannel(channel)

				// Determine which group this channel belongs to
				var groupName string
				if ops.individualChunks {
					// Each topic gets its own chunk
					groupName = channel.Topic
				} else {
					groupName = getTopicGroup(channel.Topic, topicGroups)
				}
				channelGroups[channel.ID] = groupName
			}
		case mcap.TokenMessage:
			msg, err := mcap.ParseMessage(data)
			if err != nil {
				return err
			}

			// Get the group for this channel
			groupName, ok := channelGroups[msg.ChannelID]
			if !ok {
				return fmt.Errorf("message references unknown channel %d", msg.ChannelID)
			}

			// Create chunk writer for this group if it doesn't exist
			if _, ok := groupChunks[groupName]; !ok {
				chunkWriter, err := newChunkWriter(ops)
				if err != nil {
					return err
				}
				chunkWriter.group = groupName
				groupChunks[groupName] = chunkWriter
			}

			chunkWriter := groupChunks[groupName]

			// Add schema and channel to chunk if not already present
			var channel *mcap.Channel
			var schema *mcap.Schema
			if channel, ok = channels[msg.ChannelID]; !ok {
				return fmt.Errorf("message references unknown channel %d", msg.ChannelID)
			}
			if schema, ok = schemas[channel.SchemaID]; !ok {
				return fmt.Errorf(
					"channel %d references unknown schema %d",
					msg.ChannelID,
					channel.SchemaID,
				)
			}

			// Check if we need to add schema and channel to this chunk
			// (we add them for each chunk since chunks are independent)
			if !chunkWriter.writtenSchemas[schema.ID] {
				if err := chunkWriter.writeSchema(schema); err != nil {
					return err
				}
				chunkWriter.writtenSchemas[schema.ID] = true
			}
			if !chunkWriter.writtenChannels[channel.ID] {
				if err := chunkWriter.writeChannel(channel); err != nil {
					return err
				}
				chunkWriter.writtenChannels[channel.ID] = true
			}

			chunkWriter.writeMessage(msg)

			if len(chunkWriter.buf.Bytes()) >= int(ops.chunkSize) {
				err := chunkWriter.finalize(mcapWriter)
				if err != nil {
					return fmt.Errorf("failed to finalize chunk for group %s: %w", groupName, err)
				}
				delete(groupChunks, groupName)
			}
		case mcap.TokenDataEnd, mcap.TokenFooter:
			// data section is over, either because the file is over or the summary section starts.
			return nil
		case mcap.TokenChunk:
		case mcap.TokenError:
			return errors.New("received error token but lexer did not return error on Next")
		}
	}
}

func init() {
	var rechunkCmd = &cobra.Command{
		Use:   "rechunk [file]",
		Short: "Rechunk an MCAP file",
		Long:  "This subcommand reads an MCAP file and writes a new file with a different chunking strategy.",
	}
	output := rechunkCmd.PersistentFlags().StringP("output", "o", "", "output filename")
	chunkSize := rechunkCmd.PersistentFlags().Int64P("chunk-size", "", 4*1024*1024, "chunk size of output file")
	compression := rechunkCmd.PersistentFlags().String(
		"compression",
		"zstd",
		"compression algorithm to use on output file",
	)
	topicGroups := rechunkCmd.PersistentFlags().StringSliceP("topic-group", "g", nil, "group topics by regex patterns (e.g., -g '/sensor.* /camera.*' -g '/gnss.* /gps.*')")
	individualChunks := rechunkCmd.PersistentFlags().BoolP("individual", "i", false, "put each topic in its own chunk")
	var compressionFormat mcap.CompressionFormat
	switch *compression {
	case CompressionFormatZstd:
		compressionFormat = mcap.CompressionZSTD
	case CompressionFormatLz4:
		compressionFormat = mcap.CompressionLZ4
	case CompressionFormatNone:
	case "":
		compressionFormat = mcap.CompressionNone
	default:
		die(
			"unrecognized compression format '%s': valid options are 'lz4', 'zstd', or 'none'",
			*compression,
		)
	}
	rechunkCmd.Run = func(_ *cobra.Command, args []string) {
		// if len(*groupedTopics) == 0 {
		// 	die("no topics specified to group by")
		// }

		var reader io.Reader
		if len(args) == 0 {
			stat, err := os.Stdin.Stat()
			if err != nil {
				die("failed to check stdin state: %s", err)
			}
			if stat.Mode()&os.ModeCharDevice == 0 {
				reader = os.Stdin
			} else {
				die("please supply a file. see --help for usage details.")
			}
		} else {
			closeFile, newReader, err := utils.GetReader(context.Background(), args[0])
			if err != nil {
				die("failed to open source for reading: %s", err)
			}
			defer func() {
				if closeErr := closeFile(); closeErr != nil {
					die("error closing read source: %s", closeErr)
				}
			}()
			reader = newReader
		}

		var writer io.Writer
		if *output == "" {
			if !utils.StdoutRedirected() {
				die(PleaseRedirect)
			}
			writer = os.Stdout
		} else {
			newWriter, err := os.Create(*output)
			if err != nil {
				die("failed to open %s for writing: %s", *output, err)
			}
			defer func() {
				if err := newWriter.Close(); err != nil {
					die("error closing write target: %s", err)
				}
			}()
			writer = newWriter
		}
		err := rechunkRun(reader, writer, &rechunkOptions{
			chunkSize:        *chunkSize,
			compression:      compressionFormat,
			topicGroups:      *topicGroups,
			individualChunks: *individualChunks,
		})
		if err != nil {
			die("failed to recover: %s", err)
		}
	}
	rootCmd.AddCommand(rechunkCmd)
}
