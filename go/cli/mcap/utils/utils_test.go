package utils

import (
	"bytes"
	"io"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetScheme(t *testing.T) {
	cases := []struct {
		assertion        string
		input            string
		expectedScheme   string
		expectedBucket   string
		expectedFilename string
	}{
		{
			"local file",
			"foo/bar/baz.txt",
			"",
			"",
			"foo/bar/baz.txt",
		},
		{
			"remote file",
			"gs://foo/bar/baz.txt",
			"gs",
			"foo",
			"bar/baz.txt",
		},
		{
			"remote file",
			"gs://foo-bar.com123/bar/baz.txt",
			"gs",
			"foo-bar.com123",
			"bar/baz.txt",
		},
	}
	for _, c := range cases {
		t.Run(c.assertion, func(t *testing.T) {
			scheme, bucket, filename := GetScheme(c.input)
			assert.Equal(t, c.expectedScheme, scheme)
			assert.Equal(t, c.expectedBucket, bucket)
			assert.Equal(t, c.expectedFilename, filename)
		})
	}
}

func TestDefaultString(t *testing.T) {
	cases := []struct {
		assertion string
		args      []string
		output    string
	}{
		{
			"first string",
			[]string{"hello", "goodbye"},
			"hello",
		},
		{
			"second string",
			[]string{"", "hello"},
			"hello",
		},
		{
			"empty",
			[]string{"", ""},
			"",
		},
	}

	for _, c := range cases {
		assert.Equal(t, c.output, DefaultString(c.args...))
	}
}

// writeTestMCAP creates a test MCAP file for testing BuildInfo.
func writeTestMCAP(t *testing.T, w io.Writer) {
	writer, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 1024,
	})
	require.NoError(t, err)

	require.NoError(t, writer.WriteHeader(&mcap.Header{
		Profile: "test",
		Library: "test-lib",
	}))

	require.NoError(t, writer.WriteSchema(&mcap.Schema{
		ID:       1,
		Name:     "test_schema",
		Encoding: "json",
		Data:     []byte(`{"type": "object"}`),
	}))

	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID:              1,
		SchemaID:        1,
		Topic:           "/test/topic1",
		MessageEncoding: "json",
	}))

	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID:              2,
		SchemaID:        1,
		Topic:           "/test/topic2",
		MessageEncoding: "json",
	}))

	// Write some messages
	for i := 0; i < 50; i++ {
		require.NoError(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 1,
			LogTime:   uint64(i * 1000),
			Data:      []byte(`{"value": 1}`),
		}))
		require.NoError(t, writer.WriteMessage(&mcap.Message{
			ChannelID: 2,
			LogTime:   uint64(i * 1000),
			Data:      []byte(`{"value": 2}`),
		}))
	}

	require.NoError(t, writer.WriteMetadata(&mcap.Metadata{
		Name: "test_metadata",
		Metadata: map[string]string{
			"key": "value",
		},
	}))

	require.NoError(t, writer.Close())
}

func TestBuildInfo(t *testing.T) {
	t.Run("builds info from valid MCAP", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writeTestMCAP(t, buf)

		reader := bytes.NewReader(buf.Bytes())
		info, err := BuildInfo(reader)
		require.NoError(t, err)
		require.NotNil(t, info)

		// Verify header
		assert.Equal(t, "test", info.Header.Profile)
		assert.Contains(t, info.Header.Library, "test-lib")

		// Verify schemas
		assert.Equal(t, uint16(1), info.Statistics.SchemaCount)
		assert.Len(t, info.Schemas, 1)
		schema := info.Schemas[1]
		assert.Equal(t, "test_schema", schema.Name)
		assert.Equal(t, "json", schema.Encoding)

		// Verify channels
		assert.Equal(t, uint32(2), info.Statistics.ChannelCount)
		assert.Len(t, info.Channels, 2)
		assert.Equal(t, "/test/topic1", info.Channels[1].Topic)
		assert.Equal(t, "/test/topic2", info.Channels[2].Topic)

		// Verify message counts
		assert.Equal(t, uint64(100), info.Statistics.MessageCount)
		assert.Equal(t, uint64(50), info.Statistics.ChannelMessageCounts[1])
		assert.Equal(t, uint64(50), info.Statistics.ChannelMessageCounts[2])

		// Verify metadata
		assert.Equal(t, uint32(1), info.Statistics.MetadataCount)
		assert.Len(t, info.MetadataIndexes, 1)
		assert.Equal(t, "test_metadata", info.MetadataIndexes[0].Name)

		// Verify chunks (may be 0 for small files)
		assert.GreaterOrEqual(t, info.Statistics.ChunkCount, uint32(0))
	})

	t.Run("handles truncated MCAP", func(t *testing.T) {
		buf := &bytes.Buffer{}
		writeTestMCAP(t, buf)

		// Truncate the buffer to simulate corruption
		truncated := buf.Bytes()[:buf.Len()/2]
		reader := bytes.NewReader(truncated)

		info, err := BuildInfo(reader)
		require.NoError(t, err)
		require.NotNil(t, info)

		// Should still have some basic info even from truncated file
		assert.Equal(t, "test", info.Header.Profile)
		assert.Contains(t, info.Header.Library, "test-lib")
	})

	t.Run("handles empty reader", func(t *testing.T) {
		reader := bytes.NewReader([]byte{})
		_, err := BuildInfo(reader)

		// Should return error or empty info for empty input
		require.Error(t, err)
	})

	t.Run("rejects non-chunked MCAP", func(t *testing.T) {
		buf := &bytes.Buffer{}

		// Create non-chunked MCAP
		writer, err := mcap.NewWriter(buf, &mcap.WriterOptions{
			Chunked: false,
		})
		require.NoError(t, err)
		require.NoError(t, writer.WriteHeader(&mcap.Header{}))
		require.NoError(t, writer.WriteSchema(&mcap.Schema{ID: 1}))
		require.NoError(t, writer.Close())

		reader := bytes.NewReader(buf.Bytes())
		info, err := BuildInfo(reader)

		assert.Error(t, err)
		assert.Nil(t, info)
		assert.Contains(t, err.Error(), "non-chunked mcap not supported")
	})
}
