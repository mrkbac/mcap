package cmd

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNoErrorOnMessagelessChunks(t *testing.T) {
	buf := bytes.Buffer{}
	writer, err := mcap.NewWriter(&buf, &mcap.WriterOptions{
		Chunked:   true,
		ChunkSize: 10,
	})
	require.NoError(t, err)
	require.NoError(t, writer.WriteHeader(&mcap.Header{
		Profile: "",
		Library: "",
	}))
	require.NoError(t, writer.WriteChannel(&mcap.Channel{
		ID:       1,
		SchemaID: 0,
		Topic:    "schemaless_topic",
	}))
	require.NoError(t, writer.Close())

	rs := bytes.NewReader(buf.Bytes())

	doctor := newMcapDoctor(rs)
	diagnosis := doctor.Examine()
	assert.Empty(t, diagnosis.Errors)
}

func TestRequiresDuplicatedSchemasForIndexedMessages(t *testing.T) {
	rs, err := os.Open("../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx-pad.mcap")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rs.Close())
	}()
	doctor := newMcapDoctor(rs)
	diagnosis := doctor.Examine()
	assert.Len(t, diagnosis.Errors, 2)
	assert.Equal(t,
		"Indexed chunk at offset 28 contains messages referencing channel (1) not duplicated in summary section",
		diagnosis.Errors[0],
	)
	assert.Equal(t,
		"Indexed chunk at offset 28 contains messages referencing schema (1) not duplicated in summary section",
		diagnosis.Errors[1],
	)
}

func TestPassesIndexedMessagesWithRepeatedSchemas(t *testing.T) {
	rs, err := os.Open("../../../../tests/conformance/data/OneMessage/OneMessage-ch-chx-pad-rch-rsh.mcap")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rs.Close())
	}()
	doctor := newMcapDoctor(rs)
	diagnosis := doctor.Examine()
	assert.Empty(t, diagnosis.Errors)
}

func TestNoErrorOnRepeatedSchemaAndChannel(t *testing.T) {
	inputs := []string{}
	err := filepath.Walk("../../../../tests/conformance/data/OneMessage/", func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && filepath.Ext(path) == ".mcap" {
			features := strings.Split(strings.TrimSuffix(info.Name(), filepath.Ext(info.Name())), "-")

			// Files repeating schema and channel must not cause errors
			if slices.Contains(features, "rsh") && slices.Contains(features, "rch") {
				inputs = append(inputs, path)
			}
		}
		return nil
	})
	require.NoError(t, err)

	for _, path := range inputs {
		t.Run(path, func(t *testing.T) {
			rs, err := os.Open(path)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, rs.Close())
			}()
			doctor := newMcapDoctor(rs)
			diagnosis := doctor.Examine()
			assert.Empty(t, diagnosis.Errors)
		})
	}
	require.NoError(t, err)
}
