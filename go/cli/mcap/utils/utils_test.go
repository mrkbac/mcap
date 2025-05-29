package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
		{
			"remote file http",
			"http://example.com/foo/bar/baz.txt",
			"http",
			"example.com",
			"foo/bar/baz.txt",
		},
		{
			"remote file https",
			"https://example.com/foo/bar/baz.txt",
			"https",
			"example.com",
			"foo/bar/baz.txt",
		},
		{
			"remote file https with query",
			"https://example.com/foo/bar/baz.txt?query=123",
			"https",
			"example.com",
			"foo/bar/baz.txt?query=123",
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
