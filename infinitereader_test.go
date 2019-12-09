package main

import (
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"testing"
)

func TestTardisReaer(t *testing.T) {
	t.Run("10", func(t *testing.T) {
		size := 10
		reader := InfiniteReader{Size: int64(size)}
		out, err := ioutil.ReadAll(&reader)
		require.NoError(t, err)
		require.Equal(t, size, len(out))
	})

	t.Run("100", func(t *testing.T) {
		size := 10
		reader := InfiniteReader{Size: int64(size)}
		out, err := ioutil.ReadAll(&reader)
		require.NoError(t, err)
		require.Equal(t, size, len(out))
	})

	t.Run("1000", func(t *testing.T) {
		size := 1000
		reader := InfiniteReader{Size: int64(size)}
		out, err := ioutil.ReadAll(&reader)
		require.NoError(t, err)
		require.Equal(t, size, len(out))
	})

	t.Run("100000", func(t *testing.T) {
		size := 100000
		reader := InfiniteReader{Size: int64(size)}
		out, err := ioutil.ReadAll(&reader)
		require.NoError(t, err)
		require.Equal(t, size, len(out))
	})

	t.Run("1000000", func(t *testing.T) {
		size := 1000000
		reader := InfiniteReader{Size: int64(size)}
		out, err := ioutil.ReadAll(&reader)
		require.NoError(t, err)
		require.Equal(t, size, len(out))
	})

	t.Run("1000000000", func(t *testing.T) {
		size := 1000000000
		reader := InfiniteReader{Size: int64(size)}
		out, err := ioutil.ReadAll(&reader)
		require.NoError(t, err)
		require.Equal(t, size, len(out))
	})
}
