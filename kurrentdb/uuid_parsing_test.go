package kurrentdb_test

import (
	"github.com/EventStore/EventStore-Client-Go/v1/kurrentdb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func UUIDParsingTests(t *testing.T) {
	t.Run("UUIDParsingTests", func(t *testing.T) {
		expected := uuid.New()
		most, least := kurrentdb.UUIDAsInt64(expected)
		actual, err := kurrentdb.ParseUUIDFromInt64(most, least)

		require.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
}
