package kurrentdb_test

import (
	"github.com/EventStore/EventStore-Client-Go/v1/kurrentdb"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPositionParsing(t *testing.T) {
	t.Run("StreamPositionTests", func(t *testing.T) {
		pos, err := kurrentdb.ParseStreamPosition("C:123/P:456")
		assert.NoError(t, err)
		assert.NotNil(t, pos)

		obj, err := kurrentdb.ParseStreamPosition("C:-1/P:-1")
		assert.NoError(t, err)

		_, ok := obj.(kurrentdb.End)
		assert.True(t, ok)

		obj, err = kurrentdb.ParseStreamPosition("C:0/P:0")
		assert.NoError(t, err)

		_, ok = obj.(kurrentdb.Start)
		assert.True(t, ok)

		obj, err = kurrentdb.ParseStreamPosition("-1")
		assert.NoError(t, err)

		_, ok = obj.(kurrentdb.End)
		assert.True(t, ok)

		obj, err = kurrentdb.ParseStreamPosition("0")
		assert.NoError(t, err)

		_, ok = obj.(kurrentdb.Start)
		assert.True(t, ok)

		obj, err = kurrentdb.ParseStreamPosition("42")
		assert.NoError(t, err)

		value, ok := obj.(kurrentdb.StreamRevision)
		assert.True(t, ok)
		assert.Equal(t, uint64(42), value.Value)
	})
}
