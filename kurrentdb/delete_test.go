package kurrentdb_test

import (
	"context"
	"github.com/EventStore/EventStore-Client-Go/v1/kurrentdb"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func DeleteTests(t *testing.T, db *kurrentdb.Client) {
	t.Run("DeleteTests", func(t *testing.T) {
		t.Run("canDeleteStream", canDeleteStream(db))
		t.Run("canTombstoneStream", canTombstoneStream(db))
		t.Run("detectStreamDeleted", detectStreamDeleted(db))
	})
}
func canDeleteStream(db *kurrentdb.Client) TestCall {
	return func(t *testing.T) {
		opts := kurrentdb.DeleteStreamOptions{
			StreamState: kurrentdb.Revision(0),
		}

		streamID := NAME_GENERATOR.Generate()

		_, err := db.AppendToStream(context.Background(), streamID, kurrentdb.AppendToStreamOptions{}, createTestEvent())
		assert.NoError(t, err)
		deleteResult, err := db.DeleteStream(context.Background(), streamID, opts)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.True(t, deleteResult.Position.Commit > 0)
		assert.True(t, deleteResult.Position.Prepare > 0)
	}
}

func canTombstoneStream(db *kurrentdb.Client) TestCall {
	return func(t *testing.T) {
		streamId := NAME_GENERATOR.Generate()

		_, err := db.AppendToStream(context.Background(), streamId, kurrentdb.AppendToStreamOptions{}, createTestEvent())
		deleteResult, err := db.TombstoneStream(context.Background(), streamId, kurrentdb.TombstoneStreamOptions{
			StreamState: kurrentdb.Revision(0),
		})

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.True(t, deleteResult.Position.Commit > 0)
		assert.True(t, deleteResult.Position.Prepare > 0)

		_, err = db.AppendToStream(context.Background(), streamId, kurrentdb.AppendToStreamOptions{}, createTestEvent())
		require.Error(t, err)
	}
}

func detectStreamDeleted(db *kurrentdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		event := createTestEvent()

		_, err := db.AppendToStream(context.Background(), streamID, kurrentdb.AppendToStreamOptions{}, event)
		require.Nil(t, err)

		_, err = db.TombstoneStream(context.Background(), streamID, kurrentdb.TombstoneStreamOptions{})
		require.Nil(t, err)

		stream, err := db.ReadStream(context.Background(), streamID, kurrentdb.ReadStreamOptions{}, 1)
		require.NoError(t, err)
		defer stream.Close()

		evt, err := stream.Recv()
		require.Nil(t, evt)
		esdbErr, ok := kurrentdb.FromError(err)
		require.False(t, ok)
		require.Equal(t, esdbErr.Code(), kurrentdb.ErrorCodeStreamDeleted)
	}
}
