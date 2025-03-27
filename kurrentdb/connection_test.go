package kurrentdb_test

import (
	"context"
	"github.com/EventStore/EventStore-Client-Go/v1/kurrentdb"
	"github.com/google/uuid"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func ConnectionTests(t *testing.T, emptyDB *Container) {
	t.Run("ConnectionTests", func(t *testing.T) {
		t.Run("closeConnection", closeConnection(emptyDB))
	})
}

func closeConnection(container *Container) TestCall {
	return func(t *testing.T) {
		db := GetClient(t, container)

		testEvent := createTestEvent()
		testEvent.EventID = uuid.MustParse("38fffbc2-339e-11ea-8c7b-784f43837872")

		streamID := uuid.New()
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()
		opts := kurrentdb.AppendToStreamOptions{
			ExpectedRevision: kurrentdb.NoStream{},
		}
		_, err := db.AppendToStream(context, streamID.String(), opts, testEvent)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		db.Close()
		opts.ExpectedRevision = kurrentdb.Any{}
		_, err = db.AppendToStream(context, streamID.String(), opts, testEvent)

		esdbErr, ok := kurrentdb.FromError(err)
		assert.False(t, ok)
		assert.Equal(t, esdbErr.Code(), kurrentdb.ErrorCodeConnectionClosed)
	}
}
