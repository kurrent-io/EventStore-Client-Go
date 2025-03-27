package kurrentdb_test

import (
	"context"
	"github.com/EventStore/EventStore-Client-Go/v1/kurrentdb"
	"testing"
	"time"
)

func InsecureAuthenticationTests(t *testing.T, client *kurrentdb.Client) {
	t.Run("AuthenticationTests", func(t *testing.T) {
		t.Run("callInsecureWithoutCredentials", callInsecureWithoutCredentials(client))
		t.Run("callInsecureWithInvalidCredentials", callInsecureWithInvalidCredentials(client))
	})
}

func callInsecureWithoutCredentials(db *kurrentdb.Client) TestCall {
	return func(t *testing.T) {
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		err := db.CreatePersistentSubscription(context, NAME_GENERATOR.Generate(), NAME_GENERATOR.Generate(), kurrentdb.PersistentStreamSubscriptionOptions{})

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}
	}
}

func callInsecureWithInvalidCredentials(db *kurrentdb.Client) TestCall {
	return func(t *testing.T) {
		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		opts := kurrentdb.PersistentStreamSubscriptionOptions{
			Authenticated: &kurrentdb.Credentials{
				Login:    "invalid",
				Password: "invalid",
			},
		}
		err := db.CreatePersistentSubscription(context, NAME_GENERATOR.Generate(), NAME_GENERATOR.Generate(), opts)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}
	}
}
