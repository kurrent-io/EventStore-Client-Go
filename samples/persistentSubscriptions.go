package samples

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/EventStore/EventStore-Client-Go/v1/kurrentdb"
)

func createPersistentSubscription(client *kurrentdb.Client) {
	// #region create-persistent-subscription-to-stream
	err := client.CreatePersistentSubscription(context.Background(), "test-stream", "subscription-group", kurrentdb.PersistentStreamSubscriptionOptions{})

	if err != nil {
		panic(err)
	}
	// #endregion create-persistent-subscription-to-stream
}

func connectToPersistentSubscriptionToStream(client *kurrentdb.Client) {
	// #region subscribe-to-persistent-subscription-to-stream
	sub, err := client.SubscribeToPersistentSubscription(context.Background(), "test-stream", "subscription-group", kurrentdb.SubscribeToPersistentSubscriptionOptions{})

	if err != nil {
		panic(err)
	}

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			sub.Ack(event.EventAppeared.Event)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}
	// #endregion subscribe-to-persistent-subscription-to-stream
}

func connectToPersistentSubscriptionToAll(client *kurrentdb.Client) {
	// #region subscribe-to-persistent-subscription-to-all
	sub, err := client.SubscribeToPersistentSubscriptionToAll(context.Background(), "subscription-group", kurrentdb.SubscribeToPersistentSubscriptionOptions{})

	if err != nil {
		panic(err)
	}

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			sub.Ack(event.EventAppeared.Event)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}
	// #endregion subscribe-to-persistent-subscription-to-all
}

func createPersistentSubscriptionToAll(client *kurrentdb.Client) {
	// #region create-persistent-subscription-to-all
	options := kurrentdb.PersistentAllSubscriptionOptions{
		Filter: &kurrentdb.SubscriptionFilter{
			Type:     kurrentdb.StreamFilterType,
			Prefixes: []string{"test"},
		},
	}

	err := client.CreatePersistentSubscriptionToAll(context.Background(), "subscription-group", options)

	if err != nil {
		panic(err)
	}
	// #endregion create-persistent-subscription-to-all
}

func connectToPersistentSubscriptionWithManualAcks(client *kurrentdb.Client) {
	// #region subscribe-to-persistent-subscription-with-manual-acks
	sub, err := client.SubscribeToPersistentSubscription(context.Background(), "test-stream", "subscription-group", kurrentdb.SubscribeToPersistentSubscriptionOptions{})

	if err != nil {
		panic(err)
	}

	for {
		event := sub.Recv()

		if event.EventAppeared != nil {
			sub.Ack(event.EventAppeared.Event)
		}

		if event.SubscriptionDropped != nil {
			break
		}
	}
	// #endregion subscribe-to-persistent-subscription-with-manual-acks
}

func updatePersistentSubscription(client *kurrentdb.Client) {
	// #region update-persistent-subscription
	options := kurrentdb.PersistentStreamSubscriptionOptions{
		Settings: &kurrentdb.PersistentSubscriptionSettings{
			ResolveLinkTos:       true,
			CheckpointLowerBound: 20,
		},
	}

	err := client.UpdatePersistentSubscription(context.Background(), "test-stream", "subscription-group", options)

	if err != nil {
		panic(err)
	}
	// #endregion update-persistent-subscription
}

func deletePersistentSubscription(client *kurrentdb.Client) {
	// #region delete-persistent-subscription
	err := client.DeletePersistentSubscription(context.Background(), "test-stream", "subscription-group", kurrentdb.DeletePersistentSubscriptionOptions{})

	if err != nil {
		panic(err)
	}
	// #endregion delete-persistent-subscription
}

func deletePersistentSubscriptionToAll(client *kurrentdb.Client) {
	// #region delete-persistent-subscription-all
	err := client.DeletePersistentSubscriptionToAll(context.Background(), "test-stream", kurrentdb.DeletePersistentSubscriptionOptions{})

	if err != nil {
		panic(err)
	}
	// #endregion delete-persistent-subscription-all
}

func getPersistentSubscriptionToStreamInfo(client *kurrentdb.Client) {
	// #region get-persistent-subscription-to-stream-info
	info, err := client.GetPersistentSubscriptionInfo(context.Background(), "test-stream", "subscription-group", kurrentdb.GetPersistentSubscriptionOptions{})

	if err != nil {
		panic(err)
	}

	log.Printf("groupName: %s eventsource: %s status: %s", info.GroupName, info.EventSource, info.Status)
	// #endregion get-persistent-subscription-to-stream-info
}

func getPersistentSubscriptionToAllInfo(client *kurrentdb.Client) {
	// #region get-persistent-subscription-to-all-info
	info, err := client.GetPersistentSubscriptionInfoToAll(context.Background(), "subscription-group", kurrentdb.GetPersistentSubscriptionOptions{})

	if err != nil {
		panic(err)
	}

	log.Printf("groupName: %s eventsource: %s status: %s", info.GroupName, info.EventSource, info.Status)
	// #endregion get-persistent-subscription-to-all-info
}

func replayParkedToStream(client *kurrentdb.Client) {
	// #region replay-parked-of-persistent-subscription-to-stream
	err := client.ReplayParkedMessages(context.Background(), "test-stream", "subscription-group", kurrentdb.ReplayParkedMessagesOptions{
		StopAt: 10,
	})

	if err != nil {
		panic(err)
	}
	// #endregion replay-parked-of-persistent-subscription-to-stream
}

func replayParkedToAll(client *kurrentdb.Client) {
	// #region replay-parked-of-persistent-subscription-to-all
	err := client.ReplayParkedMessagesToAll(context.Background(), "subscription-group", kurrentdb.ReplayParkedMessagesOptions{
		StopAt: 10,
	})

	if err != nil {
		panic(err)
	}
	// #endregion replay-parked-of-persistent-subscription-to-all
}

func listPersistentSubscriptionsToStream(client *kurrentdb.Client) {
	// #region list-persistent-subscriptions-to-stream
	subs, err := client.ListPersistentSubscriptionsForStream(context.Background(), "test-stream", kurrentdb.ListPersistentSubscriptionsOptions{})

	if err != nil {
		panic(err)
	}

	var entries []string

	for i := range subs {
		entries = append(
			entries,
			fmt.Sprintf(
				"groupName: %s eventSource: %s status: %s",
				subs[i].GroupName,
				subs[i].EventSource,
				subs[i].Status,
			),
		)
	}

	log.Printf("subscriptions to stream: [ %s ]", strings.Join(entries, ","))
	// #endregion list-persistent-subscriptions-to-stream
}

func listPersistentSubscriptionsToAll(client *kurrentdb.Client) {
	// #region list-persistent-subscriptions-to-all
	subs, err := client.ListPersistentSubscriptionsToAll(context.Background(), kurrentdb.ListPersistentSubscriptionsOptions{})

	if err != nil {
		panic(err)
	}

	var entries []string

	for i := range subs {
		entries = append(
			entries,
			fmt.Sprintf(
				"groupName: %s eventSource: %s status: %s",
				subs[i].GroupName,
				subs[i].EventSource,
				subs[i].Status,
			),
		)
	}

	log.Printf("subscriptions to stream: [ %s ]", strings.Join(entries, ","))
	// #endregion list-persistent-subscriptions-to-all
}

func restartPersistentSubscriptionSubsystem(client *kurrentdb.Client) {
	// #region restart-persistent-subscription-subsystem
	err := client.RestartPersistentSubscriptionSubsystem(context.Background(), kurrentdb.RestartPersistentSubscriptionSubsystemOptions{})

	if err != nil {
		panic(err)
	}
	// #endregion restart-persistent-subscription-subsystem
}
