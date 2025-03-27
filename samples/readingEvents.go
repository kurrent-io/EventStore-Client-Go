package samples

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/EventStore/EventStore-Client-Go/v1/kurrentdb"
)

func ReadFromStream(db *kurrentdb.Client) {
	// region read-from-stream
	options := kurrentdb.ReadStreamOptions{
		From:      kurrentdb.Start{},
		Direction: kurrentdb.Forwards,
	}
	stream, err := db.ReadStream(context.Background(), "some-stream", options, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-stream
	// region iterate-stream
	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion iterate-stream
}

func ReadFromStreamPosition(db *kurrentdb.Client) {
	// region read-from-stream-position
	ropts := kurrentdb.ReadStreamOptions{
		From: kurrentdb.Revision(10),
	}

	stream, err := db.ReadStream(context.Background(), "some-stream", ropts, 20)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-stream-position
	// region iterate-stream
	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion iterate-stream
}

func ReadStreamOverridingUserCredentials(db *kurrentdb.Client) {
	// region overriding-user-credentials
	options := kurrentdb.ReadStreamOptions{
		From: kurrentdb.Start{},
		Authenticated: &kurrentdb.Credentials{
			Login:    "admin",
			Password: "changeit",
		},
	}
	stream, err := db.ReadStream(context.Background(), "some-stream", options, 100)
	// endregion overriding-user-credentials

	if err != nil {
		panic(err)
	}

	stream.Close()
}

func ReadFromStreamPositionCheck(db *kurrentdb.Client) {
	// region checking-for-stream-presence
	ropts := kurrentdb.ReadStreamOptions{
		From: kurrentdb.Revision(10),
	}

	stream, err := db.ReadStream(context.Background(), "some-stream", ropts, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if err, ok := kurrentdb.FromError(err); !ok {
			if err.Code() == kurrentdb.ErrorCodeResourceNotFound {
				fmt.Print("Stream not found")
			} else if errors.Is(err, io.EOF) {
				break
			} else {
				panic(err)
			}
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion checking-for-stream-presence
}

func ReadStreamBackwards(db *kurrentdb.Client) {
	// region reading-backwards
	ropts := kurrentdb.ReadStreamOptions{
		Direction: kurrentdb.Backwards,
		From:      kurrentdb.End{},
	}

	stream, err := db.ReadStream(context.Background(), "some-stream", ropts, 10)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion reading-backwards
}

func ReadFromAllStream(db *kurrentdb.Client) {
	// region read-from-all-stream
	options := kurrentdb.ReadAllOptions{
		From:      kurrentdb.Start{},
		Direction: kurrentdb.Forwards,
	}
	stream, err := db.ReadAll(context.Background(), options, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-all-stream
	// region read-from-all-stream-iterate
	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion read-from-all-stream-iterate
}

func IgnoreSystemEvents(db *kurrentdb.Client) {
	// region ignore-system-events
	stream, err := db.ReadAll(context.Background(), kurrentdb.ReadAllOptions{}, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)

		if strings.HasPrefix(event.OriginalEvent().EventType, "$") {
			continue
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion ignore-system-events
}

func ReadFromAllBackwards(db *kurrentdb.Client) {
	// region read-from-all-stream-backwards
	ropts := kurrentdb.ReadAllOptions{
		Direction: kurrentdb.Backwards,
		From:      kurrentdb.End{},
	}

	stream, err := db.ReadAll(context.Background(), ropts, 100)

	if err != nil {
		panic(err)
	}

	defer stream.Close()
	// endregion read-from-all-stream-backwards
	// region read-from-all-stream-backwards-iterate
	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		fmt.Printf("Event> %v", event)
	}
	// endregion read-from-all-stream-backwards-iterate
}

func ReadFromStreamResolvingLinkToS(db *kurrentdb.Client) {
	// region read-from-all-stream-resolving-link-Tos
	ropts := kurrentdb.ReadAllOptions{
		ResolveLinkTos: true,
	}

	stream, err := db.ReadAll(context.Background(), ropts, 100)
	// endregion read-from-all-stream-resolving-link-Tos

	if err != nil {
		panic(err)
	}

	defer stream.Close()
}

func ReadAllOverridingUserCredentials(db *kurrentdb.Client) {
	// region read-all-overriding-user-credentials
	ropts := kurrentdb.ReadAllOptions{
		From: kurrentdb.Start{},
		Authenticated: &kurrentdb.Credentials{
			Login:    "admin",
			Password: "changeit",
		},
	}
	stream, err := db.ReadAll(context.Background(), ropts, 100)
	// endregion read-all-overriding-user-credentials

	if err != nil {
		panic(err)
	}

	stream.Close()
}
