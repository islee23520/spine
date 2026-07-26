package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/NARUBROWN/spine/pkg/boot"
)

type liveRabbitEvent struct {
	name string
	at   time.Time
}

func (e liveRabbitEvent) Name() string          { return e.name }
func (e liveRabbitEvent) OccurredAt() time.Time { return e.at }

// TestRabbitMqLiveDeliveryContracts is opt-in because it requires a real broker.
// Example: SPINE_TEST_RABBITMQ_URL=amqp://guest:guest@localhost:5672/ go test
// ./internal/event/infra/rabbitmq -run TestRabbitMqLiveDeliveryContracts -count=1.
func TestRabbitMqLiveDeliveryContracts(t *testing.T) {
	rawURL := os.Getenv("SPINE_TEST_RABBITMQ_URL")
	if rawURL == "" {
		t.Skip("SPINE_TEST_RABBITMQ_URL is not configured")
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	exchange := "spine.live." + suffix
	queue := "spine.live.queue." + suffix
	routingKey := "spine.live.event." + suffix
	reader, err := NewRabbitMqReader(RabbitMqOptions{
		URL:                    rawURL,
		AllowInsecureTransport: true,
		Read: &RabbitMqReadOptions{
			Queue:         queue,
			Exchange:      exchange,
			RoutingKey:    routingKey,
			PrefetchCount: 1,
			FailurePolicy: RabbitMqFailureReject,
		},
	})
	if err != nil {
		t.Fatalf("create live RabbitMQ reader: %v", err)
	}
	defer reader.Close()
	defer func() {
		if reader.channel != nil {
			_, _ = reader.channel.QueueDelete(queue, false, false, false)
			_ = reader.channel.ExchangeDelete(exchange, false, false)
		}
	}()

	writer, err := NewRabbitMqWriter(boot.RabbitMqOptions{
		URL:                    rawURL,
		AllowInsecureTransport: true,
		PublisherRetry: boot.PublisherRetryOptions{
			InitialDelay:   20 * time.Millisecond,
			MaxDelay:       50 * time.Millisecond,
			MaxAttempts:    3,
			ConfirmTimeout: 2 * time.Second,
		},
		Write: &boot.RabbitMqWriteOptions{Exchange: exchange},
	})
	if err != nil {
		t.Fatalf("create live RabbitMQ writer: %v", err)
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := writer.Publish(ctx, liveRabbitEvent{name: routingKey, at: time.Now()}); err != nil {
		t.Fatalf("confirmed routed publish: %v", err)
	}
	message, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read confirmed routed publish: %v", err)
	}
	if err := message.Ack(); err != nil {
		t.Fatalf("ack confirmed routed publish: %v", err)
	}

	unroutable := liveRabbitEvent{name: routingKey + ".missing", at: time.Now()}
	if err := writer.Publish(ctx, unroutable); !errors.Is(err, errUnroutable) {
		t.Fatalf("mandatory unroutable publish must fail with basic.return: %v", err)
	}

	for range 2 {
		if err := writer.Publish(ctx, liveRabbitEvent{name: routingKey, at: time.Now()}); err != nil {
			t.Fatalf("publish QoS probe: %v", err)
		}
	}
	first, err := reader.Read(ctx)
	if err != nil {
		t.Fatalf("read first QoS probe: %v", err)
	}
	type readResult struct {
		message interface{ Ack() error }
		err     error
	}
	secondResult := make(chan readResult, 1)
	go func() {
		message, err := reader.Read(ctx)
		secondResult <- readResult{message: message, err: err}
	}()
	select {
	case result := <-secondResult:
		t.Fatalf("prefetch=1 delivered a second message before the first ACK: %+v", result)
	case <-time.After(200 * time.Millisecond):
	}
	if err := first.Ack(); err != nil {
		t.Fatalf("ack first QoS probe: %v", err)
	}
	select {
	case result := <-secondResult:
		if result.err != nil {
			t.Fatalf("read second QoS probe after ACK: %v", result.err)
		}
		if err := result.message.Ack(); err != nil {
			t.Fatalf("ack second QoS probe: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("second QoS probe was not delivered after ACK: %v", ctx.Err())
	}
}
