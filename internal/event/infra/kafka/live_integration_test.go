package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/NARUBROWN/spine/internal/event/consumer"
	"github.com/NARUBROWN/spine/pkg/boot"
	segmentio "github.com/segmentio/kafka-go"
)

// TestKafkaLiveNackRedeliversSameOffset is opt-in because it requires a real
// broker. Example: SPINE_TEST_KAFKA_BROKER=localhost:9092 go test
// ./internal/event/infra/kafka -run TestKafkaLiveNackRedeliversSameOffset.
func TestKafkaLiveNackRedeliversSameOffset(t *testing.T) {
	broker := os.Getenv("SPINE_TEST_KAFKA_BROKER")
	if broker == "" {
		t.Skip("SPINE_TEST_KAFKA_BROKER is not configured")
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	topic := "spine-live-" + suffix
	groupID := "spine-live-group-" + suffix
	admin, err := segmentio.Dial("tcp", broker)
	if err != nil {
		t.Fatalf("connect Kafka admin client: %v", err)
	}
	controller, err := admin.Controller()
	_ = admin.Close()
	if err != nil {
		t.Fatalf("discover Kafka controller: %v", err)
	}
	controllerConn, err := segmentio.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		t.Fatalf("connect Kafka controller: %v", err)
	}
	if err := controllerConn.CreateTopics(segmentio.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		_ = controllerConn.Close()
		t.Fatalf("create Kafka live topic: %v", err)
	}
	defer func() {
		_ = controllerConn.DeleteTopics(topic)
		_ = controllerConn.Close()
	}()

	writer := &segmentio.Writer{
		Addr:                   segmentio.TCP(broker),
		Topic:                  topic,
		RequiredAcks:           segmentio.RequireAll,
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := writer.WriteMessages(ctx,
		segmentio.Message{Value: []byte("offset-0-failed")},
		segmentio.Message{Value: []byte("offset-1-higher")},
	); err != nil {
		t.Fatalf("seed Kafka live topic: %v", err)
	}

	opts := boot.KafkaOptions{
		Brokers:                []string{broker},
		AllowInsecureTransport: true,
		Read:                   &boot.KafkaReadOptions{GroupID: groupID},
	}
	if err := NewRunnerFactory(opts).ValidateStartup(ctx, consumer.Registration{Topic: topic}); err != nil {
		t.Fatalf("eager Kafka startup handshake: %v", err)
	}
	firstReader, err := NewKafkaReader(topic, opts)
	if err != nil {
		t.Fatalf("create first Kafka reader: %v", err)
	}
	first, err := firstReader.Read(ctx)
	if err != nil {
		_ = firstReader.Close()
		t.Fatalf("read failed offset: %v", err)
	}
	if string(first.Payload) != "offset-0-failed" {
		_ = firstReader.Close()
		t.Fatalf("first payload = %q", first.Payload)
	}
	if err := first.Nack(); !errors.Is(err, consumer.ErrReaderInvalidated) {
		_ = firstReader.Close()
		t.Fatalf("Kafka NACK must require reader invalidation: %v", err)
	}
	if err := firstReader.Close(); err != nil {
		t.Fatalf("close invalidated Kafka reader: %v", err)
	}

	secondReader, err := NewKafkaReader(topic, opts)
	if err != nil {
		t.Fatalf("rebuild Kafka reader: %v", err)
	}
	defer secondReader.Close()
	replayed, err := secondReader.Read(ctx)
	if err != nil {
		t.Fatalf("read replayed offset: %v", err)
	}
	if string(replayed.Payload) != "offset-0-failed" {
		t.Fatalf("NACKed offset was skipped after rebuild: payload=%q", replayed.Payload)
	}
	if err := replayed.Ack(); err != nil {
		t.Fatalf("commit replayed offset: %v", err)
	}
	higher, err := secondReader.Read(ctx)
	if err != nil {
		t.Fatalf("read higher offset after replay commit: %v", err)
	}
	if string(higher.Payload) != "offset-1-higher" {
		t.Fatalf("higher offset payload = %q", higher.Payload)
	}
	if err := higher.Ack(); err != nil {
		t.Fatalf("commit higher offset: %v", err)
	}
}
