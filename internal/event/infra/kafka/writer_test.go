package kafka

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/NARUBROWN/spine/pkg/boot"
	eventpublish "github.com/NARUBROWN/spine/pkg/event/publish"
	"github.com/segmentio/kafka-go"
)

type fakeKafkaWriter struct {
	messages []kafka.Message
}

func TestEffectiveTransportUsesSharedTLSWithoutMutatingOverride(t *testing.T) {
	shared := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "broker.example"}
	override := &kafka.Transport{ClientID: "advanced", MetadataTopics: []string{"orders"}}
	transport := effectiveTransport(boot.KafkaOptions{TLS: shared, Transport: override})
	if transport == override || transport.TLS == shared || transport.TLS.ServerName != "broker.example" {
		t.Fatalf("shared TLS should be cloned into a cloned override: %#v", transport)
	}
	if transport.ClientID != "advanced" || len(transport.MetadataTopics) != 1 || transport.MetadataTopics[0] != "orders" {
		t.Fatalf("advanced transport settings must be preserved: %#v", transport)
	}
	transport.MetadataTopics[0] = "changed"
	if override.MetadataTopics[0] != "orders" {
		t.Fatal("advanced override slices must not be aliased")
	}
	if override.TLS != nil {
		t.Fatal("advanced override must not be mutated")
	}
}

func (w *fakeKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	w.messages = append(w.messages, msgs...)
	return nil
}

func (w *fakeKafkaWriter) Close() error { return nil }

type fakeDomainEvent struct {
	name string
	at   time.Time
}

func (e fakeDomainEvent) Name() string          { return e.name }
func (e fakeDomainEvent) OccurredAt() time.Time { return e.at }

var _ eventpublish.DomainEvent = fakeDomainEvent{}

func TestKafkaPublisher_PublishUsesTopicPrefix(t *testing.T) {
	writer := &fakeKafkaWriter{}
	publisher := &KafkaPublisher{
		writer:      writer,
		topicPrefix: "dev-",
	}

	if err := publisher.Publish(context.Background(), fakeDomainEvent{
		name: "orders.created",
		at:   time.Unix(1700000000, 0),
	}); err != nil {
		t.Fatalf("Publish 실패: %v", err)
	}

	if len(writer.messages) != 1 {
		t.Fatalf("메시지는 하나만 발행되어야 합니다. 실제=%d", len(writer.messages))
	}
	if writer.messages[0].Topic != "dev-orders.created" {
		t.Fatalf("TopicPrefix가 반영되지 않았습니다: %s", writer.messages[0].Topic)
	}
}

func TestNewKafkaPublisher_RequiresWriteOptions(t *testing.T) {
	_, err := NewKafkaPublisher(&boot.KafkaOptions{
		Brokers: []string{"localhost:9092"},
	})
	if err == nil {
		t.Fatal("Write 옵션 누락 시 에러가 발생해야 합니다")
	}
}

func TestNewKafkaPublisher_UsesImplicitSecureTransportByDefault(t *testing.T) {
	publisher, err := NewKafkaPublisher(&boot.KafkaOptions{
		Brokers: []string{"localhost:9092"},
		Write:   &boot.KafkaWriteOptions{},
	})
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := publisher.Writer.Transport.(*kafka.Transport)
	if !ok || transport.TLS == nil || transport.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatalf("publisher must use Spine's implicit TLS 1.2+ transport: %#v", publisher.Writer.Transport)
	}
}

func TestKafkaPublisher_InsecureDefaultTransportDoesNotPanicOnWriteMessages(t *testing.T) {
	publisher, err := NewKafkaPublisher(&boot.KafkaOptions{
		Brokers:                []string{"127.0.0.1:1"},
		AllowInsecureTransport: true,
		Write:                  &boot.KafkaWriteOptions{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if publisher.Writer.Transport != nil {
		t.Fatalf("insecure default must leave kafka-go transport unset, got %#v", publisher.Writer.Transport)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = publisher.Publish(ctx, fakeDomainEvent{name: "orders.created", at: time.Unix(1700000000, 0)})
	if err == nil {
		t.Fatal("WriteMessages to an unavailable broker must return an error without panicking")
	}
}
