package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/NARUBROWN/spine/internal/event/consumer"
	"github.com/NARUBROWN/spine/pkg/boot"
	segmentio "github.com/segmentio/kafka-go"
)

type fakeKafkaReader struct {
	messages  []segmentio.Message
	fetched   int
	committed []segmentio.Message
	commitErr error
	closed    bool
}

func (r *fakeKafkaReader) FetchMessage(context.Context) (segmentio.Message, error) {
	if r.fetched >= len(r.messages) {
		return segmentio.Message{}, errors.New("no more messages")
	}
	msg := r.messages[r.fetched]
	r.fetched++
	return msg, nil
}

func (r *fakeKafkaReader) CommitMessages(_ context.Context, messages ...segmentio.Message) error {
	r.committed = append(r.committed, messages...)
	return r.commitErr
}

func (r *fakeKafkaReader) Close() error {
	r.closed = true
	return nil
}

func TestReaderNackRequiresReaderInvalidation(t *testing.T) {
	backend := &fakeKafkaReader{messages: []segmentio.Message{{Topic: "orders", Partition: 2, Offset: 10, Value: []byte("failed")}}}
	reader := &Reader{reader: backend}
	msg, err := reader.Read(context.Background())
	if err != nil {
		t.Fatalf("read Kafka message: %v", err)
	}
	if err := msg.Nack(); !errors.Is(err, consumer.ErrReaderInvalidated) {
		t.Fatalf("Kafka NACK must invalidate the reader, got %v", err)
	}
	if len(backend.committed) != 0 {
		t.Fatalf("NACK must not commit an offset: %+v", backend.committed)
	}
}

func TestReaderAckReturnsCommitFailure(t *testing.T) {
	commitErr := errors.New("commit rejected")
	backend := &fakeKafkaReader{
		messages:  []segmentio.Message{{Topic: "orders", Partition: 2, Offset: 10, Value: []byte("payload")}},
		commitErr: commitErr,
	}
	reader := &Reader{reader: backend}
	msg, err := reader.Read(context.Background())
	if err != nil {
		t.Fatalf("read Kafka message: %v", err)
	}
	if err := msg.Ack(); !errors.Is(err, commitErr) {
		t.Fatalf("ACK must surface commit failure, got %v", err)
	}
	if len(backend.committed) != 1 || backend.committed[0].Offset != 10 {
		t.Fatalf("ACK committed unexpected messages: %+v", backend.committed)
	}
}

func TestEffectiveDialerUsesSharedTLSWithoutMutatingOverride(t *testing.T) {
	shared := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "broker.example"}
	override := &segmentio.Dialer{}
	dialer := effectiveDialer(boot.KafkaOptions{TLS: shared, Dialer: override})
	if dialer == override || dialer.TLS == shared || dialer.TLS.ServerName != "broker.example" {
		t.Fatalf("shared TLS should be cloned into a cloned override: %#v", dialer)
	}
	if override.TLS != nil {
		t.Fatal("advanced override must not be mutated")
	}
}

func TestEffectiveDialerSharedTLSPreservesKafkaDefaultsWithoutMutation(t *testing.T) {
	defaultTimeout := segmentio.DefaultDialer.Timeout
	defaultDualStack := segmentio.DefaultDialer.DualStack
	defaultTLS := segmentio.DefaultDialer.TLS
	shared := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "broker.example"}

	dialer := effectiveDialer(boot.KafkaOptions{TLS: shared})

	if dialer == segmentio.DefaultDialer {
		t.Fatal("effective dialer must clone kafka.DefaultDialer")
	}
	if dialer.Timeout != 10*time.Second {
		t.Fatalf("default timeout must remain 10s, got %s", dialer.Timeout)
	}
	if !dialer.DualStack {
		t.Fatal("default DualStack must remain enabled")
	}
	if dialer.TLS == shared || dialer.TLS.ServerName != "broker.example" {
		t.Fatal("shared TLS must be cloned into the effective dialer")
	}
	if segmentio.DefaultDialer.Timeout != defaultTimeout || segmentio.DefaultDialer.DualStack != defaultDualStack || segmentio.DefaultDialer.TLS != defaultTLS {
		t.Fatal("effectiveDialer must not mutate kafka.DefaultDialer")
	}
}

func TestEffectiveDialerUsesImplicitTLS12Default(t *testing.T) {
	dialer := effectiveDialer(boot.KafkaOptions{})
	if dialer == nil || dialer.TLS == nil {
		t.Fatal("secure Kafka dialer must be created implicitly")
	}
	if dialer.TLS.MinVersion != tls.VersionTLS12 {
		t.Fatalf("minimum TLS version = %d, want TLS 1.2", dialer.TLS.MinVersion)
	}
	if dialer.Timeout != 10*time.Second || !dialer.DualStack {
		t.Fatalf("Kafka defaults must remain intact: %+v", dialer)
	}
}

func TestEffectiveDialerPreservesAdvancedTLSOverride(t *testing.T) {
	advancedTLS := &tls.Config{ServerName: "advanced.example"}
	dialer := effectiveDialer(boot.KafkaOptions{
		TLS:    &tls.Config{ServerName: "shared.example"},
		Dialer: &segmentio.Dialer{TLS: advancedTLS},
	})
	if dialer.TLS != advancedTLS {
		t.Fatal("advanced TLS override should take precedence")
	}
}
