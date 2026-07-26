package rabbitmq

import (
	"context"
	"strings"
	"testing"

	"github.com/rabbitmq/amqp091-go"
)

type fakeAcknowledger struct {
	ackCalled    bool
	ackTag       uint64
	ackMultiple  bool
	nackCalled   bool
	nackTag      uint64
	nackMultiple bool
	nackRequeue  bool
}

type fakeQosConfigurer struct {
	prefetchCount int
	prefetchSize  int
	global        bool
	err           error
}

func (q *fakeQosConfigurer) Qos(prefetchCount, prefetchSize int, global bool) error {
	q.prefetchCount = prefetchCount
	q.prefetchSize = prefetchSize
	q.global = global
	return q.err
}

func (a *fakeAcknowledger) Ack(tag uint64, multiple bool) error {
	a.ackCalled = true
	a.ackTag = tag
	a.ackMultiple = multiple
	return nil
}

func (a *fakeAcknowledger) Nack(tag uint64, multiple bool, requeue bool) error {
	a.nackCalled = true
	a.nackTag = tag
	a.nackMultiple = multiple
	a.nackRequeue = requeue
	return nil
}

func (a *fakeAcknowledger) Reject(tag uint64, requeue bool) error { return nil }

func TestNewRabbitMqReader_Validation(t *testing.T) {
	if _, err := NewRabbitMqReader(RabbitMqOptions{}); err == nil {
		t.Fatal("Read 옵션이 없으면 에러여야 합니다")
	}
	if _, err := NewRabbitMqReader(RabbitMqOptions{
		Read: &RabbitMqReadOptions{},
	}); err == nil {
		t.Fatal("Exchange가 비어 있으면 에러여야 합니다")
	}
}

func TestValidateBrokerURL_RequiresAMQPSByDefault(t *testing.T) {
	if err := validateBrokerURL("amqp://guest:guest@localhost:5672/", false); err == nil {
		t.Fatal("plaintext AMQP must require an explicit insecure-development opt-in")
	}
	if err := validateBrokerURL("amqp://guest:guest@localhost:5672/", true); err != nil {
		t.Fatalf("explicit insecure-development opt-in should be accepted: %v", err)
	}
	if err := validateBrokerURL("amqps://broker.example:5671/", false); err != nil {
		t.Fatalf("AMQPS should be accepted: %v", err)
	}
}

func TestReader_ReadContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	reader := &Reader{msgs: make(chan amqp091.Delivery)}
	_, err := reader.Read(ctx)
	if err != context.Canceled {
		t.Fatalf("context cancel 에러가 반환되어야 합니다: %v", err)
	}
}

func TestReader_ReadClosedChannel(t *testing.T) {
	msgs := make(chan amqp091.Delivery)
	close(msgs)

	reader := &Reader{msgs: msgs}
	_, err := reader.Read(context.Background())
	if err == nil || !strings.Contains(err.Error(), "channel is closed") {
		t.Fatalf("closed channel error should be returned: %v", err)
	}
}

func TestReader_ReadBuildsMessageAndAckNack(t *testing.T) {
	msgs := make(chan amqp091.Delivery, 1)
	ack := &fakeAcknowledger{}
	msgs <- amqp091.Delivery{
		Acknowledger: ack,
		DeliveryTag:  7,
		Type:         "untrusted.type",
		Body:         []byte(`{"id":1}`),
		RoutingKey:   "orders.created",
	}

	reader := &Reader{msgs: msgs}
	msg, err := reader.Read(context.Background())
	if err != nil {
		t.Fatalf("Read 실패: %v", err)
	}

	if msg.EventName != "orders.created" {
		t.Fatalf("event name이 잘못되었습니다: %s", msg.EventName)
	}
	if string(msg.Payload) != `{"id":1}` {
		t.Fatalf("payload가 잘못되었습니다: %s", string(msg.Payload))
	}
	if msg.Metadata["routing_key"] != "orders.created" {
		t.Fatalf("metadata가 잘못되었습니다: %+v", msg.Metadata)
	}
	if msg.Metadata["amqp_type"] != "untrusted.type" || msg.Metadata["dispatch_key"] != "orders.created" {
		t.Fatalf("dispatch diagnostics metadata is incomplete: %+v", msg.Metadata)
	}

	if err := msg.Ack(); err != nil {
		t.Fatalf("Ack 실패: %v", err)
	}
	if !ack.ackCalled || ack.ackTag != 7 || ack.ackMultiple {
		t.Fatalf("Ack 매핑이 잘못되었습니다: %+v", ack)
	}

	if err := msg.Nack(); err != nil {
		t.Fatalf("Nack 실패: %v", err)
	}
	if !ack.nackCalled || ack.nackTag != 7 || ack.nackMultiple || ack.nackRequeue {
		t.Fatalf("Nack 매핑이 잘못되었습니다: %+v", ack)
	}
}

func TestEffectiveFailurePolicyPreservesLegacyRequeue(t *testing.T) {
	if got := effectiveFailurePolicy(&RabbitMqReadOptions{RequeueOnError: true}); got != RabbitMqFailureRequeue {
		t.Fatalf("legacy RequeueOnError should map to requeue, got %q", got)
	}
	if got := effectiveFailurePolicy(&RabbitMqReadOptions{}); got != RabbitMqFailureReject {
		t.Fatalf("default should reject poison messages, got %q", got)
	}
}

func TestValidateReadOptionsRejectsPolicyConflictAndEmptyDLX(t *testing.T) {
	base := RabbitMqReadOptions{Queue: "orders", RoutingKey: "orders.created"}
	conflict := base
	conflict.FailurePolicy = RabbitMqFailureReject
	conflict.RequeueOnError = true
	if err := validateReadOptions(&conflict); err == nil {
		t.Fatal("conflicting legacy and explicit policies must fail")
	}
	emptyDLX := base
	emptyDLX.DeadLetter = &RabbitMqDeadLetterOptions{}
	if err := validateReadOptions(&emptyDLX); err == nil {
		t.Fatal("empty dead-letter exchange must fail")
	}
}

func TestValidateReadOptionsRequiresDerivedQueueAndRoutingKey(t *testing.T) {
	if err := validateReadOptions(&RabbitMqReadOptions{RoutingKey: "orders.created"}); err == nil || !strings.Contains(err.Error(), "queue cannot be empty") {
		t.Fatalf("empty queue should fail before dialing: %v", err)
	}
	if err := validateReadOptions(&RabbitMqReadOptions{Queue: "orders"}); err == nil || !strings.Contains(err.Error(), "routing key cannot be empty") {
		t.Fatalf("empty routing key should fail before dialing: %v", err)
	}
}

func TestRabbitMqReaderPrefetchDefaultsToOneAndAppliesPerConsumerQos(t *testing.T) {
	if got := effectivePrefetchCount(&RabbitMqReadOptions{}); got != 1 {
		t.Fatalf("zero prefetch must default to 1, got %d", got)
	}
	if got := effectivePrefetchCount(&RabbitMqReadOptions{PrefetchCount: 16}); got != 16 {
		t.Fatalf("explicit prefetch must be preserved, got %d", got)
	}

	qos := &fakeQosConfigurer{}
	if err := applyConsumerQos(qos, 16); err != nil {
		t.Fatalf("QoS setup failed: %v", err)
	}
	if qos.prefetchCount != 16 || qos.prefetchSize != 0 || qos.global {
		t.Fatalf("unexpected QoS arguments: %+v", qos)
	}
}

func TestValidateReadOptionsRejectsNegativePrefetch(t *testing.T) {
	err := validateReadOptions(&RabbitMqReadOptions{
		Queue:         "orders",
		RoutingKey:    "orders.created",
		PrefetchCount: -1,
	})
	if err == nil || !strings.Contains(err.Error(), "prefetch count cannot be negative") {
		t.Fatalf("negative prefetch must fail before dialing: %v", err)
	}
}

func TestReaderAndWriter_CloseNilSafe(t *testing.T) {
	if err := (&Reader{}).Close(); err != nil {
		t.Fatalf("Reader.Close는 nil-safe 해야 합니다: %v", err)
	}
	if err := (&Writer{}).Close(); err != nil {
		t.Fatalf("Writer.Close는 nil-safe 해야 합니다: %v", err)
	}
}
