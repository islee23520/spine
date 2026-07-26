package rabbitmq

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NARUBROWN/spine/pkg/boot"
	pkgpublish "github.com/NARUBROWN/spine/pkg/event/publish"
	"github.com/rabbitmq/amqp091-go"
)

type writerTestEvent struct {
	Value string `json:"value"`
}

func (writerTestEvent) Name() string          { return "orders.created" }
func (writerTestEvent) OccurredAt() time.Time { return time.Unix(123, 0) }

var _ pkgpublish.DomainEvent = writerTestEvent{}

type fakeWriterConfirmation struct {
	done chan struct{}
	ack  bool
}

func completedWriterConfirmation(ack bool) *fakeWriterConfirmation {
	done := make(chan struct{})
	close(done)
	return &fakeWriterConfirmation{done: done, ack: ack}
}

func (c *fakeWriterConfirmation) Done() <-chan struct{} { return c.done }
func (c *fakeWriterConfirmation) Acked() bool           { return c.ack }

type fakeWriterPublishResult struct {
	confirmation publisherConfirmation
	err          error
}

type fakeWriterSession struct {
	mu          sync.Mutex
	results     []fakeWriterPublishResult
	returns     chan amqp091.Return
	published   []amqp091.Publishing
	mandatory   []bool
	exchanges   []string
	routingKeys []string
	closed      int
}

func newFakeWriterSession(results ...fakeWriterPublishResult) *fakeWriterSession {
	return &fakeWriterSession{results: results, returns: make(chan amqp091.Return, 4)}
}

func (s *fakeWriterSession) Publish(_ context.Context, exchange, routingKey string, mandatory bool, message amqp091.Publishing) (publisherConfirmation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.published = append(s.published, message)
	s.mandatory = append(s.mandatory, mandatory)
	s.exchanges = append(s.exchanges, exchange)
	s.routingKeys = append(s.routingKeys, routingKey)
	if len(s.results) == 0 {
		return completedWriterConfirmation(true), nil
	}
	result := s.results[0]
	s.results = s.results[1:]
	return result.confirmation, result.err
}

func (s *fakeWriterSession) Returns() <-chan amqp091.Return { return s.returns }
func (s *fakeWriterSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed++
	return nil
}

type fakeWriterConnection struct {
	mu     sync.Mutex
	closed int
}

func (c *fakeWriterConnection) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed++
	return nil
}

func testWriterOptions() boot.RabbitMqOptions {
	return boot.RabbitMqOptions{
		URL:                    "amqp://guest:guest@localhost:5672/",
		AllowInsecureTransport: true,
		Write:                  &boot.RabbitMqWriteOptions{Exchange: "events"},
		PublisherRetry: boot.PublisherRetryOptions{
			InitialDelay:   time.Nanosecond,
			MaxDelay:       time.Nanosecond,
			Multiplier:     1,
			MaxAttempts:    2,
			ConfirmTimeout: time.Second,
		},
	}
}

func newTestWriter(t *testing.T, opts boot.RabbitMqOptions, sessions ...*fakeWriterSession) (*Writer, []*fakeWriterConnection) {
	t.Helper()
	connections := make([]*fakeWriterConnection, 0, len(sessions))
	next := 0
	w, err := newRabbitMqWriter(opts, func(rawURL, exchange string) (writerConnection, writerSession, error) {
		if rawURL != opts.URL || exchange != opts.Write.Exchange {
			t.Fatalf("unexpected dial parameters: %q %q", rawURL, exchange)
		}
		if next >= len(sessions) {
			return nil, nil, errors.New("unexpected extra dial")
		}
		conn := &fakeWriterConnection{}
		connections = append(connections, conn)
		session := sessions[next]
		next++
		return conn, session, nil
	})
	if err != nil {
		t.Fatalf("writer setup failed: %v", err)
	}
	w.wait = func(context.Context, time.Duration) bool { return true }
	w.random = func() float64 { return 0.5 }
	return w, connections
}

func TestNewRabbitMqWriter_RequiresWriteOptions(t *testing.T) {
	_, err := NewRabbitMqWriter(boot.RabbitMqOptions{
		URL: "amqp://guest:guest@localhost:5672/",
	})
	if err == nil {
		t.Fatal("Write 옵션 누락 시 에러가 발생해야 합니다")
	}
}

func TestNewRabbitMqWriter_RequiresExchangeBeforeDial(t *testing.T) {
	_, err := NewRabbitMqWriter(boot.RabbitMqOptions{
		URL:   "amqps://unreachable.invalid:5671/",
		Write: &boot.RabbitMqWriteOptions{},
	})
	if err == nil || !strings.Contains(err.Error(), "exchange cannot be empty") {
		t.Fatalf("empty exchange should fail before dialing: %v", err)
	}
}

func TestNewRabbitMqWriterRetriesInitialConnectionWithConfiguredBackoff(t *testing.T) {
	opts := testWriterOptions()
	opts.PublisherRetry.InitialDelay = 10 * time.Millisecond
	opts.PublisherRetry.MaxDelay = 20 * time.Millisecond
	opts.PublisherRetry.Multiplier = 2
	opts.PublisherRetry.MaxAttempts = 3
	session := newFakeWriterSession()
	connection := &fakeWriterConnection{}
	dials := 0
	var waits []time.Duration

	w, err := newRabbitMqWriterWithRuntime(
		opts,
		func(string, string) (writerConnection, writerSession, error) {
			dials++
			if dials < 3 {
				return nil, nil, errors.New("broker starting")
			}
			return connection, session, nil
		},
		func(_ context.Context, delay time.Duration) bool {
			waits = append(waits, delay)
			return true
		},
		func() float64 { return 0.5 },
	)
	if err != nil {
		t.Fatalf("initial connection should recover within configured attempts: %v", err)
	}
	defer w.Close()
	if dials != 3 {
		t.Fatalf("expected three total connection attempts, got %d", dials)
	}
	wantWaits := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	if len(waits) != len(wantWaits) || waits[0] != wantWaits[0] || waits[1] != wantWaits[1] {
		t.Fatalf("unexpected initial connection backoff: got %v want %v", waits, wantWaits)
	}
}

func TestNewRabbitMqWriterExhaustsBoundedInitialConnectionAttempts(t *testing.T) {
	opts := testWriterOptions()
	opts.PublisherRetry.MaxAttempts = 2
	sentinel := errors.New("broker unavailable")
	dials := 0
	waits := 0

	w, err := newRabbitMqWriterWithRuntime(
		opts,
		func(string, string) (writerConnection, writerSession, error) {
			dials++
			return nil, nil, sentinel
		},
		func(context.Context, time.Duration) bool {
			waits++
			return true
		},
		func() float64 { return 0.5 },
	)
	if w != nil {
		t.Fatal("writer must not be returned after initial connection exhaustion")
	}
	if !errors.Is(err, sentinel) || !strings.Contains(err.Error(), "after 2 attempt(s)") {
		t.Fatalf("initial connection exhaustion must preserve the final cause: %v", err)
	}
	if dials != 2 || waits != 1 {
		t.Fatalf("constructor retry must be bounded: dials=%d waits=%d", dials, waits)
	}
}

func TestEffectivePublisherRetryPolicyClampsInitialDelayToDefaultMaximum(t *testing.T) {
	policy := effectivePublisherRetryPolicy(boot.PublisherRetryOptions{
		InitialDelay: 10 * time.Second,
	})
	if policy.initialDelay != defaultPublisherRetryMaxDelay {
		t.Fatalf("initial delay = %s, want effective max delay %s", policy.initialDelay, defaultPublisherRetryMaxDelay)
	}
}

func TestWriterPublishUsesPersistentMandatoryMessageAndPositiveConfirm(t *testing.T) {
	session := newFakeWriterSession(fakeWriterPublishResult{confirmation: completedWriterConfirmation(true)})
	w, _ := newTestWriter(t, testWriterOptions(), session)
	defer w.Close()

	if err := w.Publish(context.Background(), writerTestEvent{Value: "ok"}); err != nil {
		t.Fatalf("confirmed routed publish should succeed: %v", err)
	}
	if len(session.published) != 1 {
		t.Fatalf("expected one publish, got %d", len(session.published))
	}
	message := session.published[0]
	if message.DeliveryMode != amqp091.Persistent {
		t.Fatalf("message must be persistent, got delivery mode %d", message.DeliveryMode)
	}
	if !session.mandatory[0] {
		t.Fatal("publish must require routing with mandatory=true")
	}
	if session.exchanges[0] != "events" || session.routingKeys[0] != "orders.created" {
		t.Fatalf("unexpected publish target: %q %q", session.exchanges[0], session.routingKeys[0])
	}
}

func TestWriterPublishReturnsUnroutableErrorWithoutRetry(t *testing.T) {
	session := newFakeWriterSession(fakeWriterPublishResult{confirmation: completedWriterConfirmation(true)})
	session.returns <- amqp091.Return{
		ReplyCode:  312,
		ReplyText:  "NO_ROUTE",
		Exchange:   "events",
		RoutingKey: "orders.created",
	}
	w, _ := newTestWriter(t, testWriterOptions(), session)
	defer w.Close()

	err := w.Publish(context.Background(), writerTestEvent{})
	if !errors.Is(err, errUnroutable) {
		t.Fatalf("unroutable publish must fail explicitly: %v", err)
	}
	if len(session.published) != 1 {
		t.Fatalf("unroutable topology error must not be blindly retried: %d", len(session.published))
	}
}

func TestWriterPublishReconnectsAndRetriesAfterNegativeConfirm(t *testing.T) {
	first := newFakeWriterSession(fakeWriterPublishResult{confirmation: completedWriterConfirmation(false)})
	second := newFakeWriterSession(fakeWriterPublishResult{confirmation: completedWriterConfirmation(true)})
	w, connections := newTestWriter(t, testWriterOptions(), first, second)
	defer w.Close()

	if err := w.Publish(context.Background(), writerTestEvent{}); err != nil {
		t.Fatalf("publish should reconnect after a negative confirm: %v", err)
	}
	if len(first.published) != 1 || len(second.published) != 1 {
		t.Fatalf("expected one attempt per session: first=%d second=%d", len(first.published), len(second.published))
	}
	if first.closed != 1 || connections[0].closed != 1 {
		t.Fatalf("failed channel and connection must be invalidated: session=%d connection=%d", first.closed, connections[0].closed)
	}
}

func TestWriterPublishReconnectsAfterSendFailure(t *testing.T) {
	sentinel := errors.New("socket closed")
	first := newFakeWriterSession(fakeWriterPublishResult{err: sentinel})
	second := newFakeWriterSession(fakeWriterPublishResult{confirmation: completedWriterConfirmation(true)})
	w, _ := newTestWriter(t, testWriterOptions(), first, second)
	defer w.Close()

	if err := w.Publish(context.Background(), writerTestEvent{}); err != nil {
		t.Fatalf("retriable send failure should reconnect: %v", err)
	}
}

func TestWriterPublishBacksOffAcrossReconnectDialFailure(t *testing.T) {
	opts := testWriterOptions()
	opts.PublisherRetry.MaxAttempts = 3
	first := newFakeWriterSession(fakeWriterPublishResult{err: errors.New("channel closed")})
	third := newFakeWriterSession(fakeWriterPublishResult{confirmation: completedWriterConfirmation(true)})
	dials := 0
	w, err := newRabbitMqWriter(opts, func(string, string) (writerConnection, writerSession, error) {
		dials++
		switch dials {
		case 1:
			return &fakeWriterConnection{}, first, nil
		case 2:
			return nil, nil, errors.New("broker unavailable")
		case 3:
			return &fakeWriterConnection{}, third, nil
		default:
			return nil, nil, errors.New("unexpected extra dial")
		}
	})
	if err != nil {
		t.Fatalf("writer setup failed: %v", err)
	}
	defer w.Close()
	w.wait = func(context.Context, time.Duration) bool { return true }
	w.random = func() float64 { return 0.5 }

	if err := w.Publish(context.Background(), writerTestEvent{}); err != nil {
		t.Fatalf("dial failure during reconnect should be retried: %v", err)
	}
	if dials != 3 || len(third.published) != 1 {
		t.Fatalf("expected reconnect after transient dial failure: dials=%d publishes=%d", dials, len(third.published))
	}
}

func TestWriterPublishConfirmTimeoutFailsAfterConfiguredAttempts(t *testing.T) {
	opts := testWriterOptions()
	opts.PublisherRetry.MaxAttempts = 1
	opts.PublisherRetry.ConfirmTimeout = time.Millisecond
	pending := &fakeWriterConfirmation{done: make(chan struct{})}
	session := newFakeWriterSession(fakeWriterPublishResult{confirmation: pending})
	w, _ := newTestWriter(t, opts, session)
	defer w.Close()

	err := w.Publish(context.Background(), writerTestEvent{})
	if err == nil || !strings.Contains(err.Error(), "confirm wait failed") {
		t.Fatalf("confirm timeout must fail publish: %v", err)
	}
}

func TestWriterCloseCancelsPendingPublishConfirmation(t *testing.T) {
	opts := testWriterOptions()
	opts.PublisherRetry.MaxAttempts = 1
	opts.PublisherRetry.ConfirmTimeout = time.Hour
	pending := &fakeWriterConfirmation{done: make(chan struct{})}
	session := newFakeWriterSession(fakeWriterPublishResult{confirmation: pending})
	w, _ := newTestWriter(t, opts, session)

	publishDone := make(chan error, 1)
	go func() {
		publishDone <- w.Publish(context.Background(), writerTestEvent{})
	}()
	deadline := time.Now().Add(time.Second)
	for {
		session.mu.Lock()
		published := len(session.published)
		session.mu.Unlock()
		if published == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("publish가 confirm 대기 상태에 진입하지 않았습니다")
		}
		time.Sleep(time.Millisecond)
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- w.Close() }()
	select {
	case err := <-publishDone:
		if err == nil || !strings.Contains(err.Error(), "context canceled") {
			t.Fatalf("종료된 publish는 context 취소 오류를 반환해야 합니다: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Writer.Close가 pending confirm을 취소하지 않았습니다")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Writer.Close 실패: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending publish 취소 후 Writer.Close가 반환되지 않았습니다")
	}
}
