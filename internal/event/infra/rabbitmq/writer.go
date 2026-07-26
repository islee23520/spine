package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/NARUBROWN/spine/pkg/boot"
	"github.com/NARUBROWN/spine/pkg/event/publish"
	"github.com/rabbitmq/amqp091-go"
)

const (
	defaultPublisherRetryInitialDelay = 100 * time.Millisecond
	defaultPublisherRetryMaxDelay     = 5 * time.Second
	defaultPublisherRetryAttempts     = 3
	defaultPublisherConfirmTimeout    = 5 * time.Second
)

type publisherConfirmation interface {
	Done() <-chan struct{}
	Acked() bool
}

type writerSession interface {
	Publish(context.Context, string, string, bool, amqp091.Publishing) (publisherConfirmation, error)
	Returns() <-chan amqp091.Return
	Close() error
}

type writerConnection interface {
	Close() error
}

type writerDialer func(string, string) (writerConnection, writerSession, error)

type amqpWriterSession struct {
	channel *amqp091.Channel
	returns <-chan amqp091.Return
}

func (s *amqpWriterSession) Publish(ctx context.Context, exchange, routingKey string, mandatory bool, message amqp091.Publishing) (publisherConfirmation, error) {
	return s.channel.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, mandatory, false, message)
}

func (s *amqpWriterSession) Returns() <-chan amqp091.Return { return s.returns }
func (s *amqpWriterSession) Close() error                   { return s.channel.Close() }

type publisherRetryPolicy struct {
	initialDelay   time.Duration
	maxDelay       time.Duration
	multiplier     float64
	jitter         float64
	maxAttempts    int
	confirmTimeout time.Duration
}

type Writer struct {
	mu             sync.Mutex
	conn           writerConnection
	session        writerSession
	url            string
	exchange       string
	dial           writerDialer
	retry          publisherRetryPolicy
	wait           func(context.Context, time.Duration) bool
	random         func() float64
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
	closed         bool
}

func NewRabbitMqWriter(opts boot.RabbitMqOptions) (*Writer, error) {
	return newRabbitMqWriter(opts, dialRabbitMqWriter)
}

func newRabbitMqWriter(opts boot.RabbitMqOptions, dial writerDialer) (*Writer, error) {
	return newRabbitMqWriterWithRuntime(opts, dial, waitPublisherRetry, rand.Float64)
}

func newRabbitMqWriterWithRuntime(
	opts boot.RabbitMqOptions,
	dial writerDialer,
	wait func(context.Context, time.Duration) bool,
	random func() float64,
) (*Writer, error) {
	if opts.Write == nil {
		return nil, errors.New("RabbitMQ write options are not configured")
	}
	if opts.Write.Exchange == "" {
		return nil, errors.New("RabbitMQ write exchange cannot be empty")
	}
	if err := validateBrokerURL(opts.URL, opts.AllowInsecureTransport); err != nil {
		return nil, err
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	w := &Writer{
		url:            opts.URL,
		exchange:       opts.Write.Exchange,
		dial:           dial,
		retry:          effectivePublisherRetryPolicy(opts.PublisherRetry),
		wait:           wait,
		random:         random,
		shutdownCtx:    shutdownCtx,
		shutdownCancel: shutdownCancel,
	}
	if err := w.connectWithRetryLocked(context.Background()); err != nil {
		shutdownCancel()
		return nil, err
	}

	log.Println("[RabbitMQ][Write] Event publisher initialized with mandatory routing and publisher confirms")
	return w, nil
}

func (w *Writer) connectWithRetryLocked(ctx context.Context) error {
	delay := w.retry.initialDelay
	var lastErr error
	for attempt := 1; attempt <= w.retry.maxAttempts; attempt++ {
		if err := w.connectLocked(); err == nil {
			return nil
		} else {
			lastErr = err
		}
		if attempt == w.retry.maxAttempts {
			break
		}
		if !w.wait(ctx, jitterPublisherDelay(delay, w.retry.jitter, w.random())) {
			return fmt.Errorf("RabbitMQ initial connection retry interrupted: %w", ctx.Err())
		}
		delay = nextPublisherRetryDelay(delay, w.retry)
	}
	return fmt.Errorf("RabbitMQ initial connection failed after %d attempt(s): %w", w.retry.maxAttempts, lastErr)
}

func dialRabbitMqWriter(rawURL, exchange string) (writerConnection, writerSession, error) {
	conn, err := amqp091.Dial(rawURL)
	if err != nil {
		return nil, nil, fmt.Errorf("RabbitMQ connection failed: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("RabbitMQ channel creation failed: %w", err)
	}

	cleanup := func() {
		_ = ch.Close()
		_ = conn.Close()
	}
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("RabbitMQ exchange declaration failed: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("RabbitMQ publisher confirm setup failed: %w", err)
	}
	returns := ch.NotifyReturn(make(chan amqp091.Return, 1))
	return conn, &amqpWriterSession{channel: ch, returns: returns}, nil
}

func effectivePublisherRetryPolicy(opts boot.PublisherRetryOptions) publisherRetryPolicy {
	policy := publisherRetryPolicy{
		initialDelay:   opts.InitialDelay,
		maxDelay:       opts.MaxDelay,
		multiplier:     opts.Multiplier,
		jitter:         opts.Jitter,
		maxAttempts:    opts.MaxAttempts,
		confirmTimeout: opts.ConfirmTimeout,
	}
	if policy.initialDelay == 0 {
		policy.initialDelay = defaultPublisherRetryInitialDelay
	}
	if policy.maxDelay == 0 {
		policy.maxDelay = defaultPublisherRetryMaxDelay
	}
	if policy.initialDelay > policy.maxDelay {
		policy.initialDelay = policy.maxDelay
	}
	if policy.multiplier == 0 {
		policy.multiplier = 2
	}
	if policy.jitter == 0 {
		policy.jitter = 0.2
	}
	if policy.maxAttempts == 0 {
		policy.maxAttempts = defaultPublisherRetryAttempts
	}
	if policy.confirmTimeout == 0 {
		policy.confirmTimeout = defaultPublisherConfirmTimeout
	}
	return policy
}

func (w *Writer) Publish(ctx context.Context, event publish.DomainEvent) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("RabbitMQ event serialization failed: %w", err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	publishCtx, cancelPublish := context.WithCancel(ctx)
	stopShutdownCancellation := context.AfterFunc(w.shutdownCtx, cancelPublish)
	defer func() {
		stopShutdownCancellation()
		cancelPublish()
	}()
	ctx = publishCtx
	eventName := event.Name()

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("RabbitMQ writer is closed")
	}

	message := amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		Body:         payload,
		Timestamp:    event.OccurredAt(),
		Type:         eventName,
	}

	delay := w.retry.initialDelay
	var lastErr error
	for attempt := 1; attempt <= w.retry.maxAttempts; attempt++ {
		if w.session == nil {
			if err := w.connectLocked(); err != nil {
				lastErr = err
			} else {
				lastErr = w.publishOnceLocked(ctx, eventName, message)
			}
		} else {
			lastErr = w.publishOnceLocked(ctx, eventName, message)
		}

		if lastErr == nil {
			return nil
		}
		if errors.Is(lastErr, errUnroutable) || ctx.Err() != nil {
			return lastErr
		}

		w.invalidateLocked()
		if attempt == w.retry.maxAttempts {
			break
		}
		if !w.wait(ctx, jitterPublisherDelay(delay, w.retry.jitter, w.random())) {
			return fmt.Errorf("RabbitMQ publish retry interrupted: %w", ctx.Err())
		}
		delay = nextPublisherRetryDelay(delay, w.retry)
	}

	return fmt.Errorf("RabbitMQ publish failed after %d attempt(s): %w", w.retry.maxAttempts, lastErr)
}

var errUnroutable = errors.New("RabbitMQ message was unroutable")

func (w *Writer) publishOnceLocked(ctx context.Context, routingKey string, message amqp091.Publishing) error {
	// mandatory=true makes an unroutable message observable via basic.return.
	confirmation, err := w.session.Publish(ctx, w.exchange, routingKey, true, message)
	if err != nil {
		return fmt.Errorf("RabbitMQ publish send failed: %w", err)
	}
	if confirmation == nil {
		return errors.New("RabbitMQ publisher confirm was not enabled")
	}

	confirmCtx, cancel := context.WithTimeout(ctx, w.retry.confirmTimeout)
	defer cancel()
	returns := w.session.Returns()
	var returned *amqp091.Return
	for {
		select {
		case ret, ok := <-returns:
			if !ok {
				returns = nil
				continue
			}
			returned = &ret
		case <-confirmation.Done():
			// RabbitMQ sends basic.return before the corresponding confirm. The
			// listener is buffered, so drain it once after observing the confirm.
			if returned == nil && returns != nil {
				select {
				case ret, ok := <-returns:
					if ok {
						returned = &ret
					}
				default:
				}
			}
			if returned != nil {
				return fmt.Errorf("%w: reply_code=%d reply_text=%q exchange=%q routing_key=%q", errUnroutable, returned.ReplyCode, returned.ReplyText, returned.Exchange, returned.RoutingKey)
			}
			if !confirmation.Acked() {
				return errors.New("RabbitMQ broker negatively acknowledged publish")
			}
			return nil
		case <-confirmCtx.Done():
			return fmt.Errorf("RabbitMQ publisher confirm wait failed: %w", confirmCtx.Err())
		}
	}
}

func (w *Writer) connectLocked() error {
	conn, session, err := w.dial(w.url, w.exchange)
	if err != nil {
		return err
	}
	w.conn = conn
	w.session = session
	return nil
}

func (w *Writer) invalidateLocked() {
	if w.session != nil {
		_ = w.session.Close()
	}
	if w.conn != nil {
		_ = w.conn.Close()
	}
	w.session = nil
	w.conn = nil
}

func (w *Writer) Close() error {
	if w.shutdownCancel != nil {
		w.shutdownCancel()
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	var closeErrs []error
	if w.session != nil {
		closeErrs = append(closeErrs, w.session.Close())
	}
	if w.conn != nil {
		closeErrs = append(closeErrs, w.conn.Close())
	}
	w.session = nil
	w.conn = nil
	return errors.Join(closeErrs...)
}

func waitPublisherRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func jitterPublisherDelay(delay time.Duration, jitter, random float64) time.Duration {
	if jitter <= 0 {
		return delay
	}
	factor := 1 + ((random*2)-1)*jitter
	return time.Duration(float64(delay) * factor)
}

func nextPublisherRetryDelay(delay time.Duration, policy publisherRetryPolicy) time.Duration {
	next := time.Duration(float64(delay) * policy.multiplier)
	if next > policy.maxDelay {
		return policy.maxDelay
	}
	return next
}
