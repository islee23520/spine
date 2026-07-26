package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/NARUBROWN/spine/internal/event/consumer"
	"github.com/NARUBROWN/spine/pkg/boot"
	segmentio "github.com/segmentio/kafka-go"
)

const kafkaStartupProtocolTimeout = 10 * time.Second

type RunnerFactory struct {
	opts boot.KafkaOptions
}

func NewRunnerFactory(opts boot.KafkaOptions) *RunnerFactory {
	return &RunnerFactory{opts: opts}
}

func (f *RunnerFactory) Build(registration consumer.Registration) (consumer.Reader, error) {
	return NewKafkaReader(
		registration.Topic,
		f.opts,
	)
}

// ValidateStartup은 HTTP listener를 열기 전에 실제 Kafka metadata 요청까지
// 완료합니다. kafka.NewReader는 지연 연결하므로 Build/Close만으로는 broker가
// Kafka 프로토콜에 응답하는지 확인할 수 없습니다.
func (f *RunnerFactory) ValidateStartup(ctx context.Context, registration consumer.Registration) error {
	if err := f.opts.Validate(); err != nil {
		return err
	}
	dialer := effectiveDialer(f.opts)
	if dialer == nil {
		cloned := *segmentio.DefaultDialer
		dialer = &cloned
	}
	var dialErrs []error
	for _, broker := range f.opts.Brokers {
		brokerCtx, cancelBroker := context.WithTimeout(ctx, kafkaStartupProtocolTimeout)
		conn, err := dialer.DialContext(brokerCtx, "tcp", broker)
		if err == nil {
			protocolErr := validateKafkaProtocol(brokerCtx, conn)
			_ = conn.Close()
			if protocolErr == nil {
				cancelBroker()
				return nil
			}
			err = protocolErr
		}
		cancelBroker()
		dialErrs = append(dialErrs, fmt.Errorf("%s: %w", broker, err))
		if ctx.Err() != nil {
			break
		}
	}
	return fmt.Errorf("Kafka startup handshake failed for topic %q: %w", registration.Topic, errors.Join(dialErrs...))
}

func validateKafkaProtocol(ctx context.Context, conn *segmentio.Conn) error {
	deadline := time.Now().Add(kafkaStartupProtocolTimeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return fmt.Errorf("Kafka startup deadline setup failed: %w", err)
	}
	stopCancellation := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
	})
	defer stopCancellation()

	brokers, err := conn.Brokers()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("Kafka metadata request failed: %w", err)
	}
	if len(brokers) == 0 {
		return errors.New("Kafka metadata response did not contain any brokers")
	}
	return nil
}

func (f *RunnerFactory) ConsumerRetryPolicy() consumer.TransportRetryPolicy {
	return consumer.NewTransportRetryPolicy(
		f.opts.ConsumerRetry.InitialDelay,
		f.opts.ConsumerRetry.MaxDelay,
		f.opts.ConsumerRetry.Multiplier,
		f.opts.ConsumerRetry.Jitter,
		f.opts.ConsumerRetry.MaxAttempts,
	)
}
