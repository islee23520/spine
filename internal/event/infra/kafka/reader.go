package kafka

import (
	"context"
	"crypto/tls"
	"errors"

	"github.com/NARUBROWN/spine/internal/event/consumer"
	"github.com/NARUBROWN/spine/pkg/boot"
	"github.com/segmentio/kafka-go"
)

type Reader struct {
	reader *kafka.Reader
	opts   boot.KafkaOptions
}

func NewKafkaReader(topic string, opts boot.KafkaOptions) (*Reader, error) {
	if opts.Read == nil {
		return nil, errors.New("Kafka read options are not configured")
	}
	if topic == "" {
		return nil, errors.New("Kafka topic cannot be empty")
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: opts.Brokers,
		Topic:   topic,
		GroupID: opts.Read.GroupID,
		Dialer:  effectiveDialer(opts),
	})

	return &Reader{
		reader: reader,
		opts:   opts,
	}, nil
}

func effectiveDialer(opts boot.KafkaOptions) *kafka.Dialer {
	tlsConfig := effectiveTLSConfig(opts)
	if opts.Dialer != nil {
		dialer := *opts.Dialer
		if dialer.TLS == nil && tlsConfig != nil {
			dialer.TLS = tlsConfig
		}
		return &dialer
	}
	if tlsConfig == nil {
		return nil
	}
	// kafka-go의 암묵적 기본값에는 10초 제한 시간과 DualStack 설정이 포함됩니다.
	// Spine이 공유 TLS만 주입할 때도 이 기본 동작을 유지하고,
	// 패키지 수준의 DefaultDialer는 변경하지 않습니다.
	dialer := *kafka.DefaultDialer
	dialer.TLS = tlsConfig
	return &dialer
}

func effectiveTLSConfig(opts boot.KafkaOptions) *tls.Config {
	if opts.TLS != nil {
		return opts.TLS.Clone()
	}
	if opts.AllowInsecureTransport {
		return nil
	}
	return &tls.Config{MinVersion: tls.VersionTLS12}
}

func (r *Reader) Read(ctx context.Context) (*consumer.Message, error) {
	m, err := r.reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}

	msg := &consumer.Message{
		EventName: m.Topic,
		Payload:   m.Value,
	}

	// ACK 콜백 설정: 핸들러 성공 시 커밋
	msg.SetAckHandler(func() error {
		return r.reader.CommitMessages(context.Background(), m)
	})

	// NACK 콜백 설정: Kafka는 명시적 NACK이 없으므로 커밋하지 않음
	// (컨슈머 그룹 재시작 시 재처리됨)
	msg.SetNackHandler(func() error {
		// Kafka는 명시적 NACK 대신 커밋하지 않으면 됨
		return nil
	})

	return msg, nil
}

func (r *Reader) Close() error {
	return r.reader.Close()
}
